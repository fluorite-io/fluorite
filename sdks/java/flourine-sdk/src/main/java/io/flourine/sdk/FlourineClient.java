package io.flourine.sdk;

import com.google.protobuf.ByteString;
import io.flourine.sdk.proto.BatchAck;
import io.flourine.sdk.proto.PartitionResult;
import io.flourine.sdk.proto.Record;
import io.flourine.sdk.proto.RecordBatch;
import io.flourine.sdk.schema.FlourineSchema;
import io.flourine.sdk.schema.Schemas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * High-level typed client. Handles schema registration, serialization,
 * topic resolution, and partitioning automatically.
 *
 * <pre>{@code
 * try (FlourineClient client = FlourineClient.connect(new ClientConfig().apiKey("tb_..."))) {
 *     client.send(new OrderEvent("abc", 100));
 *     client.consume(OrderEvent.class, "my-group", event -> {
 *         System.out.println(event.orderId);
 *     });
 * }
 * }</pre>
 */
public class FlourineClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FlourineClient.class);

    private final Writer writer;
    private final AdminClient admin;
    private final ClientConfig config;
    private final ConcurrentHashMap<String, TopicInfo> topicCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, Integer> schemaCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> rrCounters = new ConcurrentHashMap<>();

    private static class TopicInfo {
        final int topicId;
        final int partitionCount;

        TopicInfo(int topicId, int partitionCount) {
            this.topicId = topicId;
            this.partitionCount = partitionCount;
        }
    }

    private FlourineClient(Writer writer, AdminClient admin, ClientConfig config) {
        this.writer = writer;
        this.admin = admin;
        this.config = config;
    }

    public static FlourineClient connect(ClientConfig config) throws FlourineException {
        WriterConfig writerConfig = new WriterConfig()
                .url(config.getWsUrl())
                .apiKey(config.getApiKey())
                .maxInFlight(config.getMaxInFlight())
                .timeout(config.getTimeout());
        Writer writer = Writer.connect(writerConfig);
        AdminClient admin = new AdminClient(config.getAdminUrl(), config.getApiKey(), config.getTimeout());
        return new FlourineClient(writer, admin, config);
    }

    /** Send a single typed object. Topic resolved from @FlourineSchema annotation. */
    public BatchAck send(Object obj) throws FlourineException {
        return send(obj, (byte[]) null, null, null);
    }

    /** Send with key-based partitioning. */
    public BatchAck send(Object obj, byte[] key) throws FlourineException {
        return send(obj, key, null, null);
    }

    /** Send with explicit partition. */
    public BatchAck send(Object obj, int partition) throws FlourineException {
        return send(obj, null, partition, null);
    }

    /** Send with topic override. */
    public BatchAck sendToTopic(Object obj, String topic) throws FlourineException {
        return send(obj, null, null, topic);
    }

    /**
     * Send a single typed object with full control over routing.
     *
     * @param obj       the object to send (must be annotated with @FlourineSchema)
     * @param key       optional partition key (hash-based routing)
     * @param partition optional explicit partition number
     * @param topic     optional topic name override
     */
    public BatchAck send(Object obj, byte[] key, Integer partition, String topic) throws FlourineException {
        Class<?> cls = obj.getClass();
        String topicName = resolveTopic(cls, topic);
        TopicInfo info = resolveTopicInfo(topicName);
        int schemaId = resolveSchemaId(cls, info.topicId);
        int partitionId = pickPartition(topicName, info.partitionCount, key, partition);

        byte[] value = Schemas.toBytes(obj);
        Record.Builder recordBuilder = Record.newBuilder().setValue(ByteString.copyFrom(value));
        if (key != null) {
            recordBuilder.setKey(ByteString.copyFrom(key));
        }

        return writer.send(info.topicId, partitionId, schemaId, List.of(recordBuilder.build()));
    }

    /** Send a batch of typed objects (all same type). */
    public List<BatchAck> sendBatch(List<?> objects) throws FlourineException {
        return sendBatch(objects, null, null, null);
    }

    public List<BatchAck> sendBatch(List<?> objects, byte[] key, Integer partition, String topic)
            throws FlourineException {
        if (objects.isEmpty()) {
            return List.of();
        }

        Class<?> cls = objects.get(0).getClass();
        String topicName = resolveTopic(cls, topic);
        TopicInfo info = resolveTopicInfo(topicName);
        int schemaId = resolveSchemaId(cls, info.topicId);
        int partitionId = pickPartition(topicName, info.partitionCount, key, partition);

        List<Record> records = new ArrayList<>(objects.size());
        for (Object obj : objects) {
            Record.Builder rb = Record.newBuilder().setValue(ByteString.copyFrom(Schemas.toBytes(obj)));
            if (key != null) {
                rb.setKey(ByteString.copyFrom(key));
            }
            records.add(rb.build());
        }

        RecordBatch batch = RecordBatch.newBuilder()
                .setTopicId(info.topicId)
                .setPartitionId(partitionId)
                .setSchemaId(schemaId)
                .addAllRecords(records)
                .build();
        return writer.sendBatch(List.of(batch));
    }

    /**
     * Consume typed objects from a topic. Blocks the calling thread.
     *
     * @param cls      the class to deserialize into
     * @param groupId  consumer group ID
     * @param callback called for each deserialized object
     */
    public <T> void consume(Class<T> cls, String groupId, Consumer<T> callback) throws FlourineException {
        consume(cls, groupId, null, callback);
    }

    public <T> void consume(Class<T> cls, String groupId, String topic, Consumer<T> callback)
            throws FlourineException {
        String topicName = resolveTopic(cls, topic);
        TopicInfo info = resolveTopicInfo(topicName);

        ReaderConfig readerConfig = new ReaderConfig()
                .url(config.getWsUrl())
                .apiKey(config.getApiKey())
                .groupId(groupId)
                .topicId(info.topicId)
                .timeout(config.getTimeout());

        try (GroupReader reader = GroupReader.join(readerConfig)) {
            reader.startHeartbeat();
            while (true) {
                List<PartitionResult> results = reader.poll();
                for (PartitionResult pr : results) {
                    for (io.flourine.sdk.proto.Record record : pr.getRecordsList()) {
                        T obj = Schemas.fromBytes(cls, record.getValue().toByteArray());
                        callback.accept(obj);
                    }
                }
                if (results.isEmpty()) {
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String resolveTopic(Class<?> cls, String override) throws FlourineException {
        if (override != null && !override.isEmpty()) {
            return override;
        }
        FlourineSchema ann = cls.getAnnotation(FlourineSchema.class);
        if (ann == null || ann.topic().isEmpty()) {
            throw new FlourineException(
                    cls.getSimpleName() + " has no topic. Use @FlourineSchema(topic=...) or pass topic parameter"
            );
        }
        return ann.topic();
    }

    private TopicInfo resolveTopicInfo(String name) throws FlourineException {
        TopicInfo cached = topicCache.get(name);
        if (cached != null) {
            return cached;
        }

        List<Map<String, Object>> topics = admin.listTopics();
        for (Map<String, Object> t : topics) {
            String tName = (String) t.get("name");
            int tId = ((Number) t.get("topic_id")).intValue();
            int tPartitions = ((Number) t.get("partition_count")).intValue();
            topicCache.put(tName, new TopicInfo(tId, tPartitions));
        }

        TopicInfo info = topicCache.get(name);
        if (info == null) {
            throw new FlourineException("Topic not found: " + name);
        }
        return info;
    }

    private int resolveSchemaId(Class<?> cls, int topicId) throws FlourineException {
        Integer cached = schemaCache.get(cls);
        if (cached != null) {
            return cached;
        }
        int schemaId = admin.registerSchema(topicId, Schemas.schema(cls));
        schemaCache.put(cls, schemaId);
        return schemaId;
    }

    private int pickPartition(String topicName, int partitionCount, byte[] key, Integer explicit)
            throws FlourineException {
        if (explicit != null) {
            if (explicit < 0 || explicit >= partitionCount) {
                throw new FlourineException(
                        "Partition " + explicit + " out of range [0, " + partitionCount + ")"
                );
            }
            return explicit;
        }
        if (key != null) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] hash = md.digest(key);
                int h = ((hash[0] & 0xFF) << 24) | ((hash[1] & 0xFF) << 16)
                        | ((hash[2] & 0xFF) << 8) | (hash[3] & 0xFF);
                return Math.abs(h) % partitionCount;
            } catch (NoSuchAlgorithmException e) {
                throw new FlourineException("MD5 not available", e);
            }
        }
        AtomicInteger counter = rrCounters.computeIfAbsent(topicName, k -> new AtomicInteger(0));
        return Math.abs(counter.getAndIncrement()) % partitionCount;
    }

    @Override
    public void close() {
        writer.close();
    }
}

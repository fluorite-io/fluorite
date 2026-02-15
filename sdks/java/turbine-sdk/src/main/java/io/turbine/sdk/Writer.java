package io.turbine.sdk;

import com.google.protobuf.ByteString;
import io.turbine.sdk.proto.AppendRequest;
import io.turbine.sdk.proto.AppendResponse;
import io.turbine.sdk.proto.AuthRequest;
import io.turbine.sdk.proto.BatchAck;
import io.turbine.sdk.proto.ClientMessage;
import io.turbine.sdk.proto.Record;
import io.turbine.sdk.proto.RecordBatch;
import io.turbine.sdk.proto.ServerMessage;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer client for sending messages to Turbine.
 * <p>
 * Wire payloads use generated protobuf message classes directly.
 */
public class Writer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Writer.class);
    private static final int RESPONSE_QUEUE_CAPACITY = 8192;
    private static final int ERR_BACKPRESSURE = 1004;

    private final UUID writerId;
    private final AtomicLong seq = new AtomicLong(1);
    private final WriterConfig config;
    private final TurbineWebSocketClient client;
    private final BlockingQueue<byte[]> responseQueue;
    private final Map<Long, CompletableFuture<AppendResponse>> pendingResponses;
    private final ScheduledExecutorService scheduler;
    private final Semaphore inFlight;
    private final Thread dispatcherThread;
    private volatile boolean running;

    private Writer(WriterConfig config, TurbineWebSocketClient client, BlockingQueue<byte[]> responseQueue) {
        this.writerId = UUID.randomUUID();
        this.config = config;
        this.client = client;
        this.responseQueue = responseQueue;
        this.pendingResponses = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "turbine-writer-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.inFlight = new Semaphore(Math.max(1, config.getMaxInFlight()));
        this.running = true;
        this.dispatcherThread = new Thread(this::dispatchLoop, "turbine-writer-dispatcher");
        this.dispatcherThread.setDaemon(true);
    }

    public static Writer connect(String url) throws TurbineException {
        return connect(new WriterConfig().url(url));
    }

    public static Writer connect(WriterConfig config) throws TurbineException {
        try {
            URI uri = new URI(config.getUrl());
            BlockingQueue<byte[]> responseQueue = new LinkedBlockingQueue<>(RESPONSE_QUEUE_CAPACITY);
            TurbineWebSocketClient client = new TurbineWebSocketClient(uri, responseQueue);

            if (!client.connectBlocking(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TurbineException.ConnectionException("Failed to connect to " + config.getUrl());
            }

            Writer writer = new Writer(config, client, responseQueue);
            if (config.getApiKey() != null) {
                writer.authenticate(config.getApiKey());
            }
            writer.startDispatcher();
            return writer;
        } catch (Exception e) {
            if (e instanceof TurbineException) {
                throw (TurbineException) e;
            }
            throw new TurbineException.ConnectionException("Failed to connect", e);
        }
    }

    private void startDispatcher() {
        dispatcherThread.start();
    }

    private static ByteString uuidToByteString(UUID id) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return ByteString.copyFrom(bb.array());
    }

    private void authenticate(String apiKey) throws TurbineException {
        ClientMessage authMessage = ClientMessage.newBuilder()
                .setAuth(AuthRequest.newBuilder().setApiKey(apiKey).build())
                .build();
        client.send(authMessage.toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new TurbineException.TimeoutException("Authentication timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.AUTH) {
                throw new TurbineException.ProtocolException("Unexpected auth response");
            }

            io.turbine.sdk.proto.AuthResponse auth = envelope.getAuth();
            if (!auth.getSuccess()) {
                throw new TurbineException.AuthenticationException(
                        "Auth failed (" + auth.getErrorCode() + "): " + auth.getErrorMessage()
                );
            }
            log.debug("Authentication successful");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TurbineException.ConnectionException("Interrupted during auth", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new TurbineException.ProtocolException("Failed to decode auth response", e);
        }
    }

    private void dispatchLoop() {
        while (running) {
            try {
                byte[] response = responseQueue.poll(100, TimeUnit.MILLISECONDS);
                if (response == null) {
                    continue;
                }

                ServerMessage envelope = ServerMessage.parseFrom(response);
                if (envelope.getMessageCase() != ServerMessage.MessageCase.APPEND) {
                    continue;
                }

                AppendResponse resp = envelope.getAppend();
                CompletableFuture<AppendResponse> pending = pendingResponses.remove(resp.getAppendSeq());
                if (pending != null) {
                    pending.complete(resp);
                }
            } catch (InterruptedException e) {
                if (!running) {
                    break;
                }
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("Failed to decode writer response", e);
            }
        }
    }

    public UUID getWriterId() {
        return writerId;
    }

    public BatchAck send(int topicId, int partitionId, int schemaId, List<Record> records) throws TurbineException {
        RecordBatch batch = RecordBatch.newBuilder()
                .setTopicId(topicId)
                .setPartitionId(partitionId)
                .setSchemaId(schemaId)
                .addAllRecords(records)
                .build();
        List<BatchAck> acks = sendBatch(List.of(batch));
        if (acks.isEmpty()) {
            throw new TurbineException.ProtocolException("No acks received");
        }
        return acks.get(0);
    }

    public CompletableFuture<BatchAck> sendAsync(
            int topicId,
            int partitionId,
            int schemaId,
            List<Record> records
    ) {
        RecordBatch batch = RecordBatch.newBuilder()
                .setTopicId(topicId)
                .setPartitionId(partitionId)
                .setSchemaId(schemaId)
                .addAllRecords(records)
                .build();
        return sendBatchAsync(List.of(batch)).thenApply(acks -> {
            if (acks.isEmpty()) {
                throw new CompletionException(new TurbineException.ProtocolException("No acks received"));
            }
            return acks.get(0);
        });
    }

    public BatchAck sendOne(int topicId, int partitionId, int schemaId, byte[] key, byte[] value)
            throws TurbineException {
        Record.Builder recordBuilder = Record.newBuilder().setValue(ByteString.copyFrom(value));
        if (key != null) {
            recordBuilder.setKey(ByteString.copyFrom(key));
        }
        return send(topicId, partitionId, schemaId, List.of(recordBuilder.build()));
    }

    public List<BatchAck> sendBatch(List<RecordBatch> batches) throws TurbineException {
        CompletableFuture<List<BatchAck>> future = sendBatchAsync(batches);
        try {
            return future.get(awaitBudgetMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            throw new TurbineException.TimeoutException("Request timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TurbineException.ConnectionException("Interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = unwrapCompletionThrowable(e.getCause());
            if (cause instanceof TurbineException) {
                throw (TurbineException) cause;
            }
            throw new TurbineException.ProtocolException("Send failed", cause);
        }
    }

    public CompletableFuture<List<BatchAck>> sendBatchAsync(List<RecordBatch> batches) {
        long currentSeq = seq.getAndIncrement();
        AppendRequest req = AppendRequest.newBuilder()
                .setWriterId(uuidToByteString(writerId))
                .setAppendSeq(currentSeq)
                .addAllBatches(batches)
                .build();
        CompletableFuture<List<BatchAck>> result = new CompletableFuture<>();
        sendWithRetryAsync(req, 0, config.getInitialBackoff(), result);
        return result;
    }

    private void sendWithRetryAsync(
            AppendRequest req,
            int retries,
            Duration backoff,
            CompletableFuture<List<BatchAck>> result
    ) {
        if (result.isDone()) {
            return;
        }

        CompletableFuture<AppendResponse> responseFuture;
        try {
            responseFuture = sendRequestAsync(req);
        } catch (TurbineException e) {
            result.completeExceptionally(e);
            return;
        }

        responseFuture.whenComplete((resp, err) -> {
            if (err != null) {
                result.completeExceptionally(unwrapCompletionThrowable(err));
                return;
            }

            if (!resp.getSuccess()) {
                if (resp.getErrorCode() == ERR_BACKPRESSURE) {
                    if (retries >= config.getMaxRetries()) {
                        result.completeExceptionally(
                                new TurbineException.BackpressureException(
                                        "Server backpressure after " + retries + " retries"
                                )
                        );
                        return;
                    }

                    Duration nextBackoff = Duration.ofMillis(
                            Math.min(backoff.toMillis() * 2, config.getMaxBackoff().toMillis())
                    );
                    scheduler.schedule(
                            () -> sendWithRetryAsync(req, retries + 1, nextBackoff, result),
                            backoff.toMillis(),
                            TimeUnit.MILLISECONDS
                    );
                    return;
                }

                result.completeExceptionally(
                        new TurbineException.ProtocolException(
                                "Append failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                        )
                );
                return;
            }

            result.complete(resp.getAppendAcksList());
        });
    }

    private CompletableFuture<AppendResponse> sendRequestAsync(AppendRequest req) throws TurbineException {
        if (!running) {
            throw new TurbineException.ConnectionException("Writer is closed");
        }

        try {
            if (!inFlight.tryAcquire(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TurbineException.TimeoutException("Timed out waiting for in-flight capacity");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TurbineException.ConnectionException("Interrupted while waiting for in-flight capacity", e);
        }

        long seqNum = req.getAppendSeq();
        CompletableFuture<AppendResponse> future = new CompletableFuture<>();
        pendingResponses.put(seqNum, future);

        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            CompletableFuture<AppendResponse> pending = pendingResponses.remove(seqNum);
            if (pending != null) {
                pending.completeExceptionally(new TurbineException.TimeoutException("Request timeout"));
            }
        }, config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

        future.whenComplete((ignored, ignoredErr) -> {
            timeoutTask.cancel(false);
            inFlight.release();
        });

        try {
            client.send(ClientMessage.newBuilder().setAppend(req).build().toByteArray());
        } catch (Exception e) {
            pendingResponses.remove(seqNum);
            future.completeExceptionally(new TurbineException.ConnectionException("Failed to send request", e));
        }

        return future;
    }

    private long awaitBudgetMillis() {
        long timeout = config.getTimeout().toMillis();
        long retries = (long) config.getMaxRetries() + 1L;
        long retryBudget = config.getMaxBackoff().toMillis() * retries;
        return timeout + retryBudget + 1000;
    }

    private static Throwable unwrapCompletionThrowable(Throwable t) {
        if (t instanceof CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    private void failPending(TurbineException error) {
        for (Map.Entry<Long, CompletableFuture<AppendResponse>> entry : pendingResponses.entrySet()) {
            entry.getValue().completeExceptionally(error);
        }
        pendingResponses.clear();
    }

    @Override
    public void close() {
        running = false;
        dispatcherThread.interrupt();
        failPending(new TurbineException.ConnectionException("Writer closed"));
        scheduler.shutdownNow();
        client.close();
    }

    /**
     * Internal WebSocket client.
     */
    private static class TurbineWebSocketClient extends WebSocketClient {
        private static final Logger log = LoggerFactory.getLogger(TurbineWebSocketClient.class);
        private final BlockingQueue<byte[]> responseQueue;

        public TurbineWebSocketClient(URI serverUri, BlockingQueue<byte[]> responseQueue) {
            super(serverUri);
            this.responseQueue = responseQueue;
        }

        @Override
        public void onOpen(ServerHandshake handshake) {
            log.debug("WebSocket connected");
        }

        @Override
        public void onMessage(String message) {
            log.warn("Received text message, expected binary");
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            byte[] data = new byte[bytes.remaining()];
            bytes.get(data);
            if (!responseQueue.offer(data)) {
                log.warn("Writer response queue full, dropping response frame");
            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.debug("WebSocket closed: {} - {}", code, reason);
        }

        @Override
        public void onError(Exception ex) {
            log.error("WebSocket error", ex);
        }
    }
}

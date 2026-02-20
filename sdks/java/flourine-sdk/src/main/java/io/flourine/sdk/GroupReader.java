package io.flourine.sdk;

import io.flourine.sdk.proto.AuthRequest;
import io.flourine.sdk.proto.ClientMessage;
import io.flourine.sdk.proto.CommitRequest;
import io.flourine.sdk.proto.HeartbeatRequest;
import io.flourine.sdk.proto.HeartbeatStatus;
import io.flourine.sdk.proto.JoinGroupRequest;
import io.flourine.sdk.proto.LeaveGroupRequest;
import io.flourine.sdk.proto.PartitionAssignment;
import io.flourine.sdk.proto.PartitionCommit;
import io.flourine.sdk.proto.PartitionRead;
import io.flourine.sdk.proto.PartitionResult;
import io.flourine.sdk.proto.ReadRequest;
import io.flourine.sdk.proto.ReadResponse;
import io.flourine.sdk.proto.RejoinRequest;
import io.flourine.sdk.proto.RejoinStatus;
import io.flourine.sdk.proto.ServerMessage;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reader client with reader group support.
 * <p>
 * Wire payloads use generated protobuf message classes directly.
 */
public class GroupReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GroupReader.class);
    private static final int RESPONSE_QUEUE_CAPACITY = 1000;
    private static final int MAX_REJOIN_RETRIES = 10;

    public enum State {
        INIT,
        ACTIVE,
        REBALANCING,
        STOPPED
    }

    private final ReaderConfig config;
    private final FlourineWebSocketClient client;
    private final BlockingQueue<byte[]> responseQueue;

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private State state = State.INIT;
    private long generation = 0;
    private List<PartitionAssignment> assignments = new ArrayList<>();
    private final Map<Integer, Long> offsets = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ScheduledExecutorService heartbeatExecutor;

    private GroupReader(ReaderConfig config, FlourineWebSocketClient client, BlockingQueue<byte[]> responseQueue) {
        this.config = config;
        this.client = client;
        this.responseQueue = responseQueue;
    }

    public static GroupReader join(ReaderConfig config) throws FlourineException {
        try {
            URI uri = new URI(config.getUrl());
            BlockingQueue<byte[]> responseQueue = new LinkedBlockingQueue<>(RESPONSE_QUEUE_CAPACITY);
            FlourineWebSocketClient client = new FlourineWebSocketClient(uri, responseQueue);

            if (!client.connectBlocking(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                throw new FlourineException.ConnectionException("Failed to connect to " + config.getUrl());
            }

            GroupReader reader = new GroupReader(config, client, responseQueue);
            if (config.getApiKey() != null) {
                reader.authenticate(config.getApiKey());
            }
            reader.doJoin();
            return reader;
        } catch (Exception e) {
            if (e instanceof FlourineException) {
                throw (FlourineException) e;
            }
            throw new FlourineException.ConnectionException("Failed to connect", e);
        }
    }

    private void authenticate(String apiKey) throws FlourineException {
        ClientMessage authMessage = ClientMessage.newBuilder()
                .setAuth(AuthRequest.newBuilder().setApiKey(apiKey).build())
                .build();
        client.send(authMessage.toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FlourineException.TimeoutException("Authentication timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.AUTH) {
                throw new FlourineException.ProtocolException("Unexpected auth response");
            }

            io.flourine.sdk.proto.AuthResponse auth = envelope.getAuth();
            if (!auth.getSuccess()) {
                throw new FlourineException.AuthenticationException(
                        "Auth failed (" + auth.getErrorCode() + "): " + auth.getErrorMessage()
                );
            }
            log.debug("Authentication successful");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted during auth", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode auth response", e);
        }
    }

    public void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flourine-heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(
                this::heartbeatTick,
                config.getHeartbeatInterval().toMillis(),
                config.getHeartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private void heartbeatTick() {
        if (!running.get()) {
            return;
        }

        try {
            long gen;
            stateLock.readLock().lock();
            try {
                gen = generation;
            } finally {
                stateLock.readLock().unlock();
            }

            HeartbeatRequest req = HeartbeatRequest.newBuilder()
                    .setGroupId(config.getGroupId())
                    .setTopicId(config.getTopicId())
                    .setReaderId(config.getReaderId())
                    .setGeneration(gen)
                    .build();

            client.send(ClientMessage.newBuilder().setHeartbeat(req).build().toByteArray());

            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                log.warn("Heartbeat timeout");
                return;
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.HEARTBEAT) {
                log.warn("Unexpected response to heartbeat");
                return;
            }

            io.flourine.sdk.proto.HeartbeatResponse resp = envelope.getHeartbeat();
            if (!resp.getSuccess()) {
                log.warn("Heartbeat failed ({}): {}", resp.getErrorCode(), resp.getErrorMessage());
                return;
            }

            if (resp.getStatus() == HeartbeatStatus.HEARTBEAT_STATUS_OK) {
                log.debug("Heartbeat OK at generation {}", resp.getGeneration());
            } else if (resp.getStatus() == HeartbeatStatus.HEARTBEAT_STATUS_REBALANCE_NEEDED) {
                log.info("Rebalance needed, new generation {}", resp.getGeneration());
                doRejoin(resp.getGeneration());
            } else if (resp.getStatus() == HeartbeatStatus.HEARTBEAT_STATUS_UNKNOWN_MEMBER) {
                log.warn("Unknown member, rejoining group");
                doJoin();
            }
        } catch (Exception e) {
            log.error("Heartbeat failed", e);
        }
    }

    public void stop() throws FlourineException {
        running.set(false);

        stateLock.writeLock().lock();
        try {
            state = State.STOPPED;
        } finally {
            stateLock.writeLock().unlock();
        }

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }

        doLeave();
    }

    public State getState() {
        stateLock.readLock().lock();
        try {
            return state;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public long getGeneration() {
        stateLock.readLock().lock();
        try {
            return generation;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public List<PartitionAssignment> getAssignments() {
        stateLock.readLock().lock();
        try {
            return new ArrayList<>(assignments);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public List<PartitionResult> poll() throws FlourineException {
        stateLock.readLock().lock();
        State currentState;
        List<PartitionAssignment> currentAssignments;
        long currentGen;
        try {
            currentState = state;
            currentAssignments = new ArrayList<>(assignments);
            currentGen = generation;
        } finally {
            stateLock.readLock().unlock();
        }

        if (currentState != State.ACTIVE) {
            throw new FlourineException.ProtocolException("Reader not active: " + currentState);
        }

        if (currentAssignments.isEmpty()) {
            return List.of();
        }

        List<PartitionRead> reads = new ArrayList<>();
        for (PartitionAssignment a : currentAssignments) {
            long offset = offsets.getOrDefault(a.getPartitionId(), a.getCommittedOffset());
            reads.add(PartitionRead.newBuilder()
                    .setTopicId(config.getTopicId())
                    .setPartitionId(a.getPartitionId())
                    .setOffset(offset)
                    .setMaxBytes(config.getMaxBytes())
                    .build());
        }

        ReadRequest req = ReadRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .setGeneration(currentGen)
                .addAllReads(reads)
                .build();
        client.send(ClientMessage.newBuilder().setRead(req).build().toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FlourineException.TimeoutException("Read timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.READ) {
                throw new FlourineException.ProtocolException("Unexpected response type or empty response");
            }

            ReadResponse resp = envelope.getRead();
            if (!resp.getSuccess()) {
                throw new FlourineException.ProtocolException(
                        "Read failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            for (PartitionResult pr : resp.getResultsList()) {
                if (pr.getRecordsCount() > 0) {
                    long current = offsets.getOrDefault(pr.getPartitionId(), 0L);
                    offsets.put(pr.getPartitionId(), current + pr.getRecordsCount());
                }
            }

            return new ArrayList<>(resp.getResultsList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode response", e);
        }
    }

    public void commit() throws FlourineException {
        long gen;
        stateLock.readLock().lock();
        try {
            gen = generation;
        } finally {
            stateLock.readLock().unlock();
        }

        if (offsets.isEmpty()) {
            return;
        }

        List<PartitionCommit> commits = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            commits.add(PartitionCommit.newBuilder()
                    .setTopicId(config.getTopicId())
                    .setPartitionId(entry.getKey())
                    .setOffset(entry.getValue())
                    .build());
        }

        CommitRequest req = CommitRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .setGeneration(gen)
                .addAllCommits(commits)
                .build();
        client.send(ClientMessage.newBuilder().setCommit(req).build().toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FlourineException.TimeoutException("Commit timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.COMMIT) {
                throw new FlourineException.ProtocolException("Unexpected response type or empty response");
            }

            io.flourine.sdk.proto.CommitResponse resp = envelope.getCommit();
            if (!resp.getSuccess()) {
                throw new FlourineException.ProtocolException(
                        "Commit failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode response", e);
        }
    }

    private void doJoin() throws FlourineException {
        JoinGroupRequest req = JoinGroupRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .addTopicIds(config.getTopicId())
                .build();
        client.send(ClientMessage.newBuilder().setJoinGroup(req).build().toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FlourineException.TimeoutException("Join timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.JOIN_GROUP) {
                throw new FlourineException.ProtocolException("Unexpected response type or empty response");
            }

            io.flourine.sdk.proto.JoinGroupResponse resp = envelope.getJoinGroup();
            if (!resp.getSuccess()) {
                throw new FlourineException.ProtocolException(
                        "Join failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            stateLock.writeLock().lock();
            try {
                generation = resp.getGeneration();
                assignments = new ArrayList<>(resp.getAssignmentsList());
                offsets.clear();
                for (PartitionAssignment a : assignments) {
                    offsets.put(a.getPartitionId(), a.getCommittedOffset());
                }
                state = State.ACTIVE;
            } finally {
                stateLock.writeLock().unlock();
            }

            log.info("Joined group {} at generation {}", config.getGroupId(), resp.getGeneration());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode response", e);
        }
    }

    private void doRejoin(long newGeneration) throws FlourineException {
        stateLock.writeLock().lock();
        try {
            state = State.REBALANCING;
        } finally {
            stateLock.writeLock().unlock();
        }

        try {
            commit();
        } catch (FlourineException e) {
            log.warn("Failed to commit offsets during rebalance", e);
        }

        try {
            Thread.sleep(config.getRebalanceDelay().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        }

        long currentGen = newGeneration;
        int retries = 0;
        while (retries < MAX_REJOIN_RETRIES) {
            RejoinRequest req = RejoinRequest.newBuilder()
                    .setGroupId(config.getGroupId())
                    .setTopicId(config.getTopicId())
                    .setReaderId(config.getReaderId())
                    .setGeneration(currentGen)
                    .build();

            client.send(ClientMessage.newBuilder().setRejoin(req).build().toByteArray());

            try {
                byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (response == null) {
                    throw new FlourineException.TimeoutException("Rejoin timeout");
                }

                ServerMessage envelope = ServerMessage.parseFrom(response);
                if (envelope.getMessageCase() != ServerMessage.MessageCase.REJOIN) {
                    throw new FlourineException.ProtocolException("Unexpected response type or empty response");
                }

                io.flourine.sdk.proto.RejoinResponse resp = envelope.getRejoin();
                if (!resp.getSuccess()) {
                    throw new FlourineException.ProtocolException(
                            "Rejoin failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                    );
                }

                if (resp.getStatus() == RejoinStatus.REJOIN_STATUS_REBALANCE_NEEDED) {
                    currentGen = resp.getGeneration();
                    retries++;
                    log.debug("Rebalance still in progress, retry {}/{}", retries, MAX_REJOIN_RETRIES);
                    Thread.sleep(config.getRebalanceDelay().toMillis());
                    continue;
                }

                stateLock.writeLock().lock();
                try {
                    generation = resp.getGeneration();
                    assignments = new ArrayList<>(resp.getAssignmentsList());
                    offsets.clear();
                    for (PartitionAssignment a : assignments) {
                        offsets.put(a.getPartitionId(), a.getCommittedOffset());
                    }
                    state = State.ACTIVE;
                } finally {
                    stateLock.writeLock().unlock();
                }

                log.info("Rejoined group {} at generation {}", config.getGroupId(), resp.getGeneration());
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlourineException.ConnectionException("Interrupted", e);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new FlourineException.ProtocolException("Failed to decode response", e);
            }
        }

        throw new FlourineException.ProtocolException("Rejoin failed after " + MAX_REJOIN_RETRIES + " retries");
    }

    private void doLeave() throws FlourineException {
        try {
            commit();
        } catch (FlourineException e) {
            log.warn("Failed to commit offsets during leave", e);
        }

        LeaveGroupRequest req = LeaveGroupRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setTopicId(config.getTopicId())
                .setReaderId(config.getReaderId())
                .build();
        client.send(ClientMessage.newBuilder().setLeaveGroup(req).build().toByteArray());
        log.info("Left group {}", config.getGroupId());
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (FlourineException e) {
            log.warn("Error during close", e);
        }
        client.close();
    }

    private static class FlourineWebSocketClient extends WebSocketClient {
        private static final Logger log = LoggerFactory.getLogger(FlourineWebSocketClient.class);
        private final BlockingQueue<byte[]> responseQueue;

        public FlourineWebSocketClient(URI serverUri, BlockingQueue<byte[]> responseQueue) {
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
            responseQueue.offer(data);
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

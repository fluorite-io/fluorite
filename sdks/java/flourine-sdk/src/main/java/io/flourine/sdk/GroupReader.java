package io.flourine.sdk;

import io.flourine.sdk.proto.AuthRequest;
import io.flourine.sdk.proto.ClientMessage;
import io.flourine.sdk.proto.CommitRequest;
import io.flourine.sdk.proto.HeartbeatRequest;
import io.flourine.sdk.proto.HeartbeatStatus;
import io.flourine.sdk.proto.JoinGroupRequest;
import io.flourine.sdk.proto.LeaveGroupRequest;
import io.flourine.sdk.proto.PollRequest;
import io.flourine.sdk.proto.PollResponse;
import io.flourine.sdk.proto.ServerMessage;
import io.flourine.sdk.proto.TopicResult;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reader client with reader group support.
 * <p>
 * Uses a poll-based model where the broker dispatches work to readers.
 * Wire payloads use generated protobuf message classes directly.
 */
public class GroupReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GroupReader.class);
    private static final int RESPONSE_QUEUE_CAPACITY = 1000;

    public enum State {
        INIT,
        ACTIVE,
        STOPPED
    }

    private final ReaderConfig config;
    private final FlourineWebSocketClient client;
    private final BlockingQueue<byte[]> responseQueue;

    /**
     * A batch of results from a single poll, with offset range and lease deadline.
     * Pass this to {@link #commit(PollBatch)} to commit the specific range.
     */
    public static class PollBatch {
        private final List<TopicResult> results;
        private final long startOffset;
        private final long endOffset;
        private final long leaseDeadlineMs;

        public PollBatch(List<TopicResult> results, long startOffset, long endOffset, long leaseDeadlineMs) {
            this.results = results;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.leaseDeadlineMs = leaseDeadlineMs;
        }

        public List<TopicResult> getResults() { return results; }
        public long getStartOffset() { return startOffset; }
        public long getEndOffset() { return endOffset; }
        public long getLeaseDeadlineMs() { return leaseDeadlineMs; }
    }

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private State state = State.INIT;
    private final List<long[]> inflight = new ArrayList<>();

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
            HeartbeatRequest req = HeartbeatRequest.newBuilder()
                    .setGroupId(config.getGroupId())
                    .setTopicId(config.getTopicId())
                    .setReaderId(config.getReaderId())
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
                log.debug("Heartbeat OK");
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

    public PollBatch poll() throws FlourineException {
        stateLock.readLock().lock();
        try {
            if (state != State.ACTIVE) {
                throw new FlourineException.ProtocolException("Reader not active: " + state);
            }
        } finally {
            stateLock.readLock().unlock();
        }

        PollRequest req = PollRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setTopicId(config.getTopicId())
                .setReaderId(config.getReaderId())
                .setMaxBytes(config.getMaxBytes())
                .build();
        client.send(ClientMessage.newBuilder().setPoll(req).build().toByteArray());

        try {
            byte[] response = responseQueue.poll(config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (response == null) {
                throw new FlourineException.TimeoutException("Poll timeout");
            }

            ServerMessage envelope = ServerMessage.parseFrom(response);
            if (envelope.getMessageCase() != ServerMessage.MessageCase.POLL) {
                throw new FlourineException.ProtocolException("Unexpected response type or empty response");
            }

            PollResponse resp = envelope.getPoll();
            if (!resp.getSuccess()) {
                throw new FlourineException.ProtocolException(
                        "Poll failed (" + resp.getErrorCode() + "): " + resp.getErrorMessage()
                );
            }

            if (resp.getStartOffset() != resp.getEndOffset()) {
                stateLock.writeLock().lock();
                try {
                    inflight.add(new long[]{resp.getStartOffset(), resp.getEndOffset()});
                } finally {
                    stateLock.writeLock().unlock();
                }
            }

            return new PollBatch(
                    new ArrayList<>(resp.getResultsList()),
                    resp.getStartOffset(),
                    resp.getEndOffset(),
                    resp.getLeaseDeadlineMs()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode response", e);
        }
    }

    public void commit(PollBatch batch) throws FlourineException {
        if (batch.getStartOffset() == batch.getEndOffset()) {
            return;
        }

        CommitRequest req = CommitRequest.newBuilder()
                .setGroupId(config.getGroupId())
                .setReaderId(config.getReaderId())
                .setTopicId(config.getTopicId())
                .setStartOffset(batch.getStartOffset())
                .setEndOffset(batch.getEndOffset())
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

            stateLock.writeLock().lock();
            try {
                long s = batch.getStartOffset(), e = batch.getEndOffset();
                inflight.removeIf(r -> r[0] == s && r[1] == e);
            } finally {
                stateLock.writeLock().unlock();
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
                inflight.clear();
                state = State.ACTIVE;
            } finally {
                stateLock.writeLock().unlock();
            }

            log.info("Joined group {}", config.getGroupId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlourineException.ConnectionException("Interrupted", e);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new FlourineException.ProtocolException("Failed to decode response", e);
        }
    }

    private void doLeave() throws FlourineException {
        List<long[]> ranges;
        stateLock.readLock().lock();
        try {
            ranges = new ArrayList<>(inflight);
        } finally {
            stateLock.readLock().unlock();
        }
        for (long[] range : ranges) {
            try {
                commit(new PollBatch(List.of(), range[0], range[1], 0));
            } catch (FlourineException e) {
                log.warn("Failed to commit range [{}, {}) during leave", range[0], range[1], e);
            }
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

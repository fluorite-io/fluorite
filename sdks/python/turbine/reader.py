"""Reader client for reading messages from Turbine."""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator, Optional
from uuid import uuid4

import websockets
from websockets.asyncio.client import ClientConnection

from turbine.proto import turbine_wire_pb2 as pb

from .exceptions import (
    AuthenticationException,
    ConnectionException,
    ProtocolException,
    TimeoutException,
)

logger = logging.getLogger(__name__)


@dataclass
class ReaderConfig:
    """Configuration for the Turbine reader."""

    url: str = "ws://localhost:9000"
    api_key: Optional[str] = None
    group_id: str = "default"
    reader_id: Optional[str] = None
    topic_id: int = 1
    max_bytes: int = 1024 * 1024  # 1 MB
    timeout: float = 30.0  # seconds
    heartbeat_interval: float = 10.0  # seconds
    rebalance_delay: float = 5.0  # seconds

    def __post_init__(self):
        if self.reader_id is None:
            self.reader_id = str(uuid4())


class ReaderState(Enum):
    """Reader state."""

    INIT = "init"
    ACTIVE = "active"
    REBALANCING = "rebalancing"
    STOPPED = "stopped"


class GroupReader:
    """Reader client with reader group support."""

    def __init__(
        self,
        ws: ClientConnection,
        config: ReaderConfig,
    ):
        self._ws = ws
        self._config = config
        self._state = ReaderState.INIT
        self._generation = 0
        self._assignments: list[pb.PartitionAssignment] = []
        self._offsets: dict[int, int] = {}
        self._running = True
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @classmethod
    async def join(cls, config: ReaderConfig) -> "GroupReader":
        try:
            ws = await asyncio.wait_for(
                websockets.connect(config.url),
                timeout=config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Connection timeout")
        except Exception as e:
            raise ConnectionException(f"Failed to connect: {e}")

        reader = cls(ws, config)
        if config.api_key:
            await reader._authenticate(config.api_key)
        await reader._do_join()
        return reader

    async def _authenticate(self, api_key: str) -> None:
        envelope = pb.ClientMessage()
        envelope.auth.api_key = api_key
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Authentication timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "auth":
            raise ProtocolException("Unexpected auth response")

        auth_resp = server_msg.auth
        if not auth_resp.success:
            raise AuthenticationException(auth_resp.error_message)

        logger.debug("Authentication successful")

    def start_heartbeat(self) -> None:
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        self._running = False

        async with self._lock:
            self._state = ReaderState.STOPPED

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self._do_leave()

    @property
    def state(self) -> ReaderState:
        return self._state

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def assignments(self) -> list[pb.PartitionAssignment]:
        return self._assignments.copy()

    async def poll(self) -> list[pb.PartitionResult]:
        async with self._lock:
            if self._state != ReaderState.ACTIVE:
                raise ProtocolException(f"Reader not active: {self._state}")

            current_assignments = self._assignments.copy()
            current_gen = self._generation

        if not current_assignments:
            return []

        reads = []
        for a in current_assignments:
            offset = self._offsets.get(a.partition_id, a.committed_offset)
            reads.append(
                pb.PartitionRead(
                    topic_id=self._config.topic_id,
                    partition_id=a.partition_id,
                    offset=offset,
                    max_bytes=self._config.max_bytes,
                )
            )

        req = pb.ReadRequest(
            group_id=self._config.group_id,
            reader_id=self._config.reader_id,
            generation=current_gen,
            reads=reads,
        )
        envelope = pb.ClientMessage()
        envelope.read.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Read timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "read":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.read
        if not resp.success:
            raise ProtocolException(
                f"Read failed ({resp.error_code}): {resp.error_message}"
            )

        for pr in resp.results:
            if pr.records:
                current = self._offsets.get(pr.partition_id, 0)
                self._offsets[pr.partition_id] = current + len(pr.records)

        return list(resp.results)

    async def poll_loop(
        self, interval: float = 0.1
    ) -> AsyncIterator[list[pb.PartitionResult]]:
        while self._running:
            try:
                results = await self.poll()
                if results:
                    yield results
                else:
                    await asyncio.sleep(interval)
            except TimeoutException as e:
                logger.warning("Poll timeout, retrying: %s", e)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    async def commit(self) -> None:
        async with self._lock:
            gen = self._generation

        if not self._offsets:
            return

        commits = []
        for partition_id, offset in self._offsets.items():
            commits.append(
                pb.PartitionCommit(
                    topic_id=self._config.topic_id,
                    partition_id=partition_id,
                    offset=offset,
                )
            )

        req = pb.CommitRequest(
            group_id=self._config.group_id,
            reader_id=self._config.reader_id,
            generation=gen,
            commits=commits,
        )
        envelope = pb.ClientMessage()
        envelope.commit.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Commit timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "commit":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.commit
        if not resp.success:
            raise ProtocolException(
                f"Commit failed ({resp.error_code}): {resp.error_message}"
            )

    async def _do_join(self) -> None:
        req = pb.JoinGroupRequest(
            group_id=self._config.group_id,
            reader_id=self._config.reader_id,
            topic_ids=[self._config.topic_id],
        )
        envelope = pb.ClientMessage()
        envelope.join_group.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        try:
            response = await asyncio.wait_for(
                self._ws.recv(),
                timeout=self._config.timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutException("Join timeout")

        if not isinstance(response, bytes):
            raise ProtocolException("Expected binary response")

        server_msg = pb.ServerMessage()
        server_msg.ParseFromString(response)
        if server_msg.WhichOneof("message") != "join_group":
            raise ProtocolException("Unexpected response type or empty response")

        resp = server_msg.join_group
        if not resp.success:
            raise ProtocolException(
                f"Join failed ({resp.error_code}): {resp.error_message}"
            )

        async with self._lock:
            self._generation = resp.generation
            self._assignments = list(resp.assignments)
            self._offsets.clear()
            for a in self._assignments:
                self._offsets[a.partition_id] = a.committed_offset
            self._state = ReaderState.ACTIVE

        logger.info(
            "Joined group %s at generation %d",
            self._config.group_id,
            resp.generation,
        )

    async def _do_rejoin(self, new_generation: int, max_retries: int = 10) -> None:
        async with self._lock:
            self._state = ReaderState.REBALANCING

        try:
            await self.commit()
        except Exception as e:
            logger.warning("Failed to commit offsets during rebalance: %s", e)

        await asyncio.sleep(self._config.rebalance_delay)

        current_gen = new_generation
        retries = 0
        while retries < max_retries:
            req = pb.RejoinRequest(
                group_id=self._config.group_id,
                topic_id=self._config.topic_id,
                reader_id=self._config.reader_id,
                generation=current_gen,
            )
            envelope = pb.ClientMessage()
            envelope.rejoin.CopyFrom(req)
            await self._ws.send(envelope.SerializeToString())

            try:
                response = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=self._config.timeout,
                )
            except asyncio.TimeoutError:
                raise TimeoutException("Rejoin timeout")

            if not isinstance(response, bytes):
                raise ProtocolException("Expected binary response")

            server_msg = pb.ServerMessage()
            server_msg.ParseFromString(response)
            if server_msg.WhichOneof("message") != "rejoin":
                raise ProtocolException("Unexpected response type or empty response")

            resp = server_msg.rejoin
            if not resp.success:
                raise ProtocolException(
                    f"Rejoin failed ({resp.error_code}): {resp.error_message}"
                )

            if resp.status == pb.REJOIN_STATUS_REBALANCE_NEEDED:
                current_gen = resp.generation
                retries += 1
                logger.debug(
                    "Rebalance still in progress, retry %d/%d", retries, max_retries
                )
                await asyncio.sleep(self._config.rebalance_delay)
                continue

            async with self._lock:
                self._generation = resp.generation
                self._assignments = list(resp.assignments)
                self._offsets.clear()
                for a in self._assignments:
                    self._offsets[a.partition_id] = a.committed_offset
                self._state = ReaderState.ACTIVE

            logger.info(
                "Rejoined group %s at generation %d",
                self._config.group_id,
                resp.generation,
            )
            return

        raise ProtocolException(f"Rejoin failed after {max_retries} retries")

    async def _do_leave(self) -> None:
        try:
            await self.commit()
        except Exception as e:
            logger.warning("Failed to commit offsets during leave: %s", e)

        req = pb.LeaveGroupRequest(
            group_id=self._config.group_id,
            topic_id=self._config.topic_id,
            reader_id=self._config.reader_id,
        )
        envelope = pb.ClientMessage()
        envelope.leave_group.CopyFrom(req)
        await self._ws.send(envelope.SerializeToString())

        logger.info("Left group %s", self._config.group_id)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval)
            if not self._running:
                break

            try:
                async with self._lock:
                    gen = self._generation

                req = pb.HeartbeatRequest(
                    group_id=self._config.group_id,
                    topic_id=self._config.topic_id,
                    reader_id=self._config.reader_id,
                    generation=gen,
                )
                envelope = pb.ClientMessage()
                envelope.heartbeat.CopyFrom(req)
                await self._ws.send(envelope.SerializeToString())

                response = await asyncio.wait_for(
                    self._ws.recv(),
                    timeout=self._config.timeout,
                )

                if not isinstance(response, bytes):
                    continue

                server_msg = pb.ServerMessage()
                server_msg.ParseFromString(response)
                if server_msg.WhichOneof("message") != "heartbeat":
                    continue

                resp = server_msg.heartbeat
                if not resp.success:
                    logger.warning(
                        "Heartbeat failed (%d): %s",
                        resp.error_code,
                        resp.error_message,
                    )
                    continue

                if resp.status == pb.HEARTBEAT_STATUS_OK:
                    logger.debug("Heartbeat OK at generation %d", resp.generation)
                elif resp.status == pb.HEARTBEAT_STATUS_REBALANCE_NEEDED:
                    logger.info("Rebalance needed, new generation %d", resp.generation)
                    await self._do_rejoin(resp.generation)
                elif resp.status == pb.HEARTBEAT_STATUS_UNKNOWN_MEMBER:
                    logger.warning("Unknown member, rejoining group")
                    await self._do_join()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Heartbeat failed: %s", e)

    async def close(self) -> None:
        await self._ws.close()

    async def __aenter__(self) -> "GroupReader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        await self.close()

"""Turbine SDK for Python.

Example usage:

    from turbine import Writer, GroupReader, WriterConfig, ReaderConfig

    # Writer
    async with Writer.connect("ws://localhost:9000") as writer:
        await writer.send(topic_id=1, partition_id=0, schema_id=100, records=[
            {"key": b"key1", "value": b"value1"},
        ])

    # Reader
    config = ReaderConfig(url="ws://localhost:9000", group_id="my-group", topic_id=1)
    async with GroupReader.join(config) as reader:
        async for results in reader.poll_loop():
            for result in results:
                print(result.records)
"""

from .writer import Writer, WriterConfig
from .reader import GroupReader, ReaderConfig
from .proto import turbine_wire_pb2
from .exceptions import (
    TurbineException,
    ConnectionException,
    AuthenticationException,
    TimeoutException,
    BackpressureException,
    ProtocolException,
)

__all__ = [
    "Writer",
    "WriterConfig",
    "GroupReader",
    "ReaderConfig",
    "turbine_wire_pb2",
    "TurbineException",
    "ConnectionException",
    "AuthenticationException",
    "TimeoutException",
    "BackpressureException",
    "ProtocolException",
]

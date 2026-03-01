# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Fluorite SDK for Python.

Example usage:

    from fluorite import Writer, GroupReader, WriterConfig, ReaderConfig

    # Writer
    async with Writer.connect("ws://localhost:9000") as writer:
        await writer.send(topic_id=1, schema_id=100, records=[
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
from .reader import GroupReader, ReaderConfig, PollBatch
from .client import FluoriteClient, ClientConfig
from .proto import fluorite_wire_pb2
from ._schema import schema, Int32, Float32, NonNull
from .exceptions import (
    FluoriteException,
    ConnectionException,
    AuthenticationException,
    TimeoutException,
    BackpressureException,
    ProtocolException,
    SchemaException,
)

__all__ = [
    "Writer",
    "WriterConfig",
    "GroupReader",
    "ReaderConfig",
    "PollBatch",
    "FluoriteClient",
    "ClientConfig",
    "fluorite_wire_pb2",
    "schema",
    "Int32",
    "Float32",
    "NonNull",
    "FluoriteException",
    "ConnectionException",
    "AuthenticationException",
    "TimeoutException",
    "BackpressureException",
    "ProtocolException",
    "SchemaException",
]
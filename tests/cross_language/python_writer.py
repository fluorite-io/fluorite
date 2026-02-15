#!/usr/bin/env python3
"""
Cross-language E2E test: Python writer.

Usage:
    python python_writer.py <url> <topic_id> <partition_id> <num_records>

Appends records and prints the ack JSON to stdout.
"""

import asyncio
import json
import sys
import os

# Add the SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../sdks/python'))

from turbine import Writer, WriterConfig
from turbine.proto import turbine_wire_pb2 as pb


async def main():
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <url> <topic_id> <partition_id> <num_records>", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    topic_id = int(sys.argv[2])
    partition_id = int(sys.argv[3])
    num_records = int(sys.argv[4])

    config = WriterConfig(url=url, timeout=30.0)

    writer = await Writer.connect_with_config(config)
    async with writer:
        records = []
        for i in range(num_records):
            key = f"py-key-{i}".encode()
            value = json.dumps({"source": "python", "index": i, "data": f"hello from python {i}"}).encode()
            records.append(pb.Record(key=key, value=value))

        ack = await writer.send(topic_id, partition_id, 100, records)

        result = {
            "writer": "python",
            "writer_id": str(writer.writer_id),
            "topic_id": topic_id,
            "partition_id": partition_id,
            "start_offset": ack.start_offset,
            "end_offset": ack.end_offset,
            "record_count": num_records,
        }
        print(json.dumps(result))


if __name__ == "__main__":
    asyncio.run(main())

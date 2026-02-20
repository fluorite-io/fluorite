# Flourine Python SDK

Python client library for Flourine Event Bus.

## Installation

```bash
pip install flourine-sdk
```

Or install from source:

```bash
cd sdks/python
pip install -e .
```

## Usage

### Writer

```python
import asyncio
from flourine import Writer, WriterConfig
from flourine.proto import flourine_wire_pb2 as pb

async def main():
    # Connect
    async with Writer.connect("ws://localhost:9000") as writer:
        # Send records
        ack = await writer.send(
            topic_id=1,
            partition_id=0,
            schema_id=100,
            records=[
                pb.Record(key=b"key1", value=b"value1"),
                pb.Record(key=b"key2", value=b"value2"),
            ],
        )
        print(f"Wrote records at offsets {ack.start_offset} to {ack.end_offset}")

    # Or with authentication
    config = WriterConfig(
        url="ws://localhost:9000",
        api_key="tb_your_api_key",
    )
    async with Writer.connect_with_config(config) as writer:
        await writer.send_one(1, 0, 100, b"key", b"value")

asyncio.run(main())
```

### Reader (with Reader Groups)

```python
import asyncio
from flourine import GroupReader, ReaderConfig

async def main():
    config = ReaderConfig(
        url="ws://localhost:9000",
        group_id="my-group",
        topic_id=1,
    )

    async with GroupReader.join(config) as reader:
        # Start heartbeat loop
        reader.start_heartbeat()

        # Poll for records
        async for results in reader.poll_loop():
            for result in results:
                for record in result.records:
                    print(f"Received: {record.value}")

            # Commit offsets
            await reader.commit()

asyncio.run(main())
```

### Simple Polling

```python
import asyncio
from flourine import GroupReader, ReaderConfig

async def main():
    config = ReaderConfig(
        url="ws://localhost:9000",
        group_id="my-group",
        topic_id=1,
    )

    async with GroupReader.join(config) as reader:
        reader.start_heartbeat()

        while True:
            results = await reader.poll()
            for result in results:
                print(f"Partition {result.partition_id}: {len(result.records)} records")

            await reader.commit()
            await asyncio.sleep(0.1)

asyncio.run(main())
```

## Configuration

### WriterConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `ws://localhost:9000` | Server URL |
| `api_key` | `None` | API key for authentication |
| `max_retries` | `5` | Max retries on backpressure |
| `initial_backoff` | `0.1` | Initial backoff in seconds |
| `max_backoff` | `10.0` | Maximum backoff in seconds |
| `timeout` | `30.0` | Request timeout in seconds |

### ReaderConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `ws://localhost:9000` | Server URL |
| `api_key` | `None` | API key for authentication |
| `group_id` | `default` | Reader group ID |
| `reader_id` | `<uuid>` | Reader ID within group |
| `topic_id` | `1` | Topic to subscribe to |
| `max_bytes` | `1048576` | Max bytes per read (1MB) |
| `timeout` | `30.0` | Request timeout in seconds |
| `heartbeat_interval` | `10.0` | Heartbeat interval in seconds |
| `rebalance_delay` | `5.0` | Delay before claiming partitions |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/
```

## Wire Protocol

The SDK uses generated protobuf messages from `proto/flourine_wire.proto` for:
- outer WebSocket envelope (`ClientMessage`/`ServerMessage`)
- all request/response payloads

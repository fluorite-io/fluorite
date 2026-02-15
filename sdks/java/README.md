# Turbine Java SDK

Java client library for Turbine Event Bus.

## Installation

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>io.turbine</groupId>
    <artifactId>turbine-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

### Writer

```java
import io.turbine.sdk.*;
import io.turbine.sdk.proto.BatchAck;
import io.turbine.sdk.proto.Record;
import com.google.protobuf.ByteString;

// Connect
Writer writer = Writer.connect("ws://localhost:9000");

// Or with authentication
WriterConfig config = new WriterConfig()
    .url("ws://localhost:9000")
    .apiKey("tb_your_api_key");
Writer writer = Writer.connect(config);

// Send records
Record record = Record.newBuilder()
    .setKey(ByteString.copyFrom("key".getBytes()))
    .setValue(ByteString.copyFrom("value".getBytes()))
    .build();
BatchAck ack = writer.send(1, 0, 100, List.of(record));

System.out.println("Wrote records at offsets " + ack.getStartOffset() + " to " + ack.getEndOffset());

// Close
writer.close();
```

### Reader (with Reader Groups)

```java
import io.turbine.sdk.*;
import io.turbine.sdk.proto.PartitionResult;
import io.turbine.sdk.proto.Record;

// Configure
ReaderConfig config = new ReaderConfig()
    .url("ws://localhost:9000")
    .groupId("my-group")
    .topicId(1);

// Join the group
GroupReader reader = GroupReader.join(config);

// Start heartbeat loop
reader.startHeartbeat();

// Poll for records
while (true) {
    List<PartitionResult> results = reader.poll();
    for (PartitionResult result : results) {
        for (Record record : result.getRecordsList()) {
            System.out.println("Received: " + new String(record.getValue().toByteArray()));
        }
    }

    // Commit offsets
    reader.commit();
}

// Stop
reader.stop();
reader.close();
```

## Building

```bash
cd sdks/java/turbine-sdk
mvn clean install
```

## Testing

```bash
mvn test
```

## Wire Protocol

The SDK uses generated protobuf messages from `proto/turbine_wire.proto` for:
- outer WebSocket envelope (`ClientMessage`/`ServerMessage`)
- all request/response payloads

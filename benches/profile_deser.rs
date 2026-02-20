//! Simple binary for profiling Value deserialization
//! Run with: cargo flamegraph --bin profile_deser -o flamegraph.svg

use std::hint::black_box;

const COMPLEX_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "ComplexDocument",
    "namespace": "benchmark",
    "fields": [
        {"name": "id", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "version", "type": "int"},
        {"name": "active", "type": "boolean"},
        {"name": "score", "type": "double"},
        {
            "name": "author",
            "type": {
                "type": "record",
                "name": "Author",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "joined_date", "type": {"type": "int", "logicalType": "date"}}
                ]
            }
        },
        {
            "name": "tags",
            "type": {"type": "array", "items": "string"}
        },
        {
            "name": "metadata",
            "type": {"type": "map", "values": "string"}
        },
        {
            "name": "comments",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Comment",
                    "fields": [
                        {"name": "author", "type": "string"},
                        {"name": "text", "type": "string"},
                        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                        {"name": "likes", "type": "int"}
                    ]
                }
            }
        },
        {
            "name": "related_ids",
            "type": {"type": "array", "items": "long"}
        },
        {
            "name": "priority",
            "type": {
                "type": "enum",
                "name": "Priority",
                "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
            }
        },
        {
            "name": "optional_note",
            "type": ["null", "string"]
        }
    ]
}
"#;

fn make_complex_document(
    num_tags: usize,
    num_metadata: usize,
    num_comments: usize,
    num_related: usize,
) -> apache_avro::types::Value {
    use apache_avro::types::Value;

    let author = Value::Record(vec![
        ("name".to_string(), Value::String("John Doe".to_string())),
        ("email".to_string(), Value::String("john.doe@example.com".to_string())),
        ("joined_date".to_string(), Value::Date(19000)),
    ]);

    let tags: Vec<Value> = (0..num_tags)
        .map(|i| Value::String(format!("tag_{}", i)))
        .collect();

    let metadata: std::collections::HashMap<String, Value> = (0..num_metadata)
        .map(|i| (format!("key_{}", i), Value::String(format!("value_{}", i))))
        .collect();

    let comments: Vec<Value> = (0..num_comments)
        .map(|i| {
            Value::Record(vec![
                ("author".to_string(), Value::String(format!("commenter_{}", i))),
                ("text".to_string(), Value::String(format!("This is comment number {} with some text content.", i))),
                ("timestamp".to_string(), Value::TimestampMillis(1700000000000 + i as i64 * 1000)),
                ("likes".to_string(), Value::Int((i * 5) as i32)),
            ])
        })
        .collect();

    let related_ids: Vec<Value> = (0..num_related)
        .map(|i| Value::Long(1000000 + i as i64))
        .collect();

    Value::Record(vec![
        ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
        ("created_at".to_string(), Value::TimestampMillis(1700000000000)),
        ("version".to_string(), Value::Int(42)),
        ("active".to_string(), Value::Boolean(true)),
        ("score".to_string(), Value::Double(98.6)),
        ("author".to_string(), author),
        ("tags".to_string(), Value::Array(tags)),
        ("metadata".to_string(), Value::Map(metadata)),
        ("comments".to_string(), Value::Array(comments)),
        ("related_ids".to_string(), Value::Array(related_ids)),
        ("priority".to_string(), Value::Enum(2, "HIGH".to_string())),
        ("optional_note".to_string(), Value::Union(1, Box::new(Value::String("Optional note".to_string())))),
    ])
}

fn main() {
    use flourine::avro::value::BatchDeserializer;

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: flourine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Use large-sized document (matching criterion "large" benchmark)
    let document = make_complex_document(50, 50, 30, 200);
    let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

    // Run many iterations for profiling
    const ITERATIONS: usize = 500_000;

    // Use bump allocator for profiling
    let mut batch = BatchDeserializer::new(&fast_schema);

    for _ in 0..ITERATIONS {
        batch.reset();
        let value = batch.deserialize(&datum).unwrap();
        let _ = black_box(&value);
    }
}

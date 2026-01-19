//! Benchmark comparing turbine::avro::Value vs apache_avro::Value
//! Tests nested structs, arrays, maps, and logical types

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// Complex schema with nested records, arrays, maps, and logical types
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
        (
            "email".to_string(),
            Value::String("john.doe@example.com".to_string()),
        ),
        ("joined_date".to_string(), Value::Date(19000)), // Some date
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
                (
                    "author".to_string(),
                    Value::String(format!("commenter_{}", i)),
                ),
                (
                    "text".to_string(),
                    Value::String(format!("This is comment number {} with some text content that makes it realistic in size.", i)),
                ),
                ("timestamp".to_string(), Value::TimestampMillis(1700000000000 + i as i64 * 1000)),
                ("likes".to_string(), Value::Int((i * 5) as i32)),
            ])
        })
        .collect();

    let related_ids: Vec<Value> = (0..num_related)
        .map(|i| Value::Long(1000000 + i as i64))
        .collect();

    Value::Record(vec![
        (
            "id".to_string(),
            Value::Uuid(uuid::Uuid::new_v4()),
        ),
        (
            "created_at".to_string(),
            Value::TimestampMillis(1700000000000),
        ),
        ("version".to_string(), Value::Int(42)),
        ("active".to_string(), Value::Boolean(true)),
        ("score".to_string(), Value::Double(98.6)),
        ("author".to_string(), author),
        ("tags".to_string(), Value::Array(tags)),
        ("metadata".to_string(), Value::Map(metadata)),
        ("comments".to_string(), Value::Array(comments)),
        ("related_ids".to_string(), Value::Array(related_ids)),
        ("priority".to_string(), Value::Enum(2, "HIGH".to_string())),
        (
            "optional_note".to_string(),
            Value::Union(
                1,
                Box::new(Value::String(
                    "This is an optional note with some content".to_string(),
                )),
            ),
        ),
    ])
}

fn bench_value_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("Value Deserialization");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Test with different sizes
    for (num_tags, num_metadata, num_comments, num_related, label) in [
        (5, 5, 3, 10, "small"),
        (20, 20, 10, 50, "medium"),
        (50, 50, 30, 200, "large"),
    ] {
        let document = make_complex_document(num_tags, num_metadata, num_comments, num_related);
        let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

        group.throughput(Throughput::Bytes(datum.len() as u64));

        // Apache Avro - deserialize to Value
        group.bench_with_input(
            BenchmarkId::new("apache_avro::Value", label),
            &datum,
            |b, datum| {
                b.iter(|| {
                    let value =
                        apache_avro::from_avro_datum(&apache_schema, &mut datum.as_slice(), None)
                            .unwrap();
                    black_box(value)
                })
            },
        );

        // turbine::avro - deserialize to Value
        group.bench_with_input(
            BenchmarkId::new("turbine::avro::Value", label),
            &datum,
            |b, datum| {
                b.iter(|| {
                    let value: turbine::avro::Value =
                        turbine::avro::from_datum_slice(datum, &fast_schema).unwrap();
                    black_box(value)
                })
            },
        );
    }

    group.finish();
}

fn bench_value_field_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("Value Field Access");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    let document = make_complex_document(20, 20, 10, 50);
    let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

    // Pre-deserialize for field access benchmarks
    let apache_value =
        apache_avro::from_avro_datum(&apache_schema, &mut datum.as_slice(), None).unwrap();
    let fast_value: turbine::avro::Value =
        turbine::avro::from_datum_slice(&datum, &fast_schema).unwrap();

    // Apache Avro - access nested field
    group.bench_function("apache_avro nested field", |b| {
        b.iter(|| {
            if let apache_avro::types::Value::Record(fields) = &apache_value {
                // Linear scan for each field access
                for (name, value) in fields {
                    if name == "author" {
                        if let apache_avro::types::Value::Record(author_fields) = value {
                            for (name, value) in author_fields {
                                if name == "name" {
                                    return black_box(value.clone());
                                }
                            }
                        }
                    }
                }
            }
            unreachable!()
        })
    });

    // turbine::avro - access nested field (O(1) lookup)
    let complex_fields = fast_schema.record_index().get_record_fields("benchmark.ComplexDocument").unwrap();
    let author_fields = fast_schema.record_index().get_record_fields("benchmark.Author").unwrap();
    group.bench_function("turbine::avro nested field", |b| {
        b.iter(|| {
            if let turbine::avro::Value::Record(record) = &fast_value {
                if let Some(turbine::avro::Value::Record(author)) = record.get("author", complex_fields) {
                    if let Some(name) = author.get("name", author_fields) {
                        return black_box(name.clone());
                    }
                }
            }
            unreachable!()
        })
    });

    // Apache Avro - access multiple fields
    group.bench_function("apache_avro 5 field accesses", |b| {
        b.iter(|| {
            let mut results = Vec::with_capacity(5);
            if let apache_avro::types::Value::Record(fields) = &apache_value {
                for (name, value) in fields {
                    match name.as_str() {
                        "id" | "version" | "active" | "score" | "priority" => {
                            results.push(value.clone());
                        }
                        _ => {}
                    }
                }
            }
            black_box(results)
        })
    });

    // turbine::avro - access multiple fields
    group.bench_function("turbine::avro 5 field accesses", |b| {
        b.iter(|| {
            let mut results = Vec::with_capacity(5);
            if let turbine::avro::Value::Record(record) = &fast_value {
                if let Some(v) = record.get("id", complex_fields) {
                    results.push(v.clone());
                }
                if let Some(v) = record.get("version", complex_fields) {
                    results.push(v.clone());
                }
                if let Some(v) = record.get("active", complex_fields) {
                    results.push(v.clone());
                }
                if let Some(v) = record.get("score", complex_fields) {
                    results.push(v.clone());
                }
                if let Some(v) = record.get("priority", complex_fields) {
                    results.push(v.clone());
                }
            }
            black_box(results)
        })
    });

    group.finish();
}

fn bench_value_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("Value Serialization");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    for (num_tags, num_metadata, num_comments, num_related, label) in [
        (5, 5, 3, 10, "small"),
        (20, 20, 10, 50, "medium"),
    ] {
        let document = make_complex_document(num_tags, num_metadata, num_comments, num_related);
        let datum = apache_avro::to_avro_datum(&apache_schema, document.clone()).unwrap();

        // Pre-deserialize for serialization benchmarks
        let fast_value: turbine::avro::Value =
            turbine::avro::from_datum_slice(&datum, &fast_schema).unwrap();

        group.throughput(Throughput::Bytes(datum.len() as u64));

        // Apache Avro - serialize Value
        group.bench_with_input(
            BenchmarkId::new("apache_avro::Value", label),
            &document,
            |b, document| {
                b.iter(|| {
                    let encoded = apache_avro::to_avro_datum(&apache_schema, document.clone()).unwrap();
                    black_box(encoded)
                })
            },
        );

        // turbine::avro - serialize Value
        group.bench_with_input(
            BenchmarkId::new("turbine::avro::Value", label),
            &fast_value,
            |b, fast_value| {
                b.iter(|| {
                    let mut config = turbine::avro::ser::SerializerConfig::new(&fast_schema);
                    let encoded =
                        turbine::avro::to_datum_vec(fast_value, &mut config).unwrap();
                    black_box(encoded)
                })
            },
        );
    }

    group.finish();
}

fn bench_array_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("Array Iteration");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Large array for iteration benchmark
    let document = make_complex_document(100, 10, 100, 1000);
    let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

    let apache_value =
        apache_avro::from_avro_datum(&apache_schema, &mut datum.as_slice(), None).unwrap();
    let fast_value: turbine::avro::Value =
        turbine::avro::from_datum_slice(&datum, &fast_schema).unwrap();

    // Apache Avro - iterate over related_ids array
    group.bench_function("apache_avro array sum", |b| {
        b.iter(|| {
            let mut sum: i64 = 0;
            if let apache_avro::types::Value::Record(fields) = &apache_value {
                for (name, value) in fields {
                    if name == "related_ids" {
                        if let apache_avro::types::Value::Array(arr) = value {
                            for item in arr {
                                if let apache_avro::types::Value::Long(n) = item {
                                    sum += n;
                                }
                            }
                        }
                    }
                }
            }
            black_box(sum)
        })
    });

    // turbine::avro - iterate over related_ids array
    let complex_fields = fast_schema.record_index().get_record_fields("benchmark.ComplexDocument").unwrap();
    group.bench_function("turbine::avro array sum", |b| {
        b.iter(|| {
            let mut sum: i64 = 0;
            if let turbine::avro::Value::Record(record) = &fast_value {
                if let Some(turbine::avro::Value::Array(arr)) = record.get("related_ids", complex_fields) {
                    for item in arr {
                        if let turbine::avro::Value::Long(n) = item {
                            sum += n;
                        }
                    }
                }
            }
            black_box(sum)
        })
    });

    group.finish();
}

fn bench_map_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("Map Access");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Large map for access benchmark
    let document = make_complex_document(10, 100, 5, 10);
    let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

    let apache_value =
        apache_avro::from_avro_datum(&apache_schema, &mut datum.as_slice(), None).unwrap();
    let fast_value: turbine::avro::Value =
        turbine::avro::from_datum_slice(&datum, &fast_schema).unwrap();

    // Apache Avro - access map keys
    group.bench_function("apache_avro map lookup", |b| {
        b.iter(|| {
            let mut found = 0;
            if let apache_avro::types::Value::Record(fields) = &apache_value {
                for (name, value) in fields {
                    if name == "metadata" {
                        if let apache_avro::types::Value::Map(map) = value {
                            // Look up specific keys
                            for key in ["key_0", "key_25", "key_50", "key_75", "key_99"] {
                                if map.contains_key(key) {
                                    found += 1;
                                }
                            }
                        }
                    }
                }
            }
            black_box(found)
        })
    });

    // turbine::avro - access map keys (maps are HashMap<Cow<str>, Value>)
    let complex_fields = fast_schema.record_index().get_record_fields("benchmark.ComplexDocument").unwrap();
    group.bench_function("turbine::avro map lookup", |b| {
        b.iter(|| {
            let mut found = 0;
            if let turbine::avro::Value::Record(record) = &fast_value {
                if let Some(turbine::avro::Value::Map(map)) = record.get("metadata", complex_fields) {
                    // Look up specific keys
                    for key in ["key_0", "key_25", "key_50", "key_75", "key_99"] {
                        if map.contains_key(key) {
                            found += 1;
                        }
                    }
                }
            }
            black_box(found)
        })
    });

    group.finish();
}

fn bench_string_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("String Heavy Workload");

    // Schema with many string fields to test zero-copy benefits
    let string_schema = r#"
    {
        "type": "record",
        "name": "StringRecord",
        "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "string"},
            {"name": "field3", "type": "string"},
            {"name": "field4", "type": "string"},
            {"name": "field5", "type": "string"},
            {"name": "field6", "type": "string"},
            {"name": "field7", "type": "string"},
            {"name": "field8", "type": "string"},
            {"name": "field9", "type": "string"},
            {"name": "field10", "type": "string"}
        ]
    }
    "#;

    let apache_schema = apache_avro::Schema::parse_str(string_schema).unwrap();
    let fast_schema: turbine::avro::Schema = string_schema.parse().unwrap();

    // Create a record with long strings
    let long_string = "This is a longer string that would require allocation in apache-avro but can be borrowed in turbine::avro. ".repeat(10);
    let document = apache_avro::types::Value::Record(
        (1..=10)
            .map(|i| {
                (
                    format!("field{}", i),
                    apache_avro::types::Value::String(long_string.clone()),
                )
            })
            .collect(),
    );
    let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

    group.throughput(Throughput::Bytes(datum.len() as u64));

    group.bench_function("apache_avro (allocating)", |b| {
        b.iter(|| {
            let value =
                apache_avro::from_avro_datum(&apache_schema, &mut datum.as_slice(), None).unwrap();
            black_box(value)
        })
    });

    group.bench_function("turbine::avro (zero-copy)", |b| {
        b.iter(|| {
            let value: turbine::avro::Value =
                turbine::avro::from_datum_slice(&datum, &fast_schema).unwrap();
            black_box(value)
        })
    });

    group.finish();
}

fn bench_bump_allocation(c: &mut Criterion) {
    use turbine::avro::value::BatchDeserializer;

    let mut group = c.benchmark_group("Bump vs Regular Allocation");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Test with different sizes
    for (num_tags, num_metadata, num_comments, num_related, label) in [
        (5, 5, 3, 10, "small"),
        (20, 20, 10, 50, "medium"),
        (50, 50, 30, 200, "large"),
    ] {
        let document = make_complex_document(num_tags, num_metadata, num_comments, num_related);
        let datum = apache_avro::to_avro_datum(&apache_schema, document).unwrap();

        group.throughput(Throughput::Bytes(datum.len() as u64));

        // Regular allocation (Vec-based)
        group.bench_with_input(
            BenchmarkId::new("regular_alloc", label),
            &datum,
            |b, datum| {
                b.iter(|| {
                    let value: turbine::avro::Value =
                        turbine::avro::from_datum_slice(datum, &fast_schema).unwrap();
                    black_box(value)
                })
            },
        );

        // Bump allocation (reusing allocator)
        group.bench_with_input(
            BenchmarkId::new("bump_alloc", label),
            &datum,
            |b, datum| {
                let mut batch = BatchDeserializer::new(&fast_schema);
                b.iter(|| {
                    batch.reset();
                    let value = batch.deserialize(datum).unwrap();
                    // Access the value to prevent dead code elimination without escaping the closure
                    let _ = black_box(&value);
                })
            },
        );
    }

    group.finish();
}

fn bench_batch_processing(c: &mut Criterion) {
    use turbine::avro::value::BatchDeserializer;

    let mut group = c.benchmark_group("Batch Processing");

    let apache_schema = apache_avro::Schema::parse_str(COMPLEX_SCHEMA).unwrap();
    let fast_schema: turbine::avro::Schema = COMPLEX_SCHEMA.parse().unwrap();

    // Generate a batch of documents
    let batch_sizes = [10, 100, 1000];

    for batch_size in batch_sizes {
        // Create batch of encoded records
        let records: Vec<Vec<u8>> = (0..batch_size)
            .map(|i| {
                let document = make_complex_document(5 + (i % 10), 5, 3, 10);
                apache_avro::to_avro_datum(&apache_schema, document).unwrap()
            })
            .collect();

        let total_bytes: usize = records.iter().map(|r| r.len()).sum();
        group.throughput(Throughput::Bytes(total_bytes as u64));

        // Regular allocation - new allocations for each record
        group.bench_with_input(
            BenchmarkId::new("regular_alloc", batch_size),
            &records,
            |b, records| {
                b.iter(|| {
                    for record in records {
                        let value: turbine::avro::Value =
                            turbine::avro::from_datum_slice(record, &fast_schema).unwrap();
                        black_box(&value);
                    }
                })
            },
        );

        // Bump allocation - reset between records (simulates consuming each record before next)
        group.bench_with_input(
            BenchmarkId::new("bump_alloc_reset_each", batch_size),
            &records,
            |b, records| {
                let mut batch = BatchDeserializer::new(&fast_schema);
                b.iter(|| {
                    for record in records {
                        batch.reset();
                        let value = batch.deserialize(record).unwrap();
                        let _ = black_box(&value);
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_value_deserialization,
    bench_value_field_access,
    bench_value_serialization,
    bench_array_iteration,
    bench_map_access,
    bench_string_heavy,
    bench_bump_allocation,
    bench_batch_processing,
);
criterion_main!(benches);

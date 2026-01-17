use std::sync::Arc;

use apache_avro::types::Value as AvroValue;
use apache_avro::{from_avro_datum, Schema as AvroSchema};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::buffer::error::{Result, TurbineError};

/// Converts an Avro schema to an Arrow schema
pub fn avro_schema_to_arrow(avro_schema: &AvroSchema) -> Result<ArrowSchema> {
    match avro_schema {
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>> = record
                .fields
                .iter()
                .map(|f| {
                    let data_type = avro_type_to_arrow(&f.schema)?;
                    let nullable = matches!(&f.schema, AvroSchema::Union(_));
                    Ok(Field::new(&f.name, data_type, nullable))
                })
                .collect();
            Ok(ArrowSchema::new(fields?))
        }
        _ => Err(TurbineError::InvalidSchema(
            "Root Avro schema must be a Record".to_string(),
        )),
    }
}

/// Converts an Avro type to an Arrow DataType
fn avro_type_to_arrow(avro_schema: &AvroSchema) -> Result<DataType> {
    match avro_schema {
        AvroSchema::Null => Ok(DataType::Null),
        AvroSchema::Boolean => Ok(DataType::Boolean),
        AvroSchema::Int => Ok(DataType::Int32),
        AvroSchema::Long => Ok(DataType::Int64),
        AvroSchema::Float => Ok(DataType::Float32),
        AvroSchema::Double => Ok(DataType::Float64),
        AvroSchema::Bytes => Ok(DataType::Binary),
        AvroSchema::String => Ok(DataType::Utf8),
        AvroSchema::Array(array_schema) => {
            let inner_type = avro_type_to_arrow(&array_schema.items)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
        AvroSchema::Map(map_schema) => {
            let value_type = avro_type_to_arrow(&map_schema.types)?;
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ))
        }
        AvroSchema::Union(union) => {
            // Handle nullable types (union with null)
            let non_null: Vec<_> = union
                .variants()
                .iter()
                .filter(|s| !matches!(s, AvroSchema::Null))
                .collect();

            if non_null.len() == 1 {
                avro_type_to_arrow(non_null[0])
            } else {
                // Complex union - use string representation
                Ok(DataType::Utf8)
            }
        }
        AvroSchema::Record(record) => {
            let fields: Result<Vec<Field>> = record
                .fields
                .iter()
                .map(|f| {
                    let data_type = avro_type_to_arrow(&f.schema)?;
                    let nullable = matches!(&f.schema, AvroSchema::Union(_));
                    Ok(Field::new(&f.name, data_type, nullable))
                })
                .collect();
            Ok(DataType::Struct(fields?.into()))
        }
        AvroSchema::Enum { .. } => Ok(DataType::Utf8),
        AvroSchema::Fixed(fixed) => Ok(DataType::FixedSizeBinary(fixed.size as i32)),
        AvroSchema::Decimal(decimal) => Ok(DataType::Decimal128(
            decimal.precision as u8,
            decimal.scale as i8,
        )),
        AvroSchema::Uuid => Ok(DataType::Utf8),
        AvroSchema::Date => Ok(DataType::Date32),
        AvroSchema::TimeMillis => Ok(DataType::Time32(TimeUnit::Millisecond)),
        AvroSchema::TimeMicros => Ok(DataType::Time64(TimeUnit::Microsecond)),
        AvroSchema::TimestampMillis => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        AvroSchema::TimestampMicros => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        AvroSchema::Duration => Ok(DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)),
        AvroSchema::LocalTimestampMillis => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        AvroSchema::LocalTimestampMicros => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        AvroSchema::BigDecimal => Ok(DataType::Utf8), // Serialize as string
        AvroSchema::LocalTimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        AvroSchema::TimestampNanos => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        AvroSchema::Ref { name } => Err(TurbineError::InvalidSchema(format!(
            "Schema reference '{}' not supported directly",
            name.fullname(None)
        ))),
    }
}

/// Builder for converting Avro records to Arrow RecordBatch
pub struct RecordBatchBuilder {
    arrow_schema: Arc<ArrowSchema>,
    avro_schema: AvroSchema,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
}

impl RecordBatchBuilder {
    pub fn new(avro_schema: AvroSchema) -> Result<Self> {
        let arrow_schema = Arc::new(avro_schema_to_arrow(&avro_schema)?);
        let builders = create_builders(&arrow_schema)?;

        Ok(Self {
            arrow_schema,
            avro_schema,
            builders,
            row_count: 0,
        })
    }

    pub fn append_avro_bytes(&mut self, data: &[u8]) -> Result<()> {
        let mut reader = data;
        let value = from_avro_datum(&self.avro_schema, &mut reader, None)?;
        self.append_value(&value)?;
        Ok(())
    }

    pub fn append_value(&mut self, value: &AvroValue) -> Result<()> {
        if let AvroValue::Record(fields) = value {
            for (i, (_, field_value)) in fields.iter().enumerate() {
                append_value_to_builder(&mut self.builders[i], field_value)?;
            }
            self.row_count += 1;
            Ok(())
        } else {
            Err(TurbineError::Conversion(
                "Expected Avro Record value".to_string(),
            ))
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(|b| b.finish())
            .collect();

        Ok(RecordBatch::try_new(self.arrow_schema, arrays)?)
    }

    pub fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }
}

fn create_builders(schema: &ArrowSchema) -> Result<Vec<Box<dyn ArrayBuilder>>> {
    schema
        .fields()
        .iter()
        .map(|f| create_builder(f.data_type()))
        .collect()
}

fn create_builder(data_type: &DataType) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match data_type {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time32(TimeUnit::Millisecond) => Box::new(Time32MillisecondBuilder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::new())
        }
        DataType::FixedSizeBinary(size) => Box::new(FixedSizeBinaryBuilder::new(*size)),
        DataType::List(field) => {
            let inner = create_builder(field.data_type())?;
            Box::new(ListBuilder::new(inner))
        }
        _ => Box::new(StringBuilder::new()), // Fallback to string
    })
}

fn append_value_to_builder(builder: &mut Box<dyn ArrayBuilder>, value: &AvroValue) -> Result<()> {
    match value {
        AvroValue::Null => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                b.append_null();
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
                b.append_null();
            }
        }
        AvroValue::Boolean(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Int(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Time32MillisecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Long(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Time64MicrosecondBuilder>() {
                b.append_value(*v);
            } else if let Some(b) = builder.as_any_mut().downcast_mut::<Time64NanosecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Float(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Float32Builder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Double(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Bytes(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<BinaryBuilder>() {
                b.append_value(v);
            }
        }
        AvroValue::String(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_value(v);
            }
        }
        AvroValue::Enum(_, v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_value(v);
            }
        }
        AvroValue::Union(_, inner) => {
            append_value_to_builder(builder, inner)?;
        }
        AvroValue::Fixed(_, v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<FixedSizeBinaryBuilder>() {
                b.append_value(v)?;
            }
        }
        AvroValue::Date(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                b.append_value(*v);
            }
        }
        AvroValue::TimestampMillis(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::TimestampMicros(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::TimestampNanos(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::LocalTimestampMillis(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::LocalTimestampMicros(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::LocalTimestampNanos(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
                b.append_value(*v);
            }
        }
        AvroValue::Uuid(v) => {
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_value(v.to_string());
            }
        }
        _ => {
            // For complex types, serialize to JSON string
            if let Some(b) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                b.append_value(format!("{:?}", value));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;

    fn sample_schema() -> AvroSchema {
        AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": ["null", "string"]},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean"},
                    {"name": "score", "type": "double"}
                ]
            }
            "#,
        )
        .unwrap()
    }

    #[test]
    fn test_avro_schema_to_arrow_basic_types() {
        let schema = sample_schema();
        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 6);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(2).name(), "email");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(2).is_nullable());
        assert_eq!(arrow_schema.field(3).name(), "age");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(4).name(), "active");
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Boolean);
        assert_eq!(arrow_schema.field(5).name(), "score");
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_avro_schema_to_arrow_array_type() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "WithArray",
                "fields": [
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(arrow_schema.fields().len(), 1);

        match arrow_schema.field(0).data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn test_avro_schema_to_arrow_nested_record() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "Outer",
                "fields": [
                    {
                        "name": "inner",
                        "type": {
                            "type": "record",
                            "name": "Inner",
                            "fields": [
                                {"name": "value", "type": "int"}
                            ]
                        }
                    }
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(arrow_schema.fields().len(), 1);

        match arrow_schema.field(0).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name(), "value");
                assert_eq!(fields[0].data_type(), &DataType::Int32);
            }
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_avro_schema_to_arrow_temporal_types() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "Temporal",
                "fields": [
                    {"name": "date", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "ts_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "ts_micros", "type": {"type": "long", "logicalType": "timestamp-micros"}}
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Date32);
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_avro_schema_non_record_error() {
        let schema = AvroSchema::String;
        let result = avro_schema_to_arrow(&schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Root Avro schema must be a Record"));
    }

    #[test]
    fn test_record_batch_builder_single_record() {
        let schema = sample_schema();
        let mut builder = RecordBatchBuilder::new(schema.clone()).unwrap();

        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("Alice".to_string())),
            (
                "email".to_string(),
                AvroValue::Union(1, Box::new(AvroValue::String("alice@example.com".to_string()))),
            ),
            ("age".to_string(), AvroValue::Int(30)),
            ("active".to_string(), AvroValue::Boolean(true)),
            ("score".to_string(), AvroValue::Double(95.5)),
        ]);

        builder.append_value(&record).unwrap();
        assert_eq!(builder.row_count(), 1);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn test_record_batch_builder_multiple_records() {
        let schema = sample_schema();
        let mut builder = RecordBatchBuilder::new(schema.clone()).unwrap();

        for i in 0..100 {
            let record = AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Long(i)),
                ("name".to_string(), AvroValue::String(format!("User{}", i))),
                ("email".to_string(), AvroValue::Union(0, Box::new(AvroValue::Null))),
                ("age".to_string(), AvroValue::Int(20 + (i % 50) as i32)),
                ("active".to_string(), AvroValue::Boolean(i % 2 == 0)),
                ("score".to_string(), AvroValue::Double(i as f64 * 1.5)),
            ]);
            builder.append_value(&record).unwrap();
        }

        assert_eq!(builder.row_count(), 100);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }

    #[test]
    fn test_record_batch_builder_with_nulls() {
        let schema = sample_schema();
        let mut builder = RecordBatchBuilder::new(schema.clone()).unwrap();

        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("Bob".to_string())),
            ("email".to_string(), AvroValue::Union(0, Box::new(AvroValue::Null))),
            ("age".to_string(), AvroValue::Int(25)),
            ("active".to_string(), AvroValue::Boolean(false)),
            ("score".to_string(), AvroValue::Double(0.0)),
        ]);

        builder.append_value(&record).unwrap();
        let batch = builder.finish().unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_record_batch_builder_non_record_error() {
        let schema = sample_schema();
        let mut builder = RecordBatchBuilder::new(schema).unwrap();

        let result = builder.append_value(&AvroValue::String("not a record".to_string()));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected Avro Record value"));
    }

    #[test]
    fn test_record_batch_builder_from_avro_bytes() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "Simple",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            }
            "#,
        )
        .unwrap();

        let mut builder = RecordBatchBuilder::new(schema.clone()).unwrap();

        // Create Avro binary data
        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(42)),
            ("name".to_string(), AvroValue::String("Test".to_string())),
        ]);
        let bytes = to_avro_datum(&schema, record).unwrap();

        builder.append_avro_bytes(&bytes).unwrap();
        assert_eq!(builder.row_count(), 1);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_avro_enum_to_string() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "WithEnum",
                "fields": [
                    {
                        "name": "status",
                        "type": {
                            "type": "enum",
                            "name": "Status",
                            "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
                        }
                    }
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);

        let mut builder = RecordBatchBuilder::new(schema).unwrap();
        let record = AvroValue::Record(vec![(
            "status".to_string(),
            AvroValue::Enum(0, "ACTIVE".to_string()),
        )]);
        builder.append_value(&record).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_avro_map_schema() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "WithMap",
                "fields": [
                    {"name": "metadata", "type": {"type": "map", "values": "string"}}
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        match arrow_schema.field(0).data_type() {
            DataType::Map(_, _) => {}
            other => panic!("Expected Map type, got {:?}", other),
        }
    }

    #[test]
    fn test_fixed_type() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "WithFixed",
                "fields": [
                    {"name": "hash", "type": {"type": "fixed", "name": "MD5", "size": 16}}
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn test_bytes_type() {
        let schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "WithBytes",
                "fields": [
                    {"name": "data", "type": "bytes"}
                ]
            }
            "#,
        )
        .unwrap();

        let arrow_schema = avro_schema_to_arrow(&schema).unwrap();
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Binary);

        let mut builder = RecordBatchBuilder::new(schema).unwrap();
        let record = AvroValue::Record(vec![(
            "data".to_string(),
            AvroValue::Bytes(vec![0x01, 0x02, 0x03, 0x04]),
        )]);
        builder.append_value(&record).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}

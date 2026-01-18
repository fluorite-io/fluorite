//! Tests for the Value type - dynamic Avro value representation

use turbine::avro::{from_datum_slice, ser::SerializerConfig, to_datum_vec, GenericRecord, Schema, Value};
use std::borrow::Cow;

#[test]
fn test_null() {
	let schema: Schema = r#""null""#.parse().unwrap();
	let data = &[];
	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::Null);
	assert!(value.is_null());
}

#[test]
fn test_boolean() {
	let schema: Schema = r#""boolean""#.parse().unwrap();

	let value: Value = from_datum_slice(&[1], &schema).unwrap();
	assert_eq!(value, Value::Boolean(true));
	assert_eq!(value.as_bool(), Some(true));

	let value: Value = from_datum_slice(&[0], &schema).unwrap();
	assert_eq!(value, Value::Boolean(false));
	assert_eq!(value.as_bool(), Some(false));
}

#[test]
fn test_int() {
	let schema: Schema = r#""int""#.parse().unwrap();

	let value: Value = from_datum_slice(&[4], &schema).unwrap();
	assert_eq!(value, Value::Int(2));
	assert_eq!(value.as_int(), Some(2));

	let value: Value = from_datum_slice(&[3], &schema).unwrap();
	assert_eq!(value, Value::Int(-2));
}

#[test]
fn test_long() {
	let schema: Schema = r#""long""#.parse().unwrap();

	let value: Value = from_datum_slice(&[200, 1], &schema).unwrap();
	assert_eq!(value, Value::Long(100));
	assert_eq!(value.as_long(), Some(100));
}

#[test]
fn test_float() {
	let schema: Schema = r#""float""#.parse().unwrap();
	let data = 3.14f32.to_le_bytes();

	let value: Value = from_datum_slice(&data, &schema).unwrap();
	assert_eq!(value, Value::Float(3.14));
	assert!((value.as_float().unwrap() - 3.14).abs() < 0.001);
}

#[test]
fn test_double() {
	let schema: Schema = r#""double""#.parse().unwrap();
	let data = 3.14159265359f64.to_le_bytes();

	let value: Value = from_datum_slice(&data, &schema).unwrap();
	assert_eq!(value, Value::Double(3.14159265359));
	assert!((value.as_double().unwrap() - 3.14159265359).abs() < 0.0001);
}

#[test]
fn test_string() {
	let schema: Schema = r#""string""#.parse().unwrap();
	let data = &[10, b'h', b'e', b'l', b'l', b'o'];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::String(Cow::Borrowed("hello")));
	assert_eq!(value.as_str(), Some("hello"));
}

#[test]
fn test_string_zero_copy() {
	let schema: Schema = r#""string""#.parse().unwrap();
	let data: &[u8] = &[10, b'h', b'e', b'l', b'l', b'o'];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	if let Value::String(Cow::Borrowed(s)) = &value {
		// Verify it's actually borrowing from the input
		let s_ptr = s.as_ptr();
		let data_ptr = data[1..].as_ptr();
		assert_eq!(s_ptr, data_ptr);
	} else {
		panic!("Expected borrowed string");
	}
}

#[test]
fn test_bytes() {
	let schema: Schema = r#""bytes""#.parse().unwrap();
	let data = &[6, 1, 2, 3];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::Bytes(Cow::Borrowed(&[1, 2, 3])));
	assert_eq!(value.as_bytes(), Some(&[1u8, 2, 3][..]));
}

#[test]
fn test_array() {
	let schema: Schema = r#"{"type": "array", "items": "int"}"#.parse().unwrap();
	// Block with 3 elements: [2, 4, 6] (zigzag encoded as [4, 8, 12])
	let data = &[6, 4, 8, 12, 0];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let expected = Value::Array(vec![Value::Int(2), Value::Int(4), Value::Int(6)]);
	assert_eq!(value, expected);

	let arr = value.as_array().unwrap();
	assert_eq!(arr.len(), 3);
	assert_eq!(arr[0], Value::Int(2));
}

#[test]
fn test_record() {
	let schema: Schema = r#"{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}"#
	.parse()
	.unwrap();
	// id=2 (zigzag: 4), name="John" (len=4, "John")
	let data = &[4, 8, b'J', b'o', b'h', b'n'];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let record = value.as_record().unwrap();

	assert_eq!(record.len(), 2);
	assert_eq!(record.get("id"), Some(&Value::Int(2)));
	assert_eq!(record.get("name"), Some(&Value::String(Cow::Borrowed("John"))));
	assert_eq!(record.get("unknown"), None);
}

#[test]
fn test_record_field_order() {
	let schema: Schema = r#"{
		"type": "record",
		"name": "Test",
		"fields": [
			{"name": "a", "type": "int"},
			{"name": "b", "type": "int"},
			{"name": "c", "type": "int"}
		]
	}"#
	.parse()
	.unwrap();
	let data = &[2, 4, 6];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let record = value.as_record().unwrap();

	// Verify iteration order matches schema order
	let keys: Vec<&str> = record.keys().collect();
	assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn test_enum() {
	let schema: Schema = r#"{
		"type": "enum",
		"name": "Color",
		"symbols": ["RED", "GREEN", "BLUE"]
	}"#
	.parse()
	.unwrap();

	// Discriminant 1 = GREEN
	let value: Value = from_datum_slice(&[2], &schema).unwrap();
	if let Value::String(s) = &value {
		assert_eq!(s.as_ref(), "GREEN");
	} else {
		panic!("Expected enum to deserialize as string, got {:?}", value);
	}
}

#[test]
fn test_fixed() {
	let schema: Schema = r#"{"type": "fixed", "name": "FourBytes", "size": 4}"#
		.parse()
		.unwrap();
	let data = &[1, 2, 3, 4];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::Bytes(Cow::Borrowed(&[1, 2, 3, 4])));
}

#[test]
fn test_union_null() {
	let schema: Schema = r#"["null", "string"]"#.parse().unwrap();
	let data = &[0]; // discriminant 0 = null

	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::Null);
}

#[test]
fn test_union_string() {
	let schema: Schema = r#"["null", "string"]"#.parse().unwrap();
	// discriminant 1 = string, then "hi"
	let data = &[2, 4, b'h', b'i'];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	assert_eq!(value, Value::String(Cow::Borrowed("hi")));
}

#[test]
fn test_map() {
	let schema: Schema = r#"{"type": "map", "values": "int"}"#.parse().unwrap();
	// Block with 2 entries, then end
	// Entry: key="a" (len=1), value=10 (zigzag: 20)
	// Entry: key="b" (len=1), value=20 (zigzag: 40)
	let data = &[4, 2, b'a', 20, 2, b'b', 40, 0];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let record = value.as_record().unwrap();

	assert_eq!(record.get("a"), Some(&Value::Int(10)));
	assert_eq!(record.get("b"), Some(&Value::Int(20)));
}

#[test]
fn test_nested_record() {
	let schema: Schema = r#"{
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
	}"#
	.parse()
	.unwrap();
	let data = &[42]; // inner.value = 21 (zigzag: 42)

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let outer = value.as_record().unwrap();
	let inner = outer.get("inner").unwrap().as_record().unwrap();
	assert_eq!(inner.get("value"), Some(&Value::Int(21)));
}

#[test]
fn test_value_into_owned() {
	let schema: Schema = r#""string""#.parse().unwrap();
	let data = &[10, b'h', b'e', b'l', b'l', b'o'];

	let value: Value = from_datum_slice(data, &schema).unwrap();

	// Original borrows from data
	if let Value::String(Cow::Borrowed(_)) = &value {
		// good
	} else {
		panic!("Expected borrowed string");
	}

	// After into_owned, it should be owned
	let owned = value.into_owned();
	if let Value::String(Cow::Owned(s)) = owned {
		assert_eq!(s, "hello");
	} else {
		panic!("Expected owned string");
	}
}

#[test]
fn test_generic_record_operations() {
	let mut record = GenericRecord::new();
	assert!(record.is_empty());

	record.put("a", Value::Int(1));
	record.put("b", Value::Int(2));
	record.put("c", Value::Int(3));

	assert_eq!(record.len(), 3);
	assert!(!record.is_empty());
	assert!(record.contains("a"));
	assert!(!record.contains("d"));

	// Update existing field
	let old = record.put("a", Value::Int(10));
	assert_eq!(old, Some(Value::Int(1)));
	assert_eq!(record.get("a"), Some(&Value::Int(10)));

	// Iteration
	let values: Vec<i32> = record
		.values()
		.map(|v| v.as_int().unwrap())
		.collect();
	assert_eq!(values, vec![10, 2, 3]);
}

#[test]
fn test_value_round_trip_primitives() {
	// Test that Value can be serialized back to Avro
	let schema: Schema = r#""int""#.parse().unwrap();
	let config = &mut SerializerConfig::new(&schema);

	let original: Value = Value::Int(42);
	let encoded = to_datum_vec(&original, config).unwrap();
	let decoded: Value = from_datum_slice(&encoded, &schema).unwrap();
	assert_eq!(decoded, Value::Int(42));
}

#[test]
fn test_value_round_trip_string() {
	let schema: Schema = r#""string""#.parse().unwrap();
	let config = &mut SerializerConfig::new(&schema);

	let original = Value::String(Cow::Borrowed("hello"));
	let encoded = to_datum_vec(&original, config).unwrap();
	let decoded: Value = from_datum_slice(&encoded, &schema).unwrap();
	assert_eq!(decoded, Value::String(Cow::Borrowed("hello")));
}

#[test]
fn test_value_round_trip_record() {
	let schema: Schema = r#"{
		"type": "record",
		"name": "Test",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}"#
	.parse()
	.unwrap();
	let config = &mut SerializerConfig::new(&schema);

	let mut record = GenericRecord::new();
	record.put("id", Value::Int(42));
	record.put("name", Value::String(Cow::Borrowed("test")));
	let original = Value::Record(record);

	let encoded = to_datum_vec(&original, config).unwrap();
	let decoded: Value = from_datum_slice(&encoded, &schema).unwrap();

	let decoded_record = decoded.as_record().unwrap();
	assert_eq!(decoded_record.get("id"), Some(&Value::Int(42)));
	assert_eq!(
		decoded_record.get("name"),
		Some(&Value::String(Cow::Borrowed("test")))
	);
}

#[test]
fn test_date_logical_type() {
	let schema: Schema = r#"{"type": "int", "logicalType": "date"}"#
		.parse()
		.unwrap();
	// Serialize a known value and then deserialize it
	let config = &mut SerializerConfig::new(&schema);
	let encoded = to_datum_vec(&19000i32, config).unwrap();
	let value: Value = from_datum_slice(&encoded, &schema).unwrap();
	// Date is deserialized as int through serde
	assert_eq!(value, Value::Int(19000));
}

#[test]
fn test_timestamp_millis_logical_type() {
	let schema: Schema = r#"{"type": "long", "logicalType": "timestamp-millis"}"#
		.parse()
		.unwrap();
	let value: Value = from_datum_slice(&[200, 1], &schema).unwrap();
	assert_eq!(value, Value::Long(100));
}

#[test]
fn test_uuid_logical_type() {
	let schema: Schema = r#"{"type": "string", "logicalType": "uuid"}"#
		.parse()
		.unwrap();
	let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
	let mut data = vec![(uuid_str.len() * 2) as u8];
	data.extend_from_slice(uuid_str.as_bytes());

	let value: Value = from_datum_slice(&data, &schema).unwrap();
	assert_eq!(value.as_str(), Some(uuid_str));
}

#[test]
fn test_complex_nested_structure() {
	let schema: Schema = r#"{
		"type": "record",
		"name": "Document",
		"fields": [
			{"name": "id", "type": "string"},
			{
				"name": "tags",
				"type": {"type": "array", "items": "string"}
			},
			{
				"name": "metadata",
				"type": {"type": "map", "values": "int"}
			}
		]
	}"#
	.parse()
	.unwrap();

	// Build the data manually:
	// id = "doc1" (len=4)
	// tags = ["a", "b"] (block of 2, then 0)
	// metadata = {"x": 1, "y": 2} (block of 2, then 0)
	let data = &[
		8, b'd', b'o', b'c', b'1', // id = "doc1"
		4, 2, b'a', 2, b'b', 0, // tags = ["a", "b"]
		4, 2, b'x', 2, 2, b'y', 4, 0, // metadata = {"x": 1, "y": 2}
	];

	let value: Value = from_datum_slice(data, &schema).unwrap();
	let record = value.as_record().unwrap();

	assert_eq!(record.get("id"), Some(&Value::String(Cow::Borrowed("doc1"))));

	let tags = record.get("tags").unwrap().as_array().unwrap();
	assert_eq!(tags.len(), 2);
	assert_eq!(tags[0].as_str(), Some("a"));
	assert_eq!(tags[1].as_str(), Some("b"));

	let metadata = record.get("metadata").unwrap().as_record().unwrap();
	assert_eq!(metadata.get("x"), Some(&Value::Int(1)));
	assert_eq!(metadata.get("y"), Some(&Value::Int(2)));
}

#[test]
fn test_value_equality() {
	let mut r1 = GenericRecord::new();
	r1.put("a", Value::Int(1));
	r1.put("b", Value::Int(2));

	let mut r2 = GenericRecord::new();
	r2.put("a", Value::Int(1));
	r2.put("b", Value::Int(2));

	assert_eq!(Value::Record(r1.clone()), Value::Record(r2.clone()));

	// Different order results in different records
	let mut r3 = GenericRecord::new();
	r3.put("b", Value::Int(2));
	r3.put("a", Value::Int(1));

	assert_ne!(Value::Record(r1), Value::Record(r3));
}

#[test]
fn test_value_clone() {
	let mut record = GenericRecord::new();
	record.put("x", Value::String(Cow::Borrowed("test")));

	let value = Value::Record(record);
	let cloned = value.clone();

	assert_eq!(value, cloned);
}

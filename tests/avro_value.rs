//! Tests for the BumpValue type - dynamic Avro value representation

use turbine::avro::{
	value::{from_datum_slice_bump, BumpValue},
	Schema,
};

#[test]
fn test_null() {
	let schema: Schema = r#""null""#.parse().unwrap();
	let data = &[];
	let bump = bumpalo::Bump::new();
	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Null));
}

#[test]
fn test_boolean() {
	let schema: Schema = r#""boolean""#.parse().unwrap();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&[1], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Boolean(true)));

	let value = from_datum_slice_bump(&[0], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Boolean(false)));
}

#[test]
fn test_int() {
	let schema: Schema = r#""int""#.parse().unwrap();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&[4], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Int(2)));

	let value = from_datum_slice_bump(&[3], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Int(-2)));
}

#[test]
fn test_long() {
	let schema: Schema = r#""long""#.parse().unwrap();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&[200, 1], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Long(100)));
}

#[test]
fn test_float() {
	let schema: Schema = r#""float""#.parse().unwrap();
	let data = 3.14f32.to_le_bytes();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&data, &schema, &bump).unwrap();
	if let BumpValue::Float(f) = value {
		assert!((f - 3.14).abs() < 0.001);
	} else {
		panic!("Expected Float");
	}
}

#[test]
fn test_double() {
	let schema: Schema = r#""double""#.parse().unwrap();
	let data = 3.14159265359f64.to_le_bytes();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&data, &schema, &bump).unwrap();
	if let BumpValue::Double(d) = value {
		assert!((d - 3.14159265359).abs() < 0.0001);
	} else {
		panic!("Expected Double");
	}
}

#[test]
fn test_string() {
	let schema: Schema = r#""string""#.parse().unwrap();
	let data = &[10, b'h', b'e', b'l', b'l', b'o'];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::String("hello")));
}

#[test]
fn test_bytes() {
	let schema: Schema = r#""bytes""#.parse().unwrap();
	let data = &[6, 1, 2, 3];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Bytes(b) = value {
		assert_eq!(b, &[1, 2, 3]);
	} else {
		panic!("Expected Bytes");
	}
}

#[test]
fn test_array() {
	let schema: Schema = r#"{"type": "array", "items": "int"}"#.parse().unwrap();
	// Block with 3 elements: [2, 4, 6] (zigzag encoded as [4, 8, 12])
	let data = &[6, 4, 8, 12, 0];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Array(arr) = value {
		assert_eq!(arr.len(), 3);
		assert!(matches!(arr[0], BumpValue::Int(2)));
		assert!(matches!(arr[1], BumpValue::Int(4)));
		assert!(matches!(arr[2], BumpValue::Int(6)));
	} else {
		panic!("Expected Array");
	}
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
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Record(fields) = value {
		assert_eq!(fields.len(), 2);
		assert!(matches!(fields[0], BumpValue::Int(2)));
		assert!(matches!(fields[1], BumpValue::String("John")));
	} else {
		panic!("Expected Record");
	}
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
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Record(fields) = value {
		assert!(matches!(fields[0], BumpValue::Int(1)));
		assert!(matches!(fields[1], BumpValue::Int(2)));
		assert!(matches!(fields[2], BumpValue::Int(3)));
	} else {
		panic!("Expected Record");
	}
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
	let bump = bumpalo::Bump::new();

	// Discriminant 1 = GREEN
	let value = from_datum_slice_bump(&[2], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::String("GREEN")));
}

#[test]
fn test_fixed() {
	let schema: Schema = r#"{"type": "fixed", "name": "FourBytes", "size": 4}"#
		.parse()
		.unwrap();
	let data = &[1, 2, 3, 4];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Bytes(b) = value {
		assert_eq!(b, &[1, 2, 3, 4]);
	} else {
		panic!("Expected Bytes for fixed");
	}
}

#[test]
fn test_union_null() {
	let schema: Schema = r#"["null", "string"]"#.parse().unwrap();
	let data = &[0]; // discriminant 0 = null
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Null));
}

#[test]
fn test_union_string() {
	let schema: Schema = r#"["null", "string"]"#.parse().unwrap();
	// discriminant 1 = string, then "hi"
	let data = &[2, 4, b'h', b'i'];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::String("hi")));
}

#[test]
fn test_map() {
	let schema: Schema = r#"{"type": "map", "values": "int"}"#.parse().unwrap();
	// Block with 2 entries, then end
	// Entry: key="a" (len=1), value=10 (zigzag: 20)
	// Entry: key="b" (len=1), value=20 (zigzag: 40)
	let data = &[4, 2, b'a', 20, 2, b'b', 40, 0];
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Map(entries) = value {
		assert_eq!(entries.len(), 2);
		// Map entries are (key, value) tuples
		let mut found_a = false;
		let mut found_b = false;
		for (key, val) in entries {
			match *key {
				"a" => {
					assert!(matches!(val, BumpValue::Int(10)));
					found_a = true;
				}
				"b" => {
					assert!(matches!(val, BumpValue::Int(20)));
					found_b = true;
				}
				_ => panic!("Unexpected key"),
			}
		}
		assert!(found_a && found_b);
	} else {
		panic!("Expected Map");
	}
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
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Record(outer_fields) = value {
		if let BumpValue::Record(inner_fields) = &outer_fields[0] {
			assert!(matches!(inner_fields[0], BumpValue::Int(21)));
		} else {
			panic!("Expected inner Record");
		}
	} else {
		panic!("Expected outer Record");
	}
}

#[test]
fn test_date_logical_type() {
	let schema: Schema = r#"{"type": "int", "logicalType": "date"}"#
		.parse()
		.unwrap();
	// zigzag encode 19000 -> 38000
	// varint: 38000 & 0x7F = 0x70, (38000 >> 7) & 0x7F = 0x28, 38000 >> 14 = 2
	let data = &[0xf0, 0xa8, 0x02]; // varint for 38000
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Int(19000)));
}

#[test]
fn test_timestamp_millis_logical_type() {
	let schema: Schema = r#"{"type": "long", "logicalType": "timestamp-millis"}"#
		.parse()
		.unwrap();
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&[200, 1], &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::Long(100)));
}

#[test]
fn test_uuid_logical_type() {
	let schema: Schema = r#"{"type": "string", "logicalType": "uuid"}"#
		.parse()
		.unwrap();
	let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
	let mut data = vec![(uuid_str.len() * 2) as u8];
	data.extend_from_slice(uuid_str.as_bytes());
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(&data, &schema, &bump).unwrap();
	assert!(matches!(value, BumpValue::String(s) if s == uuid_str));
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
	let bump = bumpalo::Bump::new();

	let value = from_datum_slice_bump(data, &schema, &bump).unwrap();
	if let BumpValue::Record(fields) = value {
		assert!(matches!(fields[0], BumpValue::String("doc1")));

		if let BumpValue::Array(tags) = &fields[1] {
			assert_eq!(tags.len(), 2);
			assert!(matches!(tags[0], BumpValue::String("a")));
			assert!(matches!(tags[1], BumpValue::String("b")));
		} else {
			panic!("Expected Array for tags");
		}

		if let BumpValue::Map(metadata) = &fields[2] {
			assert_eq!(metadata.len(), 2);
		} else {
			panic!("Expected Map for metadata");
		}
	} else {
		panic!("Expected Record");
	}
}

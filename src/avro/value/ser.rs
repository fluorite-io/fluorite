//! Serialization implementation for Value.
//!
//! Provides serde `Serialize` implementation for round-trip support.

use super::{GenericRecord, Value};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

impl Serialize for Value<'_> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			Value::Null => serializer.serialize_unit(),
			Value::Boolean(b) => serializer.serialize_bool(*b),
			Value::Int(i) => serializer.serialize_i32(*i),
			Value::Long(l) => serializer.serialize_i64(*l),
			Value::Float(f) => serializer.serialize_f32(*f),
			Value::Double(d) => serializer.serialize_f64(*d),
			Value::Bytes(b) => serializer.serialize_bytes(b),
			Value::String(s) => serializer.serialize_str(s),
			Value::Array(arr) => {
				let mut seq = serializer.serialize_seq(Some(arr.len()))?;
				for item in arr {
					seq.serialize_element(item)?;
				}
				seq.end()
			}
			Value::Map(map) => {
				let mut ser_map = serializer.serialize_map(Some(map.len()))?;
				for (key, value) in map {
					ser_map.serialize_entry(key.as_ref(), value)?;
				}
				ser_map.end()
			}
			Value::Record(record) => record.serialize(serializer),
			Value::Enum { symbol, .. } => {
				// Serialize enum as its string symbol
				serializer.serialize_str(symbol)
			}
			Value::Union { value, .. } => {
				// Serialize the inner value directly
				value.serialize(serializer)
			}
			Value::Fixed { data, .. } => {
				// Serialize fixed as bytes
				serializer.serialize_bytes(data)
			}
			Value::Decimal(d) => Serialize::serialize(d, serializer),
			Value::Uuid(u) => serializer.serialize_str(u),
			Value::Date(d) => serializer.serialize_i32(*d),
			Value::TimeMillis(t) => serializer.serialize_i32(*t),
			Value::TimeMicros(t) => serializer.serialize_i64(*t),
			Value::TimestampMillis(t) => serializer.serialize_i64(*t),
			Value::TimestampMicros(t) => serializer.serialize_i64(*t),
			Value::Duration {
				months,
				days,
				milliseconds,
			} => {
				// Serialize duration as a struct with three fields
				use serde::ser::SerializeStruct;
				let mut s = serializer.serialize_struct("Duration", 3)?;
				s.serialize_field("months", months)?;
				s.serialize_field("days", days)?;
				s.serialize_field("milliseconds", milliseconds)?;
				s.end()
			}
		}
	}
}

impl Serialize for GenericRecord<'_> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut map = serializer.serialize_map(Some(self.len()))?;
		for (key, value) in self.iter() {
			map.serialize_entry(key, value)?;
		}
		map.end()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::borrow::Cow;

	#[test]
	fn test_serialize_primitives() {
		// Just verify it compiles and doesn't panic
		let _ = serde_json::to_string(&Value::Null).unwrap();
		let _ = serde_json::to_string(&Value::Boolean(true)).unwrap();
		let _ = serde_json::to_string(&Value::Int(42)).unwrap();
		let _ = serde_json::to_string(&Value::Long(42)).unwrap();
		let _ = serde_json::to_string(&Value::Float(3.14)).unwrap();
		let _ = serde_json::to_string(&Value::Double(3.14)).unwrap();
		let _ = serde_json::to_string(&Value::String(Cow::Borrowed("hello"))).unwrap();
	}

	#[test]
	fn test_serialize_record() {
		let mut record = GenericRecord::new();
		record.put("name", Value::String(Cow::Borrowed("Alice")));
		record.put("age", Value::Int(30));

		let json = serde_json::to_string(&Value::Record(record)).unwrap();
		assert!(json.contains("\"name\""));
		assert!(json.contains("\"Alice\""));
		assert!(json.contains("\"age\""));
		assert!(json.contains("30"));
	}
}

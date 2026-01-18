//! Fast Avro serialization/deserialization with zero-copy support.
//!
//! ## Features
//!
//! - **Zero-copy deserialization**: Strings and bytes borrow directly from input
//! - **Dynamic Value type**: `Value` enum for schema-agnostic data handling
//! - **O(1) record field lookup**: GenericRecord uses HashMap for fast access

pub mod de;
pub mod schema;
pub mod ser;
pub mod value;

pub use schema::Schema;
pub use value::{GenericRecord, Value};

/// Deserialize from an Avro datum slice.
///
/// This is zero-copy - your structure may contain `&'a str`s that point
/// directly into this slice.
pub fn from_datum_slice<'a, T>(slice: &'a [u8], schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::Deserialize<'a>,
{
	serde::Deserialize::deserialize(de::DeserializerState::from_slice(slice, schema).deserializer())
}

/// Deserialize from an Avro datum reader.
///
/// If you only have an `impl Read`, wrap it in a `BufReader` first.
/// Prefer `from_datum_slice` when deserializing from a slice for better performance.
pub fn from_datum_reader<R, T>(reader: R, schema: &Schema) -> Result<T, de::DeError>
where
	T: serde::de::DeserializeOwned,
	R: std::io::BufRead,
{
	serde::Deserialize::deserialize(
		de::DeserializerState::from_reader(reader, schema).deserializer(),
	)
}

/// Serialize an Avro datum to the provided writer.
pub fn to_datum<T, W>(
	value: &T,
	writer: W,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<W, ser::SerError>
where
	T: serde::Serialize + ?Sized,
	W: std::io::Write,
{
	let mut serializer_state = ser::SerializerState::from_writer(writer, serializer_config);
	serde::Serialize::serialize(value, serializer_state.serializer())?;
	Ok(serializer_state.into_writer())
}

/// Serialize an Avro datum to a newly allocated Vec.
///
/// For better performance when reusing buffers, use `to_datum` instead.
pub fn to_datum_vec<T>(
	value: &T,
	serializer_config: &mut ser::SerializerConfig<'_>,
) -> Result<Vec<u8>, ser::SerError>
where
	T: serde::Serialize + ?Sized,
{
	to_datum(value, Vec::new(), serializer_config)
}

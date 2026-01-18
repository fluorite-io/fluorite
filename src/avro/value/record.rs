//! GenericRecord implementation for Avro records with O(1) field lookup.

use super::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// A generic Avro record with named fields and O(1) field lookup.
///
/// This struct stores fields in insertion order while maintaining a HashMap
/// index for fast field access by name. This provides both ordered iteration
/// and efficient lookups.
///
/// # Example
///
/// ```
/// use turbine::avro::value::{GenericRecord, Value};
/// use std::borrow::Cow;
///
/// let mut record = GenericRecord::new();
/// record.put("name", Value::String(Cow::Borrowed("Alice")));
/// record.put("age", Value::Int(30));
///
/// assert_eq!(record.get("name"), Some(&Value::String(Cow::Borrowed("Alice"))));
/// assert_eq!(record.get("age"), Some(&Value::Int(30)));
/// assert_eq!(record.get("unknown"), None);
/// ```
#[derive(Debug, Clone)]
pub struct GenericRecord<'a> {
	/// Fields stored in insertion order as (name, value) pairs
	fields: Vec<(Cow<'a, str>, Value<'a>)>,
	/// Index mapping field names to their position in the fields vector
	index: HashMap<Cow<'a, str>, usize>,
}

impl<'a> GenericRecord<'a> {
	/// Create a new empty record.
	pub fn new() -> Self {
		Self {
			fields: Vec::new(),
			index: HashMap::new(),
		}
	}

	/// Create a new record with the specified capacity.
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			fields: Vec::with_capacity(capacity),
			index: HashMap::with_capacity(capacity),
		}
	}

	/// Get a reference to a field value by name.
	///
	/// Returns `None` if the field does not exist.
	///
	/// This is an O(1) operation.
	pub fn get(&self, name: &str) -> Option<&Value<'a>> {
		self.index.get(name).map(|&idx| &self.fields[idx].1)
	}

	/// Get a mutable reference to a field value by name.
	///
	/// Returns `None` if the field does not exist.
	///
	/// This is an O(1) operation.
	pub fn get_mut(&mut self, name: &str) -> Option<&mut Value<'a>> {
		self.index.get(name).map(|&idx| &mut self.fields[idx].1)
	}

	/// Insert or update a field.
	///
	/// If a field with the same name already exists, its value is updated
	/// and the old value is returned. Otherwise, the field is appended
	/// and `None` is returned.
	pub fn put(&mut self, name: impl Into<Cow<'a, str>>, value: Value<'a>) -> Option<Value<'a>> {
		let name = name.into();
		if let Some(&idx) = self.index.get(&name) {
			Some(std::mem::replace(&mut self.fields[idx].1, value))
		} else {
			let idx = self.fields.len();
			self.index.insert(name.clone(), idx);
			self.fields.push((name, value));
			None
		}
	}

	/// Returns the number of fields in the record.
	pub fn len(&self) -> usize {
		self.fields.len()
	}

	/// Returns `true` if the record has no fields.
	pub fn is_empty(&self) -> bool {
		self.fields.is_empty()
	}

	/// Returns `true` if the record contains a field with the given name.
	pub fn contains(&self, name: &str) -> bool {
		self.index.contains_key(name)
	}

	/// Returns an iterator over the fields in insertion order.
	pub fn iter(&self) -> impl Iterator<Item = (&str, &Value<'a>)> {
		self.fields.iter().map(|(k, v)| (k.as_ref(), v))
	}

	/// Returns an iterator over the field names in insertion order.
	pub fn keys(&self) -> impl Iterator<Item = &str> {
		self.fields.iter().map(|(k, _)| k.as_ref())
	}

	/// Returns an iterator over the field values in insertion order.
	pub fn values(&self) -> impl Iterator<Item = &Value<'a>> {
		self.fields.iter().map(|(_, v)| v)
	}

	/// Convert this record into an owned version with `'static` lifetime.
	pub fn into_owned(self) -> GenericRecord<'static> {
		GenericRecord {
			fields: self
				.fields
				.into_iter()
				.map(|(k, v)| (Cow::Owned(k.into_owned()), v.into_owned()))
				.collect(),
			index: self
				.index
				.into_iter()
				.map(|(k, v)| (Cow::Owned(k.into_owned()), v))
				.collect(),
		}
	}
}

impl Default for GenericRecord<'_> {
	fn default() -> Self {
		Self::new()
	}
}

impl<'a> PartialEq for GenericRecord<'a> {
	fn eq(&self, other: &Self) -> bool {
		if self.fields.len() != other.fields.len() {
			return false;
		}
		// Compare by field order and values
		self.fields == other.fields
	}
}

impl<'a> IntoIterator for GenericRecord<'a> {
	type Item = (Cow<'a, str>, Value<'a>);
	type IntoIter = std::vec::IntoIter<Self::Item>;

	fn into_iter(self) -> Self::IntoIter {
		self.fields.into_iter()
	}
}

impl<'a, 'b> IntoIterator for &'b GenericRecord<'a> {
	type Item = &'b (Cow<'a, str>, Value<'a>);
	type IntoIter = std::slice::Iter<'b, (Cow<'a, str>, Value<'a>)>;

	fn into_iter(self) -> Self::IntoIter {
		self.fields.iter()
	}
}

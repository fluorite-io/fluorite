//! Schema compatibility checking.
//!
//! Implements BACKWARD compatibility: new schema must be able to read
//! data written with the old schema.
//!
//! Rules are the intersection of Avro backward-compatibility and Iceberg
//! schema evolution constraints.

use apache_avro::Schema;
use serde_json::Value;

use crate::SchemaError;

/// Check if a new schema is backward compatible with an old schema.
///
/// BACKWARD compatibility means: new schema can read data written with old schema.
///
/// Compatible changes:
/// - Add field with default
/// - Add nullable field (["null", T]) with default null
/// - Remove field
/// - Widen int → long
/// - Widen float → double
///
/// Incompatible changes:
/// - Add non-nullable field without default
/// - Change field type (e.g. long → string)
/// - Rename field
/// - Narrow numeric (long → int)
/// - Widen int → float (Avro OK, Iceberg NO)
/// - Add union member beyond ["null", T]
pub fn is_backward_compatible(new_schema: &Value, old_schema: &Value) -> Result<bool, SchemaError> {
    // First check Iceberg-specific constraints on the new schema
    if let Err(msg) = check_iceberg_constraints(new_schema) {
        return Err(SchemaError::IncompatibleSchema { message: msg });
    }

    // Parse both schemas
    let new_parsed = Schema::parse(new_schema).map_err(|e| SchemaError::InvalidSchema {
        message: format!("new schema: {}", e),
    })?;

    let old_parsed = Schema::parse(old_schema).map_err(|e| SchemaError::InvalidSchema {
        message: format!("old schema: {}", e),
    })?;

    // Check if new can read old using Avro's resolution rules
    // We do this by checking if the schemas are compatible for reading
    Ok(check_read_compatibility(&new_parsed, &old_parsed))
}

/// Check Iceberg-specific constraints on a schema.
fn check_iceberg_constraints(schema: &Value) -> Result<(), String> {
    check_no_complex_unions(schema)?;
    Ok(())
}

/// Check that unions are only ["null", T] (Iceberg doesn't support complex unions).
fn check_no_complex_unions(value: &Value) -> Result<(), String> {
    match value {
        Value::Array(arr) => {
            // This might be a union type
            if arr.len() > 2 {
                // Check if it's a union (array of type definitions)
                let is_union = arr.iter().all(|v| {
                    matches!(v, Value::String(_) | Value::Object(_))
                });
                if is_union {
                    return Err(format!(
                        "unions with more than 2 types are not supported (Iceberg constraint): {:?}",
                        arr
                    ));
                }
            }
            // Recurse into array elements
            for item in arr {
                check_no_complex_unions(item)?;
            }
        }
        Value::Object(map) => {
            // Check the "type" field if it's a union
            if let Some(Value::Array(arr)) = map.get("type") {
                if arr.len() > 2 {
                    let is_union = arr.iter().all(|v| {
                        matches!(v, Value::String(_) | Value::Object(_))
                    });
                    if is_union {
                        return Err(format!(
                            "unions with more than 2 types are not supported: {:?}",
                            arr
                        ));
                    }
                }
            }
            // Recurse into object values
            for v in map.values() {
                check_no_complex_unions(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Check if reader schema can read data written with writer schema.
fn check_read_compatibility(reader: &Schema, writer: &Schema) -> bool {
    // Use Apache Avro's built-in compatibility check
    // The ResolvedSchema mechanism checks if reader can read writer's data
    match (reader, writer) {
        (Schema::Record(r_rec), Schema::Record(w_rec)) => {
            // For records, check field-by-field compatibility
            check_record_compatibility(r_rec, w_rec)
        }
        // For primitive types, check promotion rules
        (Schema::Long, Schema::Int) => true,   // int → long is OK
        (Schema::Double, Schema::Float) => true, // float → double is OK
        (Schema::Double, Schema::Int) => false,  // int → double NOT OK (Iceberg)
        (Schema::Double, Schema::Long) => false, // long → double NOT OK (Iceberg)
        (Schema::Float, Schema::Int) => false,   // int → float NOT OK (Iceberg)
        (Schema::Float, Schema::Long) => false,  // long → float NOT OK (Iceberg)
        // Same types are compatible
        _ if reader == writer => true,
        // Union handling
        (Schema::Union(r_union), Schema::Union(w_union)) => {
            // Each writer variant must be readable by some reader variant
            w_union.variants().iter().all(|w_var| {
                r_union.variants().iter().any(|r_var| {
                    check_read_compatibility(r_var, w_var)
                })
            })
        }
        (Schema::Union(r_union), writer) => {
            // Writer is not a union, reader is - writer type must match a variant
            r_union.variants().iter().any(|r_var| {
                check_read_compatibility(r_var, writer)
            })
        }
        (reader, Schema::Union(w_union)) => {
            // Writer is a union, reader is not - all writer variants must be readable
            w_union.variants().iter().all(|w_var| {
                w_var == &Schema::Null || check_read_compatibility(reader, w_var)
            })
        }
        // Arrays
        (Schema::Array(r_arr), Schema::Array(w_arr)) => {
            check_read_compatibility(&r_arr.items, &w_arr.items)
        }
        // Maps
        (Schema::Map(r_map), Schema::Map(w_map)) => {
            check_read_compatibility(&r_map.types, &w_map.types)
        }
        // Enums - reader must have all writer symbols
        (Schema::Enum(r_enum), Schema::Enum(w_enum)) => {
            w_enum.symbols.iter().all(|s| r_enum.symbols.contains(s))
        }
        // Everything else is incompatible
        _ => false,
    }
}

/// Check record field-by-field compatibility.
fn check_record_compatibility(
    reader: &apache_avro::schema::RecordSchema,
    writer: &apache_avro::schema::RecordSchema,
) -> bool {
    // Rule 1: Writer fields that exist in reader must have compatible types
    for w_field in &writer.fields {
        if let Some(r_field) = reader.fields.iter().find(|f| f.name == w_field.name) {
            if !check_read_compatibility(&r_field.schema, &w_field.schema) {
                return false;
            }
        }
        // If writer field not in reader, reader ignores it (OK for backward compat)
    }

    // Rule 2: Reader fields not in writer must have defaults
    for r_field in &reader.fields {
        let in_writer = writer.fields.iter().any(|f| f.name == r_field.name);
        if !in_writer && r_field.default.is_none() {
            // Reader has a field without default that writer doesn't have
            // This is only OK if the field is nullable (union with null)
            if !is_nullable_schema(&r_field.schema) {
                return false;
            }
        }
    }

    true
}

/// Check if a schema is nullable (union with null as first or second element).
fn is_nullable_schema(schema: &Schema) -> bool {
    match schema {
        Schema::Union(union) => {
            union.variants().iter().any(|v| matches!(v, Schema::Null))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_same_schema_compatible() {
        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        assert!(is_backward_compatible(&schema, &schema).unwrap());
    }

    #[test]
    fn test_add_field_with_default_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string", "default": "unknown"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_add_nullable_field_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_remove_field_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "deprecated", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_widen_int_to_long_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "int"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "long"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_widen_float_to_double_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "float"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "double"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_add_required_field_without_default_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "required_field", "type": "string"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_change_field_type_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "string"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_narrow_long_to_int_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "int"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_complex_union_rejected() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": ["null", "string", "long"]}
            ]
        });

        let result = is_backward_compatible(&new, &old);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unions"));
    }

    #[test]
    fn test_int_to_float_rejected_iceberg() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "int"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "float"}
            ]
        });

        // Avro allows this, but Iceberg doesn't
        assert!(!is_backward_compatible(&new, &old).unwrap());
    }
}

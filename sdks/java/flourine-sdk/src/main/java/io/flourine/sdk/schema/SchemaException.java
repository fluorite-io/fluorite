package io.flourine.sdk.schema;

/** Unchecked exception for schema generation errors (programming mistakes). */
public class SchemaException extends RuntimeException {
    public SchemaException(String message) {
        super(message);
    }
}

package io.flourine.sdk.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a field as non-nullable, overriding the default nullable behavior. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface NonNull {
}

package io.flourine.sdk.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a class for schema generation via {@link Schemas}. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FlourineSchema {
    String namespace() default "";
    Rename[] renames() default {};
    String[] deletions() default {};
}

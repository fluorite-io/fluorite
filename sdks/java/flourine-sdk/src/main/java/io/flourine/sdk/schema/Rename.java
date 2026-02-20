package io.flourine.sdk.schema;

import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/** A single rename pair: the old field name and the current field name. */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Rename {
    String from();
    String to();
}

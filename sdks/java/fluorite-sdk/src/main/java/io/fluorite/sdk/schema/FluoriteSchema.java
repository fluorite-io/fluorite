// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.fluorite.sdk.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Marks a class for schema generation via {@link Schemas}. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FluoriteSchema {
    String topic() default "";
    String namespace() default "";
    Rename[] renames() default {};
    String[] deletions() default {};
}
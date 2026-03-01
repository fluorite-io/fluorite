// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.fluorite.sdk;

/**
 * Base exception for Fluorite SDK errors.
 */
public class FluoriteException extends Exception {
    public FluoriteException(String message) {
        super(message);
    }

    public FluoriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ConnectionException extends FluoriteException {
        public ConnectionException(String message) {
            super(message);
        }

        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class AuthenticationException extends FluoriteException {
        public AuthenticationException(String message) {
            super(message);
        }
    }

    public static class TimeoutException extends FluoriteException {
        public TimeoutException(String message) {
            super(message);
        }
    }

    public static class BackpressureException extends FluoriteException {
        public BackpressureException(String message) {
            super(message);
        }
    }

    public static class ProtocolException extends FluoriteException {
        public ProtocolException(String message) {
            super(message);
        }

        public ProtocolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
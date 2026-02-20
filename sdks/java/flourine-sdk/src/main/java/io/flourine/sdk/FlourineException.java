package io.flourine.sdk;

/**
 * Base exception for Flourine SDK errors.
 */
public class FlourineException extends Exception {
    public FlourineException(String message) {
        super(message);
    }

    public FlourineException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ConnectionException extends FlourineException {
        public ConnectionException(String message) {
            super(message);
        }

        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class AuthenticationException extends FlourineException {
        public AuthenticationException(String message) {
            super(message);
        }
    }

    public static class TimeoutException extends FlourineException {
        public TimeoutException(String message) {
            super(message);
        }
    }

    public static class BackpressureException extends FlourineException {
        public BackpressureException(String message) {
            super(message);
        }
    }

    public static class ProtocolException extends FlourineException {
        public ProtocolException(String message) {
            super(message);
        }

        public ProtocolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

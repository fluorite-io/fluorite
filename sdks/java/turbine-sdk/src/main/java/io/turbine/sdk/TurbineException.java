package io.turbine.sdk;

/**
 * Base exception for Turbine SDK errors.
 */
public class TurbineException extends Exception {
    public TurbineException(String message) {
        super(message);
    }

    public TurbineException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ConnectionException extends TurbineException {
        public ConnectionException(String message) {
            super(message);
        }

        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class AuthenticationException extends TurbineException {
        public AuthenticationException(String message) {
            super(message);
        }
    }

    public static class TimeoutException extends TurbineException {
        public TimeoutException(String message) {
            super(message);
        }
    }

    public static class BackpressureException extends TurbineException {
        public BackpressureException(String message) {
            super(message);
        }
    }

    public static class ProtocolException extends TurbineException {
        public ProtocolException(String message) {
            super(message);
        }

        public ProtocolException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

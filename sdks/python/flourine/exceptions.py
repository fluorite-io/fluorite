"""Flourine SDK exceptions."""


class FlourineException(Exception):
    """Base exception for Flourine SDK errors."""

    pass


class ConnectionException(FlourineException):
    """Connection error."""

    pass


class AuthenticationException(FlourineException):
    """Authentication failed."""

    pass


class TimeoutException(FlourineException):
    """Request timeout."""

    pass


class BackpressureException(FlourineException):
    """Server backpressure - too many retries."""

    pass


class ProtocolException(FlourineException):
    """Protocol error."""

    pass


class SchemaException(FlourineException):
    """Schema generation or validation error."""

    pass

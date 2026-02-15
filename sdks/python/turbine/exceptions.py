"""Turbine SDK exceptions."""


class TurbineException(Exception):
    """Base exception for Turbine SDK errors."""

    pass


class ConnectionException(TurbineException):
    """Connection error."""

    pass


class AuthenticationException(TurbineException):
    """Authentication failed."""

    pass


class TimeoutException(TurbineException):
    """Request timeout."""

    pass


class BackpressureException(TurbineException):
    """Server backpressure - too many retries."""

    pass


class ProtocolException(TurbineException):
    """Protocol error."""

    pass

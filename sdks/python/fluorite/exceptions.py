# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Fluorite SDK exceptions."""


class FluoriteException(Exception):
    """Base exception for Fluorite SDK errors."""

    pass


class ConnectionException(FluoriteException):
    """Connection error."""

    pass


class AuthenticationException(FluoriteException):
    """Authentication failed."""

    pass


class TimeoutException(FluoriteException):
    """Request timeout."""

    pass


class BackpressureException(FluoriteException):
    """Server backpressure - too many retries."""

    pass


class ProtocolException(FluoriteException):
    """Protocol error."""

    pass


class SchemaException(FluoriteException):
    """Schema generation or validation error."""

    pass
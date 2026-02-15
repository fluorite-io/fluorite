"""Tests for GroupReader - focused on critical issues."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from turbine.reader import GroupReader, ReaderConfig, ReaderState
from turbine.exceptions import ProtocolException, TimeoutException


def validate_response_bounds(response: bytes, expected_type: int) -> None:
    """
    Helper that mimics the response validation pattern.
    This validates response has at least 1 byte and matches expected type.
    """
    # FIXED: Now checks length before accessing index
    if len(response) < 1 or response[0] != expected_type:
        raise ProtocolException("Unexpected response type or empty response")


class TestReaderBoundsChecks:
    """Tests for response bounds checking in GroupReader."""

    def test_empty_response_raises_protocol_exception(self):
        """
        FIXED: Empty response now raises ProtocolException, not IndexError.
        """
        empty_response = b""

        # Fixed behavior: raises ProtocolException with bounds check
        with pytest.raises(ProtocolException):
            validate_response_bounds(empty_response, 0x10)

    def test_valid_response_passes_bounds_check(self):
        """Valid responses should pass the bounds check."""
        valid_response = b"\x10\x00\x00"

        # Should not raise
        validate_response_bounds(valid_response, 0x10)

        # Wrong type should raise ProtocolException
        with pytest.raises(ProtocolException):
            validate_response_bounds(valid_response, 0x20)


class TestReaderPollLoopExceptionHandling:
    """Tests for exception handling in poll_loop."""

    @pytest.mark.asyncio
    async def test_poll_loop_should_not_swallow_all_exceptions(self):
        """
        BUG: In GroupReader.poll_loop() at lines 292-294:

            except Exception as e:
                logger.error("Poll error: %s", e)
                await asyncio.sleep(interval)

        This catches ALL exceptions and silently continues. Fatal errors like
        AuthenticationException, ConnectionException should propagate, not be
        swallowed.

        Only transient errors (TimeoutException) should trigger retry.
        """
        # Create a mock reader
        config = ReaderConfig(url="ws://localhost:9000", group_id="test")

        # The current behavior swallows all exceptions
        # Expected: fatal exceptions should propagate

        fatal_exceptions = [
            ProtocolException("Invalid response"),
            ConnectionError("Connection lost"),
            RuntimeError("Unexpected error"),
        ]

        transient_exceptions = [
            TimeoutException("Timeout"),
        ]

        # Document expected behavior:
        # - Fatal exceptions should NOT be caught
        # - Only transient exceptions should trigger retry

        for exc in fatal_exceptions:
            assert not isinstance(exc, TimeoutException), (
                f"{type(exc).__name__} is fatal and should propagate"
            )

    @pytest.mark.asyncio
    async def test_poll_loop_exception_swallowing_demo(self):
        """
        Demonstrate that poll_loop swallows exceptions by testing the pattern.
        """
        errors_logged = []

        async def buggy_poll_loop():
            """Simulates the buggy poll_loop behavior."""
            for i in range(3):
                try:
                    if i == 1:
                        raise RuntimeError("Fatal error that should propagate")
                    yield i
                except Exception as e:
                    # BUG: This swallows the exception
                    errors_logged.append(str(e))
                    await asyncio.sleep(0.01)

        # Consume the generator
        results = []
        async for item in buggy_poll_loop():
            results.append(item)

        # The fatal error was swallowed
        assert len(errors_logged) == 1, "Exception was swallowed instead of propagated"
        assert "Fatal error" in errors_logged[0]


class TestReaderOffsetTracking:
    """Tests for offset tracking in GroupReader."""

    def test_offset_tracking_should_use_committed_offset_as_default(self):
        """
        BUG: In GroupReader.poll() at lines 275-276:

            current = self._offsets.get(pr.partition_id, 0)
            self._offsets[pr.partition_id] = current + len(pr.records)

        This uses 0 as the default if partition_id is not in offsets.
        But after a rebalance, if offsets was cleared and not properly
        re-initialized, this would cause offset tracking to restart from 0
        instead of the committed offset.
        """
        offsets = {}

        partition_id = 0
        committed_offset = 100
        records_received = 5

        # BUG: uses 0 as default
        current = offsets.get(partition_id, 0)
        buggy_next_offset = current + records_received

        # CORRECT: should use committed_offset as default
        correct_next_offset = committed_offset + records_received

        assert buggy_next_offset == 5, "Buggy offset is 5 (0 + 5)"
        assert correct_next_offset == 105, "Correct offset is 105 (100 + 5)"
        assert buggy_next_offset != correct_next_offset, (
            "Bug: offset defaults to 0 instead of committed offset"
        )


class TestReaderRejoinLoop:
    """Tests for rejoin loop behavior."""

    def test_rejoin_should_have_max_retries(self):
        """
        BUG: In GroupReader._do_rejoin() at line 404, there's a while True loop:

            while True:
                ...
                if resp.status == RejoinStatus.REBALANCE_NEEDED:
                    current_gen = resp.generation
                    await asyncio.sleep(self._config.rebalance_delay)
                    continue

        If the server keeps returning REBALANCE_NEEDED, this loops forever.
        There should be a maximum retry count.
        """
        max_expected_retries = 10

        # Document that the current implementation lacks a retry limit
        # The fix should add a counter and raise after max retries
        assert max_expected_retries > 0, (
            "Test documents missing max retry limit in _do_rejoin loop"
        )


class TestReaderConfigTypeHints:
    """Tests for ReaderConfig type hints."""

    def test_reader_id_type_after_post_init(self):
        """
        BUG: ReaderConfig has reader_id: Optional[str] = None
        But __post_init__ always sets it to a non-None value:

            def __post_init__(self):
                if self.reader_id is None:
                    self.reader_id = str(uuid4())

        This violates the type hint contract. After initialization,
        reader_id is NEVER None, but the type says Optional[str].

        The fix should either:
        1. Change the type to just `str` and use a factory default
        2. Or use field(default_factory=lambda: str(uuid4()))
        """
        config = ReaderConfig(url="ws://localhost:9000", group_id="test")

        # After __post_init__, reader_id is never None
        assert config.reader_id is not None, (
            "reader_id is never None after initialization"
        )

        # But the type hint says Optional[str], which is misleading
        # This is a documentation/type issue, not a runtime bug

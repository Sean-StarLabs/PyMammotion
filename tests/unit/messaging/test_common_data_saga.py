"""Tests for common-data response ownership."""

from unittest.mock import AsyncMock, MagicMock

from pymammotion.messaging.common_data_saga import CommonDataSaga
from pymammotion.messaging.dynamics_line_saga import DynamicsLineSaga
from pymammotion.proto import NavGetCommDataAck


def test_common_data_matches_request_shape() -> None:
    """Only successful responses for the requested action, type and hash match."""
    saga = CommonDataSaga(MagicMock(), AsyncMock(), action=8, type=18, hash_num=123)

    assert saga._matches_frame(NavGetCommDataAck(result=0, action=8, type=18, hash=123))  # noqa: SLF001
    assert not saga._matches_frame(NavGetCommDataAck(result=1, action=8, type=18, hash=123))  # noqa: SLF001
    assert not saga._matches_frame(NavGetCommDataAck(result=0, action=7, type=18, hash=123))  # noqa: SLF001
    assert not saga._matches_frame(NavGetCommDataAck(result=0, action=8, type=17, hash=123))  # noqa: SLF001
    assert not saga._matches_frame(NavGetCommDataAck(result=0, action=8, type=18, hash=456))  # noqa: SLF001


def test_unhashed_common_data_accepts_response_hash() -> None:
    """A request with hash zero does not assume the device echoes zero."""
    saga = CommonDataSaga(MagicMock(), AsyncMock(), action=8, type=18)

    assert saga._matches_frame(NavGetCommDataAck(result=0, action=8, type=18, hash=456))  # noqa: SLF001


def test_dynamics_line_uses_next_poll_instead_of_immediate_retry() -> None:
    """Uncorrelated type-18 reads make only one request per poll saga."""
    assert DynamicsLineSaga.max_attempts == 1

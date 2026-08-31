"""Tests for task ownership of native dynamics-line geometry."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from pymammotion.data.model.device import MowingDevice
from pymammotion.data.model.hash_list import CommDataCouple
from pymammotion.device.dynamics_line_loop import _enqueue_dynamics_line_saga


def _make_handle(path_hash: int) -> tuple[MagicMock, MowingDevice]:
    device = MowingDevice(name="Luba-Test")
    device.report_data.work.path_hash = path_hash
    handle = MagicMock()
    handle.device_name = device.name
    handle.snapshot.raw = device
    handle.commands = MagicMock()
    handle.send_raw = AsyncMock()
    handle.commit_dynamics_line = AsyncMock()
    return handle, device


async def test_dynamics_line_is_committed_for_same_task() -> None:
    """A completed fetch is published with its device-reported task hash."""
    handle, device = _make_handle(123)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.task_path_hash = saga._get_task_path_hash()
        saga.result = [CommDataCouple(x=1, y=2)]
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=1, y=2)], 123)


async def test_dynamics_line_is_discarded_after_task_change() -> None:
    """Frames fetched for an old task cannot replace current route geometry."""
    handle, device = _make_handle(123)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.task_path_hash = saga._get_task_path_hash()
        saga.result = [CommDataCouple(x=1, y=2)]
        saga.transfer_complete = True
        device.report_data.work.path_hash = 456
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=1, y=2)], 123)


async def test_dynamics_line_queued_without_task_exits_at_execution() -> None:
    """Task identity is checked when queued work starts, not when it is enqueued."""
    handle, _device = _make_handle(0)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.task_path_hash = saga._get_task_path_hash()
        saga.result = []
        saga.transfer_complete = False
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.enqueue_saga.assert_awaited_once()
    handle.commit_dynamics_line.assert_not_awaited()


async def test_dynamics_line_captures_task_when_queued_work_starts() -> None:
    """A task switch while queued binds the transfer to the newer task."""
    handle, device = _make_handle(123)

    async def enqueue(saga: object, on_complete: object) -> None:
        device.report_data.work.path_hash = 456
        saga.task_path_hash = saga._get_task_path_hash()
        saga.result = [CommDataCouple(x=3, y=4)]
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=3, y=4)], 456)


async def test_completed_empty_dynamics_line_clears_previous_geometry() -> None:
    """A successful empty transfer is data, not a skipped update."""
    handle, _device = _make_handle(123)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.task_path_hash = saga._get_task_path_hash()
        saga.result = []
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([], 123)

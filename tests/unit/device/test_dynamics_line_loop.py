"""Tests for task ownership of native dynamics-line geometry."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

from pymammotion.data.model.device import MowingDevice
from pymammotion.data.model.hash_list import CommDataCouple
from pymammotion.device.dynamics_line_loop import (
    _DYNAMICS_LINE_CLOUD_POLL_INTERVAL,
    _DYNAMICS_LINE_POLL_INTERVAL,
    _enqueue_dynamics_line_saga,
    dynamics_line_loop,
)
from pymammotion.device.modes import _DeviceMode
from pymammotion.transport.base import TransportType
from pymammotion.utility.constant import WorkMode


def _make_handle(session_id: int) -> tuple[MagicMock, MowingDevice]:
    device = MowingDevice(name="Luba-Test", mow_session_id=session_id)
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    handle = MagicMock()
    handle.device_name = device.name
    handle.snapshot.raw = device
    handle.commands = MagicMock()
    handle.send_raw = AsyncMock()
    handle.commit_dynamics_line = AsyncMock()
    return handle, device


async def test_dynamics_line_is_committed_for_same_session() -> None:
    """A completed fetch is published with its mowing-session identity."""
    handle, _device = _make_handle(3)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.mow_session_id = saga._get_mow_session_id()
        saga.result = [CommDataCouple(x=1, y=2)]
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=1, y=2)], 3)


async def test_dynamics_line_is_discarded_after_session_change() -> None:
    """Frames fetched for an old session cannot replace current trail geometry."""
    handle, device = _make_handle(3)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.mow_session_id = saga._get_mow_session_id()
        saga.result = [CommDataCouple(x=1, y=2)]
        saga.transfer_complete = True
        device.mow_session_id = 4
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=1, y=2)], 3)


async def test_dynamics_line_queued_without_task_exits_at_execution() -> None:
    """Task identity is checked when queued work starts, not when it is enqueued."""
    handle, _device = _make_handle(0)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.mow_session_id = saga._get_mow_session_id()
        saga.result = []
        saga.transfer_complete = False
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.enqueue_saga.assert_awaited_once()
    handle.commit_dynamics_line.assert_not_awaited()


async def test_dynamics_line_captures_session_when_queued_work_starts() -> None:
    """A session switch while queued binds the transfer to the newer session."""
    handle, device = _make_handle(3)

    async def enqueue(saga: object, on_complete: object) -> None:
        device.mow_session_id = 4
        saga.mow_session_id = saga._get_mow_session_id()
        saga.result = [CommDataCouple(x=3, y=4)]
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([CommDataCouple(x=3, y=4)], 4)


async def test_completed_empty_dynamics_line_is_forwarded_to_state_reducer() -> None:
    """The state reducer decides whether an empty transfer changes geometry."""
    handle, _device = _make_handle(123)

    async def enqueue(saga: object, on_complete: object) -> None:
        saga.mow_session_id = saga._get_mow_session_id()
        saga.result = []
        saga.transfer_complete = True
        await on_complete()

    handle.enqueue_saga = AsyncMock(side_effect=enqueue)

    await _enqueue_dynamics_line_saga(handle)

    handle.commit_dynamics_line.assert_awaited_once_with([], 123)


async def test_dynamics_loop_uses_its_own_poll_cadence() -> None:
    """Completing a saga cannot wake the dynamics loop's next poll early."""
    handle, _device = _make_handle(3)
    handle._stopping = False
    handle._transports = {TransportType.BLE: MagicMock(is_connected=True)}
    handle.device_mode.return_value = _DeviceMode.ACTIVE
    handle.queue.is_saga_active = False
    handle._dynamics_line_rearm_event = asyncio.Event()

    async def stop_loop(_handle: object, _seconds: float) -> None:
        handle._stopping = True

    with patch(
        "pymammotion.device.dynamics_line_loop._sleep_until_transport_change",
        new_callable=AsyncMock,
        side_effect=stop_loop,
    ) as sleep:
        await dynamics_line_loop(handle)

    sleep.assert_awaited_once()
    sleep.assert_awaited_once_with(handle, _DYNAMICS_LINE_POLL_INTERVAL)


async def test_dynamics_loop_uses_slower_cloud_cadence() -> None:
    """Cloud-only trail polling preserves the older firmware send budget."""
    handle, _device = _make_handle(3)
    handle.device_name = "Yuka-MLYQ73XB"
    handle.firmware_version = "1.30.25.1"
    handle._stopping = False
    handle._transports = {
        TransportType.BLE: MagicMock(is_connected=False),
        TransportType.CLOUD_ALIYUN: MagicMock(is_connected=True),
    }
    handle.device_mode.return_value = _DeviceMode.ACTIVE
    handle.queue.is_saga_active = False
    handle._dynamics_line_rearm_event = asyncio.Event()

    sleep_count = 0

    async def stop_after_one_poll(_handle: object, _seconds: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 2:
            handle._stopping = True

    with patch(
        "pymammotion.device.dynamics_line_loop._sleep_until_transport_change",
        new_callable=AsyncMock,
        side_effect=stop_after_one_poll,
    ) as sleep, patch(
        "pymammotion.device.dynamics_line_loop._enqueue_dynamics_line_saga",
        new_callable=AsyncMock,
    ) as enqueue:
        await dynamics_line_loop(handle)

    sleep.assert_has_awaits(
        [
            call(handle, _DYNAMICS_LINE_CLOUD_POLL_INTERVAL),
            call(handle, _DYNAMICS_LINE_CLOUD_POLL_INTERVAL),
        ]
    )
    enqueue.assert_awaited_once_with(handle)


async def test_dynamics_loop_skips_cloud_poll_on_quota_limited_firmware() -> None:
    """Automatic trail refresh cannot consume old firmware's command quota."""
    handle, _device = _make_handle(3)
    handle.device_name = "Yuka-MLYQ73XB"
    handle.firmware_version = "1.10.0.0"
    handle._stopping = False
    handle._transports = {
        TransportType.BLE: MagicMock(is_connected=False),
        TransportType.CLOUD_ALIYUN: MagicMock(is_connected=True),
    }
    handle.device_mode.return_value = _DeviceMode.ACTIVE
    handle.queue.is_saga_active = False
    handle._dynamics_line_rearm_event = asyncio.Event()

    sleep_count = 0

    async def stop_after_tick(_handle: object, _seconds: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 2:
            handle._stopping = True

    with patch(
        "pymammotion.device.dynamics_line_loop._sleep_until_transport_change",
        new_callable=AsyncMock,
        side_effect=stop_after_tick,
    ), patch(
        "pymammotion.device.dynamics_line_loop._enqueue_dynamics_line_saga",
        new_callable=AsyncMock,
    ) as enqueue:
        await dynamics_line_loop(handle)

    enqueue.assert_not_awaited()


async def test_ble_reconnect_switches_cloud_poll_to_ble_cadence() -> None:
    """A BLE transition interrupts the long cloud-only trail wait."""
    handle, _device = _make_handle(3)
    ble = MagicMock(is_connected=False)
    handle.device_name = "Yuka-MLYQ73XB"
    handle._stopping = False
    handle._transports = {
        TransportType.BLE: ble,
        TransportType.CLOUD_ALIYUN: MagicMock(is_connected=True),
    }
    handle._dynamics_line_rearm_event = asyncio.Event()
    handle.device_mode.return_value = _DeviceMode.ACTIVE
    handle.queue.is_saga_active = False

    async def reconnect_then_stop(_handle: object, _seconds: float) -> None:
        if not ble.is_connected:
            ble.is_connected = True
        else:
            handle._stopping = True

    with patch(
        "pymammotion.device.dynamics_line_loop._sleep_until_transport_change",
        new_callable=AsyncMock,
        side_effect=reconnect_then_stop,
    ) as sleep, patch(
        "pymammotion.device.dynamics_line_loop._enqueue_dynamics_line_saga",
        new_callable=AsyncMock,
    ) as enqueue:
        await dynamics_line_loop(handle)

    sleep.assert_has_awaits(
        [
            call(handle, _DYNAMICS_LINE_CLOUD_POLL_INTERVAL),
            call(handle, _DYNAMICS_LINE_POLL_INTERVAL),
        ]
    )
    enqueue.assert_awaited_once_with(handle)

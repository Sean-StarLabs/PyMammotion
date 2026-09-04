"""Dynamics-line poll loop — mirrors APK ``HashDataManager`` 100003 timer.

The APK polls ``NavGetCommData(action=8, type=18)`` every 10 s while the
device is mowing/returning, for devices where
``DeviceType.isSupportDynamicsLine()`` is true (LUBA_HM included).  The
response carries the live cut-path so far, which the UI overlays as the
mower's progress.

This loop replicates that behaviour for pymammotion.  It prefers BLE at the
app's 10 s cadence, but falls back to a slower cloud cadence when the mower is
outside Bluetooth range.  Native paths are useful precisely while the mower
is moving around a property, so tying their lifecycle to a nearby BLE link
would make them disappear during ordinary use.

Per-tick gates:

* device is in ACTIVE mode (``DeviceHandle.device_mode``)
* at least one transport is connected
* device type supports dynamics line (re-checked because LUBA_VA is
  firmware-gated and firmware may not be known at loop-start)
* no other saga is currently running on the device queue

When all gates pass, a ``CommonDataSaga`` is enqueued; on completion the
assembled point list is stored on ``device.map.dynamics_line``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from pymammotion.data.model.device import MowerDevice
from pymammotion.device.modes import _DeviceMode
from pymammotion.messaging.dynamics_line_saga import DynamicsLineSaga
from pymammotion.transport.base import TransportType
from pymammotion.utility.device_type import DeviceType

if TYPE_CHECKING:
    from pymammotion.device.handle import DeviceHandle

_logger = logging.getLogger(__name__)

#: Poll cadence — matches APK ``HashDataManager.handlerType_getDynamicsLine``
#: (10 000 ms, see ``HashDataManager.java:133, :873``).
_DYNAMICS_LINE_POLL_INTERVAL: float = 10.0
#: Cloud requests count toward older firmware's rolling send limit.  One poll
#: per minute keeps a useful live trail without consuming the budget at the
#: app's BLE cadence.
_DYNAMICS_LINE_CLOUD_POLL_INTERVAL: float = 60.0


async def dynamics_line_loop(handle: DeviceHandle) -> None:
    """Periodically fetch the native trail over BLE or cloud.

    LUBA_VA is firmware-gated (must be >= 1.15.3.4422 per APK
    ``DeviceType.isSupportDynamicsLine``).  Because the main-controller
    version isn't known until the first report arrives, the loop re-checks
    ``is_support_dynamics_line(fw)`` on every tick using the current
    ``device_firmwares.main_controller``.
    """
    device_type = DeviceType.value_of_str(handle.device_name)

    while not handle._stopping:  # noqa: SLF001
        # The handle's shared rearm event is set after every saga. Using it here
        # would make this saga wake itself and retry at the transfer timeout.
        prefer_ble = _ble_connected(handle)
        await asyncio.sleep(
            _DYNAMICS_LINE_POLL_INTERVAL
            if prefer_ble
            else _DYNAMICS_LINE_CLOUD_POLL_INTERVAL
        )

        if handle._stopping:  # noqa: SLF001
            return

        prefer_ble = _ble_connected(handle)
        if not prefer_ble and not _cloud_connected(handle):
            continue

        if handle.device_mode() != _DeviceMode.ACTIVE:
            continue

        if handle.queue.is_saga_active:
            continue

        # Re-check on every tick — LUBA_VA depends on firmware version which
        # may not be known until reports start arriving.
        if not device_type.is_support_dynamics_line(_main_controller_version(handle)):
            continue

        await _enqueue_dynamics_line_saga(handle)


def _ble_connected(handle: DeviceHandle) -> bool:
    """Return whether the mower has a connected Bluetooth transport."""
    ble = handle._transports.get(TransportType.BLE)  # noqa: SLF001
    return ble is not None and ble.is_connected


def _cloud_connected(handle: DeviceHandle) -> bool:
    """Return whether either cloud transport is connected."""
    return any(
        (transport := handle._transports.get(transport_type)) is not None  # noqa: SLF001
        and transport.is_connected
        for transport_type in (
            TransportType.CLOUD_ALIYUN,
            TransportType.CLOUD_MAMMOTION,
        )
    )


def _main_controller_version(handle: DeviceHandle) -> str | None:
    """Return the device's main-controller firmware version, or None if unknown.

    Pulls ``device_firmwares.main_controller`` off the current state snapshot;
    that's the field ``DeviceVersionUtils.isLessThanInputVersion`` consults in
    the APK.
    """
    raw = handle.snapshot.raw
    if not isinstance(raw, MowerDevice):
        return None
    fw = raw.device_firmwares.main_controller
    return fw or None


async def _enqueue_dynamics_line_saga(handle: DeviceHandle) -> None:
    """Enqueue a ``CommonDataSaga`` for the dynamics line and wire the update.

    On successful completion the assembled point list is stored on
    ``device.map.dynamics_line`` and the WGS-84 geojson is regenerated using
    the current RTK location, mirroring ``MammotionClient.get_dynamics_line``.
    """

    def _mow_session_id() -> int:
        raw = handle.snapshot.raw
        return raw.mow_session_id if isinstance(raw, MowerDevice) else 0

    saga = DynamicsLineSaga(
        command_builder=handle.commands,
        send_command=handle.send_raw,
        get_mow_session_id=_mow_session_id,
    )

    async def _on_complete() -> None:
        if not saga.transfer_complete:
            return
        await handle.commit_dynamics_line(saga.result, saga.mow_session_id)

    try:
        await handle.enqueue_saga(saga, on_complete=_on_complete)
    except Exception:  # noqa: BLE001
        _logger.debug("dynamics_line_loop [%s]: enqueue failed", handle.device_name, exc_info=True)

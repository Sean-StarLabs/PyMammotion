"""Regression coverage for authoritative restored-route verification."""

from __future__ import annotations

from unittest.mock import AsyncMock

from pymammotion.data.model.hash_list import HashList, NavGetHashListData, RootHashList
from pymammotion.messaging.broker import DeviceMessageBroker
from pymammotion.messaging.mow_path_saga import MowPathSaga
from tests.unit.messaging._helpers import make_command_builder as _make_command_builder


async def test_forced_refresh_does_not_reuse_a_silent_manifest() -> None:
    """Silence during session verification leaves the existing route unverified."""
    saga = MowPathSaga(
        command_builder=_make_command_builder(),
        send_command=AsyncMock(),
        get_map=HashList,
        zone_hashs=[],
        device_name="Luba-Test",
        force_refresh=True,
    )
    saga.step_timeout = 0.01
    saga.result_root_hash_list = RootHashList(
        sub_cmd=3,
        data=[NavGetHashListData(total_frame=1, current_frame=1, data_couple=[1234567890])],
    )

    root = await saga._fetch_line_hash_list(DeviceMessageBroker())  # noqa: SLF001

    assert root.data == []

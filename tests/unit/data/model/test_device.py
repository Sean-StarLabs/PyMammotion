"""Tests for MowingDevice JSON serialization with int-keyed HashList fields."""

from __future__ import annotations

import json

from pymammotion.data.model.device import MowingDevice
from pymammotion.data.model.hash_list import (
    FrameList,
    HashList,
    MowPath,
    NavGetCommData,
    RootHashList,
)
from pymammotion.data.model.report_info import WorkData
from pymammotion.proto import ReportInfoData, RptDevStatus, RptWork
from pymammotion.utility.constant import WorkMode


def _make_hash_list_with_int_keys() -> HashList:
    hl = HashList()
    hl.area[12345] = FrameList(total_frame=2, sub_cmd=0, data=[NavGetCommData(hash=12345)])
    hl.obstacle[99999] = FrameList(total_frame=1, sub_cmd=1)
    hl.path[77777] = FrameList(total_frame=3, sub_cmd=2)
    hl.current_mow_path[1] = {0: MowPath(area=12345, total_frame=1)}
    return hl


def test_mowing_device_to_json_with_int_keys() -> None:
    """MowingDevice.to_json() must not raise when HashList has int-keyed dicts."""
    device = MowingDevice(name="test-device")
    device.map = _make_hash_list_with_int_keys()

    json_str = device.to_json()
    assert isinstance(json_str, str)

    data = json.loads(json_str)
    # orjson serialises int keys as strings in JSON (the JSON spec requires string keys)
    assert "12345" in data["map"]["area"]
    assert "99999" in data["map"]["obstacle"]
    assert "77777" in data["map"]["path"]
    assert "1" in data["map"]["current_mow_path"]


def test_mowing_device_to_jsonb_with_int_keys() -> None:
    """MowingDevice.to_jsonb() must not raise and return bytes."""
    device = MowingDevice(name="test-device")
    device.map = _make_hash_list_with_int_keys()

    raw = device.to_jsonb()
    assert isinstance(raw, bytes)
    assert b"12345" in raw


def test_empty_mowing_device_roundtrip() -> None:
    """Empty MowingDevice serialises and deserialises cleanly."""
    device = MowingDevice(name="empty")
    json_str = device.to_json()
    assert json_str
    data = json.loads(json_str)
    assert data["name"] == "empty"


def test_new_mow_session_clears_route_from_the_previous_session() -> None:
    """Only a reported transition into a new mowing lifecycle clears a route."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=4)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 4
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path == {}


def test_new_mow_session_clears_manifest_without_cached_route() -> None:
    """A prior manifest cannot be reused when the next session has no route packets."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=4)
    device.map.root_hash_lists = [RootHashList(sub_cmd=3)]
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.root_hash_lists == []


def test_planned_route_is_adopted_by_the_next_mow_session() -> None:
    """A route planned for the next session survives the start transition."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=4)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.map.planned_mow_path_pending = True
    device.map.root_hash_lists = [RootHashList(sub_cmd=3)]
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path
    assert device.map.current_mow_path_session_id == 5
    assert device.map.planned_mow_path_pending is False
    assert [root.sub_cmd for root in device.map.root_hash_lists] == [3]


def test_path_hash_rotation_does_not_change_session_or_clear_route() -> None:
    """Yuka path hashes may rotate repeatedly during one reported mowing job."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=5)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123, ub_path_hash=456)

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=789, ub_path_hash=987),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path


def test_pause_return_and_charging_pause_remain_in_same_session() -> None:
    """Every non-terminal mowing mode retains the current session route."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=5)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING

    for mode in (
        WorkMode.MODE_PAUSE,
        WorkMode.MODE_RETURNING,
        WorkMode.MODE_CHARGING_PAUSE,
        WorkMode.MODE_WORKING,
    ):
        device.update_report_data(
            ReportInfoData(
                dev=RptDevStatus(sys_status=mode, sys_time_stamp=1),
                work=RptWork(path_hash=1),
            )
        )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path


def test_completed_route_is_retained_until_the_next_session() -> None:
    """Finishing a job keeps its native route available for the map."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=5)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_READY, sys_time_stamp=1),
            work=RptWork(path_hash=1),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path


def test_returning_from_ready_does_not_create_a_mow_session() -> None:
    """A standalone return-to-dock trip is not a new mowing job."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=5)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_RETURNING, sys_time_stamp=1),
            work=RptWork(path_hash=1),
        )
    )

    assert device.mow_session_id == 5
    assert device.map.current_mow_path


def test_active_job_bootstraps_session_identity_after_upgrade() -> None:
    """Persisted active telemetry without the new field acquires an identity."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_CHARGING_PAUSE

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_CHARGING_PAUSE, sys_time_stamp=1),
            work=RptWork(path_hash=123),
        )
    )

    assert device.mow_session_id == 1


def test_status_only_frame_starts_mow_session() -> None:
    """Status and work telemetry can arrive in separate device reports."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=4)
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING))
    )

    assert device.mow_session_id == 5


def test_partial_reports_preserve_active_session_and_path_hash() -> None:
    """Incomplete telemetry cannot create a false session boundary."""
    device = MowingDevice(name="Yuka-Test", mow_session_id=5)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_session_id = 5
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(battery_val=42),
            work=RptWork(path_hash=1),
        )
    )

    assert device.report_data.dev.sys_status == WorkMode.MODE_WORKING
    assert device.report_data.work.path_hash == 123
    assert device.mow_session_id == 5
    assert device.map.current_mow_path


def test_mow_session_id_survives_serialization() -> None:
    """Restored active jobs keep the lifecycle identity that owns their route."""
    restored = MowingDevice().from_json(
        MowingDevice(name="Yuka-Test", mow_session_id=7).to_json()
    )

    assert restored.mow_session_id == 7


# ===========================================================================
# The OTA check (CheckDeviceVersion.current_version) is the cloud's view of the
# ===========================================================================
from pymammotion.data.model.device import Device, MowerDevice, RTKBaseStationDevice, create_device
from pymammotion.http.model.http import CheckDeviceVersion


def _check(version: str, *, device_id: str = "iot-1") -> CheckDeviceVersion:
    return CheckDeviceVersion(current_version=version, device_id=device_id)


def test_mower_seeds_device_version() -> None:
    device = MowerDevice(name="Luba-VS123")
    check = _check("1.12.0.466")
    device.apply_version_check(check)
    assert device.update_check is check
    assert device.device_firmwares.device_version == "1.12.0.466"


def test_rtk_seeds_device_version() -> None:
    device = RTKBaseStationDevice(name="RTK-abc")
    device.apply_version_check(_check("3.0.1"))
    assert device.device_firmwares.device_version == "3.0.1"


def test_empty_current_version_does_not_overwrite() -> None:
    device = MowerDevice(name="Luba-VS123")
    device.device_firmwares.device_version = "1.12.0.466"
    device.apply_version_check(_check(""))  # empty cloud value
    assert device.device_firmwares.device_version == "1.12.0.466"  # preserved


def test_base_device_without_firmware_field_is_safe() -> None:
    # Base Device has update_check but no device_firmwares — must not raise.
    device = Device(name="x")
    device.apply_version_check(_check("9.9.9"))
    assert device.update_check.current_version == "9.9.9"


def test_seeds_version_feeds_detection_gate() -> None:
    # End-to-end: OTA version flows into the firmware-gated obstacle options.
    from pymammotion.data.model.mowing_modes import DetectionStrategy

    device = create_device("Luba-VS123", "a1pvCnb3PPu")
    device.apply_version_check(_check("1.11.0"))  # below the 1.12.0 threshold
    options = DetectionStrategy.for_device(device.name, device.device_firmwares.device_version)
    assert DetectionStrategy.slow_touch in options  # old-firmware option set

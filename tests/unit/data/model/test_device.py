"""Tests for MowingDevice JSON serialization with int-keyed HashList fields."""
from __future__ import annotations

import json

from pymammotion.data.model.device import MowingDevice
from pymammotion.data.model.hash_list import FrameList, HashList, MowPath, NavGetCommData
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


def test_new_active_job_clears_cached_mow_path() -> None:
    """An observed idle-to-working transition starts a new route session."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=0, ub_path_hash=1),
        )
    )

    assert device.map.current_mow_path == {}


def test_active_restart_preserves_current_mow_path() -> None:
    """A same-job active report after restore must keep native route state."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=0, ub_path_hash=1),
        )
    )

    assert device.map.current_mow_path


def test_idle_path_hash_one_clears_cached_mow_path() -> None:
    """The path-hash end sentinel clears a completed route session."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_READY),
            work=RptWork(path_hash=1, ub_path_hash=0),
        )
    )

    assert device.map.current_mow_path == {}


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

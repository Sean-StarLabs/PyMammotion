"""Tests for MowingDevice JSON serialization with int-keyed HashList fields."""

from __future__ import annotations

import json

from pymammotion.data.model.device import MowingDevice
from pymammotion.data.model.hash_list import (
    CommDataCouple,
    FrameList,
    HashList,
    MowPath,
    MowPathPacket,
    NavGetCommData,
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


def test_work_data_task_path_hash_excludes_segments_and_end_sentinels() -> None:
    """Only the stable route hash identifies a task."""
    assert WorkData(path_hash=123, ub_path_hash=456).task_path_hash == 123
    assert WorkData(path_hash=1, ub_path_hash=456).task_path_hash == 0
    assert WorkData(path_hash=0, ub_path_hash=789).task_path_hash == 0


def test_resuming_same_task_preserves_cached_mow_path() -> None:
    """An idle-to-working transition does not imply a different task."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 123
    device.report_data.dev.sys_status = WorkMode.MODE_READY

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 123


def test_reported_new_task_clears_cached_mow_path() -> None:
    """A different reported task identity invalidates the previous route."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 123
    device.report_data.work = WorkData(path_hash=123)

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=456),
        )
    )

    assert device.map.current_mow_path == {}
    assert device.map.current_mow_path_hash == 0


def test_unverified_legacy_cached_path_is_cleared_for_reported_task() -> None:
    """An old route is not guessed to belong to the first task seen after restore."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.map.current_mow_path == {}
    assert device.map.current_mow_path_hash == 0


def test_verified_legacy_cached_path_adopts_reported_task_hash() -> None:
    """Packet metadata can safely associate a restored route with its task."""
    device = MowingDevice(name="Yuka-Test")
    device.map.current_mow_path = {
        1: {
            1: MowPath(
                transaction_id=1,
                current_frame=1,
                total_frame=1,
                path_packets=[MowPathPacket(path_hash=123)],
            )
        }
    }

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 123


def test_breakpoint_change_preserves_task_route() -> None:
    """Advancing to another segment does not invalidate the task route."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 123
    device.report_data.work = WorkData(path_hash=123, ub_path_hash=10)

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123, ub_path_hash=20),
        )
    )

    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 123


def test_new_task_clears_previous_dynamics_line() -> None:
    """A direct task transition removes live geometry owned by the old task."""
    device = MowingDevice(name="Yuka-Test")
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123
    device.report_data.work = WorkData(path_hash=123)

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=456),
        )
    )

    assert device.map.dynamics_line == []
    assert device.map.dynamics_line_path_hash == 0


def test_active_transient_task_sentinel_preserves_dynamics_line() -> None:
    """An active report with no stable task hash does not erase current geometry."""
    device = MowingDevice(name="Yuka-Test")
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=1, ub_path_hash=456),
        )
    )

    assert device.map.dynamics_line
    assert device.map.dynamics_line_path_hash == 123


def test_inactive_task_end_sentinel_clears_dynamics_line() -> None:
    """A confirmed inactive end report clears geometry for the finished task."""
    device = MowingDevice(name="Yuka-Test")
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_READY),
            work=RptWork(path_hash=1),
        )
    )

    assert device.map.dynamics_line == []
    assert device.map.dynamics_line_path_hash == 0


def test_partial_work_report_uses_previous_active_status() -> None:
    """A work-only progress report cannot look like a confirmed task end."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(ReportInfoData(work=RptWork()))

    assert device.map.dynamics_line
    assert device.map.dynamics_line_path_hash == 123
    assert device.report_data.work.path_hash == 123


def test_dev_only_task_end_clears_dynamics_line() -> None:
    """A confirmed inactive status ends live geometry without a work payload."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(ReportInfoData(dev=RptDevStatus(sys_status=WorkMode.MODE_READY)))

    assert device.map.dynamics_line == []
    assert device.map.dynamics_line_path_hash == 0


def test_battery_only_dev_report_preserves_active_dynamics_line() -> None:
    """A default scalar status in partial telemetry is not a task end."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(ReportInfoData(dev=RptDevStatus(battery_val=42)))

    assert device.map.dynamics_line
    assert device.map.dynamics_line_path_hash == 123


def test_combined_partial_report_preserves_active_task_geometry() -> None:
    """Default dev status plus a work sentinel cannot end an active task."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 123
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(battery_val=42),
            work=RptWork(path_hash=1),
        )
    )

    assert device.report_data.work.path_hash == 123
    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 123
    assert device.map.dynamics_line
    assert device.map.dynamics_line_path_hash == 123


def test_new_active_sentinel_does_not_restore_ended_task_hash() -> None:
    """A task confirmed ended cannot be rebound to a later active sentinel."""
    device = MowingDevice(name="Yuka-Test")
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING
    device.report_data.work = WorkData(path_hash=123)
    device.map.dynamics_line = [CommDataCouple(x=1, y=2)]
    device.map.dynamics_line_path_hash = 123

    device.update_report_data(ReportInfoData(dev=RptDevStatus(sys_status=WorkMode.MODE_READY)))
    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=1),
        )
    )

    assert device.report_data.work.path_hash == 1
    assert device.map.dynamics_line == []
    assert device.map.dynamics_line_path_hash == 0


def test_active_report_preserves_current_job_path() -> None:
    """A report for an already active task keeps its associated route."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 123
    device.report_data.dev.sys_status = WorkMode.MODE_WORKING

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=123),
        )
    )

    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 123


def test_planned_route_survives_previous_reported_hash() -> None:
    """Repeated reports for the pre-planning task do not erase the preview."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 200
    device.map.planned_mow_path_pending = True
    device.map.pending_planned_mow_path_previous_hash = 100

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_READY),
            work=RptWork(path_hash=100),
        )
    )

    assert device.map.current_mow_path
    assert device.map.current_mow_path_hash == 200
    assert device.map.pending_planned_mow_path_previous_hash == 100


def test_planned_route_is_confirmed_by_its_reported_hash() -> None:
    """The planned hash clears the pending previous-task marker."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 200
    device.map.planned_mow_path_pending = True
    device.map.pending_planned_mow_path_previous_hash = 100

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=200),
        )
    )

    assert device.map.current_mow_path
    assert device.map.planned_mow_path_pending is False
    assert device.map.pending_planned_mow_path_previous_hash == 0


def test_third_task_hash_invalidates_pending_planned_route() -> None:
    """A task unrelated to either side of planning supersedes the preview."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 200
    device.map.planned_mow_path_pending = True
    device.map.pending_planned_mow_path_previous_hash = 100

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=300),
        )
    )

    assert device.map.current_mow_path == {}
    assert device.map.planned_mow_path_pending is False
    assert device.map.pending_planned_mow_path_previous_hash == 0


def test_first_planned_route_survives_idle_zero_hash() -> None:
    """An idle sentinel cannot erase a preview created before any task existed."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 200
    device.map.planned_mow_path_pending = True

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_READY),
            work=RptWork(path_hash=0),
        )
    )

    assert device.map.current_mow_path
    assert device.map.planned_mow_path_pending is True


def test_active_previous_task_invalidates_pending_planned_route() -> None:
    """A resumed old task supersedes a preview that was never started."""
    device = MowingDevice(name="Yuka-Test")
    device.map = _make_hash_list_with_int_keys()
    device.map.current_mow_path_hash = 200
    device.map.planned_mow_path_pending = True
    device.map.pending_planned_mow_path_previous_hash = 100

    device.update_report_data(
        ReportInfoData(
            dev=RptDevStatus(sys_status=WorkMode.MODE_WORKING),
            work=RptWork(path_hash=100),
        )
    )

    assert device.map.current_mow_path == {}
    assert device.map.planned_mow_path_pending is False


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

"""MowPathSaga — plan a route and collect the mowing path from the device."""

from __future__ import annotations

import copy
import logging
import time
from typing import TYPE_CHECKING, Any

import betterproto2

from pymammotion.data.model import GenerateRouteInformation
from pymammotion.data.model.hash_list import HashList, MowPath, NavGetHashListData, RootHashList
from pymammotion.messaging.saga import Saga
from pymammotion.messaging.transfers import ack_stream
from pymammotion.transport.base import CommandTimeoutError, SagaFailedError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymammotion.messaging.broker import DeviceMessageBroker

_logger = logging.getLogger(__name__)


class MowPathSaga(Saga):
    """Plan a mowing route and collect the resulting cover-path frames.

    Execution order — planning mode (skip_planning=False):
      1. Send generate_route_information (bidire_reqconver_path, sub_cmd=0)
         and wait for the device's sub_cmd=0 confirmation.
      2. Send get_all_boundary_hash_list(sub_cmd=3) and collect the generated
         route's hash frames, acknowledging each with get_hash_response.
      3. Send get_line_info_list with each frame's hashes + a timestamp transaction_id.
      4. Collect all cover_path_upload frames until none are missing.

    Execution order — running task mode (skip_planning=True):
      1. Use the reported task's route configuration.
      2. Request its current line-hash manifest (sub_cmd=3).
      3–4. Same as planning mode steps 3–4.

    result is a dict[transaction_id, dict[frame_num, MowPath]] on success,
    empty dict until then.
    """

    name = "mow_path_fetch"
    max_attempts = 1
    #: Matches the Android app's per-frame watchdog for the same fetch
    #: (HashDataManager.handlerType_12333, armed at 3000 ms after each
    #: getRegionalData and cancelled on every received frame).
    #:
    #: Must stay comfortably *above* the device's own ~1 s frame-retransmit
    #: interval.  At 1.0 s this raced it exactly: the device re-sends an unacked
    #: frame after 1.000 s, so the saga could time out at the very moment the
    #: retransmit was in flight and abandon a run that was about to succeed —
    #: and with ``max_attempts = 1`` there is no retry to cover for it.
    step_timeout = 3.0
    _last_fallback_transaction_id = 0

    def __init__(
        self,
        command_builder: Any,
        send_command: Callable[[bytes], Awaitable[None]],
        get_map: Callable[[], HashList],
        zone_hashs: list[int],
        route_info: GenerateRouteInformation | None = None,
        *,
        skip_planning: bool = False,
        device_name: str = "",
        sync_type: int = 3,
        next_transaction_id: Callable[[], int] | None = None,
        get_mow_session_id: Callable[[], int] | None = None,
        force_refresh: bool = False,
    ) -> None:
        """Initialise the saga.

        Args:
            command_builder: Navigation command builder (MammotionCommand or similar).
            send_command: Async callable that transmits raw bytes to the device.
            get_map: Returns the device's current HashList (e.g.
                     ``lambda: handle.snapshot.raw.map``).  Used as the source of
                     truth for received cover-path frames across retries.
            zone_hashs: Area/zone hash IDs to mow (from HashList.area.keys()).
                        Used as fallback when the device returns an empty line hash list.
            route_info: Optional pre-built GenerateRouteInformation; defaults are
                        used if not supplied.
            skip_planning: When True, skip generate_route_information and instead query
                           the currently running job's route info (sub_cmd=2) to obtain
                           the zone hashes before fetching the line hash list.
            force_refresh: Ignore cached route packets and fetch one complete
                           replacement for restored-session verification.

        """
        self._command_builder = command_builder
        self._send_command = send_command
        self._get_map = get_map
        self._zone_hashs = zone_hashs
        self._route_info = route_info
        self._skip_planning = skip_planning
        self.interruptible = skip_planning
        self._device_name = device_name
        self._sync_type = sync_type  # 2 = BLE, 3 = IoT/MQTT
        self._next_transaction_id = next_transaction_id
        self._get_mow_session_id = get_mow_session_id
        self._force_refresh = force_refresh
        self._pending_transactions: dict[int, dict[int, MowPath]] = {}
        self._completed_hashes: set[int] = set()
        self.result: dict[int, dict[int, MowPath]] = {}
        self.result_root_hash_list = RootHashList(sub_cmd=3)
        self.started_session_id = 0
        self.result_session_id = 0
        self._task_identity_captured = False
        self._route_val: GenerateRouteInformation | None = route_info if skip_planning else None

    def _capture_task_identity(self) -> None:
        """Bind this operation to the task present on its first execution."""
        if self._task_identity_captured:
            return
        self.started_session_id = self._get_mow_session_id() if self._get_mow_session_id is not None else 0
        self.result_session_id = self.started_session_id if self._skip_planning else self.started_session_id + 1
        self._task_identity_captured = True

    async def progress(self) -> Any:
        """Route resolution plus banked cover-path frames.

        Drives the base class's attempt-budget refresh.  Replaces both the manual
        ``_reset_attempt_counter`` and the ``_budget_reset_granted`` one-shot that
        guarded it: a value derived from state only changes when the fetch really
        advances, so it cannot refresh the budget on every run the way a flag set
        inside ``_run`` did.
        """
        return (self._route_val is not None, len(self._completed_hashes))

    def _new_transaction_id(self) -> int:
        """Return a transaction ID unique within this device command stream."""
        if self._next_transaction_id is not None:
            return self._next_transaction_id()
        transaction_id = max(int(time.time() * 1000), MowPathSaga._last_fallback_transaction_id + 1)
        MowPathSaga._last_fallback_transaction_id = transaction_id
        return transaction_id

    def _hashes_to_fetch(self, all_hashes: list[int], current_map: HashList) -> list[int]:
        """Return the route hashes that must be requested for this operation."""
        remaining = [path_hash for path_hash in all_hashes if path_hash not in self._completed_hashes]
        if not self._skip_planning or self._force_refresh:
            # Planned and verification routes are authoritative replacements.
            # Reusing packets would either mix routes or incorrectly certify a
            # restored path from a job that completed while the client was down.
            return remaining
        cache_matches_session = not current_map.current_mow_path or self.result_session_id in {
            0,
            current_map.current_mow_path_session_id,
        }
        if not cache_matches_session:
            return remaining
        return [path_hash for path_hash in remaining if not current_map.has_mow_path_for_hash(path_hash)]

    @staticmethod
    def _store_batch_frame(
        transactions: dict[int, dict[int, MowPath]],
        frame: MowPath,
        transaction_id: int,
        expected_hashes: set[int] | None = None,
    ) -> bool:
        """Store a valid frame and return whether its transaction is complete."""
        if frame.transaction_id != transaction_id:
            return False
        if (
            frame.result != 0
            or frame.total_frame <= 0
            or frame.current_frame <= 0
            or frame.current_frame > frame.total_frame
        ):
            raise SagaFailedError(MowPathSaga.name, MowPathSaga.max_attempts)
        if expected_hashes is not None and any(
            int(packet.path_hash) not in expected_hashes for packet in frame.path_packets
        ):
            raise SagaFailedError(MowPathSaga.name, MowPathSaga.max_attempts)

        frames = transactions.setdefault(transaction_id, {})
        if frames and any(existing.total_frame != frame.total_frame for existing in frames.values()):
            raise SagaFailedError(MowPathSaga.name, MowPathSaga.max_attempts)
        frames[frame.current_frame] = frame
        complete = HashList.mow_path_transaction_complete(frames)
        if complete and expected_hashes is not None:
            observed_hashes = {
                int(packet.path_hash) for stored_frame in frames.values() for packet in stored_frame.path_packets
            }
            if observed_hashes != expected_hashes:
                transactions.pop(transaction_id, None)
                raise SagaFailedError(MowPathSaga.name, MowPathSaga.max_attempts)
        return complete

    async def _send_ble_sync(self) -> None:
        """Keep the device in its synced/responsive state before a major fetch request.

        The device only serves hash-list / route / cover-path frames while it considers the
        app "synced", and that state lapses after a few seconds.  We re-sync immediately
        before each major request (line hash list, route info, cover-path fetch) so the
        device is freshly synced when the command arrives, rather than relying on a single
        sync at the top of the run that goes stale across the intervening frame loops.
        """
        _logger.debug("MowPathSaga[%s]: sending todev_ble_sync(%d)", self._device_name, self._sync_type)
        await self._send_command(self._command_builder.send_todev_ble_sync(sync_type=self._sync_type))

    async def _fetch_line_hash_list(
        self,
        broker: DeviceMessageBroker,
        *,
        allow_cached_fallback: bool,
    ) -> RootHashList:
        """Fetch the route manifest after route resolution."""
        await self._send_ble_sync()
        with self._collect_frames(broker, "toapp_gethash_ack", lambda v: v.sub_cmd == 3) as hash_ack_queue:
            _logger.debug("MowPathSaga: requesting line hash list (sub_cmd=3)")
            await self._send_command(self._command_builder.get_all_boundary_hash_list(sub_cmd=3))

            async def _ack(ack: Any) -> None:
                await self._send_command(
                    self._command_builder.get_hash_response(
                        total_frame=ack.total_frame,
                        current_frame=ack.current_frame,
                    )
                )

            line_frames = await ack_stream(
                hash_ack_queue,
                field="toapp_gethash_ack(sub_cmd=3)",
                ack=_ack,
                timeout=self.step_timeout,
                allow_empty=True,
            )

        line_data = [
            NavGetHashListData(
                pver=int(frame.pver),
                sub_cmd=int(frame.sub_cmd),
                total_frame=int(frame.total_frame),
                current_frame=int(frame.current_frame),
                data_hash=int(frame.data_hash),
                hash_len=int(frame.hash_len),
                reserved=str(frame.reserved),
                result=int(frame.result),
                data_couple=[int(value) for value in frame.data_couple],
            )
            for frame in line_frames.values()
        ]
        if line_data:
            return RootHashList(
                total_frame=line_data[0].total_frame,
                sub_cmd=3,
                data=line_data,
            )
        if self.result_root_hash_list.data:
            return copy.deepcopy(self.result_root_hash_list)
        if allow_cached_fallback:
            previous = next(
                (root for root in self._get_map().root_hash_lists if root.sub_cmd == 3),
                None,
            )
            if previous is not None:
                return copy.deepcopy(previous)
        return RootHashList(sub_cmd=3)

    async def _run(self, broker: DeviceMessageBroker) -> None:
        """Execute all saga steps."""
        self.result = {}
        self._capture_task_identity()
        if self._skip_planning:
            if self._get_mow_session_id is not None and self.result_session_id == 0:
                _logger.debug("MowPathSaga: queued task ended before route recovery started")
                return
        # Do not wipe current_mow_path here. The model clears it at a confirmed
        # session boundary; wiping on a retry would defeat packet reuse.

        # ------------------------------------------------------------------
        # Step 1: Get route information (skip if already cached from a prior attempt).
        # ------------------------------------------------------------------
        if self._route_val is None:
            if not self._skip_planning:
                # planning mode: send generate_route_information, wait for sub_cmd=0 confirmation
                route_info = self._route_info or GenerateRouteInformation(one_hashs=self._zone_hashs)
                _logger.debug("MowPathSaga: sending generate_route_information for %d zone(s)", len(self._zone_hashs))
                # Re-sync before the route request — the step-1 frame loop above can stale
                # the run's initial sync.
                await self._send_ble_sync()
                cmd = self._command_builder.generate_route_information(route_info)
                response = await broker.send_and_wait(
                    send_fn=lambda: self._send_command(cmd),
                    expected_field="bidire_reqconver_path",
                    send_timeout=self.step_timeout,
                )
                route_frame = self.extract_nav_frame(response, "bidire_reqconver_path")
                assert route_frame is not None  # noqa: S101 — send_and_wait already matched this field
                self._route_val = route_frame[1]
                _logger.debug(
                    "MowPathSaga: route confirmed — sub_cmd=%d  path_hash=%d",
                    self._route_val.sub_cmd,
                    self._route_val.path_hash,
                )
            else:
                # skip_planning=True: a running job's route info should already be cached.
                # If it isn't, the saga cannot fetch cover paths — fail loudly instead of
                # returning silently (which left the caller with empty MowPath data).
                _logger.warning("MowPathSaga: skip_planning=True but no _route_val available — failing saga")
                raise SagaFailedError(self.name, self.max_attempts)
        else:
            _logger.debug("MowPathSaga: reusing cached route info — skipping step 1")

        # Fetch the manifest only after route generation so packets cannot be
        # attributed to the previous preview.
        self.result_root_hash_list = await self._fetch_line_hash_list(
            broker,
            allow_cached_fallback=self._skip_planning and not self._force_refresh,
        )

        # The manifest is staged with the route and published only after every
        # cover-path transaction completes.
        if not self.result_root_hash_list.data:
            # No breakpoint lines from sub_cmd=3 — nothing to fetch via get_line_info_list.
            _logger.debug("MowPathSaga: no sub_cmd=3 line hashes — no cover path to fetch")
            self._route_val = None
            return
        all_hashes = [
            h
            for frame in sorted(self.result_root_hash_list.data, key=lambda data: data.current_frame)
            for h in frame.data_couple
            if h != 0
        ]
        _logger.debug("MowPathSaga: %d total hash(es) from map", len(all_hashes))

        current_map = self._get_map()
        missing_hashes = self._hashes_to_fetch(all_hashes, current_map)
        if not missing_hashes:
            _logger.debug("MowPathSaga: all %d hash(es) already cached — skipping fetch", len(all_hashes))
            self.result = (
                {**current_map.current_mow_path, **self._pending_transactions}
                if self._skip_planning
                else self._pending_transactions
            )
            return

        if len(missing_hashes) < len(all_hashes):
            _logger.debug(
                "MowPathSaga: %d/%d hash(es) already cached — fetching %d missing",
                len(all_hashes) - len(missing_hashes),
                len(all_hashes),
                len(missing_hashes),
            )

        _BATCH_SIZE = 20
        hash_batches = [missing_hashes[i : i + _BATCH_SIZE] for i in range(0, len(missing_hashes), _BATCH_SIZE)]
        _logger.debug(
            "MowPathSaga: %d batch(es) of up to %d hash(es) each",
            len(hash_batches),
            _BATCH_SIZE,
        )

        # ------------------------------------------------------------------
        # Step 3–4: For each batch of up to 20 hashes, request cover paths and
        # collect all cover_path_upload frames before moving to the next batch.
        # ------------------------------------------------------------------
        _NO_PROGRESS_LIMIT = 10

        with self._collect_frames(broker, "cover_path_upload") as path_queue:
            # Re-sync before the cover-path fetch begins — same reasoning as the route step.
            await self._send_ble_sync()
            for batch_idx, batch_hashes in enumerate(hash_batches):
                transaction_id = self._new_transaction_id()
                _logger.debug(
                    "MowPathSaga: requesting cover path batch %d/%d — transaction_id=%d  hashes=%s",
                    batch_idx + 1,
                    len(hash_batches),
                    transaction_id,
                    batch_hashes,
                )
                cmd = self._command_builder.get_line_info_list(batch_hashes, transaction_id)
                await self._send_command(cmd)

                # Track missing-frame count to detect "frames are arriving but not advancing us"
                # (duplicates, stale tx, etc.).  Counter resets at the start of each batch so
                # the first frame of a new batch (which inflates missing as the tx is created)
                # is never the one that trips the guard.
                previous_frame_count = 0
                no_progress = 0

                try:
                    while True:
                        frame_response = await self._next_frame(path_queue, "cover_path_upload")

                        path_frame = self.extract_nav_frame(frame_response, "cover_path_upload")
                        assert path_frame is not None  # noqa: S101 — the collector already filtered on this field
                        mow_path = MowPath.from_dict(path_frame[1].to_dict(casing=betterproto2.Casing.SNAKE))

                        if mow_path.transaction_id != transaction_id:
                            _logger.debug(
                                "MowPathSaga: dropping residual frame tx=%d (current tx=%d)",
                                mow_path.transaction_id,
                                transaction_id,
                            )
                            no_progress += 1
                            if no_progress >= _NO_PROGRESS_LIMIT:
                                raise CommandTimeoutError("mow_path_stall", no_progress)
                            continue

                        _logger.debug(
                            "MowPathSaga: got cover_path_upload frame %d/%d  tx=%d  batch=%d/%d",
                            mow_path.current_frame,
                            mow_path.total_frame,
                            mow_path.transaction_id,
                            batch_idx + 1,
                            len(hash_batches),
                        )

                        complete = self._store_batch_frame(
                            self._pending_transactions,
                            mow_path,
                            transaction_id,
                            set(batch_hashes),
                        )
                        frame_count = len(self._pending_transactions[transaction_id])
                        if frame_count > previous_frame_count:
                            no_progress = 0
                        else:
                            no_progress += 1
                            if no_progress >= _NO_PROGRESS_LIMIT:
                                raise CommandTimeoutError("mow_path_stall", no_progress)
                        previous_frame_count = frame_count

                        if complete:
                            self._completed_hashes.update(batch_hashes)
                            break
                except BaseException:
                    # Only complete transactions survive a whole-saga retry.
                    self._pending_transactions.pop(transaction_id, None)
                    raise

        self.result = self._pending_transactions
        total_packets = sum(len(frames) for frames in self.result.values())
        _logger.debug("MowPathSaga: complete — %d transaction(s)  %d total frame(s)", len(self.result), total_packets)
        self._route_val = None

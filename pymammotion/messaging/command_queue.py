"""Priority command queue with exclusive slots for saga operations."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from enum import IntEnum
import logging
import time
from typing import TYPE_CHECKING

from pymammotion.aliyun.exceptions import (
    DeviceOfflineException,
    DeviceUnboundException,
    GatewayTimeoutException,
    TooManyRequestsException,
)
from pymammotion.transport import TransportError
from pymammotion.transport.base import AuthError, NoTransportAvailableError, SagaFailedError, TransportRateLimitedError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymammotion.messaging.broker import DeviceMessageBroker
    from pymammotion.messaging.saga import Saga

_logger = logging.getLogger(__name__)


class Priority(IntEnum):
    """Command execution priority. Lower value = higher priority."""

    # NOTE: EMERGENCY items skip the TTL and transport gates, but the processor is
    # strictly sequential — an item enqueued while an EXCLUSIVE saga is mid-flight
    # still waits for that saga's work() to return.  True e-stop preemption needs a
    # direct send path, not this queue.
    EMERGENCY = 0  # estop, return-to-dock — never dropped, skips TTL/transport gates
    PREEMPTING = 1  # user command ordered ahead of queued read sagas
    EXCLUSIVE = 2  # sagas (map/plan fetch) — holds processor until complete
    NORMAL = 3  # regular HA commands — waits for exclusive slot
    BACKGROUND = 4  # low-urgency polling — waits for exclusive slot


#: Commands that have not been dispatched within this window are silently dropped.
#: EMERGENCY items (e-stop, return-to-dock) are exempt.
_COMMAND_TTL = 120.0  # 2 minutes
_GATEWAY_TIMEOUT_MAX = 3


@dataclass(order=True)
class _QueueItem:
    priority: int
    sequence: int
    work: Callable[[], Awaitable[None]] = field(compare=False)
    skip_if_saga_active: bool = field(compare=False, default=False)
    dedup_key: str | None = field(compare=False, default=None)
    enqueued_at: float = field(compare=False, default_factory=time.monotonic)
    completion: asyncio.Future[None] | None = field(compare=False, default=None)
    expires: bool = field(compare=False, default=True)
    invalidate_reads_before_generation: int | None = field(compare=False, default=None)


class DeviceCommandQueue:
    """Priority command queue with exclusive slots for sagas.

    EXCLUSIVE items (sagas) hold the queue processor until the saga finishes.
    NORMAL/BACKGROUND items marked skip_if_saga_active=True are silently
    dropped while a saga is running — used by HA coordinator update() calls
    to prevent accumulation during long map/plan fetches.
    EMERGENCY items always execute and are never skipped.
    """

    def __init__(self, device_name: str = "") -> None:
        """Initialise queue, exclusive-slot event, and sequence counter."""
        self._queue: asyncio.PriorityQueue[_QueueItem] = asyncio.PriorityQueue()
        self._exclusive_active = asyncio.Event()
        self._exclusive_active.set()  # set = free (no saga running)
        # Gate cleared while the active MQTT transport is reconnecting (CONNECTING state)
        # and no BLE fallback is available.  Non-EMERGENCY items wait here so we don't
        # dispatch commands whose responses we cannot receive (MQTT subscription inactive).
        # DeviceHandle manages the gate via pause_for_reconnect() / resume_after_reconnect().
        self._transport_gate: asyncio.Event = asyncio.Event()
        self._transport_gate.set()  # set = open (no reconnection in progress)
        self._seq = 0
        self._running = False
        self._accepting_work = True
        self._task: asyncio.Task[None] | None = None
        self._device_name = device_name
        self._pending_dedup_keys: set[str] = set()
        self._interruptible_generation = 0
        self._minimum_interruptible_generation = 0
        self._active_interruptible_task: asyncio.Task[None] | None = None
        self._active_interruptible_requeue: Callable[[], Awaitable[None]] | None = None
        self._pending_completions: set[asyncio.Future[None]] = set()
        #: Called on critical errors (AuthError, SagaFailedError) so DeviceHandle can propagate them.
        self.on_critical_error: Callable[[Exception], Awaitable[None]] | None = None
        #: Fired when an exclusive saga is about to start.  Clients use this to pause the
        #: rapid-report subscription so it doesn't fight the saga for the MQTT channel.
        self.on_saga_start: Callable[[], Awaitable[None]] | None = None
        #: Fired once the saga returns (success or failure).  Pair with ``on_saga_start``
        #: to restart the subscription after the saga yields the channel.
        self.on_saga_end: Callable[[], Awaitable[None]] | None = None

    @property
    def is_saga_active(self) -> bool:
        """True when an exclusive saga is currently running."""
        return not self._exclusive_active.is_set()

    def start(self) -> None:
        """Start the background queue processor task."""
        if self._task is None or self._task.done():
            self._accepting_work = True
            self._running = True
            self._task = asyncio.get_running_loop().create_task(self._process())

    def pause_for_reconnect(self) -> None:
        """Block command dispatch while the MQTT transport is reconnecting.

        Called by DeviceHandle when the active MQTT transport transitions to
        CONNECTING state and no BLE fallback is available.  Commands accumulate
        in the queue but are not dispatched until resume_after_reconnect() is
        called (transport becomes CONNECTED, or BLE connects as a fallback).
        EMERGENCY items always bypass this gate.
        """
        self._transport_gate.clear()

    def resume_after_reconnect(self) -> None:
        """Unblock command dispatch after transport reconnection completes."""
        self._transport_gate.set()

    async def stop(self) -> None:
        """Stop the queue processor and release any held exclusive lock."""
        self._accepting_work = False
        self._running = False
        self._exclusive_active.set()  # release any waiters
        self._transport_gate.set()  # release any gate-blocked waiters
        for completion in tuple(self._pending_completions):
            completion.cancel()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        self._task = None

    async def enqueue(
        self,
        work: Callable[[], Awaitable[None]],
        priority: Priority = Priority.NORMAL,
        *,
        skip_if_saga_active: bool = False,
        dedup_key: str | None = None,
        completion: asyncio.Future[None] | None = None,
        expires: bool = True,
        invalidate_reads_before_generation: int | None = None,
    ) -> None:
        """Add work to the queue.

        If skip_if_saga_active=True and a saga is active, the item is dropped
        silently. This is correct for HA coordinator polls — they must not
        accumulate during a multi-minute map sync.

        If dedup_key is given and an item with that key is already pending,
        the new item is silently dropped. Use for idempotent commands like
        RPT_START that should only be queued once at a time.
        """
        # EMERGENCY is never skipped or blocked
        if priority == Priority.EMERGENCY:
            skip_if_saga_active = False
            expires = False

        if skip_if_saga_active and self.is_saga_active and priority > Priority.EXCLUSIVE:
            return

        if dedup_key is not None and dedup_key in self._pending_dedup_keys:
            return

        self._seq += 1
        item = _QueueItem(
            priority=int(priority),
            sequence=self._seq,
            work=work,
            skip_if_saga_active=skip_if_saga_active,
            dedup_key=dedup_key,
            completion=completion,
            expires=expires,
            invalidate_reads_before_generation=invalidate_reads_before_generation,
        )
        if dedup_key is not None:
            self._pending_dedup_keys.add(dedup_key)
        await self._queue.put(item)

    async def enqueue_saga(
        self,
        saga: Saga,
        broker: DeviceMessageBroker,
        on_complete: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Enqueue a saga as an exclusive blocking operation."""
        generation = self._interruptible_generation

        async def _run() -> None:
            if saga.interruptible and generation < self._minimum_interruptible_generation:
                return
            self._exclusive_active.clear()
            try:
                if saga.interruptible:
                    self._active_interruptible_task = asyncio.current_task()
                    self._active_interruptible_requeue = lambda: self.enqueue_saga(saga, broker, on_complete)
                if self.on_saga_start is not None:
                    try:
                        await self.on_saga_start()
                    except Exception:
                        _logger.exception("on_saga_start callback failed for saga '%s'", saga.name)
                if saga.interruptible and generation < self._minimum_interruptible_generation:
                    return
                saga.device_name = self._device_name
                try:
                    saga_task = asyncio.create_task(saga.execute(broker))
                    await saga_task
                except asyncio.CancelledError:
                    if saga.interruptible and self._running:
                        _logger.debug(
                            "DeviceCommandQueue[%s]: interrupted read saga '%s'",
                            self._device_name,
                            saga.name,
                        )
                        return
                    raise
                except Exception as exc:
                    # GatewayTimeoutException and DeviceOfflineException are
                    # expected operational errors handled by the _process retry
                    # loop — logging them here as "unhandled" is misleading noise.
                    if not isinstance(
                        exc, (GatewayTimeoutException, DeviceOfflineException, NoTransportAvailableError)
                    ):
                        _logger.exception("Saga '%s' raised an unhandled exception", saga.name)
                    raise
                if on_complete is not None:
                    try:
                        await on_complete()
                    except Exception:
                        _logger.exception("on_complete callback failed for saga '%s'", saga.name)
            finally:
                # Always release the exclusive lock and clear the work-task pointer,
                # even on cancellation or unhandled exception — otherwise the queue deadlocks.
                self._exclusive_active.set()
                if saga.interruptible:
                    self._active_interruptible_task = None
                    self._active_interruptible_requeue = None
                if self.on_saga_end is not None:
                    try:
                        await self.on_saga_end()
                    except Exception:
                        _logger.exception("on_saga_end callback failed for saga '%s'", saga.name)

        await self.enqueue(_run, priority=Priority.EXCLUSIVE, expires=False)

    async def run_after_preempting_reads(self, work: Callable[[], Awaitable[None]]) -> None:
        """Run a user command after cancelling or discarding read-only sagas.

        The command is enqueued before the active read is cancelled. It therefore
        runs after any non-interruptible write already in progress and before reads
        queued by later polling ticks.
        """
        if not self._accepting_work:
            raise asyncio.CancelledError
        self._interruptible_generation += 1
        command_generation = self._interruptible_generation
        completed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._pending_completions.add(completed)
        completed.add_done_callback(self._pending_completions.discard)
        await self.enqueue(
            work,
            priority=Priority.PREEMPTING,
            completion=completed,
            invalidate_reads_before_generation=command_generation,
        )
        task = self._active_interruptible_task
        restore_read = None
        if task is not None and not task.done():
            restore_read = self._active_interruptible_requeue
            task.cancel()
        try:
            await completed
        except BaseException:
            completed.cancel()
            if restore_read is not None and self._accepting_work:
                try:
                    await asyncio.shield(restore_read())
                except Exception:
                    _logger.exception(
                        "DeviceCommandQueue[%s]: failed to restore preempted read saga",
                        self._device_name,
                    )
            raise

    async def _execute_item(self, item: _QueueItem) -> None:
        """Run one item with retry and completion-cancellation ownership."""
        for attempt in range(1, _GATEWAY_TIMEOUT_MAX + 1):
            if item.completion is not None and item.completion.cancelled():
                raise asyncio.CancelledError
            try:
                work_task = asyncio.create_task(item.work())

                def _cancel_work(
                    completion: asyncio.Future[None],
                    task: asyncio.Task[None] = work_task,
                ) -> None:
                    if completion.cancelled() and not task.done():
                        task.cancel()

                if item.completion is not None:
                    item.completion.add_done_callback(_cancel_work)
                try:
                    await work_task
                finally:
                    if item.completion is not None:
                        item.completion.remove_done_callback(_cancel_work)
                if item.invalidate_reads_before_generation is not None:
                    self._minimum_interruptible_generation = max(
                        self._minimum_interruptible_generation,
                        item.invalidate_reads_before_generation,
                    )
                if item.completion is not None and not item.completion.done():
                    item.completion.set_result(None)
                return
            except GatewayTimeoutException:
                if attempt < _GATEWAY_TIMEOUT_MAX:
                    _logger.warning(
                        "DeviceCommandQueue[%s]: gateway timeout (attempt %d/%d) — retrying",
                        self._device_name,
                        attempt,
                        _GATEWAY_TIMEOUT_MAX,
                    )
                else:
                    _logger.warning(
                        "DeviceCommandQueue[%s]: gateway timeout after %d attempts — dropping command",
                        self._device_name,
                        attempt,
                    )
                    raise

    async def _process(self) -> None:
        """Queue processor loop — runs as an asyncio background task."""
        while self._running:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=0.4)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                # Deduplication suppresses queued duplicates only. A new trigger
                # that arrives while work is executing represents independent work.
                if item.dedup_key is not None:
                    self._pending_dedup_keys.discard(item.dedup_key)

                if item.completion is not None and item.completion.cancelled():
                    continue

                # Drop commands that have waited longer than _COMMAND_TTL without being
                # dispatched.  Checked here — before any lock/gate waits — so stale
                # commands don't execute after a long reconnect or saga pause.
                # EMERGENCY items (e-stop, return-to-dock) are exempt, and so are
                # EXCLUSIVE sagas: a queued map/plan sync routinely waits out a
                # multi-minute saga ahead of it, and silently dropping it means
                # on_complete never fires and nothing upstream learns the sync
                # didn't happen.
                if item.expires:
                    age = time.monotonic() - item.enqueued_at
                    if age > _COMMAND_TTL:
                        _logger.debug(
                            "DeviceCommandQueue[%s]: command expired after %.0fs — dropping",
                            self._device_name,
                            age,
                        )
                        if item.completion is not None and not item.completion.done():
                            item.completion.set_exception(TimeoutError("queued command expired"))
                        continue

                # Non-emergency items yield to an active exclusive op
                if item.priority > Priority.EXCLUSIVE:
                    await self._exclusive_active.wait()

                # Re-check skip condition after waiting
                if item.skip_if_saga_active and self.is_saga_active and item.priority > Priority.EXCLUSIVE:
                    continue

                # Non-emergency items hold here while the MQTT transport is reconnecting.
                # This prevents dispatching commands whose responses can't be received
                # because the MQTT subscription isn't active yet.  EMERGENCY items
                # (e-stop, return-to-dock) bypass the gate unconditionally.
                if item.priority > Priority.EMERGENCY:
                    await self._transport_gate.wait()
                    if item.completion is not None and item.completion.cancelled():
                        continue

                await self._execute_item(item)
            except asyncio.CancelledError:
                if item.completion is not None and not item.completion.done():
                    item.completion.cancel()
                current = asyncio.current_task()
                if not self._running or (current is not None and current.cancelling()):
                    break
                # A completion future canceled its active child work. The queue
                # itself remains healthy and must continue with the next item.
                continue
            except Exception as exc:
                # Expected during transport churn — quiet by default.  Recovery
                # is automatic: mqtt_reported_offline clears on inbound frames,
                # BLE rearms via the availability listener.  No retry loop or
                # caller-side gate needed; just don't pollute the log.
                if isinstance(
                    exc,
                    (
                        GatewayTimeoutException,
                        NoTransportAvailableError,
                        DeviceOfflineException,
                        DeviceUnboundException,
                    ),
                ):
                    _logger.debug("DeviceCommandQueue[%s]: %s", self._device_name, exc)
                elif isinstance(
                    exc,
                    (
                        AuthError,  # includes ReLoginRequiredError
                        SagaFailedError,
                        TooManyRequestsException,
                        TransportRateLimitedError,
                        TransportError,
                    ),
                ):
                    _logger.warning("DeviceCommandQueue[%s]: %s", self._device_name, exc)
                else:
                    _logger.exception("DeviceCommandQueue[%s]: unhandled error in work item", self._device_name)
                if self.on_critical_error is not None and isinstance(exc, (AuthError, SagaFailedError)):
                    try:
                        await self.on_critical_error(exc)
                    except Exception:
                        _logger.exception("on_critical_error callback failed")
                if item.completion is not None and not item.completion.done():
                    item.completion.set_exception(exc)
            finally:
                with contextlib.suppress(ValueError):
                    self._queue.task_done()

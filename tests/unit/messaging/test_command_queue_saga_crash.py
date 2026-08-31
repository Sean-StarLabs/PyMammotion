"""Regression tests for C2: saga crash / cancellation must release the exclusive lock.

If `saga.execute(broker)` raises an unhandled exception the ``_exclusive_active``
must still be set again so subsequent commands can run on the same device queue.
Cancellation is tested via ``stop()`` + ``start()`` — there is no separate work-item
sub-task in the current design (work runs directly in the processor task).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from pymammotion.aliyun.exceptions import GatewayTimeoutException
from pymammotion.messaging.broker import DeviceMessageBroker
from pymammotion.messaging.command_queue import DeviceCommandQueue, Priority
from pymammotion.messaging.saga import Saga
from pymammotion.transport.base import NoTransportAvailableError, ReLoginRequiredError


class _CrashingSaga(Saga):
    """Saga whose ``execute()`` raises immediately via ``_run``."""

    name = "crashing"
    max_attempts = 1
    total_timeout = 5.0

    async def _run(self, broker: DeviceMessageBroker) -> None:
        raise RuntimeError("boom")


class _SlowSaga(Saga):
    """Saga that sleeps long enough to be cancelled mid-execution."""

    name = "slow"
    max_attempts = 1
    total_timeout = 60.0

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()

    async def _run(self, broker: DeviceMessageBroker) -> None:
        self.started.set()
        await asyncio.sleep(60.0)


class _BlockingSaga(Saga):
    """Saga held by a test-controlled release event."""

    name = "blocking"

    def __init__(self, *, interruptible: bool) -> None:
        super().__init__()
        self.interruptible = interruptible
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.finished = asyncio.Event()

    async def _run(self, broker: DeviceMessageBroker) -> None:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        self.finished.set()


class _TrackingReadSaga(Saga):
    """Read saga that records its execution order."""

    name = "tracking_read"
    interruptible = True

    def __init__(self, order: list[str], label: str) -> None:
        super().__init__()
        self._order = order
        self._label = label

    async def _run(self, broker: DeviceMessageBroker) -> None:
        self._order.append(self._label)


async def test_saga_exception_releases_exclusive_lock() -> None:
    """A saga that raises inside execute() must not leave the queue blocked."""
    q = DeviceCommandQueue(device_name="dev-crash")
    broker = DeviceMessageBroker()
    q.start()
    try:
        ran: list[int] = []

        async def follow_up() -> None:
            ran.append(1)

        await q.enqueue_saga(_CrashingSaga(), broker)
        # Give the queue processor a chance to run the (failing) saga.
        await asyncio.sleep(0.2)

        # Lock must be released even though the saga raised.
        assert q.is_saga_active is False, "exclusive lock not released after saga crash"

        # A subsequent NORMAL command must execute (proves queue is not stuck).
        await q.enqueue(follow_up, priority=Priority.NORMAL)
        await asyncio.sleep(0.2)
        assert ran == [1], "follow-up command did not run — queue is deadlocked"
    finally:
        await q.stop()


async def test_saga_stop_releases_exclusive_lock() -> None:
    """stop() while a saga is running must release the exclusive lock.

    Work runs directly in the processor task, so stop() cancels the processor,
    which propagates CancelledError through the saga's finally block and releases
    the lock. A fresh start() + enqueue must then succeed.
    """
    q = DeviceCommandQueue(device_name="dev-cancel")
    broker = DeviceMessageBroker()
    q.start()

    slow = _SlowSaga()
    await q.enqueue_saga(slow, broker)

    # Wait until the saga has actually started running before stopping.
    await asyncio.wait_for(slow.started.wait(), timeout=2.0)
    assert q.is_saga_active is True

    # Stop the queue — cancels the processor task, which propagates through the saga.
    await q.stop()

    # Lock must be released after stop.
    assert q.is_saga_active is False, "exclusive lock not released after stop"

    # Restart and verify follow-up commands actually run.
    ran: list[int] = []

    async def follow_up() -> None:
        ran.append(1)

    q.start()
    try:
        await q.enqueue(follow_up, priority=Priority.NORMAL)
        await asyncio.sleep(0.2)
        assert ran == [1], "follow-up command did not run after restart"
    finally:
        await q.stop()


async def test_on_saga_start_cancellation_releases_exclusive_lock() -> None:
    """If on_saga_start raises CancelledError, the lock must still release.

    on_saga_start is awaited *after* `_exclusive_active.clear()` but *inside*
    the try/finally that re-sets it.  CancelledError in that window must not
    deadlock the queue.
    """
    q = DeviceCommandQueue(device_name="dev-cb-cancel")
    broker = DeviceMessageBroker()

    class _QuickSaga(Saga):
        name = "quick"

        async def _run(self, b: DeviceMessageBroker) -> None:
            return None

    async def cancelling_start() -> None:
        raise asyncio.CancelledError

    q.on_saga_start = cancelling_start
    q.start()
    try:
        await q.enqueue_saga(_QuickSaga(), broker)
        await asyncio.sleep(0.2)

        assert q.is_saga_active is False, "exclusive lock not released after on_saga_start cancel"
    finally:
        await q.stop()


async def test_saga_ownership_covers_completion_callback() -> None:
    """Residual frames remain owned until the completion result is published."""
    q = DeviceCommandQueue(device_name="dev-complete")
    broker = DeviceMessageBroker()
    observed: list[bool] = []
    completed = asyncio.Event()

    class _QuickSaga(Saga):
        name = "quick"

        async def _run(self, _broker: DeviceMessageBroker) -> None:
            return None

    async def on_complete() -> None:
        observed.append(q.is_saga_active)
        completed.set()

    q.start()
    try:
        await q.enqueue_saga(_QuickSaga(), broker, on_complete=on_complete)
        await asyncio.wait_for(completed.wait(), timeout=2.0)
        await asyncio.sleep(0)

        assert observed == [True]
        assert q.is_saga_active is False
    finally:
        await q.stop()


async def test_user_command_cancels_active_read_saga() -> None:
    """A user command preempts an active read and runs after its cleanup."""
    q = DeviceCommandQueue(device_name="dev-read")
    broker = DeviceMessageBroker()
    saga = _BlockingSaga(interruptible=True)
    command_ran = asyncio.Event()
    q.start()
    try:
        await q.enqueue_saga(saga, broker)
        await asyncio.wait_for(saga.started.wait(), timeout=2.0)

        await q.run_after_preempting_reads(lambda: _set_event(command_ran))

        assert saga.cancelled.is_set()
        assert command_ran.is_set()
        assert q.is_saga_active is False
    finally:
        await q.stop()


async def test_user_command_waits_for_non_interruptible_write() -> None:
    """A user command cannot interleave with an active write saga."""
    q = DeviceCommandQueue(device_name="dev-write")
    broker = DeviceMessageBroker()
    saga = _BlockingSaga(interruptible=False)
    command_ran = asyncio.Event()
    q.start()
    try:
        await q.enqueue_saga(saga, broker)
        await asyncio.wait_for(saga.started.wait(), timeout=2.0)
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.sleep(0)

        assert not command_ran.is_set()
        assert not saga.cancelled.is_set()

        saga.release.set()
        await asyncio.wait_for(command, timeout=2.0)
        assert saga.finished.is_set()
        assert command_ran.is_set()
    finally:
        await q.stop()


async def test_user_command_orders_before_new_read_sagas() -> None:
    """Stale reads are discarded and later reads follow the user command."""
    q = DeviceCommandQueue(device_name="dev-order")
    broker = DeviceMessageBroker()
    order: list[str] = []
    await q.enqueue_saga(_TrackingReadSaga(order, "stale"), broker)

    command = asyncio.create_task(q.run_after_preempting_reads(lambda: _append(order, "command")))
    await asyncio.sleep(0)
    await q.enqueue_saga(_TrackingReadSaga(order, "new"), broker)
    q.start()
    try:
        await asyncio.wait_for(command, timeout=2.0)
        await asyncio.sleep(0.1)
        assert order == ["command", "new"]
    finally:
        await q.stop()


async def test_preemption_during_saga_start_discards_read() -> None:
    """A read invalidated while its start hook awaits never begins transfer I/O."""
    q = DeviceCommandQueue(device_name="dev-start-race")
    broker = DeviceMessageBroker()
    saga = _TrackingReadSaga([], "read")
    hook_started = asyncio.Event()
    release_hook = asyncio.Event()
    command_ran = asyncio.Event()

    async def on_start() -> None:
        hook_started.set()
        await release_hook.wait()

    q.on_saga_start = on_start
    q.start()
    try:
        await q.enqueue_saga(saga, broker)
        await asyncio.wait_for(hook_started.wait(), timeout=2.0)
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.sleep(0)
        release_hook.set()
        await asyncio.wait_for(command, timeout=2.0)

        assert saga._order == []  # noqa: SLF001
        assert command_ran.is_set()
    finally:
        await q.stop()


async def test_preempting_command_retries_gateway_timeouts() -> None:
    """Preempting commands retain the queue's three-attempt retry behavior."""
    q = DeviceCommandQueue(device_name="dev-retry")
    attempts = 0

    async def work() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise GatewayTimeoutException(20056, "iot-1")

    q.start()
    try:
        await q.run_after_preempting_reads(work)
        assert attempts == 3
    finally:
        await q.stop()


async def test_preempting_command_reports_final_gateway_timeout() -> None:
    """The caller receives the timeout after the queue exhausts its retries."""
    q = DeviceCommandQueue(device_name="dev-retry-fail")
    attempts = 0

    async def work() -> None:
        nonlocal attempts
        attempts += 1
        raise GatewayTimeoutException(20056, "iot-1")

    q.start()
    try:
        with pytest.raises(GatewayTimeoutException):
            await q.run_after_preempting_reads(work)
        assert attempts == 3
    finally:
        await q.stop()


async def test_cancelled_preempting_command_does_not_run_later() -> None:
    """Caller cancellation invalidates work queued behind a write saga."""
    q = DeviceCommandQueue(device_name="dev-cancel-command")
    broker = DeviceMessageBroker()
    saga = _BlockingSaga(interruptible=False)
    command_ran = asyncio.Event()
    q.start()
    try:
        await q.enqueue_saga(saga, broker)
        await asyncio.wait_for(saga.started.wait(), timeout=2.0)
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.sleep(0)

        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        saga.release.set()
        await asyncio.wait_for(saga.finished.wait(), timeout=2.0)
        await asyncio.sleep(0.05)

        assert not command_ran.is_set()
    finally:
        await q.stop()


async def test_cancelled_preempting_command_does_not_run_after_reconnect() -> None:
    """Cancellation while the processor waits at the gate prevents a later send."""
    q = DeviceCommandQueue(device_name="dev-cancel-gate")
    command_ran = asyncio.Event()
    q.pause_for_reconnect()
    q.start()
    try:
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.sleep(0)

        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        q.resume_after_reconnect()
        await asyncio.sleep(0.05)

        assert not command_ran.is_set()
    finally:
        await q.stop()


async def test_cancelled_preempting_command_cancels_active_work() -> None:
    """Caller cancellation propagates into an already-dispatched request."""
    q = DeviceCommandQueue(device_name="dev-cancel-active")
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def work() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    q.start()
    try:
        command = asyncio.create_task(q.run_after_preempting_reads(work))
        await asyncio.wait_for(started.wait(), timeout=2.0)
        command.cancel()

        with pytest.raises(asyncio.CancelledError):
            await command
        await asyncio.wait_for(cancelled.wait(), timeout=2.0)

        follow_up = asyncio.Event()
        await q.enqueue(lambda: _set_event(follow_up))
        await asyncio.wait_for(follow_up.wait(), timeout=2.0)
    finally:
        await q.stop()


async def test_cancelled_preemption_restores_active_read() -> None:
    """A speculative read cancellation is undone when the command never runs."""
    q = DeviceCommandQueue(device_name="dev-restore-read")
    broker = DeviceMessageBroker()
    runs = 0
    first_started = asyncio.Event()
    first_cancelled = asyncio.Event()
    restarted = asyncio.Event()
    release = asyncio.Event()
    command_ran = asyncio.Event()

    class _RestartableRead(Saga):
        name = "restartable"
        interruptible = True

        async def _run(self, _broker: DeviceMessageBroker) -> None:
            nonlocal runs
            runs += 1
            if runs == 1:
                first_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    first_cancelled.set()
                    raise
            restarted.set()
            await release.wait()

    q.start()
    try:
        await q.enqueue_saga(_RestartableRead(), broker)
        await asyncio.wait_for(first_started.wait(), timeout=2.0)
        q.pause_for_reconnect()
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.wait_for(first_cancelled.wait(), timeout=2.0)

        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        q.resume_after_reconnect()

        await asyncio.wait_for(restarted.wait(), timeout=2.0)
        assert not command_ran.is_set()
        release.set()
    finally:
        await q.stop()


async def test_failed_preemption_restores_active_read() -> None:
    """A read canceled for a command is restored when delivery fails."""
    q = DeviceCommandQueue(device_name="dev-restore-failed-read")
    broker = DeviceMessageBroker()
    runs = 0
    first_started = asyncio.Event()
    restarted = asyncio.Event()
    release = asyncio.Event()

    class _RestartableRead(Saga):
        name = "restartable"
        interruptible = True

        async def _run(self, _broker: DeviceMessageBroker) -> None:
            nonlocal runs
            runs += 1
            if runs == 1:
                first_started.set()
                await asyncio.Event().wait()
            restarted.set()
            await release.wait()

    async def fail() -> None:
        raise NoTransportAvailableError("offline")

    q.start()
    try:
        await q.enqueue_saga(_RestartableRead(), broker)
        await asyncio.wait_for(first_started.wait(), timeout=2.0)

        with pytest.raises(NoTransportAvailableError):
            await q.run_after_preempting_reads(fail)

        await asyncio.wait_for(restarted.wait(), timeout=2.0)
        release.set()
    finally:
        await q.stop()


async def test_cancelled_preemption_keeps_older_queued_read() -> None:
    """Queued reads are invalidated only after the user command succeeds."""
    q = DeviceCommandQueue(device_name="dev-keep-read")
    broker = DeviceMessageBroker()
    order: list[str] = []
    await q.enqueue_saga(_TrackingReadSaga(order, "read"), broker)
    q.pause_for_reconnect()
    command = asyncio.create_task(q.run_after_preempting_reads(lambda: _append(order, "command")))
    await asyncio.sleep(0)
    q.start()
    try:
        await asyncio.sleep(0)
        command.cancel()
        with pytest.raises(asyncio.CancelledError):
            await command
        q.resume_after_reconnect()
        await asyncio.sleep(0.1)

        assert order == ["read"]
    finally:
        await q.stop()


async def test_preempting_command_expires_behind_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preemption priority does not exempt ordinary user commands from TTL."""
    monkeypatch.setattr("pymammotion.messaging.command_queue._COMMAND_TTL", 0.01)
    q = DeviceCommandQueue(device_name="dev-expire")
    broker = DeviceMessageBroker()
    saga = _BlockingSaga(interruptible=False)
    command_ran = asyncio.Event()
    q.start()
    try:
        await q.enqueue_saga(saga, broker)
        await asyncio.wait_for(saga.started.wait(), timeout=2.0)
        command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(command_ran)))
        await asyncio.sleep(0.02)
        saga.release.set()

        with pytest.raises(TimeoutError, match="queued command expired"):
            await command
        assert not command_ran.is_set()
    finally:
        await q.stop()


async def test_stop_cancels_preemption_waiting_behind_write_saga() -> None:
    """Shutdown releases a caller queued behind non-interruptible work."""
    q = DeviceCommandQueue(device_name="dev-stop-write")
    saga = _BlockingSaga(interruptible=False)
    q.start()
    await q.enqueue_saga(saga, DeviceMessageBroker())
    await asyncio.wait_for(saga.started.wait(), timeout=2.0)
    command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(asyncio.Event())))
    await asyncio.sleep(0)

    await q.stop()

    with pytest.raises(asyncio.CancelledError):
        await command


async def test_stop_cancels_preemption_waiting_on_transport_gate() -> None:
    """Shutdown releases a caller held by reconnect gating."""
    q = DeviceCommandQueue(device_name="dev-stop-gate")
    q.pause_for_reconnect()
    q.start()
    command = asyncio.create_task(q.run_after_preempting_reads(lambda: _set_event(asyncio.Event())))
    await asyncio.sleep(0)

    await q.stop()

    with pytest.raises(asyncio.CancelledError):
        await command


async def test_preempting_auth_error_notifies_critical_error_and_caller() -> None:
    """Preempting sends retain ordinary queue auth-error propagation."""
    q = DeviceCommandQueue(device_name="dev-auth")
    error = ReLoginRequiredError("account", "expired")
    q.on_critical_error = AsyncMock()

    async def fail() -> None:
        raise error

    q.start()
    try:
        with pytest.raises(ReLoginRequiredError) as exc_info:
            await q.run_after_preempting_reads(fail)
        assert exc_info.value is error
        q.on_critical_error.assert_awaited_once_with(error)
    finally:
        await q.stop()


async def _set_event(event: asyncio.Event) -> None:
    event.set()


async def _append(values: list[str], value: str) -> None:
    values.append(value)

"""Task-bound transactional fetch for the mower's live dynamics line."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pymammotion.data.model.hash_list import PathType
from pymammotion.messaging.common_data_saga import CommonDataSaga
from pymammotion.transport.base import SagaFailedError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymammotion.messaging.broker import DeviceMessageBroker

_logger = logging.getLogger(__name__)


class DynamicsLineSaga(CommonDataSaga):
    """Fetch one complete dynamics line bound to the task active at execution."""

    name = "dynamics_line_fetch"
    interruptible = True
    # Type-18 responses have no request token. An immediate restore could consume
    # frames from the cancelled transfer, so let the next poll start a fresh read.
    restore_on_failed_preemption = False
    # Type-18 responses carry no request token. Let the regular 10-second poll
    # retry instead of issuing an immediate request that could consume a late
    # response from the timed-out transfer.
    max_attempts = 1

    def __init__(
        self,
        command_builder: Any,
        send_command: Callable[[bytes], Awaitable[None]],
        get_mow_session_id: Callable[[], int],
    ) -> None:
        """Initialise the fetch with an execution-time task identity provider."""
        super().__init__(
            command_builder=command_builder,
            send_command=send_command,
            action=8,
            type=PathType.DYNAMICS_LINE,
        )
        self._get_mow_session_id = get_mow_session_id
        self.mow_session_id = 0
        self.transfer_complete = False

    async def execute(self, broker: DeviceMessageBroker) -> None:
        """Treat an ordinary poll timeout as missing data, not a device fault."""
        try:
            await super().execute(broker)
        except SagaFailedError:
            self.transfer_complete = False
            _logger.debug("Dynamics-line poll did not complete; waiting for the next cadence")

    async def _run(self, broker: DeviceMessageBroker) -> None:
        """Capture task identity immediately before issuing transfer I/O."""
        self.mow_session_id = self._get_mow_session_id()
        if self.mow_session_id == 0:
            self.result = []
            return
        await super()._run(broker)
        self.transfer_complete = True

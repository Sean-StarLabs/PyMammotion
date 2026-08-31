"""Task-bound transactional fetch for the mower's live dynamics line."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pymammotion.data.model.hash_list import PathType
from pymammotion.messaging.common_data_saga import CommonDataSaga

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymammotion.messaging.broker import DeviceMessageBroker


class DynamicsLineSaga(CommonDataSaga):
    """Fetch one complete dynamics line bound to the task active at execution."""

    name = "dynamics_line_fetch"
    interruptible = True
    # Type-18 responses carry no request token. Let the regular 10-second poll
    # retry instead of issuing an immediate request that could consume a late
    # response from the timed-out transfer.
    max_attempts = 1

    def __init__(
        self,
        command_builder: Any,
        send_command: Callable[[bytes], Awaitable[None]],
        get_task_path_hash: Callable[[], int],
    ) -> None:
        """Initialise the fetch with an execution-time task identity provider."""
        super().__init__(
            command_builder=command_builder,
            send_command=send_command,
            action=8,
            type=PathType.DYNAMICS_LINE,
        )
        self._get_task_path_hash = get_task_path_hash
        self.task_path_hash = 0
        self.transfer_complete = False

    async def _run(self, broker: DeviceMessageBroker) -> None:
        """Capture task identity immediately before issuing transfer I/O."""
        self.task_path_hash = self._get_task_path_hash()
        if self.task_path_hash == 0:
            self.result = []
            return
        await super()._run(broker)
        self.transfer_complete = True

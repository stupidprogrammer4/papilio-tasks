from typing import Protocol

from taskiq import ScheduledTask

from .base import MutableSourceContract


class MemorySourceContract(MutableSourceContract, Protocol):
    """Memory supports lookup and replacement within one event loop."""

    async def get_schedule(self, id: str) -> ScheduledTask: ...

    async def replace_schedule(self, schedule: ScheduledTask) -> None: ...

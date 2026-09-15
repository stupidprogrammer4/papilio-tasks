from typing import Protocol, runtime_checkable

from taskiq import ScheduledTask, ScheduleSource


class SourceContract(Protocol):
    @property
    def native(self) -> ScheduleSource: ...

    async def get_schedules(self) -> list[ScheduledTask]:
        """Return the native beat feed, not a complete schedule inventory."""
        ...


@runtime_checkable
class MutableSourceContract(SourceContract, Protocol):
    """Sources that support adding and deleting schedules."""

    async def add_schedule(self, schedule: ScheduledTask) -> None: ...

    async def delete_schedule(self, id: str) -> None: ...

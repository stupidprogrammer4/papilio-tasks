from taskiq import ScheduledTask, ScheduleSource

from .contracts.base import MutableSourceContract, SourceContract


class Source(SourceContract):
    """Read adapter for a native Taskiq source, including label sources.

    Pass ``native`` to TaskiqScheduler. The application owns startup/shutdown.
    No editing capability is inferred from Taskiq's optional method stubs.
    """

    def __init__(self, native: ScheduleSource) -> None:
        self._native = native

    @property
    def native(self) -> ScheduleSource:
        return self._native

    async def get_schedules(self) -> list[ScheduledTask]:
        return await self.native.get_schedules()


class MutableSource(Source, MutableSourceContract):
    """Delegate mutations to a native source that supports add/delete."""

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        await self.native.add_schedule(schedule)

    async def delete_schedule(self, id: str) -> None:
        await self.native.delete_schedule(id)

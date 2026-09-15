from collections.abc import Sequence

from taskiq import ScheduledTask, ScheduleSource
from taskiq.exceptions import ScheduledTaskCancelledError

from ..base import MutableSource
from ..contracts.memory import MemorySourceContract


class _Schedules(ScheduleSource):
    def __init__(self, schedules: Sequence[ScheduledTask]) -> None:
        self.items: dict[str, ScheduledTask] = {}
        for schedule in schedules:
            if schedule.schedule_id in self.items:
                raise ValueError(f"Duplicate schedule: {schedule.schedule_id}")
            self.items[schedule.schedule_id] = schedule.model_copy(deep=True)

    async def get_schedules(self) -> list[ScheduledTask]:
        return [item.model_copy(deep=True) for item in self.items.values()]

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        if schedule.schedule_id in self.items:
            raise ValueError(f"Duplicate schedule: {schedule.schedule_id}")
        self.items[schedule.schedule_id] = schedule.model_copy(deep=True)

    async def delete_schedule(self, schedule_id: str) -> None:
        self.items.pop(schedule_id, None)

    def pre_send(self, task: ScheduledTask) -> None:
        if not self._current(task):
            raise ScheduledTaskCancelledError

    def _current(self, task: ScheduledTask) -> bool:
        current = self.items.get(task.schedule_id)
        if current is None:
            return False
        # Taskiq adds this delivery label between pre_send and post_send.
        exclude = {"labels": {"schedule_id"}}
        return current.model_dump(exclude=exclude) == task.model_dump(
            exclude=exclude
        )

    def post_send(self, task: ScheduledTask) -> None:
        # A concurrent edit after pre_send must not be removed by this send.
        if task.time is not None and self._current(task):
            self.items.pop(task.schedule_id, None)


class MemorySource(MutableSource, MemorySourceContract):
    """Editable schedules for one process/event loop; not persistent.

    Mutations have no await points. Replacements are atomic in this event loop.
    A dispatch past pre_send can still publish its old input.
    """

    def __init__(self, schedules: Sequence[ScheduledTask] = ()) -> None:
        self._store = _Schedules(schedules)
        super().__init__(self._store)

    async def get_schedule(self, id: str) -> ScheduledTask:
        return self._store.items[id].model_copy(deep=True)

    async def replace_schedule(self, schedule: ScheduledTask) -> None:
        current = self._store.items[schedule.schedule_id]
        if current.task_name != schedule.task_name:
            raise ValueError("Cannot change a schedule's task")
        self._store.items[schedule.schedule_id] = schedule.model_copy(
            deep=True
        )

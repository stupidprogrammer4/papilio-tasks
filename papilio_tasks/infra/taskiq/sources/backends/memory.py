from collections.abc import Sequence

from taskiq import ScheduledTask, ScheduleSource
from taskiq.exceptions import ScheduledTaskCancelledError

from ..base import MutableSource
from ..contracts.memory import MemorySourceContract


class _Schedules(ScheduleSource):
    def __init__(self, schedules: Sequence[ScheduledTask]) -> None:
        self.items: dict[str, ScheduledTask] = {}
        self.feed: dict[str, ScheduledTask] = {}
        for schedule in schedules:
            if schedule.schedule_id in self.items:
                raise ValueError(f"Duplicate schedule: {schedule.schedule_id}")
            self._put(schedule)

    def _put(self, schedule: ScheduledTask) -> None:
        stored = schedule.model_copy(deep=True)
        task = ScheduledTask(
            schedule_id=stored.schedule_id,
            task_name=stored.task_name,
            task_id=stored.task_id,
            cron=stored.cron,
            cron_offset=stored.cron_offset,
            time=stored.time,
            interval=stored.interval,
            args=[],
            kwargs={},
            labels={},
        )
        self.items[stored.schedule_id] = stored
        self.feed[stored.schedule_id] = task

    async def get_schedules(self) -> list[ScheduledTask]:
        # Borrowed by Taskiq; refresh must not copy every stored payload.
        return list(self.feed.values())

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        if schedule.schedule_id in self.items:
            raise ValueError(f"Duplicate schedule: {schedule.schedule_id}")
        self._put(schedule)

    async def delete_schedule(self, schedule_id: str) -> None:
        self.items.pop(schedule_id, None)
        self.feed.pop(schedule_id, None)

    def pre_send(self, task: ScheduledTask) -> None:
        if not self._current(task):
            raise ScheduledTaskCancelledError
        # Reset each delivery: Taskiq/middleware may mutate its payload.
        stored = self.items[task.schedule_id].model_copy(deep=True)
        task.args, task.kwargs, task.labels = (
            stored.args,
            stored.kwargs,
            stored.labels,
        )

    def _current(self, task: ScheduledTask) -> bool:
        return self.feed.get(task.schedule_id) is task

    def post_send(self, task: ScheduledTask) -> None:
        # A concurrent edit after pre_send must not be removed by this send.
        if task.time is not None and self._current(task):
            self.items.pop(task.schedule_id, None)
            self.feed.pop(task.schedule_id, None)


class MemorySource(MutableSource, MemorySourceContract):
    """Editable schedules for one process/event loop; not persistent.

    Mutations have no await points. Replacements are atomic in this event loop.
    A dispatch past pre_send can still publish its old input.
    Public reads return independent data; native reads are a borrowed feed.
    """

    def __init__(self, schedules: Sequence[ScheduledTask] = ()) -> None:
        self._store = _Schedules(schedules)
        super().__init__(self._store)

    async def get_schedule(self, id: str) -> ScheduledTask:
        return self._store.items[id].model_copy(deep=True)

    async def get_schedules(self) -> list[ScheduledTask]:
        return [
            item.model_copy(deep=True) for item in self._store.items.values()
        ]

    async def replace_schedule(self, schedule: ScheduledTask) -> None:
        current = self._store.items[schedule.schedule_id]
        if current.task_name != schedule.task_name:
            raise ValueError("Cannot change a schedule's task")
        self._store._put(schedule)

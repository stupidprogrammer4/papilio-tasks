from datetime import datetime, timedelta
from typing import Any, ClassVar, Protocol

from taskiq.scheduler.created_schedule import CreatedSchedule
from taskiq.scheduler.scheduled_task import CronSpec

from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
)
from papilio_tasks.tools.retry import Retry


class SchedulerContract(Protocol):
    retry: ClassVar[Retry | None]

    async def run(self, *args: Any, **kwargs: Any) -> Any: ...

    @classmethod
    async def at(
        cls,
        source: MutableSourceContract,
        when: datetime,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]: ...

    @classmethod
    async def cron(
        cls,
        source: MutableSourceContract,
        expression: str | CronSpec,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]: ...

    @classmethod
    async def every(
        cls,
        source: MutableSourceContract,
        interval: int | timedelta,
        *args: Any,
        **kwargs: Any,
    ) -> CreatedSchedule[Any]: ...

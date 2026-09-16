from datetime import UTC, datetime, timedelta

import pytest
from taskiq import InMemoryBroker, ScheduledTask
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.schedule_sources import LabelScheduleSource

from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.infra.taskiq.sources.base import Source
from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
)
from papilio_tasks.infra.taskiq.sources.contracts.memory import (
    MemorySourceContract,
)


async def test_label_source_exposes_reading_without_mutation_contract():
    broker = InMemoryBroker()

    @broker.task(schedule=[{"cron": "* * * * *", "args": [7]}])
    async def report(value: int):
        return value

    source = Source(LabelScheduleSource(broker))
    assert not isinstance(source, MutableSourceContract)
    assert not hasattr(source, "add_schedule")
    await source.native.startup()
    try:
        schedules = await source.get_schedules()
        assert len(schedules) == 1
        assert schedules[0].task_name == report.task_name
        assert schedules[0].args == [7]
    finally:
        await source.native.shutdown()


async def test_memory_lookup_copy_conflicts_and_replacement():
    source: MemorySourceContract = MemorySource()
    assert isinstance(source, MutableSourceContract)
    schedule = ScheduledTask(
        schedule_id="report",
        task_name="report",
        labels={},
        args=[1],
        kwargs={},
        cron="* * * * *",
    )
    await source.add_schedule(schedule)
    schedule.args.append(2)
    stored = await source.get_schedule("report")
    assert stored.args == [1]
    stored.args.append(3)
    (await source.get_schedules())[0].args.append(4)
    assert (await source.get_schedule("report")).args == [1]
    with pytest.raises(ValueError, match="Duplicate"):
        await source.add_schedule(schedule)
    with pytest.raises(ValueError, match="Duplicate"):
        MemorySource([schedule, schedule])
    with pytest.raises(KeyError):
        await source.get_schedule("missing")
    with pytest.raises(KeyError):
        await source.replace_schedule(
            schedule.model_copy(update={"schedule_id": "missing"})
        )
    with pytest.raises(ValueError, match="Cannot change"):
        await source.replace_schedule(
            schedule.model_copy(update={"task_name": "different"})
        )
    await source.replace_schedule(stored)
    stored.args.append(5)
    assert (await source.get_schedule("report")).args == [1, 3]
    await source.delete_schedule("report")
    await source.delete_schedule("report")
    assert await source.get_schedules() == []


async def test_memory_stale_dispatch_and_one_shot_cleanup():
    source = MemorySource()
    at = datetime.now(UTC)
    old = ScheduledTask(
        schedule_id="report",
        task_name="report",
        labels={},
        args=[],
        kwargs={},
        time=at,
    )
    await source.add_schedule(old)
    source.native.pre_send(old)
    replacement = old.model_copy(update={"time": at + timedelta(hours=1)})
    await source.replace_schedule(replacement)
    # A send already past pre_send must not delete a subsequent replacement.
    source.native.post_send(old)
    assert await source.get_schedule("report") == replacement
    with pytest.raises(ScheduledTaskCancelledError):
        source.native.pre_send(old)
    source.native.pre_send(replacement)
    replacement.labels["schedule_id"] = "report"
    source.native.post_send(replacement)
    assert await source.get_schedules() == []
    with pytest.raises(ScheduledTaskCancelledError):
        source.native.pre_send(replacement)

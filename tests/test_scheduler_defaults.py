from datetime import timedelta

import pytest
from taskiq.schedule_sources import LabelScheduleSource

from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.retry import Retry


@pytest.fixture(params=["memory", "redis", "rabbit"])
def backend(request):
    # Keep optional transports isolated so Memory tests need no extras.
    if request.param == "redis":
        pytest.importorskip("taskiq_redis")
        from papilio_tasks.apps.schedulers.backends.redis import (
            RedisQueue,
            RedisScheduler,
            RedisStreamBroker,
        )
        from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar

        registrar = RedisRegistrar(RedisStreamBroker("redis://localhost"))
        return registrar, RedisScheduler, RedisQueue("reports")
    if request.param == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from papilio_tasks.apps.schedulers.backends.rabbit import (
            RabbitBroker,
            RabbitQueue,
            RabbitScheduler,
        )
        from papilio_tasks.apps.schedulers.registry.rabbit import (
            RabbitRegistrar,
        )

        registrar = RabbitRegistrar(
            RabbitBroker("amqp://guest:guest@localhost/")
        )
        return registrar, RabbitScheduler, RabbitQueue(name="reports")
    return Registrar(), Scheduler, None


async def test_defaults_reach_native_beat_with_queue_and_retry(backend):
    registrar, base, queue = backend

    class Report(base):
        schedule = [
            {"interval": 20},
            {
                "cron": "0 3 * * *",
                "cron_offset": "Asia/Tehran",
                "kwargs": {"account_id": 7},
            },
        ]
        retry = Retry(attempts=3, delay=1, errors=(Exception,))

        async def run(self, account_id: int = 0):
            return account_id

    if queue is not None:
        Report.queue = queue
    task = registrar.include(Report)
    source = LabelScheduleSource(registrar.broker.native)
    await source.startup()
    schedules = await source.get_schedules()
    assert len(schedules) == 2
    assert {s.task_name for s in schedules} == {task.task_name}
    assert schedules[0].interval == 20
    assert schedules[1].cron == "0 3 * * *"
    assert schedules[1].cron_offset == "Asia/Tehran"
    assert schedules[1].kwargs == {"account_id": 7}
    assert task.labels["max_retries"] == 3
    assert task.labels["retry_delay"] == 1
    if base is not Scheduler:
        assert task.labels["queue_name"] == "reports"


@pytest.mark.parametrize("override", [[], [{"interval": 60}]])
async def test_explicit_schedule_overrides_or_disables_default(
    backend, override
):
    registrar, base, queue = backend

    class Report(base):
        schedule = [{"interval": 20}]

        async def run(self):
            pass

    labels = {"schedule": override, "custom": "kept"}
    task = registrar.include(Report, labels=labels)
    source = LabelScheduleSource(registrar.broker.native)
    await source.startup()
    schedules = await source.get_schedules()
    assert [s.interval for s in schedules] == ([60] if override else [])
    assert task.labels["custom"] == "kept"
    task.labels["schedule"].append({"interval": 90})
    assert labels["schedule"] == override
    assert len(override) in (0, 1)
    assert Report.schedule == [{"interval": 20}]


def test_inherited_defaults_are_copied_between_jobs(backend):
    registrar, base, queue = backend

    class First(base):
        schedule = [{"interval": 20, "kwargs": {"ids": [7]}}]

        async def run(self, ids):
            pass

    class Second(First):
        pass

    first = registrar.include(First, labels={"custom": "kept"})
    second = registrar.include(Second)
    first.labels["schedule"][0]["kwargs"]["ids"].append(8)
    assert second.labels["schedule"][0]["kwargs"] == {"ids": [7]}
    assert First.schedule[0]["kwargs"] == {"ids": [7]}
    assert first.labels["custom"] == "kept"


async def test_without_default_accepts_independent_dynamic_schedules(backend):
    registrar, base, queue = backend

    class Price(base):
        async def run(self, asset_id: int):
            return asset_id

    task = registrar.include(Price)
    declared = LabelScheduleSource(registrar.broker.native)
    await declared.startup()
    assert await declared.get_schedules() == []
    assert "schedule" not in task.labels
    source = MemorySource()
    gold = await Price.every(source, timedelta(seconds=20), asset_id=1)
    silver = await Price.every(source, timedelta(seconds=30), asset_id=2)
    assert gold.schedule_id != silver.schedule_id
    stored_gold = await source.get_schedule(gold.schedule_id)
    stored_silver = await source.get_schedule(silver.schedule_id)
    assert stored_gold.interval == timedelta(seconds=20)
    assert stored_silver.interval == timedelta(seconds=30)
    assert stored_gold.kwargs == {"asset_id": 1}
    assert stored_silver.kwargs == {"asset_id": 2}
    await gold.unschedule()
    with pytest.raises(KeyError):
        await source.get_schedule(gold.schedule_id)
    assert await source.get_schedule(silver.schedule_id) is not None

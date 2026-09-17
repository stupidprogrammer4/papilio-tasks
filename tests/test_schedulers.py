import asyncio
import inspect
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from typing import get_type_hints

import pytest
from dishka import Provider, Scope
from pydantic import BaseModel
from taskiq.scheduler.created_schedule import CreatedSchedule
from taskiq.scheduler.scheduled_task import CronSpec

from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.apps.schedulers.application import (
    create_beat,
    create_broker,
)
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource


class Payload(BaseModel):
    value: int


def provider_for(cls):
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(cls)
    return provider


async def result(task):
    completed = await task.wait_result(timeout=5)
    if completed.is_err:
        raise completed.error
    return completed.return_value


async def test_memory_execution_signature_and_provider_instances():
    instances = []

    class Report(Scheduler):
        def __init__(self):
            instances.append(self)

        async def run(self, payload: Payload, *, factor: int = 2) -> int:
            return payload.value * factor

    original = dict(Report.run.__annotations__)
    registrar = Registrar()
    assert isinstance(registrar.broker, MemoryBroker)
    assert Registrar().broker is not registrar.broker
    task = registrar.include(Report, name="report", labels={"name": "label"})
    broker = create_broker(
        registrar=registrar, providers=[provider_for(Report)]
    )
    assert instances == []
    assert broker is registrar.broker.native
    assert task is Report.task()
    public = inspect.signature(task)
    assert public.replace(
        parameters=[
            p
            for p in public.parameters.values()
            if p.name != "dishka_container"
        ]
    ) == inspect.signature(Report.run).replace(
        parameters=list(inspect.signature(Report.run).parameters.values())[1:]
    )
    assert get_type_hints(task.original_func) == original
    assert Report.run.__annotations__ == original
    await broker.startup()
    try:
        assert await result(await Report.enqueue({"value": 3})) == 6
        assert await result(await task.kiq(Payload(value=4), factor=3)) == 12
        assert len(instances) == 2 and instances[0] is not instances[1]
        assert await Report().run(Payload(value=2)) == 4
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("kind", ["mixed", "variadic", "positional"])
async def test_run_argument_shapes_match_native_taskiq(kind, caplog):
    if kind == "mixed":

        async def plain(first, second: int, *, suffix="!"):
            return f"{first}:{second}{suffix}"

        async def run(self, first, second: int, *, suffix="!"):
            return f"{first}:{second}{suffix}"

        args, kwargs, expected = ["a", 3], {"suffix": "?"}, "a:3?"
    elif kind == "variadic":

        async def plain(*values: int, **options: str):
            return f"{sum(values)}:{options['suffix']}"

        async def run(self, *values: int, **options: str):
            return f"{sum(values)}:{options['suffix']}"

        args, kwargs, expected = [1, 2, 3], {"suffix": "ok"}, "6:ok"
    else:

        async def plain(first: int, /, second: int = 2):
            return first + second

        async def run(self, first: int, /, second: int = 2):
            return first + second

        args, kwargs, expected = [3], {}, 5

    cls = type("Report", (Scheduler,), {"run": run})
    registrar = Registrar()
    task = registrar.include(cls, name="report")
    native_task = registrar.broker.register(plain, name="plain")
    broker = create_broker(registrar=registrar, providers=[provider_for(cls)])
    await broker.startup()
    try:
        caplog.clear()
        assert await result(await native_task.kiq(*args, **kwargs)) == expected
        native_warnings = [
            r for r in caplog.records if r.levelname == "WARNING"
        ]
        caplog.clear()
        assert await result(await task.kiq(*args, **kwargs)) == expected
        adapted_warnings = [
            r for r in caplog.records if r.levelname == "WARNING"
        ]
        assert len(adapted_warnings) == len(native_warnings)
        assert all("AsyncContainer" not in r.message for r in adapted_warnings)
    finally:
        await broker.shutdown()


async def test_dishka_scopes_concurrency_failure_and_root_cleanup():
    opened, closed, root_closed = [], [], []

    class Root:
        pass

    class Service:
        def __init__(self, marker):
            self.marker = marker

    class Report(Scheduler):
        def __init__(self, service: Service, root: Root):
            self.service = service

        async def run(self, fail: bool = False):
            await asyncio.sleep(0)
            if fail:
                raise ValueError("business failure")
            return self.service.marker

    async def service():
        marker = len(opened)
        opened.append(marker)
        try:
            yield Service(marker)
        finally:
            closed.append(marker)

    async def root():
        try:
            yield Root()
        finally:
            root_closed.append(True)

    provider = provider_for(Report)
    provider.provide(service, provides=Service)
    provider.provide(root, provides=Root, scope=Scope.APP)
    registrar = Registrar()
    registrar.include(Report)
    broker = create_broker(registrar=registrar, providers=[provider])
    assert opened == []
    await broker.startup()
    try:
        tasks = await asyncio.gather(Report.enqueue(), Report.enqueue())
        assert sorted(await asyncio.gather(*(result(t) for t in tasks))) == [
            0,
            1,
        ]
        failed = await (await Report.enqueue(True)).wait_result(timeout=5)
        assert failed.is_err and isinstance(failed.error, ValueError)
        assert sorted(opened) == sorted(closed) == [0, 1, 2]
        assert broker.state.dishka_container_registry == {}
        assert root_closed == []
    finally:
        await broker.shutdown()
    assert root_closed == [True]


async def test_missing_scheduler_provider_is_not_implicitly_created():
    from dishka.exceptions import NoFactoryError

    class Report(Scheduler):
        async def run(self):
            return 1

    registrar = Registrar()
    registrar.include(Report)
    broker = create_broker(registrar=registrar)
    await broker.startup()
    try:
        failed = await (await Report.enqueue()).wait_result(timeout=5)
        assert isinstance(failed.error, NoFactoryError)
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()


def test_inclusion_errors_and_subclass_isolation():
    class Report(Scheduler):
        async def run(self):
            pass

    registrar = Registrar()
    with pytest.raises(ValueError, match="empty"):
        registrar.include(Report, name="")
    with pytest.raises(TypeError, match="implement async"):
        registrar.include(Scheduler)
    registrar.include(Report, name="report")
    with pytest.raises(ValueError, match="already registered"):
        Registrar().include(Report)

    class Child(Report):
        pass

    with pytest.raises(RuntimeError, match="not included"):
        Child.task()
    with pytest.raises(ValueError, match="already registered"):
        registrar.include(Child, name="report")
    registrar.include(Child, name="child")
    assert Child.task() is not Report.task()

    class NeedsDependency(Scheduler):
        def __init__(self, service):
            raise AssertionError("Do not construct at inclusion")

        async def run(self):
            pass

    registrar.include(NeedsDependency)

    class Reserved(Scheduler):
        async def run(self, dishka_container):
            pass

    with pytest.raises(TypeError, match="reserved"):
        registrar.include(Reserved)


async def test_native_scheduling_with_existing_source():
    executed = []

    class Report(Scheduler):
        async def run(self, value: int):
            executed.append(value)
            return value * 2

    registrar = Registrar(MemoryBroker(await_inplace=True))
    task = registrar.include(Report, name="report")
    broker = create_broker(
        registrar=registrar, providers=[provider_for(Report)]
    )
    source = MemorySource()
    at = datetime.now(UTC) + timedelta(minutes=1)
    scheduled = await task.schedule_by_time(source.native, at, 7)
    stored = (await source.get_schedules())[0]
    assert (stored.task_name, stored.args, stored.time) == ("report", [7], at)
    await scheduled.unschedule()
    assert await source.get_schedules() == []
    await Report.at(source, at, 9)
    beat = create_beat(broker, sources=[source])
    assert beat.broker is broker and beat.sources == [source.native]
    with pytest.raises(ValueError, match="already has"):
        create_broker(registrar=registrar)
    await broker.startup()
    try:
        await beat.on_ready(
            source.native, (await source.native.get_schedules())[0]
        )
        assert executed == [9]
        assert await source.get_schedules() == []
    finally:
        await broker.shutdown()


@pytest.mark.parametrize(
    ("method", "value", "field", "expected", "offset"),
    [
        (
            "at",
            datetime(2030, 1, 1, tzinfo=timezone(timedelta(hours=3))),
            "time",
            datetime(2030, 1, 1, tzinfo=timezone(timedelta(hours=3))),
            None,
        ),
        ("cron", "0 8 * * *", "cron", "0 8 * * *", None),
        (
            "cron",
            CronSpec(minutes=0, hours=8, offset="Asia/Tehran"),
            "cron",
            "0 8 * * *",
            "Asia/Tehran",
        ),
        ("every", 60, "interval", 60, None),
        (
            "every",
            timedelta(minutes=1),
            "interval",
            timedelta(minutes=1),
            None,
        ),
    ],
)
async def test_scheduler_helpers_preserve_native_plan(
    method, value, field, expected, offset
):
    class Report(Scheduler):
        async def run(self, payload: Payload, *, factor: int):
            return payload.value * factor

    labels = {"queue_name": "reports"}
    Registrar().include(Report, name="report", labels=labels)
    source, other = MemorySource(), MemorySource()
    schedule = getattr(Report, method)
    plan = await schedule(source, value, Payload(value=7), factor=3)
    assert isinstance(plan, CreatedSchedule)
    assert plan.source is source.native
    stored = await source.get_schedule(plan.schedule_id)
    assert stored.task_name == "report"
    assert stored.args == [{"value": 7}]
    assert stored.kwargs == {"factor": 3}
    assert stored.labels == labels
    assert getattr(stored, field) == expected
    assert stored.cron_offset == offset
    second = await schedule(other, value, Payload(value=8), factor=4)
    assert second.source is other.native
    assert second.schedule_id != plan.schedule_id
    await plan.unschedule()
    assert await source.get_schedules() == []
    assert (await other.get_schedule(second.schedule_id)).args == [
        {"value": 8}
    ]
    await second.unschedule()
    assert await other.get_schedules() == []


@pytest.mark.parametrize(
    ("method", "value"),
    [
        ("at", datetime(2030, 1, 1, tzinfo=UTC)),
        ("cron", "* * * * *"),
        ("every", 60),
    ],
)
async def test_scheduler_helpers_require_registration(method, value):
    class Report(Scheduler):
        async def run(self):
            pass

    source = MemorySource()
    with pytest.raises(RuntimeError, match="not included"):
        await getattr(Report, method)(source, value)
    assert await source.get_schedules() == []


def test_300_modular_registrations_without_network_or_instances():
    registrar = Registrar()
    for index in range(300):

        async def run(self, value: int):
            return value

        cls = type(
            "Report",
            (Scheduler,),
            {
                "__module__": f"app.module_{index}",
                "run": run,
            },
        )
        registrar.include(cls)
    assert len(registrar.broker.native.local_task_registry) == 300


@pytest.mark.parametrize("integration", ["memory", "rabbit", "redis"])
def test_optional_import_boundaries(integration):
    if integration != "memory":
        pytest.importorskip(
            "taskiq_aio_pika" if integration == "rabbit" else "taskiq_redis"
        )
    code = """
import importlib.abc
import sys
kind = sys.argv[1]
blocked = {'papilio', 'faststream'}
if kind != 'rabbit': blocked.add('taskiq_aio_pika')
if kind != 'redis': blocked.add('taskiq_redis')
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in blocked:
            raise ImportError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.schedulers.application import create_broker
create_broker()
if kind != 'memory':
    __import__('papilio_tasks.apps.schedulers.backends.' + kind)
    __import__('papilio_tasks.apps.schedulers.registry.' + kind)
"""
    subprocess.run(
        [sys.executable, "-c", code, integration],
        check=True,
        capture_output=True,
        text=True,
    )

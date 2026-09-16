import asyncio
import importlib
import os
import subprocess
import sys
from contextlib import aclosing
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from dishka import Provider, Scope
from taskiq import ScheduleSource, TaskiqEvents
from taskiq.cli.scheduler.run import SchedulerLoop
from taskiq.middlewares import SimpleRetryMiddleware, SmartRetryMiddleware
from taskiq.receiver import Receiver

from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.apps.schedulers.application import (
    create_beat,
    create_broker,
)
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.infra.taskiq.sources.base import MutableSource, Source
from papilio_tasks.tools.retry import Retry


def registration(kind, url=None, name=None):
    if kind == "memory":
        return Scheduler, Registrar(MemoryBroker(await_inplace=True))
    if kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from taskiq_aio_pika import Exchange

        from papilio_tasks.apps.schedulers.backends.rabbit import (
            RabbitBroker,
            RabbitQueue,
            RabbitScheduler,
        )
        from papilio_tasks.apps.schedulers.registry.rabbit import (
            RabbitRegistrar,
        )

        broker = RabbitBroker(
            url or "amqp://guest:guest@localhost/",
            queues=[RabbitQueue(name=name or "reports")],
            exchange=Exchange(name=name or "reports"),
            dead_letter_queue=RabbitQueue(name=(name or "reports") + "-dead"),
        )
        return RabbitScheduler, RabbitRegistrar(broker)
    pytest.importorskip("taskiq_redis")
    from papilio_tasks.apps.schedulers.backends.redis import (
        RedisScheduler,
        RedisStreamBroker,
    )
    from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar

    return RedisScheduler, RedisRegistrar(
        RedisStreamBroker(
            url or "redis://localhost",
            queue_name=name or "reports",
            consumer_group_name=name or "reports",
            consumer_id="0",
        )
    )


def provider_for(cls):
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(cls)
    return provider


@pytest.mark.parametrize(
    "values",
    [
        {"attempts": 0},
        {"attempts": True},
        {"attempts": 1.5},
        {"delay": -1},
        {"delay": float("nan")},
        {"delay": float("inf")},
        {"delay": True},
        {"errors": ()},
        {"errors": [ValueError]},
        {"errors": (BaseException,)},
        {"errors": (ValueError(),)},
    ],
)
def test_invalid_policies(values):
    args = dict(attempts=3, delay=1, errors=(ConnectionError,))
    args.update(values)
    with pytest.raises((TypeError, ValueError)):
        Retry(**args)


def test_policy_is_immutable_and_runtime_independent():
    policy = Retry(attempts=3, delay=0, errors=(ConnectionError,))
    with pytest.raises(FrozenInstanceError):
        policy.attempts = 5
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'taskiq', 'dishka', 'redis', 'papilio'}:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.tools.retry import Retry
Retry(attempts=1, delay=0, errors=(ValueError,))
"""
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize("kind", ["memory", "rabbit", "redis"])
def test_policy_inheritance_labels_and_missing_source(kind):
    base, registry = registration(kind)

    class Parent(base):
        retry = Retry(attempts=4, delay=10, errors=(ConnectionError,))

        async def run(self):
            pass

    class Child(Parent):
        pass

    class Disabled(Parent):
        retry = None

    with pytest.raises(ValueError, match="conflict"):
        registry.include(Child, labels={"delay": 5})
    assert not registry.broker.native.local_task_registry
    task = registry.include(Child)
    assert "delay" not in task.labels
    assert "_retries" not in task.labels
    disabled = registry.include(Disabled)
    assert not disabled.labels.get("retry_on_error", False)
    before = dict(registry.broker.native.local_task_registry)
    with pytest.raises(ValueError, match="retry_source"):
        create_broker(registrar=registry)
    assert registry.broker.native.local_task_registry == before
    assert "papilio_container" not in registry.broker.native.state
    source = MemorySource()
    broker = create_broker(registrar=registry, retry_source=source)
    middleware = [
        m for m in broker.middlewares if isinstance(m, SmartRetryMiddleware)
    ]
    assert len(middleware) == 1
    assert middleware[0].schedule_source is source.native
    with pytest.raises(ValueError, match="retry_source"):
        create_beat(broker, sources=[])
    assert create_beat(broker, sources=[source]).sources == [source.native]


@pytest.mark.parametrize("kind", ["memory", "rabbit", "redis"])
def test_discovery_validates_retry_before_registration(
    kind, tmp_path, monkeypatch
):
    base, registry = registration(kind)
    name = "retry_app_" + uuid4().hex
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "schedulers.py").write_text(
        f"from {base.__module__} import {base.__name__} as Base\n"
        "from papilio_tasks.tools.retry import Retry\n"
        "class First(Base):\n    async def run(self): pass\n"
        "class Report(Base):\n"
        "    retry = Retry(attempts=2, delay=0, errors=(ValueError,))\n"
        "    async def run(self): pass\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(ValueError, match="retry_source"):
        create_broker(registrar=registry, modules=[name])
    assert not registry.broker.native.local_task_registry
    broker = create_broker(
        registrar=registry, modules=[name], retry_source=MemorySource()
    )
    cls = importlib.import_module(name + ".schedulers").Report
    assert cls.task() is broker.find_task(name + ".schedulers.Report")
    assert cls.task().labels["max_retries"] == 2


def test_readonly_source_duplicate_retry_and_late_registration():
    with pytest.raises(TypeError, match="writable"):
        create_broker(retry_source=Source(MemorySource().native))
    registry = Registrar()
    registry.broker.native.add_middlewares(SimpleRetryMiddleware())
    with pytest.raises(ValueError, match="already has a retry"):
        create_broker(registrar=registry, retry_source=MemorySource())
    assert "papilio_container" not in registry.broker.native.state
    registry = Registrar()
    create_broker(registrar=registry)

    class Report(Scheduler):
        retry = Retry(attempts=2, delay=0, errors=(ValueError,))

        async def run(self):
            pass

    with pytest.raises(ValueError, match="retry_source"):
        registry.include(Report)
    assert not registry.broker.native.local_task_registry


@pytest.mark.parametrize("role", ["worker", "producer", "beat"])
@pytest.mark.parametrize("app", ["scheduler", "projection"])
async def test_source_lifecycle_has_one_owner(role, app):
    events = []

    class Native(ScheduleSource):
        async def get_schedules(self):
            return []

        async def startup(self):
            events.append("open")

        async def shutdown(self):
            events.append("close")

        async def add_schedule(self, task):
            pass

        async def delete_schedule(self, id):
            pass

    source = MutableSource(Native())
    # Exercise native event dispatch without making a network connection.
    from taskiq import AsyncBroker

    if app == "projection":
        from papilio_tasks.apps.projections.application import (
            create_broker as factory,
        )
    else:
        factory = create_broker
    broker = factory(retry_source=source)
    broker.is_worker_process = role == "worker"
    broker.is_scheduler_process = role == "beat"
    assert events == []
    if role == "beat":
        await source.native.startup()
    await AsyncBroker.startup(broker)
    if role == "worker":
        for handler in broker.event_handlers[TaskiqEvents.WORKER_STARTUP]:
            await handler(broker.state)
    await AsyncBroker.shutdown(broker)
    if role == "beat":
        await source.native.shutdown()
    assert events == ([] if role == "producer" else ["open", "close"])


@pytest.mark.parametrize(
    "attempts,failures,error,enabled",
    [
        (4, 2, ConnectionError, True),
        (4, 10, ConnectionError, True),
        (1, 10, ConnectionError, True),
        (4, 10, ValueError, True),
        (4, 10, ConnectionError, False),
    ],
)
async def test_native_retry_preserves_inputs_and_exact_attempts(
    attempts, failures, error, enabled
):
    calls, closed = [], []

    class Dependency:
        pass

    class Report(Scheduler):
        retry = (
            Retry(attempts=attempts, delay=0.03, errors=(ConnectionError,))
            if enabled
            else None
        )

        def __init__(self, dep: Dependency):
            self.dep = dep

        async def run(self, id: int, *, title: str):
            calls.append((id, title, self.dep))
            if len(calls) <= failures:
                raise error("failed")
            return title

    async def dependency():
        dep = Dependency()
        try:
            yield dep
        finally:
            closed.append(dep)

    provider = provider_for(Report)
    provider.provide(dependency, provides=Dependency)
    registry = Registrar(MemoryBroker(await_inplace=True))
    registry.include(Report)
    source = MemorySource()
    broker = create_broker(
        registrar=registry, providers=[provider], retry_source=source
    )
    beat = create_beat(broker, sources=[source])
    loop = SchedulerLoop(beat)
    await broker.startup()
    try:
        handle = await Report.enqueue(42, title="report")
        while schedules := await source.get_schedules():
            assert len(schedules) == 1
            task = schedules[0]
            assert task.task_id == handle.task_id
            assert task.args == [42] and task.kwargs == {"title": "report"}
            assert "types_of_exceptions" not in task.labels
            assert not loop._is_schedule_ready_to_send(task, datetime.now(UTC))
            await asyncio.sleep(
                max(0, (task.time - datetime.now(UTC)).total_seconds())
            )
            assert loop._is_schedule_ready_to_send(task, datetime.now(UTC))
            await beat.on_ready(source.native, task)
        expected = (
            min(attempts, failures + 1)
            if enabled and error is ConnectionError
            else 1
        )
        assert len(calls) == expected
        assert len({id(call[2]) for call in calls}) == expected
        assert [call[2] for call in calls] == closed
        completed = await handle.get_result()
        if expected > failures:
            assert not completed.is_err and completed.return_value == "report"
        else:
            assert type(completed.error) is error
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
@pytest.mark.parametrize("app", ["scheduler", "projection"])
async def test_retry_through_real_broker_and_redis_source(kind, app):
    url = os.getenv(
        "TEST_RABBIT_URL" if kind == "rabbit" else "TEST_REDIS_URL"
    )
    redis_url = os.getenv("TEST_REDIS_URL")
    if not url or not redis_url:
        pytest.skip("No live services")
    from redis.asyncio import Redis

    from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource

    name = "retry-" + uuid4().hex
    base, registry = registration(kind, url, name)
    calls = []

    class Report(base):
        retry = Retry(attempts=3, delay=0.05, errors=(ConnectionError,))

        async def run(self, ids: list[int]):
            calls.append(ids)
            if len(calls) < 3:
                raise ConnectionError("temporary")

    factory = create_broker
    labels = None
    if app == "projection":
        from papilio_tasks.apps.projections.application import (
            create_broker as factory,
        )

        if kind == "rabbit":
            from papilio_tasks.apps.projections.backends.rabbit import (
                RabbitDirect as Direct,
            )
            from papilio_tasks.apps.projections.backends.rabbit import (
                RabbitQueue as Queue,
            )
            from papilio_tasks.apps.projections.registry.rabbit import (
                RabbitRegistrar as Projections,
            )
        else:
            from papilio_tasks.apps.projections.backends.redis import (
                RedisDirect as Direct,
            )
            from papilio_tasks.apps.projections.backends.redis import (
                RedisQueue as Queue,
            )
            from papilio_tasks.apps.projections.registry.redis import (
                RedisRegistrar as Projections,
            )

        destination = name + "-products"
        if kind == "redis":
            from papilio_tasks.infra.taskiq.brokers.backends.redis import (
                RedisStreamBroker,
            )

            transport = RedisStreamBroker(
                url,
                queue_name=name,
                additional_streams={destination: ">"},
                consumer_group_name=name,
                consumer_id="0",
            )
        else:
            transport = registry.broker

        class Project(Direct[list[int], None]):
            retry = Report.retry
            queue = Queue(name=destination)

            def __init__(self):
                super().__init__()

            async def read(self, ids: list[int]) -> list[int]:
                return ids

            async def write(self, data: list[int]) -> None:
                calls.append(data)
                if len(calls) < 3:
                    raise ConnectionError("temporary")

        Report = Project
        registry = Projections(transport)

    registry.include(Report, name="report", labels=labels)
    if app == "projection" and kind == "rabbit":
        registry.broker.consume(destination)
    source = RedisSource(redis_url, prefix=name)
    reader = RedisSource(redis_url, prefix=name)
    broker = factory(
        registrar=registry,
        providers=[provider_for(Report)],
        retry_source=source,
    )
    # The independent reader represents beat's separately constructed source.
    from taskiq import TaskiqScheduler

    beat = TaskiqScheduler(broker, [reader.native])
    loop = SchedulerLoop(beat)
    broker.is_worker_process = True
    await reader.native.startup()
    await broker.startup()
    try:
        handle = await Report.enqueue([1, 2])
        receiver = Receiver(broker)
        async with aclosing(broker.listen()) as messages:
            async with asyncio.timeout(10):
                for attempt in range(3):
                    await receiver.callback(await anext(messages))
                    if attempt == 2:
                        break
                    tasks = await reader.get_schedules()
                    assert len(tasks) == 1
                    task = tasks[0]
                    assert task.task_id == handle.task_id
                    assert task.labels["queue_name"] == (
                        destination if app == "projection" else name
                    )
                    assert "delay" not in task.labels
                    await asyncio.sleep(
                        max(0, (task.time - datetime.now(UTC)).total_seconds())
                    )
                    assert loop._is_schedule_ready_to_send(
                        task, datetime.now(UTC)
                    )
                    await beat.on_ready(reader.native, task)
        assert calls == [[1, 2]] * 3
        assert await reader.get_schedules() == []
    finally:
        if kind == "rabbit":
            async with broker.write_conn.channel() as channel:
                await channel.queue_delete(name)
                if app == "projection":
                    await channel.queue_delete(destination)
                await channel.queue_delete(name + "-dead")
                await channel.exchange_delete(name)
        await broker.shutdown()
        await reader.native.shutdown()
        # Native taskiq-redis 1.2.3 list-source shutdown leaves its pool open.
        await source.native._connection_pool.disconnect()
        await reader.native._connection_pool.disconnect()
        async with Redis.from_url(redis_url) as redis:
            keys = [k async for k in redis.scan_iter(match=name + "*")]
            if keys:
                await redis.delete(*keys)

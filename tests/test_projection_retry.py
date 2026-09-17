import asyncio
import importlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from dishka import Provider, Scope
from hook_support import handler
from taskiq.cli.scheduler.run import SchedulerLoop
from taskiq.exceptions import SendTaskError
from taskiq.middlewares import SimpleRetryMiddleware

from papilio_tasks.apps.projections import Direct, Projection
from papilio_tasks.apps.projections.application import (
    create_beat,
    create_broker,
)
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.infra.taskiq.sources.base import Source
from papilio_tasks.tools.hooks.projection import Hooks
from papilio_tasks.tools.retry import Retry


class Product(Direct[int, int]):
    retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))

    async def read(self, id: int) -> int:
        return id

    async def write(self, data: int) -> int:
        return data


def test_inherited_disabled_and_conflicting_policies():
    class Child(Product):
        pass

    class Disabled(Product):
        retry = None

    registry = Registrar()
    with pytest.raises(ValueError, match="conflict"):
        registry.include(Child, labels={"delay": 1})
    assert not registry.broker.native.local_task_registry
    with pytest.raises(RuntimeError, match="not included"):
        Child.task()
    task = registry.include(Child)
    assert task.labels["max_retries"] == 3
    assert task.labels["retry_delay"] == 0
    assert "delay" not in task.labels and "_retries" not in task.labels
    disabled = registry.include(Disabled)
    assert not disabled.labels.get("retry_on_error", False)
    source = MemorySource()
    broker = create_broker(registrar=registry, retry_source=source)
    with pytest.raises(ValueError, match="retry_source"):
        create_beat(broker, sources=[MemorySource()])
    # Wrappers may differ, but beat must consume the configured native source.
    beat = create_beat(broker, sources=[Source(source.native)])
    assert beat.sources == [source.native]


def test_preincluded_retry_requires_source_and_cleans_binding():
    registry = Registrar()
    registry.include(Product)
    before = dict(registry.broker.native.local_task_registry)
    with pytest.raises(ValueError, match="retry_source"):
        create_broker(registrar=registry)
    assert registry.broker.native.local_task_registry == before
    assert "papilio_container" not in registry.broker.native.state
    with pytest.raises(RuntimeError, match="not included"):
        Product.task()


def test_discovery_checks_source_before_registering(tmp_path, monkeypatch):
    name = "projection_retry_" + uuid4().hex
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "projections.py").write_text(
        "from papilio_tasks.apps.projections import Direct\n"
        "from papilio_tasks.tools.retry import Retry\n"
        "class Product(Direct[int, int]):\n"
        "    retry = Retry(attempts=2, delay=0, errors=(ValueError,))\n"
        "    async def read(self, id: int) -> int: return id\n"
        "    async def write(self, data: int) -> int: return data\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    registry = Registrar()
    with pytest.raises(ValueError, match="retry_source"):
        create_broker(registrar=registry, modules=[name])
    assert not registry.broker.native.local_task_registry
    broker = create_broker(
        registrar=registry, modules=[name], retry_source=MemorySource()
    )
    cls = importlib.import_module(name + ".projections").Product
    assert cls.task() is broker.find_task(name + ".projections.Product")
    assert cls.task().labels["max_retries"] == 2


def test_invalid_source_duplicate_middleware_and_late_inclusion():
    with pytest.raises(TypeError, match="writable"):
        create_broker(retry_source=Source(MemorySource().native))
    registry = Registrar()
    registry.broker.native.add_middlewares(SimpleRetryMiddleware())
    with pytest.raises(ValueError, match="already has a retry"):
        create_broker(registrar=registry, retry_source=MemorySource())
    assert "papilio_container" not in registry.broker.native.state
    registry = Registrar()
    create_broker(registrar=registry)
    with pytest.raises(ValueError, match="retry_source"):
        registry.include(Product)
    assert not registry.broker.native.local_task_registry
    ready = Registrar()
    create_broker(registrar=ready, retry_source=MemorySource())
    assert ready.include(Product).labels["retry_on_error"]


@pytest.mark.parametrize(
    "stage,failures,error,attempts,enabled,continue_hook,expected",
    [
        ("read", 2, ConnectionError, 3, True, False, 3),
        ("transform", 2, ConnectionError, 3, True, False, 3),
        ("write", 10, ConnectionError, 3, True, False, 3),
        ("after_write", 2, ConnectionError, 3, True, False, 3),
        ("write", 10, ValueError, 3, True, False, 1),
        ("write", 10, ConnectionError, 1, True, False, 1),
        ("write", 10, ConnectionError, 3, False, False, 1),
        ("after_write", 10, ConnectionError, 3, True, True, 1),
    ],
)
async def test_native_retry_replays_pipeline_with_new_scope(
    stage, failures, error, attempts, enabled, continue_hook, expected
):
    reads, transforms, writes, errors, closed, shared = [], [], [], [], [], []

    class Dependency:
        pass

    class Shared(Hooks):
        def __init__(self, dep: Dependency):
            async def observe(event):
                shared.append(dep)

            super().__init__(after_read=(handler(observe),))

    class Job(Projection[int, str, str]):
        retry = (
            Retry(attempts=attempts, delay=0.03, errors=(ConnectionError,))
            if enabled
            else None
        )

        def __init__(self, dep: Dependency):
            self.dep = dep

            async def written(event):
                self.fail("after_write")

            async def failed(event):
                errors.append((event.stage, event.error, dep))

            super().__init__(
                hooks=Hooks(
                    after_write=(
                        handler(
                            written, "continue" if continue_hook else "raise"
                        ),
                    ),
                    on_error=(handler(failed),),
                )
            )

        def fail(self, current):
            if stage == current and len(reads) <= failures:
                raise error("temporary")

        async def read(self, id: int, *, title: str) -> int:
            reads.append((id, title, self.dep))
            self.fail("read")
            return id

        async def transform(self, data: int) -> str:
            transforms.append(data)
            self.fail("transform")
            return str(data)

        async def write(self, data: str) -> str:
            writes.append(data)
            self.fail("write")
            return data

    async def dependency():
        dep = Dependency()
        try:
            yield dep
        finally:
            closed.append(dep)

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(dependency, provides=Dependency)
    provider.provide(Job)
    provider.provide(Shared)
    registry = Registrar(MemoryBroker(await_inplace=True), hooks=Shared)
    registry.include(Job)
    source = MemorySource()
    broker = create_broker(
        registrar=registry, providers=[provider], retry_source=source
    )
    beat = create_beat(broker, sources=[source])
    loop = SchedulerLoop(beat)
    await broker.startup()
    try:
        handle = await Job.enqueue(42, title="product")

        async def dispatch_retry():
            schedules = await source.get_schedules()
            assert len(schedules) == 1
            task = schedules[0]
            assert task.task_id == handle.task_id
            assert task.args == [42] and task.kwargs == {"title": "product"}
            assert not loop._is_schedule_ready_to_send(task, datetime.now(UTC))
            await asyncio.sleep(
                max(0, (task.time - datetime.now(UTC)).total_seconds())
            )
            assert loop._is_schedule_ready_to_send(task, datetime.now(UTC))
            await beat.on_ready(
                source.native, (await source.native.get_schedules())[0]
            )

        if expected == 3:
            await dispatch_retry()
            await dispatch_retry()
        assert await source.get_schedules() == []
        assert len(reads) == expected
        assert [(id, title) for id, title, _ in reads] == [
            (42, "product")
        ] * expected
        assert [dep for _, _, dep in reads] == closed
        assert len({id(dep) for dep in closed}) == expected
        completed = await handle.get_result()
        if continue_hook or expected > failures:
            assert not completed.is_err and completed.return_value == "42"
        else:
            assert type(completed.error) is error
        failed_attempts = 0 if continue_hook else min(expected, failures)
        assert [s for s, _, _ in errors] == [stage] * failed_attempts
        assert [dep for _, _, dep in errors] == closed[:failed_attempts]
        assert all(type(err) is error for _, err, _ in errors)
        after_read = (
            expected - failed_attempts if stage == "read" else expected
        )
        assert (
            shared == closed[failed_attempts:]
            if stage == "read"
            else shared == closed
        )
        assert transforms == [42] * after_read
        write_count = (
            after_read - failed_attempts
            if stage == "transform"
            else after_read
        )
        assert writes == ["42"] * write_count
    finally:
        await broker.shutdown()


async def test_direct_run_does_not_retry():
    calls = []

    class Failing(Product):
        async def write(self, data: int) -> int:
            calls.append(data)
            raise ConnectionError("offline")

    with pytest.raises(ConnectionError, match="offline"):
        await Failing().run(42)
    assert calls == [42]


async def test_execution_policy_does_not_retry_failed_publication(monkeypatch):
    registry = Registrar()
    registry.include(Product)
    source = MemorySource()
    broker = create_broker(registrar=registry, retry_source=source)
    error = ConnectionError("offline")
    send = AsyncMock(side_effect=error)
    monkeypatch.setattr(broker, "kick", send)
    await broker.startup()
    try:
        with pytest.raises(SendTaskError) as caught:
            await Product.enqueue(42)
        assert caught.value.__cause__ is error
        send.assert_awaited_once()
        assert await source.get_schedules() == []
    finally:
        await broker.shutdown()

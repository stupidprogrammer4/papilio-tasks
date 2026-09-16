from unittest.mock import Mock

import pytest
from dishka import Provider, Scope

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.projections import application as projections
from papilio_tasks.apps.projections.registry import Registrar as Projections
from papilio_tasks.apps.schedulers import Scheduler
from papilio_tasks.apps.schedulers import application as schedulers
from papilio_tasks.apps.schedulers.registry import Registrar as Schedulers
from papilio_tasks.infra.taskiq import bindings


def case(kind):
    if kind == "projection":
        base, first, second = Direct, Projections(), Projections()
    elif kind == "memory":
        base, first, second = Scheduler, Schedulers(), Schedulers()
    elif kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from papilio_tasks.apps.schedulers.backends.rabbit import (
            RabbitBroker,
            RabbitScheduler,
        )
        from papilio_tasks.apps.schedulers.registry.rabbit import (
            RabbitRegistrar,
        )

        base = RabbitScheduler
        first = RabbitRegistrar(RabbitBroker("amqp://guest:guest@localhost/"))
        second = RabbitRegistrar(RabbitBroker("amqp://guest:guest@localhost/"))
    else:
        pytest.importorskip("taskiq_redis")
        from papilio_tasks.apps.schedulers.backends.redis import (
            RedisScheduler,
            RedisStreamBroker,
        )
        from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar

        base = RedisScheduler
        first = RedisRegistrar(RedisStreamBroker("redis://localhost"))
        second = RedisRegistrar(RedisStreamBroker("redis://localhost"))

    async def run(self):
        return 1

    async def read(self, id: int) -> int:
        return id

    async def write(self, data: int) -> int:
        return data

    methods = (
        {"read": read, "write": write}
        if kind == "projection"
        else {"run": run}
    )
    return type("Job", (base,), methods), first, second


@pytest.mark.parametrize("kind", ["projection", "memory", "rabbit", "redis"])
def test_duplicate_checked_before_backend_effects_and_class_is_unchanged(
    kind, monkeypatch
):
    cls, first, second = case(kind)
    before = dict(cls.__dict__)
    task = first.include(cls, name="custom")
    assert cls.task() is task
    assert dict(cls.__dict__) == before
    register = Mock(side_effect=AssertionError("must not register"))
    queue = Mock(side_effect=AssertionError("must not add a queue"))
    monkeypatch.setattr(second.broker, "register", register)
    if kind in ("rabbit", "redis"):
        monkeypatch.setattr(second.broker, "add_queue", queue)
    with pytest.raises(ValueError, match="already registered"):
        second.include(cls, name="another")
    with pytest.raises(ValueError, match="already registered"):
        first.include(cls, name="alias")
    register.assert_not_called()
    queue.assert_not_called()
    assert cls.task() is task


@pytest.mark.parametrize("kind", ["projection", "memory", "rabbit", "redis"])
def test_native_registration_failure_leaves_class_unbound(kind, monkeypatch):
    cls, registry, _ = case(kind)
    failed = Mock(side_effect=ValueError("native failure"))
    monkeypatch.setattr(registry.broker, "register", failed)
    with pytest.raises(ValueError, match="native failure"):
        registry.include(cls)
    failed.assert_called_once()
    with pytest.raises(RuntimeError, match="not included"):
        cls.task()


def test_exact_identity_owner_release_and_rebinding():
    cls, first, second = case("projection")
    child = type("Job", (cls,), {})
    first_task = first.include(cls, name="first")
    with pytest.raises(RuntimeError, match="not included"):
        child.task()
    second_task = second.include(child, name="second")
    with pytest.raises(ValueError, match="already registered"):
        bindings.add(cls, second_task)
    assert cls.task() is first_task
    assert child.task() is second_task
    bindings.release(first.broker.native)
    bindings.release(first.broker.native)
    assert child.task() is second_task
    with pytest.raises(RuntimeError, match="not included"):
        cls.task()
    fresh = Projections()
    replacement = fresh.include(cls, name="fresh")
    bindings.release(first.broker.native)
    assert cls.task() is replacement


@pytest.mark.parametrize("kind", ["projection", "memory"])
async def test_failed_assembly_releases_only_its_broker(kind, monkeypatch):
    cls, registry, other = case(kind)
    owned = type("Owned", (cls,), {})
    unrelated = type("Other", (cls,), {})
    invalid = type(
        "Invalid",
        (cls,),
        {"read" if kind == "projection" else "run": lambda self: 1},
    )
    registry.include(owned)
    other_task = other.include(unrelated)
    app = projections if kind == "projection" else schedulers
    monkeypatch.setattr(
        app.Bootstrapper, "classes", lambda *args: [cls, invalid]
    )
    with pytest.raises(TypeError, match="async"):
        app.create_broker(registrar=registry)
    with pytest.raises(RuntimeError, match="not included"):
        cls.task()
    with pytest.raises(RuntimeError, match="not included"):
        owned.task()
    assert unrelated.task() is other_task
    await registry.broker.native.shutdown()
    await other.broker.native.shutdown()


@pytest.mark.parametrize("kind", ["projection", "memory"])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_shutdown_releases_bindings_even_if_dependency_cleanup_fails(
    kind, close_fails
):
    cls, registry, other = case(kind)
    unrelated = type("Other", (cls,), {})
    other_task = other.include(unrelated)
    registry.include(cls)
    app = projections if kind == "projection" else schedulers

    class Resource:
        pass

    async def resource():
        try:
            yield Resource()
        finally:
            if close_fails:
                raise ValueError("resource cleanup failed")

    provider = Provider(scope=Scope.APP)
    provider.provide(resource, provides=Resource)
    broker = app.create_broker(registrar=registry, providers=[provider])
    with pytest.raises(ValueError, match="already has"):
        app.create_broker(registrar=registry)
    assert cls.task().broker is broker
    await broker.startup()
    await broker.state.papilio_container.get(Resource)
    if close_fails:
        with pytest.raises(ExceptionGroup):
            await broker.shutdown()
    else:
        await broker.shutdown()
    with pytest.raises(RuntimeError, match="not included"):
        await cls.enqueue()
    assert unrelated.task() is other_task
    await broker.shutdown()
    await other.broker.native.shutdown()

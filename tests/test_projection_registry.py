import asyncio
import inspect
from contextlib import asynccontextmanager
from typing import get_type_hints

import pytest
from dishka import Provider, Scope, make_async_container
from dishka.exceptions import NoFactoryError
from dishka.integrations.taskiq import TaskiqProvider, setup_dishka
from hook_support import handler
from pydantic import BaseModel
from taskiq import TaskiqMessage, TaskiqMiddleware

from papilio_tasks.apps.projections import Direct, Failure, Hooks, Projection
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.tools.hooks import Handler, Hook


@asynccontextmanager
async def worker(registry, provider):
    container = make_async_container(TaskiqProvider(), provider)
    broker = registry.broker.native
    setup_dishka(container, broker)
    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()
        await container.close()


async def result(task):
    completed = await task.wait_result(timeout=5)
    if completed.is_err:
        raise completed.error
    return completed.return_value


class Payload(BaseModel):
    value: int


async def test_read_signature_write_result_and_serialized_payload():
    instances, messages = [], []

    class Capture(TaskiqMiddleware):
        async def pre_send(self, message: TaskiqMessage):
            messages.append((message.args.copy(), message.kwargs.copy()))
            return message

    class Product(Projection[Payload, str, int]):
        def __init__(self):
            super().__init__()
            instances.append(self)

        async def read(self, payload: Payload, *, factor: int = 2) -> Payload:
            return Payload(value=payload.value * factor)

        async def transform(self, data: Payload) -> str:
            return str(data.value)

        async def write(self, data: str) -> int:
            return int(data)

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Product)
    registry = Registrar()
    labels = {"name": "metadata", "kind": "product"}
    original = dict(Product.read.__annotations__)
    task = registry.include(Product, name="products.single", labels=labels)
    registry.broker.native.add_middlewares(Capture())
    expected = inspect.signature(Product.read)
    expected = expected.replace(
        parameters=list(expected.parameters.values())[1:],
        return_annotation=int,
    )
    actual = inspect.signature(task)
    assert (
        actual.replace(
            parameters=[
                p
                for p in actual.parameters.values()
                if p.name != "dishka_container"
            ]
        )
        == expected
    )
    assert get_type_hints(task.original_func) == {
        "payload": Payload,
        "factor": int,
        "return": int,
    }
    assert Product.read.__annotations__ == original
    assert not hasattr(Product, "_task")
    assert labels == task.labels
    async with worker(registry, provider) as broker:
        assert instances == []
        assert await result(await task.kiq({"value": 3})) == 6
        assert await result(await task.kiq(Payload(value=4), factor=3)) == 12
        assert len(instances) == 2 and instances[0] is not instances[1]
        assert messages == [
            ([{"value": 3}], {}),
            ([{"value": 4}], {"factor": 3}),
        ]
        assert broker.state.dishka_container_registry == {}


@pytest.mark.parametrize("kind", ["mixed", "variadic", "positional"])
async def test_argument_shapes_match_native_taskiq(kind, caplog):
    if kind == "mixed":

        async def plain(first, second: int, *, suffix="!"):
            return f"{first}:{second}{suffix}"

        async def read(self, first, second: int, *, suffix="!"):
            return f"{first}:{second}{suffix}"

        args, kwargs, expected = ["a", 3], {"suffix": "?"}, "a:3?"
    elif kind == "variadic":

        async def plain(*values: int, **options: str):
            return f"{sum(values)}:{options['suffix']}"

        async def read(self, *values: int, **options: str):
            return f"{sum(values)}:{options['suffix']}"

        args, kwargs, expected = [1, 2, 3], {"suffix": "ok"}, "6:ok"
    else:

        async def plain(first: int, /, second: int = 2):
            return first + second

        async def read(self, first: int, /, second: int = 2):
            return first + second

        args, kwargs, expected = [3], {}, 5

    async def write(self, data):
        return data

    cls = type("Product", (Direct,), {"read": read, "write": write})
    registry = Registrar()
    task = registry.include(cls)
    native = registry.broker.register(plain, name="plain")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: cls(), provides=cls)
    async with worker(registry, provider):
        caplog.clear()
        assert await result(await native.kiq(*args, **kwargs)) == expected
        native_warnings = [
            r for r in caplog.records if r.levelname == "WARNING"
        ]
        caplog.clear()
        assert await result(await task.kiq(*args, **kwargs)) == expected
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == len(native_warnings)
        assert all("AsyncContainer" not in r.message for r in warnings)


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("hook_fails", [False, True])
async def test_hooks_share_job_scope_and_finish_before_cleanup(
    fail, hook_fails
):
    opened, closed, calls = [], [], []
    failure = ValueError("write failed")
    report_failure = RuntimeError("report failed")

    class Session:
        def __init__(self, id):
            self.id = id
            self.closed = False

    def record(session, phase):
        assert not session.closed
        calls.append((session.id, phase))

    class SaveError(Hook[Failure]):
        def __init__(self, session: Session):
            self.session = session

        async def run(self, event: Failure) -> None:
            record(self.session, "shared:error")
            assert event.error is failure
            if hook_fails:
                raise report_failure

    class AppHooks(Hooks[object, object, object]):
        def __init__(self, session: Session, errors: SaveError):
            async def after_read(event):
                record(session, "shared:read")

            async def after_write(event):
                record(session, "shared:write")

            super().__init__(
                after_read=(handler(after_read),),
                after_write=(handler(after_write),),
                on_error=(Handler(errors),),
            )

    class Product(Direct[int, int]):
        def __init__(self, session: Session):
            self.session = session

            async def after_read(event):
                record(session, "local:read")

            async def after_write(event):
                record(session, "local:write")

            async def error(event):
                record(session, "local:error")
                assert event.error is failure

            super().__init__(
                hooks=Hooks(
                    after_read=(handler(after_read),),
                    after_write=(handler(after_write),),
                    on_error=(handler(error),),
                )
            )

        async def read(self, id: int) -> int:
            record(self.session, "read")
            await asyncio.sleep(0)
            return id

        async def write(self, data: int) -> int:
            record(self.session, "write")
            if fail:
                raise failure
            return data

    async def session():
        current = Session(len(opened))
        opened.append(current)
        try:
            yield current
        finally:
            current.closed = True
            closed.append(current)

    registry = Registrar(hooks=AppHooks)
    task = registry.include(Product)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(session, provides=Session)
    provider.provide(Product)
    provider.provide(AppHooks)
    provider.provide(SaveError)
    assert opened == []
    async with worker(registry, provider) as broker:
        for id in range(2):
            completed = await (await task.kiq(id)).wait_result(timeout=5)
            if fail:
                assert completed.is_err and completed.error is failure
                assert failure.__cause__ is (
                    report_failure if hook_fails else None
                )
            else:
                assert not completed.is_err and completed.return_value == id
            phases = [phase for marker, phase in calls if marker == id]
            expected = ["read", "shared:read", "local:read", "write"]
            expected += (
                (
                    ["shared:error"]
                    if hook_fails
                    else ["shared:error", "local:error"]
                )
                if fail
                else ["shared:write", "local:write"]
            )
            assert phases == expected
            assert broker.state.dishka_container_registry == {}
        assert opened == closed
        assert len(opened) == 2 and opened[0] is not opened[1]


@pytest.mark.parametrize("missing", ["projection", "hooks"])
async def test_missing_provider_fails_without_implicit_construction(missing):
    class Product(Direct[int, int]):
        async def read(self):
            return 1

        async def write(self, data):
            return data

    class AppHooks(Hooks[object, object, object]):
        pass

    provider = Provider(scope=Scope.REQUEST)
    if missing != "projection":
        provider.provide(lambda: Product(), provides=Product)
    if missing != "hooks":
        provider.provide(lambda: AppHooks(), provides=AppHooks)
    registry = Registrar(hooks=AppHooks)
    task = registry.include(Product)
    async with worker(registry, provider) as broker:
        completed = await (await task.kiq()).wait_result(timeout=5)
        assert isinstance(completed.error, NoFactoryError)
        assert broker.state.dishka_container_registry == {}


def test_registration_validation_and_metadata():
    class Product(Direct[int, int]):
        async def read(self):
            return 1

        async def write(self, data):
            return data

    registry = Registrar()
    assert isinstance(registry.broker, MemoryBroker)
    assert Registrar().broker is not registry.broker
    with pytest.raises(TypeError, match="Projection class"):
        registry.include(object)
    with pytest.raises(TypeError, match="async"):
        registry.include(Projection)
    with pytest.raises(TypeError, match="Hooks class"):
        Registrar(hooks=Hooks())
    with pytest.raises(ValueError, match="empty"):
        registry.include(Product, name="")
    task = registry.include(Product, labels={"name": "label"})
    assert task.task_name == f"{Product.__module__}.{Product.__qualname__}"
    with pytest.raises(ValueError, match="already registered"):
        registry.include(Product)
    assert list(registry.broker.native.local_task_registry.values()) == [task]


@pytest.mark.parametrize("reserved", ["_job", "_hooks", "dishka_container"])
def test_reserved_argument_is_rejected_before_registration(reserved):
    async def write(self, data):
        return data

    namespace = {}
    exec(f"async def read(self, {reserved}): return {reserved}", namespace)
    cls = type(
        "Product", (Direct,), {"read": namespace["read"], "write": write}
    )
    registry = Registrar()
    with pytest.raises(TypeError, match="reserved"):
        registry.include(cls)
    assert not registry.broker.native.local_task_registry


@pytest.mark.parametrize("method", ["read", "transform", "write"])
def test_synchronous_operation_is_rejected(method):
    async def read(self):
        return 1

    async def write(self, data):
        return data

    def sync(self, *args):
        return 1

    methods = {"read": read, "write": write, method: sync}
    cls = type("Product", (Direct,), methods)
    registry = Registrar()
    with pytest.raises(TypeError, match="async"):
        registry.include(cls)
    assert not registry.broker.native.local_task_registry


async def test_concurrent_jobs_keep_shared_hook_dependencies_separate():
    markers, observed, closed = [], [], []

    class Session:
        def __init__(self, marker):
            self.marker = marker

    class AppHooks(Hooks[object, object, object]):
        def __init__(self, session: Session):
            async def after(event):
                assert session.marker not in closed
                observed.append((event.result, session.marker))

            super().__init__(after_write=(handler(after),))

    class Product(Direct[int, int]):
        def __init__(self, session: Session):
            super().__init__()
            self.session = session

        async def read(self):
            await asyncio.sleep(0)
            return self.session.marker

        async def write(self, data):
            return data

    async def session():
        marker = len(markers)
        markers.append(marker)
        try:
            yield Session(marker)
        finally:
            closed.append(marker)

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Product)
    provider.provide(AppHooks)
    provider.provide(session, provides=Session)
    registry = Registrar(hooks=AppHooks)
    task = registry.include(Product)
    async with worker(registry, provider) as broker:
        submitted = await asyncio.gather(task.kiq(), task.kiq())
        assert sorted(
            await asyncio.gather(*(result(t) for t in submitted))
        ) == [0, 1]
        assert sorted(observed) == [(0, 0), (1, 1)]
        assert sorted(closed) == [0, 1]
        assert broker.state.dishka_container_registry == {}

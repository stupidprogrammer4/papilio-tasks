import asyncio
import importlib
import inspect
import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope
from taskiq import AsyncBroker, AsyncTaskiqTask, TaskiqEvents, TaskiqMiddleware
from taskiq.exceptions import SendTaskError

from papilio_tasks.apps.projections import Direct, Hooks
from papilio_tasks.apps.projections.application import create_broker
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.infra.taskiq.brokers.base import Broker


class Product(Direct[int, int]):
    async def read(self, id: int) -> int:
        return id

    async def write(self, data: int) -> int:
        return data * 2


async def completed(handle):
    result = await handle.wait_result(timeout=5)
    assert not result.is_err, result.error
    return result.return_value


async def test_manual_registration_class_sender_and_native_result():
    made = []

    def product() -> Product:
        made.append("product")
        return Product()

    registry = Registrar()
    task = registry.include(Product, name="products.single")
    sender = Product
    jobs = Provider(scope=Scope.REQUEST)
    jobs.provide(product)
    broker = create_broker(registrar=registry, providers=[jobs])
    assert sender.task() is task
    assert broker is registry.broker.native
    assert made == []
    assert not hasattr(Product, "_task")

    await broker.startup()
    try:
        assert made == []
        handle = await sender.enqueue(id=7)
        assert isinstance(handle, AsyncTaskiqTask)
        assert await completed(handle) == 14
        assert made == ["product"]
        assert broker.state.dishka_container_registry == {}
    finally:
        await broker.shutdown()
    with pytest.raises(RuntimeError, match="not included"):
        Product.task()


async def test_discovery_and_user_providers(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    package = tmp_path / "projection_discovery"
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "shared.py").write_text(
        textwrap.dedent("""
        from papilio_tasks.apps.projections import Direct
        class Imported(Direct[int, int]):
            async def read(self, id: int) -> int: return id
            async def write(self, data: int) -> int: return data
        """)
    )
    (package / "projections.py").write_text(
        textwrap.dedent("""
        from projection_discovery.shared import Imported
        class Service:
            value = 3
        class Product(Imported):
            def __init__(self, service: Service):
                super().__init__()
                self.service = service
            async def write(self, data: int) -> int:
                return data * self.service.value
        Alias = Product
        """)
    )
    (package / "unused.py").write_text("raise AssertionError('unrelated')")
    module = importlib.import_module("projection_discovery.projections")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(module.Service)
    provider.provide(module.Product)
    registry = Registrar()
    broker = create_broker(
        registrar=registry,
        providers=[provider],
        modules=["projection_discovery", "projection_discovery"],
    )
    assert list(broker.local_task_registry) == [
        "projection_discovery.projections.Product"
    ]
    sender = module.Product
    await broker.startup()
    try:
        assert await completed(await sender.enqueue(4)) == 12
    finally:
        await broker.shutdown()


async def test_distinct_classes_have_independent_apps_and_hooks():
    from hook_support import handler

    calls = []

    class First(Hooks[object, object, object]):
        def __init__(self):
            async def wrote(event):
                calls.append(("first", event.result))

            super().__init__(after_write=(handler(wrote),))

    class Second(Hooks[object, object, object]):
        def __init__(self):
            async def wrote(event):
                calls.append(("second", event.result))

            super().__init__(after_write=(handler(wrote),))

    class OtherProduct(Product):
        pass

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: Product(), provides=Product)
    provider.provide(lambda: OtherProduct(), provides=OtherProduct)
    provider.provide(First)
    provider.provide(Second)
    first, second = Registrar(hooks=First), Registrar(hooks=Second)
    first.include(Product)
    second.include(OtherProduct)
    one = create_broker(registrar=first, providers=[provider])
    two = create_broker(registrar=second, providers=[provider])
    await asyncio.gather(one.startup(), two.startup())
    try:
        handles = await asyncio.gather(
            Product.enqueue(3),
            OtherProduct.enqueue(5),
        )
        assert await asyncio.gather(*(completed(h) for h in handles)) == [
            6,
            10,
        ]
        assert sorted(calls) == [("first", 6), ("second", 10)]
        assert not hasattr(Product, "_task")
    finally:
        await asyncio.gather(one.shutdown(), two.shutdown())


@pytest.mark.parametrize("is_worker", [False, True])
async def test_native_shutdown_closes_app_resources(is_worker):
    class Transport(AsyncBroker):
        async def kick(self, message):
            raise AssertionError("assembly must not publish")

        async def listen(self):
            yield b""

    closed = []

    class Resource:
        pass

    async def resource():
        try:
            yield Resource()
        finally:
            closed.append(True)

    provider = Provider(scope=Scope.APP)
    provider.provide(resource, provides=Resource)
    native = Transport()
    native.is_worker_process = is_worker
    registry = Registrar(Broker(native))
    registry.include(Product)
    broker = create_broker(registrar=registry, providers=[provider])
    assert closed == []
    await broker.startup()
    await broker.state.papilio_container.get(Resource)
    assert len(broker.event_handlers[TaskiqEvents.CLIENT_SHUTDOWN]) == 1
    assert len(broker.event_handlers[TaskiqEvents.WORKER_SHUTDOWN]) == 1
    await broker.shutdown()
    assert closed == [True]
    with pytest.raises(RuntimeError, match="not included"):
        Product.task()


async def test_assembly_guard_and_class_lookup():
    registry = Registrar()
    with pytest.raises(RuntimeError, match="not included"):
        Product.task()
    task = registry.include(Product, name="custom")
    assert Product.task() is task
    broker = create_broker(registrar=registry)
    before = tuple(broker.middlewares)
    with pytest.raises(ValueError, match="already has"):
        create_broker(registrar=registry)
    assert tuple(broker.middlewares) == before
    assert Product.task() is task
    await broker.shutdown()


@dataclass
class Saved:
    id: int


async def test_project_preserves_result_signature_and_awaits_enqueue(
    monkeypatch,
):
    registry = Registrar()
    registry.include(Product)
    publisher = Product
    entered, release = asyncio.Event(), asyncio.Event()

    async def send(**kwargs):
        assert kwargs == {"id": 7}
        entered.set()
        await release.wait()

    submitted = AsyncMock(side_effect=send)
    monkeypatch.setattr(Product, "enqueue", submitted)
    saved = Saved(7)

    async def create(title: str, *, active: bool = True) -> Saved:
        """Save a product."""
        assert title == "test" and active is False
        return saved

    wrapped = publisher.project(select=lambda value: {"id": value.id})(create)
    assert inspect.signature(wrapped) == inspect.signature(create)
    assert wrapped.__name__ == create.__name__
    assert wrapped.__doc__ == create.__doc__
    pending = asyncio.create_task(wrapped("test", active=False))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert not pending.done()
        release.set()
        assert await pending is saved
        submitted.assert_awaited_once_with(id=7)
    finally:
        release.set()
        await registry.broker.native.shutdown()


@pytest.mark.parametrize("stage", ["function", "select", "publish"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_project_propagates_failure_without_repeating_work(
    stage, cancel, monkeypatch
):
    registry = Registrar()
    registry.include(Product)
    publisher = Product
    calls = []
    error = asyncio.CancelledError() if cancel else ValueError(stage)

    async def create():
        calls.append("function")
        if stage == "function":
            raise error
        return Saved(3)

    def select(result):
        calls.append("select")
        if stage == "select":
            raise error
        return {"id": result.id}

    async def send(**kwargs):
        calls.append("publish")
        raise error

    monkeypatch.setattr(Product, "enqueue", AsyncMock(side_effect=send))
    wrapped = publisher.project(select=select)(create)
    try:
        with pytest.raises(type(error)) as caught:
            await wrapped()
        assert caught.value is error
        assert (
            calls
            == ["function", "select", "publish"][
                : ["function", "select", "publish"].index(stage) + 1
            ]
        )
    finally:
        await registry.broker.native.shutdown()


async def test_project_rejects_sync_function():
    registry = Registrar()
    registry.include(Product)
    try:
        with pytest.raises(TypeError, match="async"):
            Product.project(select=lambda result: {})(lambda: Saved(3))
    finally:
        await registry.broker.native.shutdown()


async def test_enqueue_propagates_native_send_failure_without_retry(
    monkeypatch,
):
    registry = Registrar()
    registry.include(Product)
    publisher = Product
    broker = create_broker(registrar=registry)
    error = OSError("broker unavailable")
    send = AsyncMock(side_effect=error)
    monkeypatch.setattr(broker, "kick", send)
    await broker.startup()
    try:
        with pytest.raises(SendTaskError) as caught:
            await publisher.enqueue(id=4)
        assert caught.value.__cause__ is error
        send.assert_awaited_once()
    finally:
        await broker.shutdown()


@pytest.mark.parametrize("batch", [False, True])
async def test_project_and_enqueue_send_equivalent_single_or_batch_data(batch):
    messages = []

    class Capture(TaskiqMiddleware):
        async def pre_send(self, message):
            messages.append((message.task_name, message.args, message.kwargs))
            return message

    class Products(Direct[list[int], int]):
        async def read(self, ids: list[int]) -> list[int]:
            return ids

        async def write(self, data: list[int]) -> int:
            return sum(data)

    cls = Products if batch else Product
    registry = Registrar()
    sender = cls
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: cls(), provides=cls)
    saved = [Saved(7), Saved(8)] if batch else Saved(7)
    select = (
        (lambda result: {"ids": [item.id for item in result]})
        if batch
        else (lambda result: {"id": result.id})
    )

    @sender.project(select=select)
    async def save():
        return saved

    with pytest.raises(RuntimeError, match="not included"):
        cls.task()
    registry.include(cls)
    broker = create_broker(registrar=registry, providers=[provider])
    broker.add_middlewares(Capture())
    await broker.startup()
    try:
        assert await save() is saved
        handle = await sender.enqueue(**select(saved))
        assert await completed(handle) == (15 if batch else 14)
        await broker.wait_all()
        assert len(messages) == 2 and messages[0] == messages[1]
        assert messages[0][2] == ({"ids": [7, 8]} if batch else {"id": 7})
        assert len(broker.result_backend.results) == 2
        assert all(
            not result.is_err
            for result in broker.result_backend.results.values()
        )
    finally:
        await broker.shutdown()


def test_class_api_types_preserve_decorated_function_and_native_result(
    tmp_path,
):
    root = Path(__file__).resolve().parents[1]
    fixture = root / "tests/typing/publication.py"
    config = tmp_path / "pyrightconfig.json"
    config.write_text(
        json.dumps(
            {
                "include": [
                    os.path.relpath(fixture, tmp_path),
                    os.path.relpath(
                        root / "papilio_tasks/apps/projections/base.py",
                        tmp_path,
                    ),
                ],
                "exclude": [],
                "extraPaths": [str(root)],
                "pythonVersion": "3.13",
                "typeCheckingMode": "strict",
            }
        )
    )
    checked = subprocess.run(
        [sys.executable, "-m", "pyright", "-p", str(config), "--outputjson"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = json.loads(checked.stdout)
    expected = {
        index
        for index, line in enumerate(fixture.read_text().splitlines())
        if "# error" in line
    }
    diagnostics = report["generalDiagnostics"]
    assert report["summary"]["filesAnalyzed"] == 2, report
    assert report["summary"]["errorCount"] == len(expected), report
    assert report["summary"]["warningCount"] == 0, report
    assert all(Path(d["file"]) == fixture for d in diagnostics), report
    assert {d["range"]["start"]["line"] for d in diagnostics} == expected

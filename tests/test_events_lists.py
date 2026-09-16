import asyncio
import importlib
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.redis")
pytest.importorskip("dishka_faststream")

from dishka import Provider, Scope
from faststream.redis import ListSub, TestRedisBroker
from faststream.redis import RedisBroker as NativeRedisBroker
from hook_support import handler

from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.lists import ListPublisher
from papilio_tasks.apps.events.registry.lists import ListRegistrar
from papilio_tasks.apps.events.subscribers.lists import ListSubscriber
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker
from papilio_tasks.tools.hooks.publish import PublishHooks


def registry(url="redis://localhost:6379"):
    return ListRegistrar(RedisBroker(NativeRedisBroker(url, logger=None)))


def declarations(name="orders"):
    class Created(ListPublisher[dict]):
        list = name

    class Finance(ListSubscriber[dict]):
        publisher = Created

        async def run(self, event: dict) -> None:
            pass

    return Created, Finance


@pytest.fixture(autouse=True)
def isolated_publishers(monkeypatch):
    monkeypatch.setattr(bindings, "_senders", {})


@pytest.mark.parametrize(
    "configured,override,reset,expected",
    [
        (None, None, False, 0.1),
        (0.2, None, False, 0.2),
        (0.2, 0.3, False, 0.3),
        (0.2, None, True, 0.1),
    ],
)
async def test_selection_inheritance_and_isolation(
    monkeypatch, configured, override, reset, expected
):
    reg = registry()
    Created, Parent = declarations()
    Parent.list = (
        ListSub("orders", polling_interval=configured) if configured else None
    )

    class Finance(Parent):
        pass

    if reset:
        Finance.list = None
    selected = (
        ListSub("orders", polling_interval=override) if override else None
    )
    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    before = dict(vars(Finance))
    reg.subscriber(Finance, list=selected)
    native = reg.broker.native.subscribers[0]
    assert native.list_sub.name == "orders"
    assert native.list_sub.polling_interval == expected
    assert not native.list_sub.batch
    assert dict(vars(Finance)) == before
    configured_route = selected or Finance.list
    if configured_route is not None:
        configured_route.polling_interval = 10
        assert native.list_sub.polling_interval == expected
    assert not reg.has_publisher(Created)
    connect.assert_not_awaited()
    assert not hasattr(Finance, "group_id")
    assert not hasattr(Finance, "ack_policy")
    await create_app(registrar=reg).stop()


@pytest.mark.parametrize(
    "invalid",
    [
        "publisher",
        "name",
        "subscriber",
        "list",
        "empty",
        "mismatch",
        "handler",
    ],
)
def test_invalid_configuration_precedes_native_registration(invalid):
    reg = registry()
    Created, Finance = declarations()
    if invalid == "publisher":
        operation = partial(reg.publisher, Publisher)
    elif invalid == "name":
        Created.list = ""
        operation = partial(reg.publisher, Created)
    elif invalid == "subscriber":
        operation = partial(reg.subscriber, Subscriber)
    elif invalid == "list":
        operation = partial(reg.subscriber, Finance, list="orders")
    elif invalid == "empty":
        selected = ListSub("orders")
        selected.name = ""
        operation = partial(reg.subscriber, Finance, list=selected)
    elif invalid == "mismatch":
        operation = partial(reg.subscriber, Finance, list=ListSub("other"))
    else:

        async def untyped(self, event):
            pass

        Finance.run = untyped
        operation = partial(reg.subscriber, Finance)
    with pytest.raises((ValueError, TypeError)):
        operation()
    assert not reg.broker.native.publishers
    assert not reg.broker.native.subscribers


@pytest.mark.parametrize("count", [1, 3])
async def test_publish_length_error_hooks_and_binding_ownership(
    monkeypatch, count
):
    reg, other = registry(), registry()
    Created, Finance = declarations()
    native = AsyncMock(return_value=count)
    monkeypatch.setattr(
        reg.broker,
        "publisher",
        lambda *args, **kw: SimpleNamespace(publish=native),
    )
    observed, errors = [], []

    async def after(sent):
        observed.append(sent)

    async def failed(error):
        errors.append(error)

    hooks = PublishHooks(
        after_send=(handler(after),), on_error=(handler(failed),)
    )
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: hooks, provides=PublishHooks)
    reg.publisher(Created)
    with pytest.raises(ValueError, match="already registered"):
        other.publisher(Created)
    reg.subscriber(Finance)
    with pytest.raises(ValueError, match="already registered"):
        reg.subscriber(Finance)
    app = create_app(
        registrar=reg, providers=[provider], publish_hooks=PublishHooks
    )
    try:
        assert (
            await Created.publish({"id": 1}, headers={"origin": "test"})
            == count
        )
        assert observed[0].result == count
        assert observed[0].call.meta == {"list": "orders"}
        native.assert_awaited_once_with({"id": 1}, headers={"origin": "test"})
        error = ConnectionError("offline")
        native.side_effect = error
        with pytest.raises(ConnectionError) as caught:
            await Created.publish({"id": 2})
        assert caught.value is error and errors[0].error is error
        assert len(observed) == 1 and native.await_count == 2
    finally:
        await app.stop()
    other.publisher(Created)
    other.close()


def test_app_import_isolation():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'aio_pika', 'aiokafka', 'confluent_kafka', 'nats'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events.registry.lists import ListRegistrar
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.cli.main import main
main(['events', 'run', '--help'])
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture
def package(tmp_path, monkeypatch):
    name = "lists_app_" + uuid4().hex
    folder = tmp_path / name
    folder.mkdir()
    (folder / "__init__.py").touch()
    monkeypatch.syspath_prepend(str(tmp_path))
    files = {
        "entry.py": """
            import os
            from faststream.redis import RedisBroker as NativeRedisBroker
            from papilio_tasks.infra.faststream.brokers.backends.redis import (
                RedisBroker,
            )
            from papilio_tasks.apps.events.registry.lists import (
                ListRegistrar,
            )
            from papilio_tasks.apps.events.application import create_app
            from .providers import Services, Sending, Receiving, Resource
            from .state import record
            from .subscribers import Finance


            def build():
                reg = ListRegistrar(
                    RedisBroker(
                        NativeRedisBroker(
                            os.environ["TEST_REDIS_URL"], logger=None
                        )
                    )
                )
                reg.subscriber(Finance)
                app = create_app(
                    registrar=reg,
                    providers=[Services()],
                    publishers=["LIST_PACKAGE"],
                    subscribers=["LIST_PACKAGE"],
                    publish_hooks=Sending,
                    subscribe_hooks=Receiving,
                )

                @app.after_startup
                async def ready():
                    await app.container.get(Resource)
                    record("ready")

                return app
        """,
        "providers.py": """
            from collections.abc import AsyncIterator
            from dishka import Provider, Scope, provide
            from faststream import StreamMessage
            from papilio_tasks.tools.hooks import Hook, Handler
            from papilio_tasks.tools.hooks.publish import (
                PublishHooks,
                Published,
            )
            from papilio_tasks.tools.hooks.subscribe import (
                SubscribeHooks,
                SubscribeCall,
            )
            from .subscribers import Finance, Session
            from .batch import Batch
            from .state import record


            class Before(Hook[SubscribeCall]):
                def __init__(self, session: Session):
                    self.session = session

                async def run(self, call: SubscribeCall) -> None:
                    record("before", session=self.session.id)


            class After(Before):
                async def run(self, call: SubscribeCall) -> None:
                    record("after", session=self.session.id)


            class Failed(Before):
                async def run(self, error) -> None:
                    record("error", session=self.session.id,
                           error=type(error.error).__name__)


            class Sent(Hook[Published]):
                async def run(self, event: Published) -> None:
                    record(
                        "sent",
                        list=event.call.meta["list"],
                        result=event.result,
                    )


            class Sending(PublishHooks):
                def __init__(self, sent: Sent):
                    super().__init__(after_send=(Handler(sent),))


            class Receiving(SubscribeHooks):
                def __init__(
                    self, before: Before, after: After, failed: Failed
                ):
                    super().__init__(
                        before_run=(Handler(before),),
                        after_run=(Handler(after),),
                        on_error=(Handler(failed),),
                    )


            class Resource:
                pass


            class Services(Provider):
                scope = Scope.REQUEST
                finance = provide(Finance)
                batch = provide(Batch)
                before = provide(Before)
                after = provide(After)
                failed = provide(Failed)
                sent = provide(Sent)
                sending = provide(Sending)
                receiving = provide(Receiving)

                @provide
                async def session(
                    self, message: StreamMessage
                ) -> AsyncIterator[Session]:
                    session = Session(message)
                    try:
                        yield session
                    finally:
                        record("closed", session=session.id)

                @provide(scope=Scope.APP)
                async def resource(self) -> AsyncIterator[Resource]:
                    try:
                        yield Resource()
                    finally:
                        record("app_closed")
        """,
        "publishers.py": """
            from pydantic import BaseModel
            from papilio_tasks.apps.events.publishers.lists import (
                ListPublisher,
            )


            class Order(BaseModel):
                id: int
                fail: bool = False


            class Created(ListPublisher[Order]):
                list = "LIST_PACKAGE.orders"
        """,
        "state.py": """
            import json, os
            from pathlib import Path

            records = []


            def record(phase, **values):
                row = dict(phase=phase, pid=os.getpid(), **values)
                records.append(row)
                if path := os.getenv("EVENT_LISTS_LOG"):
                    with Path(path).open("a") as file:
                        file.write(json.dumps(row) + "\\n")
        """,
        "subscribers.py": """
            from faststream import StreamMessage
            from papilio_tasks.apps.events.subscribers.lists import (
                ListSubscriber,
            )
            from .publishers import Created, Order
            from .state import record
            from uuid import uuid4


            class Session:
                def __init__(self, message: StreamMessage):
                    self.message = message
                    self.id = uuid4().hex


            class Finance(ListSubscriber[Order]):
                publisher = Created

                def __init__(self, session: Session):
                    self.session = session

                async def run(self, event: Order) -> None:
                    record(
                        "run",
                        role=type(self).__name__,
                        value=event.id,
                        typed=isinstance(event, Order),
                        session=self.session.id,
                        headers=self.session.message.headers,
                    )
                    if event.fail:
                        raise ValueError("handler failed")
        """,
        "batch.py": """
            from faststream.redis import ListSub
            from papilio_tasks.apps.events.subscribers.lists import (
                ListSubscriber,
            )
            from .publishers import Created, Order
            from .subscribers import Session
            from .state import record


            type Orders = list[Order]


            class Batch(ListSubscriber[Orders]):
                publisher = Created
                list = ListSub(Created.list, batch=True, max_records=2)

                def __init__(self, session: Session):
                    self.session = session

                async def run(self, event: Orders) -> None:
                    record(
                        "run", session=self.session.id,
                        typed=all(isinstance(item, Order) for item in event),
                        values=[item.id for item in event],
                    )

        """,
    }
    for file, code in files.items():
        (folder / file).write_text(
            textwrap.dedent(code).replace("LIST_PACKAGE", name)
        )
    importlib.invalidate_caches()
    yield name
    for key in tuple(sys.modules):
        if key == name or key.startswith(name + "."):
            del sys.modules[key]


def check_records(records, count):
    runs = [r for r in records if r["phase"] == "run"]
    assert len(runs) == count and all(r["typed"] for r in runs)
    for row in runs:
        phases = [
            r["phase"] for r in records if r.get("session") == row["session"]
        ]
        assert phases == ["before", "run", "after", "closed"]
    return runs


async def test_discovery_hooks_and_decorator(package, monkeypatch):
    monkeypatch.setenv("TEST_REDIS_URL", "redis://localhost:6379")
    entry = importlib.import_module(package + ".entry")
    pub = importlib.import_module(package + ".publishers")
    state = importlib.import_module(package + ".state")
    before = dict(vars(pub.Created))
    app = entry.build()
    assert len(app.registrar.broker.native.subscribers) == 1
    assert state.records == []
    result = {"id": 7, "other": True}

    @publish(pub.Created, select=lambda row: pub.Order(id=row["id"]))
    async def create():
        return result

    try:
        async with TestRedisBroker(app.registrar.broker.native):
            assert await create() is result
        assert check_records(state.records, 1)[0]["value"] == 7
        assert [r["list"] for r in state.records if r["phase"] == "sent"] == [
            pub.Created.list
        ]
        assert dict(vars(pub.Created)) == before
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await pub.Created.publish(pub.Order(id=1))


@pytest.fixture
def redis_url():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires isolated Redis")
    return url


@pytest.mark.parametrize("batch", [False, True])
async def test_live_backlog_single_and_batch_dtos(package, redis_url, batch):
    pub = importlib.import_module(package + ".publishers")
    sub = importlib.import_module(package + ".subscribers")
    services = importlib.import_module(package + ".providers")
    state = importlib.import_module(package + ".state")
    producer = registry(redis_url)
    producer_app = create_app(
        registrar=producer,
        publishers=[package],
        providers=[services.Services()],
        publish_hooks=services.Sending,
    )
    client = await producer.broker.connect()
    reg = registry(redis_url)
    batch_type = importlib.import_module(package + ".batch").Batch
    reg.subscriber(batch_type if batch else sub.Finance)
    consumer = create_app(
        registrar=reg,
        providers=[services.Services()],
        subscribe_hooks=services.Receiving,
    )
    try:
        # Before any consumer starts, RPUSH reports the growing backlog length.
        assert (
            await pub.Created.publish(
                pub.Order(id=0), headers={"origin": "test"}
            )
            == 1
        )
        assert (
            await producer.broker.publish_batch(
                *({"id": i} for i in range(1, 5)),
                list=pub.Created.list,
                headers={"origin": "test"},
            )
            == 5
        )
        assert await client.llen(pub.Created.list) == 5
        assert not any(r["phase"] == "run" for r in state.records)
        await consumer.start()
        expected_calls = 3 if batch else 5
        # Bounded observation of real consumer work and scope finalization.
        async with asyncio.timeout(10):
            while (
                sum(r["phase"] == "closed" for r in state.records)
                < expected_calls
            ):
                await asyncio.sleep(0.02)
        await consumer.stop()
        runs = check_records(state.records, expected_calls)
        if batch:
            assert [len(row["values"]) for row in runs] == [2, 2, 1]
            assert [i for row in runs for i in row["values"]] == list(range(5))
        else:
            assert [row["value"] for row in runs] == list(range(5))
            assert all(row["headers"]["origin"] == "test" for row in runs)
        assert await client.llen(pub.Created.list) == 0
        assert [
            r["result"] for r in state.records if r["phase"] == "sent"
        ] == [1]
    finally:
        await consumer.stop()
        await client.delete(pub.Created.list)
        await producer_app.stop()


async def test_live_failure_does_not_restore_message(package, redis_url):
    pub = importlib.import_module(package + ".publishers")
    sub = importlib.import_module(package + ".subscribers")
    services = importlib.import_module(package + ".providers")
    state = importlib.import_module(package + ".state")
    producer = registry(redis_url)
    producer_app = create_app(registrar=producer, publishers=[package])
    client = await producer.broker.connect()

    def consumer():
        reg = registry(redis_url)
        reg.subscriber(sub.Finance)
        return create_app(
            registrar=reg,
            providers=[services.Services()],
            subscribe_hooks=services.Receiving,
        )

    first, second = consumer(), consumer()
    try:
        assert await pub.Created.publish(pub.Order(id=1, fail=True)) == 1
        await first.start()
        # Observe completion before stopping and starting another consumer.
        async with asyncio.timeout(10):
            while not any(r["phase"] == "closed" for r in state.records):
                await asyncio.sleep(0.02)
        await first.stop()
        assert await client.llen(pub.Created.list) == 0
        assert [r["phase"] for r in state.records] == [
            "before",
            "run",
            "error",
            "closed",
        ]
        assert len({r["session"] for r in state.records}) == 1
        assert state.records[2]["error"] == "ValueError"
        await second.start()
        await pub.Created.publish(pub.Order(id=2))
        async with asyncio.timeout(10):
            while sum(r["phase"] == "closed" for r in state.records) < 2:
                await asyncio.sleep(0.02)
        await second.stop()
        assert [r["value"] for r in state.records if r["phase"] == "run"] == [
            1,
            2,
        ]
        check_records(state.records[4:], 1)
        assert await client.llen(pub.Created.list) == 0
    finally:
        await asyncio.gather(first.stop(), second.stop())
        await client.delete(pub.Created.list)
        await producer_app.stop()


async def test_live_cli_competing_processes(package, redis_url, tmp_path):
    paths = [tmp_path / f"events-{i}.jsonl" for i in range(2)]
    base_env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            (
                str(tmp_path),
                str(Path(__file__).resolve().parents[1]),
                os.getenv("PYTHONPATH", ""),
            )
        ),
    }
    cmd = [
        sys.executable,
        "-m",
        "papilio_tasks",
        "events",
        "run",
        package + ".entry:build",
        "--factory",
        "--app-dir",
        str(tmp_path),
    ]

    def records(index):
        path = paths[index]
        return (
            [json.loads(row) for row in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    def wait_for(check):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if check():
                return
            if any(worker.poll() is not None for worker in workers):
                break
            time.sleep(0.05)
        for output in outputs:
            output.flush()
            output.seek(0)
        pytest.fail("\n".join(output.read() for output in outputs))

    workers, outputs = [], []
    producer = registry(redis_url)
    app = create_app(registrar=producer, publishers=[package])
    pub = importlib.import_module(package + ".publishers")
    client = await producer.broker.connect()
    try:
        for index in range(2):
            output = (tmp_path / f"worker-{index}.log").open("w+")
            outputs.append(output)
            workers.append(
                subprocess.Popen(
                    cmd,
                    env={**base_env, "EVENT_LISTS_LOG": str(paths[index])},
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )
        await asyncio.to_thread(
            wait_for,
            lambda: all(
                any(r["phase"] == "ready" for r in records(i))
                for i in range(2)
            ),
        )
        await asyncio.gather(
            *(pub.Created.publish(pub.Order(id=i)) for i in range(12))
        )
        await asyncio.to_thread(
            wait_for,
            lambda: (
                sum(
                    r["phase"] == "closed"
                    for i in range(2)
                    for r in records(i)
                )
                == 12
            ),
        )
        for worker in workers:
            worker.send_signal(signal.SIGTERM)
        assert await asyncio.gather(
            *(asyncio.to_thread(w.wait, timeout=15) for w in workers)
        ) == [0, 0]
        all_runs = []
        for index in range(2):
            rows = records(index)
            count = sum(r["phase"] == "run" for r in rows)
            assert count > 0, "Both waiting workers must consume messages"
            all_runs.extend(check_records(rows, count))
            assert sum(r["phase"] == "app_closed" for r in rows) == 1
        assert sorted(row["value"] for row in all_runs) == list(range(12))
        assert len({row["pid"] for row in all_runs}) == 2
        assert await client.llen(pub.Created.list) == 0
        # With both workers offline the next publication remains queued.
        assert await pub.Created.publish(pub.Order(id=99)) == 1
        assert await client.llen(pub.Created.list) == 1
    finally:
        for worker in workers:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGKILL)
        await asyncio.gather(
            *(asyncio.to_thread(w.wait, timeout=5) for w in workers)
        )
        for output in outputs:
            output.close()
        await client.delete(pub.Created.list)
        await app.stop()

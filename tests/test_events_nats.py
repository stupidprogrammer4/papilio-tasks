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

pytest.importorskip("faststream.nats")
pytest.importorskip("dishka_faststream")

from dishka import Provider, Scope
from faststream.nats import NatsBroker as NativeNatsBroker
from faststream.nats import TestNatsBroker
from hook_support import handler

from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.nats import NatsPublisher
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.subscribers.nats import NatsSubscriber
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.hooks.publish import PublishHooks


def registry(url="nats://localhost:4222"):
    return NatsRegistrar(NatsBroker(NativeNatsBroker(url, logger=None)))


def declarations(name="orders"):
    class Created(NatsPublisher[dict]):
        subject = name

    class Finance(NatsSubscriber[dict]):
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
        (None, None, False, "orders"),
        ("orders.*", None, False, "orders.*"),
        ("orders.*", "other.*", False, "other.*"),
        ("orders.*", None, True, "orders"),
    ],
)
async def test_selection_inheritance_and_isolation(
    monkeypatch, configured, override, reset, expected
):
    reg = registry()
    Created, Parent = declarations()
    Parent.subject = configured

    class Finance(Parent):
        pass

    if reset:
        Finance.subject = None
    selected = override
    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    before = dict(vars(Finance))
    reg.subscriber(Finance, subject=selected)
    native = reg.broker.native.subscribers[0]
    assert native.subject.broker_address == expected
    assert dict(vars(Finance)) == before
    assert not reg.has_publisher(Created)
    connect.assert_not_awaited()
    assert not hasattr(Finance, "group_id")
    assert not hasattr(Finance, "ack_policy")
    app = create_app(registrar=reg)
    await app.stop()


@pytest.mark.parametrize(
    "invalid",
    [
        "publisher",
        "name",
        "subscriber",
        "subject",
        "empty",
        "queue",
        "handler",
    ],
)
def test_invalid_configuration_precedes_native_registration(invalid):
    reg = registry()
    Created, Finance = declarations()
    if invalid == "publisher":
        operation = partial(reg.publisher, Publisher)
    elif invalid == "name":
        Created.subject = ""
        operation = partial(reg.publisher, Created)
    elif invalid == "subscriber":
        operation = partial(reg.subscriber, Subscriber)
    elif invalid == "subject":
        operation = partial(reg.subscriber, Finance, subject=123)
    elif invalid == "empty":
        operation = partial(reg.subscriber, Finance, subject="")
    elif invalid == "queue":
        operation = partial(reg.subscriber, Finance, queue=123)
    else:

        async def untyped(self, event):
            pass

        Finance.run = untyped
        operation = partial(reg.subscriber, Finance)
    with pytest.raises((ValueError, TypeError)):
        operation()
    assert not reg.broker.native.publishers
    assert not reg.broker.native.subscribers


async def test_publish_result_error_hooks_and_binding_ownership(monkeypatch):
    reg, other = registry(), registry()
    Created, Finance = declarations()
    native = AsyncMock(return_value=None)
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
            is None
        )
        assert observed[0].result is None
        assert observed[0].call.meta == {"subject": "orders"}
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
        blocked = {'taskiq', 'aio_pika', 'aiokafka', 'confluent_kafka',
                   'redis', 'aiomqtt'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
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
    name = "nats_app_" + uuid4().hex
    folder = tmp_path / name
    folder.mkdir()
    (folder / "__init__.py").touch()
    monkeypatch.syspath_prepend(str(tmp_path))
    files = {
        "entry.py": """
            import os
            from faststream.nats import NatsBroker as NativeNatsBroker
            from papilio_tasks.infra.faststream.brokers.backends.nats import (
                NatsBroker,
            )
            from papilio_tasks.apps.events.registry.nats import (
                NatsRegistrar,
            )
            from papilio_tasks.apps.events.application import create_app
            from .providers import Services, Sending, Receiving, Resource
            from .state import record
            from .subscribers import Finance, Notify


            def build():
                reg = NatsRegistrar(
                    NatsBroker(
                        NativeNatsBroker(
                            os.environ["TEST_NATS_URL"], logger=None
                        )
                    )
                )
                reg.subscriber(Finance)
                reg.subscriber(Notify)
                app = create_app(
                    registrar=reg,
                    providers=[Services()],
                    publishers=["NATS_PACKAGE"],
                    subscribers=["NATS_PACKAGE"],
                    publish_hooks=Sending,
                    subscribe_hooks=Receiving,
                )

                @app.after_startup
                async def ready():
                    await app.container.get(Resource)
                    await reg.broker.native.connection.flush()
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
            from .subscribers import Finance, Notify, Session
            from .state import record


            class Before(Hook[SubscribeCall]):
                def __init__(self, session: Session):
                    self.session = session

                async def run(self, call: SubscribeCall) -> None:
                    record("before", session=self.session.id)


            class After(Before):
                async def run(self, call: SubscribeCall) -> None:
                    record("after", session=self.session.id)


            class Sent(Hook[Published]):
                async def run(self, event: Published) -> None:
                    record(
                        "sent",
                        subject=event.call.meta["subject"],
                        result=event.result,
                    )


            class Sending(PublishHooks):
                def __init__(self, sent: Sent):
                    super().__init__(after_send=(Handler(sent),))


            class Receiving(SubscribeHooks):
                def __init__(self, before: Before, after: After):
                    super().__init__(
                        before_run=(Handler(before),),
                        after_run=(Handler(after),),
                    )


            class Resource:
                pass


            class Services(Provider):
                scope = Scope.REQUEST
                finance = provide(Finance)
                notify = provide(Notify)
                before = provide(Before)
                after = provide(After)
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
            from papilio_tasks.apps.events.publishers.nats import (
                NatsPublisher,
            )


            class Order(BaseModel):
                id: int


            class Created(NatsPublisher[Order]):
                subject = "NATS_PACKAGE.orders"
        """,
        "state.py": """
            import json, os
            from pathlib import Path

            records = []


            def record(phase, **values):
                row = dict(phase=phase, pid=os.getpid(), **values)
                records.append(row)
                if path := os.getenv("EVENT_NATS_LOG"):
                    with Path(path).open("a") as file:
                        file.write(json.dumps(row) + "\\n")
        """,
        "subscribers.py": """
            from faststream import StreamMessage
            from papilio_tasks.apps.events.subscribers.nats import (
                NatsSubscriber,
            )
            from .publishers import Created, Order
            from .state import record
            from uuid import uuid4


            class Session:
                def __init__(self, message: StreamMessage):
                    self.message = message
                    self.id = uuid4().hex


            class Finance(NatsSubscriber[Order]):
                publisher = Created
                queue = "finance"

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


            class Notify(Finance):
                subject = "NATS_PACKAGE.*"
                queue = ""
        """,
    }
    for file, code in files.items():
        (folder / file).write_text(
            textwrap.dedent(code).replace("NATS_PACKAGE", name)
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
    monkeypatch.setenv("TEST_NATS_URL", "nats://localhost:4222")
    entry = importlib.import_module(package + ".entry")
    pub = importlib.import_module(package + ".publishers")
    state = importlib.import_module(package + ".state")
    before = dict(vars(pub.Created))
    app = entry.build()
    assert len(app.registrar.broker.native.subscribers) == 2
    assert state.records == []
    result = {"id": 7, "other": True}

    @publish(pub.Created, select=lambda row: pub.Order(id=row["id"]))
    async def create():
        return result

    try:
        async with TestNatsBroker(app.registrar.broker.native):
            assert await create() is result
        assert {r["role"] for r in check_records(state.records, 2)} == {
            "Finance",
            "Notify",
        }
        assert [
            r["subject"] for r in state.records if r["phase"] == "sent"
        ] == [pub.Created.subject]
        assert dict(vars(pub.Created)) == before
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await pub.Created.publish(pub.Order(id=1))


@pytest.mark.parametrize(
    "configured,override,expected",
    [
        ("", None, ""),
        ("finance", None, "finance"),
        ("finance", "notify", "notify"),
        ("finance", "", ""),
    ],
)
async def test_queue_precedence_and_explicit_clear(
    configured, override, expected
):
    reg = registry()
    _, Parent = declarations()
    Parent.queue = configured

    class Finance(Parent):
        pass

    before = dict(vars(Finance))
    reg.subscriber(Finance, queue=override)
    assert reg.broker.native.subscribers[0].queue == expected
    assert dict(vars(Finance)) == before
    await create_app(registrar=reg).stop()


@pytest.fixture
def nats_url():
    url = os.getenv("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS")
    return url


@pytest.mark.parametrize("pattern", ["*", ">"])
async def test_live_patterns_hooks_disconnect_and_no_replay(
    package, nats_url, pattern
):
    pub = importlib.import_module(package + ".publishers")
    sub = importlib.import_module(package + ".subscribers")
    services = importlib.import_module(package + ".providers")
    state = importlib.import_module(package + ".state")
    sub.Notify.subject = package + "." + pattern
    producer = registry(nats_url)
    app = create_app(
        registrar=producer,
        publishers=[package],
        providers=[services.Services()],
        publish_hooks=services.Sending,
    )
    client = await producer.broker.connect()

    def consumer(cls):
        reg = registry(nats_url)
        reg.subscriber(cls)
        return create_app(
            registrar=reg,
            providers=[services.Services()],
            subscribe_hooks=services.Receiving,
        )

    exact, wildcard = consumer(sub.Finance), consumer(sub.Notify)
    later = None
    try:
        assert await pub.Created.publish(pub.Order(id=0)) is None
        await client.flush()
        await asyncio.gather(exact.start(), wildcard.start())
        await asyncio.gather(
            exact.registrar.broker.native.connection.flush(),
            wildcard.registrar.broker.native.connection.flush(),
        )
        assert (
            await pub.Created.publish(
                pub.Order(id=1), headers={"origin": "test"}
            )
            is None
        )
        assert (
            await pub.Created.publish(
                pub.Order(id=2), subject=package + ".prices"
            )
            is None
        )
        assert (
            await pub.Created.publish(
                pub.Order(id=5), subject=package + ".prices.gold"
            )
            is None
        )
        count = 4 if pattern == ">" else 3
        # Bounded observation of native consumption and per-message cleanup.
        async with asyncio.timeout(10):
            while sum(r["phase"] == "closed" for r in state.records) < count:
                await asyncio.sleep(0.02)
        await asyncio.gather(exact.stop(), wildcard.stop())
        assert await pub.Created.publish(pub.Order(id=3)) is None
        await client.flush()
        later = consumer(sub.Finance)
        await later.start()
        await later.registrar.broker.native.connection.flush()
        assert await pub.Created.publish(pub.Order(id=4)) is None
        async with asyncio.timeout(10):
            while (
                sum(r["phase"] == "closed" for r in state.records) < count + 1
            ):
                await asyncio.sleep(0.02)
        await later.stop()
        runs = check_records(state.records, count + 1)
        assert {
            role: sorted(r["value"] for r in runs if r["role"] == role)
            for role in ("Finance", "Notify")
        } == {
            "Finance": [1, 4],
            "Notify": [1, 2, 5] if pattern == ">" else [1, 2],
        }
        assert all(
            r["headers"]["origin"] == "test" for r in runs if r["value"] == 1
        )
        sent = [r for r in state.records if r["phase"] == "sent"]
        assert len(sent) == 6 and all(r["result"] is None for r in sent)
        assert all(r["subject"] == pub.Created.subject for r in sent)
    finally:
        await asyncio.gather(exact.stop(), wildcard.stop())
        if later is not None:
            await later.stop()
        await app.stop()
    assert client.is_closed


async def test_live_cli_queue_competition_and_independent_broadcast(
    package, nats_url, tmp_path
):
    paths = [tmp_path / f"events-{i}.jsonl" for i in range(2)]
    env = {
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
            if any(w.poll() is not None for w in workers):
                break
            time.sleep(0.05)
        for output in outputs:
            output.flush()
            output.seek(0)
        pytest.fail("\n".join(output.read() for output in outputs))

    workers, outputs = [], []
    producer = registry(nats_url)
    app = create_app(registrar=producer, publishers=[package])
    pub = importlib.import_module(package + ".publishers")
    try:
        for index in range(2):
            output = (tmp_path / f"worker-{index}.log").open("w+")
            outputs.append(output)
            workers.append(
                subprocess.Popen(
                    cmd,
                    env={**env, "EVENT_NATS_LOG": str(paths[index])},
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
        await app.connect()
        results = await asyncio.gather(
            *(pub.Created.publish(pub.Order(id=i)) for i in range(40))
        )
        assert results == [None] * 40
        await asyncio.to_thread(
            wait_for,
            lambda: (
                sum(
                    r["phase"] == "closed"
                    for i in range(2)
                    for r in records(i)
                )
                == 120
            ),
        )
        for worker in workers:
            worker.send_signal(signal.SIGTERM)
        assert await asyncio.gather(
            *(asyncio.to_thread(w.wait, timeout=15) for w in workers)
        ) == [0, 0]
        finance = []
        for index in range(2):
            rows = records(index)
            count = sum(r["phase"] == "run" for r in rows)
            runs = check_records(rows, count)
            group = [r for r in runs if r["role"] == "Finance"]
            assert group, "Both queue group workers should consume"
            finance.extend(group)
            assert sorted(
                r["value"] for r in runs if r["role"] == "Notify"
            ) == list(range(40))
            assert sum(r["phase"] == "app_closed" for r in rows) == 1
        assert sorted(r["value"] for r in finance) == list(range(40))
        assert len({r["pid"] for r in finance}) == 2
        assert await pub.Created.publish(pub.Order(id=99)) is None
    finally:
        for worker in workers:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGKILL)
        await asyncio.gather(
            *(asyncio.to_thread(w.wait, timeout=5) for w in workers)
        )
        for output in outputs:
            output.close()
        await app.stop()

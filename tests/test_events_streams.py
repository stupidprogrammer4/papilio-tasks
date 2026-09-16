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

from dishka import Provider, Scope, provide
from faststream import AckPolicy, StreamMessage
from faststream.redis import RedisBroker as NativeRedisBroker
from faststream.redis import StreamSub, TestRedisBroker
from hook_support import handler

from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.events.subscribers.streams import StreamSubscriber
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker
from papilio_tasks.tools.hooks.publish import PublishHooks


def registry(url="redis://localhost:6379", **options):
    return StreamRegistrar(
        RedisBroker(NativeRedisBroker(url, logger=None, **options))
    )


def declarations(name="orders"):
    class Created(StreamPublisher[dict]):
        stream = name

    class Finance(StreamSubscriber[dict]):
        publisher = Created
        stream = StreamSub(name, group="finance", consumer="worker")

        async def run(self, event: dict) -> None:
            pass

    return Created, Finance


@pytest.fixture(autouse=True)
def isolated_publishers(monkeypatch):
    monkeypatch.setattr(bindings, "_senders", {})


@pytest.mark.parametrize(
    "class_policy,explicit,expected",
    [
        (None, None, AckPolicy.ACK),
        (AckPolicy.MANUAL, None, AckPolicy.MANUAL),
        (AckPolicy.MANUAL, AckPolicy.ACK_FIRST, AckPolicy.ACK_FIRST),
    ],
)
async def test_ack_precedence_override_and_configuration_isolation(
    monkeypatch, class_policy, explicit, expected
):
    reg = registry(ack_policy=AckPolicy.ACK)
    Created, Parent = declarations()
    Parent.ack_policy = class_policy

    class Finance(Parent):
        pass

    selected = StreamSub(
        Created.stream, group="other", consumer="replica", max_records=1
    )
    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    reg.subscriber(Finance, stream=selected, ack_policy=explicit)
    native = reg.broker.native.subscribers[0]
    assert native.ack_policy is expected
    assert native.stream_sub.group == "other"
    selected.group = "changed"
    assert native.stream_sub.group == "other"
    assert Finance.stream.group == "finance"
    assert not reg.has_publisher(Created)
    connect.assert_not_awaited()
    app = create_app(registrar=reg)
    await app.stop()


@pytest.mark.parametrize(
    "invalid", ["publisher", "name", "subscriber", "route", "stream", "ack"]
)
def test_invalid_registration_has_no_native_side_effects(invalid):
    reg = registry()
    Created, Finance = declarations()
    if invalid == "publisher":
        operation = partial(reg.publisher, Publisher)
    elif invalid == "name":
        Created.stream = ""
        operation = partial(reg.publisher, Created)
    elif invalid == "subscriber":
        operation = partial(reg.subscriber, Subscriber)
    elif invalid == "route":
        operation = partial(reg.subscriber, Finance, stream=StreamSub("wrong"))
    elif invalid == "stream":
        operation = partial(reg.subscriber, Finance, stream="orders")
    else:
        operation = partial(reg.subscriber, Finance, ack_policy="ack")
    with pytest.raises((ValueError, TypeError)):
        operation()
    assert not reg.broker.native.publishers
    assert not reg.broker.native.subscribers


def test_app_optional_import_isolation():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'aio_pika', 'aiokafka', 'confluent_kafka', 'nats'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
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


async def test_publication_hooks_result_error_and_binding_ownership(
    monkeypatch,
):
    reg, other = registry(), registry()
    Created, Finance = declarations()
    receipt = b"100-0"
    native = AsyncMock(return_value=receipt)
    monkeypatch.setattr(
        reg.broker,
        "publisher",
        lambda **kwargs: SimpleNamespace(publish=native),
    )
    observed, errors = [], []

    async def after(sent):
        observed.append(sent)

    async def failed(error):
        errors.append(error)

    hooks = PublishHooks(
        after_send=(handler(after),), on_error=(handler(failed),)
    )
    providers = Provider(scope=Scope.REQUEST)
    providers.provide(lambda: hooks, provides=PublishHooks)
    reg.publisher(Created)
    with pytest.raises(ValueError, match="already registered"):
        other.publisher(Created)
    reg.subscriber(Finance)
    with pytest.raises(ValueError, match="already registered"):
        reg.subscriber(Finance)
    app = create_app(
        registrar=reg, providers=[providers], publish_hooks=PublishHooks
    )
    try:
        assert await Created.publish({"id": 1}) is receipt
        assert observed[0].result is receipt
        assert observed[0].call.meta == {"stream": "orders"}
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


@pytest.fixture
def package(tmp_path, monkeypatch):
    name = "streams_app_" + uuid4().hex
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
            from papilio_tasks.apps.events.registry.streams import (
                StreamRegistrar,
            )
            from papilio_tasks.apps.events.application import create_app
            from .providers import Services, Sending, Receiving, Resource
            from .state import record
            from .subscribers import Finance, Notify


            def build():
                reg = StreamRegistrar(
                    RedisBroker(
                        NativeRedisBroker(
                            os.environ["TEST_REDIS_URL"], logger=None
                        )
                    )
                )
                reg.subscriber(Finance)
                reg.subscriber(Notify)
                app = create_app(
                    registrar=reg,
                    providers=[Services()],
                    publishers=["STREAMS_PACKAGE"],
                    subscribers=["STREAMS_PACKAGE"],
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
                    record("sent", stream=event.call.meta["stream"])


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
            from papilio_tasks.apps.events.publishers.streams import (
                StreamPublisher,
            )


            class Order(BaseModel):
                id: int


            class Created(StreamPublisher[Order]):
                stream = "STREAMS_PACKAGE"
        """,
        "state.py": """
            import json, os
            from pathlib import Path

            records = []


            def record(phase, **values):
                row = dict(phase=phase, pid=os.getpid(), **values)
                records.append(row)
                if path := os.getenv("EVENT_STREAMS_LOG"):
                    with Path(path).open("a") as file:
                        file.write(json.dumps(row) + "\\n")
        """,
        "subscribers.py": """
            import os
            from faststream.redis import StreamSub

            from faststream import StreamMessage
            from papilio_tasks.apps.events.subscribers.streams import (
                StreamSubscriber,
            )
            from .publishers import Created, Order
            from .state import record
            from uuid import uuid4


            class Session:
                def __init__(self, message: StreamMessage):
                    self.message = message
                    self.id = uuid4().hex


            class Finance(StreamSubscriber[Order]):
                publisher = Created
                stream = StreamSub(
                    Created.stream,
                    group="finance",
                    consumer="worker-" + str(os.getpid()),
                    max_records=1,
                )

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
                stream = StreamSub(
                    Created.stream,
                    group="notify",
                    consumer="worker-" + str(os.getpid()),
                    max_records=1,
                )
        """,
    }
    for file, code in files.items():
        (folder / file).write_text(
            textwrap.dedent(code).replace("STREAMS_PACKAGE", name)
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


async def test_discovery_dto_hooks_decorator_and_release(package, monkeypatch):
    monkeypatch.setenv("TEST_REDIS_URL", "redis://localhost:6379")
    entry = importlib.import_module(package + ".entry")
    pub = importlib.import_module(package + ".publishers")
    state = importlib.import_module(package + ".state")
    app = entry.build()
    assert len(app.registrar.broker.native.subscribers) == 2
    assert state.records == []
    before = dict(vars(pub.Created))
    result = {"id": 7, "other": True}

    @publish(pub.Created, select=lambda row: pub.Order(id=row["id"]))
    async def create():
        return result

    try:
        async with TestRedisBroker(app.registrar.broker.native):
            assert await create() is result
        assert {r["role"] for r in check_records(state.records, 2)} == {
            "Finance",
            "Notify",
        }
        assert [
            r["stream"] for r in state.records if r["phase"] == "sent"
        ] == [pub.Created.stream]
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


async def test_live_groups_hooks_native_id_and_ack(
    package, redis_url, monkeypatch
):
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
    workers = [registry(redis_url) for _ in range(2)]
    for index, reg in enumerate(workers):
        reg.subscriber(
            sub.Finance,
            stream=StreamSub(
                package, group="finance", consumer=str(index), max_records=1
            ),
        )
    workers[0].subscriber(sub.Notify)
    deliveries = []
    for reg in workers:
        native = next(
            s
            for s in reg.broker.native.subscribers
            if s.stream_sub.group == "finance"
        )
        spy = AsyncMock(wraps=native.consume)
        monkeypatch.setattr(native, "consume", spy)
        deliveries.append(spy)
    apps = [
        create_app(
            registrar=reg,
            providers=[services.Services()],
            subscribe_hooks=services.Receiving,
        )
        for reg in workers
    ]
    client = await producer.broker.connect()
    try:
        await asyncio.gather(*(app.start() for app in apps))
        receipts = await asyncio.gather(
            *(
                pub.Created.publish(
                    pub.Order(id=i), headers={"origin": "test"}
                )
                for i in range(8)
            )
        )
        assert all(isinstance(r, bytes) for r in receipts)
        # Bounded test polling observes message-scope cleanup and native XACK.
        async with asyncio.timeout(10):
            while sum(r["phase"] == "closed" for r in state.records) < 16:
                await asyncio.sleep(0.02)
        await asyncio.gather(*(app.stop() for app in apps))
        runs = check_records(state.records, 16)
        assert all(r["headers"]["origin"] == "test" for r in runs)
        assert all(spy.await_count > 0 for spy in deliveries)
        assert sum(spy.await_count for spy in deliveries) == 8
        assert {
            role: sorted(r["value"] for r in runs if r["role"] == role)
            for role in ("Finance", "Notify")
        } == {"Finance": list(range(8)), "Notify": list(range(8))}
        assert sum(r["phase"] == "sent" for r in state.records) == 8
        assert {row[0] for row in await client.xrange(package)} == set(
            receipts
        )
        groups = await client.xinfo_groups(package)
        assert {g["name"] for g in groups} == {b"finance", b"notify"}
        assert all(g["pending"] == 0 for g in groups)
        consumers = await client.xinfo_consumers(package, "finance")
        assert {c["name"] for c in consumers} == {b"0", b"1"}
        assert sub.Finance.stream.group == "finance"
    finally:
        await asyncio.gather(*(app.stop() for app in apps))
        try:
            await client.delete(package)
        finally:
            await producer_app.stop()


@pytest.mark.parametrize(
    "policy,fails,pending",
    [
        (AckPolicy.ACK, False, 0),
        (AckPolicy.ACK, True, 0),
        (AckPolicy.NACK_ON_ERROR, True, 1),
        (AckPolicy.REJECT_ON_ERROR, True, 1),
        (AckPolicy.MANUAL, False, 1),
    ],
)
async def test_live_ack_and_pending_are_native(
    redis_url, policy, fails, pending
):
    name = "stream-ack-" + uuid4().hex
    Created, Base = declarations(name)
    done = asyncio.Event()
    messages = []

    class Consumer(Base):
        ack_policy = policy

        def __init__(self, message: StreamMessage):
            self.message = message

        async def run(self, event: dict) -> None:
            messages.append(self.message)
            done.set()
            if fails:
                raise ValueError("business failure")

    class Services(Provider):
        consumer = provide(Consumer, scope=Scope.REQUEST)

    reg = registry(redis_url)
    reg.publisher(Created)
    reg.subscriber(Consumer)
    app = create_app(registrar=reg, providers=[Services()])
    client = await reg.broker.connect()
    try:
        await app.start()
        receipt = await Created.publish({"id": 1})
        await asyncio.wait_for(done.wait(), 10)
        await app.stop()
        info = await client.xpending(name, "finance")
        assert info["pending"] == pending
        assert (await client.xrange(name))[0][0] == receipt
        assert len(messages) == 1
    finally:
        await app.stop()
        try:
            await client.delete(name)
        finally:
            await client.aclose()


async def test_live_cli_discovery_and_shutdown(package, redis_url, tmp_path):
    log = tmp_path / "events.jsonl"
    env = {
        **os.environ,
        "EVENT_STREAMS_LOG": str(log),
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

    def records():
        return (
            [json.loads(row) for row in log.read_text().splitlines()]
            if log.exists()
            else []
        )

    def wait_for(check, worker, output):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if check(records()):
                return
            if worker.poll() is not None:
                break
            time.sleep(0.05)
        output.flush()
        output.seek(0)
        pytest.fail(output.read())

    with (tmp_path / "worker.log").open("w+") as output:
        worker = subprocess.Popen(
            cmd,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        producer = registry(redis_url)
        app = create_app(registrar=producer, publishers=[package])
        pub = importlib.import_module(package + ".publishers")
        client = await producer.broker.connect()
        try:
            await asyncio.to_thread(
                wait_for,
                lambda rows: any(r["phase"] == "ready" for r in rows),
                worker,
                output,
            )
            await pub.Created.publish(pub.Order(id=42))
            await asyncio.to_thread(
                wait_for,
                lambda rows: sum(r["phase"] == "closed" for r in rows) == 2,
                worker,
                output,
            )
            worker.send_signal(signal.SIGTERM)
            assert await asyncio.to_thread(worker.wait, timeout=15) == 0
            runs = check_records(records(), 2)
            assert {r["role"] for r in runs} == {"Finance", "Notify"}
            assert all(r["value"] == 42 for r in runs)
            assert sum(r["phase"] == "app_closed" for r in records()) == 1
        finally:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGKILL)
                await asyncio.to_thread(worker.wait, timeout=5)
            try:
                await client.delete(package)
            finally:
                await app.stop()

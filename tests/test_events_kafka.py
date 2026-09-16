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

pytest.importorskip("faststream.kafka")
pytest.importorskip("dishka_faststream")

from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from dishka import Provider, Scope
from faststream import AckPolicy
from faststream.kafka import KafkaBroker as NativeKafkaBroker
from faststream.kafka import TestKafkaBroker
from hook_support import handler

from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.kafka import KafkaPublisher
from papilio_tasks.apps.events.registry.kafka import KafkaRegistrar
from papilio_tasks.apps.events.subscribers.kafka import KafkaSubscriber
from papilio_tasks.infra.faststream.brokers.backends.kafka import KafkaBroker
from papilio_tasks.tools.hooks.publish import PublishHooks


def registry(url="localhost:9092", **options):
    return KafkaRegistrar(
        KafkaBroker(NativeKafkaBroker(url, logger=None, **options))
    )


def declarations(topic="orders"):
    class Created(KafkaPublisher[dict]):
        pass

    Created.topic = topic

    class Finance(KafkaSubscriber[dict]):
        publisher = Created
        group_id = "finance"

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
        (
            AckPolicy.MANUAL,
            AckPolicy.ACK_FIRST,
            AckPolicy.ACK_FIRST,
        ),
    ],
)
async def test_ack_precedence_and_no_registration_io(
    monkeypatch, class_policy, explicit, expected
):
    reg = registry(ack_policy=AckPolicy.ACK)
    Created, Parent = declarations()
    Parent.ack_policy = class_policy

    class Finance(Parent):
        pass

    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    before = dict(vars(Finance))
    reg.subscriber(Finance, ack_policy=explicit, auto_offset_reset="earliest")
    assert reg.broker.native.subscribers[0].ack_policy is expected
    assert dict(vars(Finance)) == before
    assert not reg.has_publisher(Created)
    connect.assert_not_awaited()
    app = create_app(registrar=reg)
    await app.stop()


@pytest.mark.parametrize(
    "invalid", ["publisher", "topic", "subscriber", "group", "ack", "handler"]
)
def test_invalid_declarations_leave_native_registration_empty(invalid):
    reg = registry()
    Created, Finance = declarations()
    if invalid == "publisher":
        operation = partial(reg.publisher, Publisher)
    elif invalid == "topic":
        Created.topic = ""
        operation = partial(reg.publisher, Created)
    elif invalid == "subscriber":
        operation = partial(reg.subscriber, Subscriber)
    elif invalid == "group":
        Finance.group_id = ""
        operation = partial(reg.subscriber, Finance)
    elif invalid == "ack":
        operation = partial(reg.subscriber, Finance, ack_policy="ack")
    else:

        async def untyped(self, event):
            pass

        Finance.run = untyped
        operation = partial(reg.subscriber, Finance)
    with pytest.raises((ValueError, TypeError)):
        operation()
    assert not reg.broker.native.publishers
    assert not reg.broker.native.subscribers
    assert not reg.has_publisher(Created)


async def test_duplicate_registration_and_binding_ownership():
    reg, other = registry(), registry()
    Created, Finance = declarations()
    reg.publisher(Created)
    with pytest.raises(ValueError, match="already registered"):
        other.publisher(Created)
    reg.subscriber(Finance)
    with pytest.raises(ValueError, match="already registered"):
        reg.subscriber(Finance)
    app = create_app(registrar=reg)
    try:
        with pytest.raises(RuntimeError, match="before creating"):
            reg.subscriber(Finance)
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await Created.publish({})
    other.publisher(Created)
    other.close()


async def test_publication_hooks_preserve_future_and_error(monkeypatch):
    reg = registry()
    Created, _ = declarations()
    pending = asyncio.get_running_loop().create_future()
    native = AsyncMock(return_value=pending)
    monkeypatch.setattr(
        reg.broker,
        "publisher",
        lambda *args, **kwargs: SimpleNamespace(publish=native),
    )
    sent, errors = [], []

    async def after(event):
        sent.append(event)

    async def failed(event):
        errors.append(event)

    hooks = PublishHooks(
        after_send=(handler(after),), on_error=(handler(failed),)
    )
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: hooks, provides=PublishHooks)
    reg.publisher(Created)
    app = create_app(
        registrar=reg, providers=[provider], publish_hooks=PublishHooks
    )
    try:
        result = await Created.publish({"id": 1}, no_confirm=True)
        assert result is pending and not pending.done()
        assert sent[0].result is pending
        assert sent[0].call.meta == {"topic": "orders"}
        assert sent[0].call.kwargs == {"no_confirm": True}
        pending.set_result("receipt")
        assert await result == "receipt"
        error = ConnectionError("offline")
        native.side_effect = error
        with pytest.raises(ConnectionError) as caught:
            await Created.publish({"id": 2})
        assert caught.value is error and errors[0].error is error
        assert len(sent) == 1 and native.await_count == 2
    finally:
        await app.stop()


def test_kafka_application_import_isolation():
    script = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'aio_pika', 'redis', 'confluent_kafka', 'nats'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.registry.kafka import KafkaRegistrar
from papilio_tasks.apps.events.publishers.kafka import KafkaPublisher
from papilio_tasks.apps.events.subscribers.kafka import KafkaSubscriber
from papilio_tasks.cli.main import main
main(['events', 'run', '--help'])
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture
def package(tmp_path, monkeypatch):
    name = "kafka_app_" + uuid4().hex
    folder = tmp_path / name
    folder.mkdir()
    (folder / "__init__.py").touch()
    monkeypatch.syspath_prepend(str(tmp_path))
    files = {
        "entry.py": """
            import os
            from faststream.kafka import KafkaBroker as NativeKafkaBroker
            from papilio_tasks.infra.faststream.brokers.backends.kafka import (
                KafkaBroker,
            )
            from papilio_tasks.apps.events.registry.kafka import (
                KafkaRegistrar,
            )
            from papilio_tasks.apps.events.application import create_app
            from .providers import Services, Sending, Receiving, Resource
            from .state import record
            from .subscribers import Finance, Notify


            def build():
                reg = KafkaRegistrar(
                    KafkaBroker(
                        NativeKafkaBroker(
                            os.environ["TEST_KAFKA_URL"], logger=None
                        )
                    )
                )
                reg.subscriber(
                    Finance,
                    auto_offset_reset="earliest",
                    session_timeout_ms=6000,
                    heartbeat_interval_ms=1000,
                )
                reg.subscriber(
                    Notify,
                    auto_offset_reset="earliest",
                    session_timeout_ms=6000,
                    heartbeat_interval_ms=1000,
                )
                app = create_app(
                    registrar=reg,
                    providers=[Services()],
                    publishers=["KAFKA_PACKAGE"],
                    subscribers=["KAFKA_PACKAGE"],
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
                    record("sent", topic=event.call.meta["topic"])


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
            from papilio_tasks.apps.events.publishers.kafka import (
                KafkaPublisher,
            )


            class Order(BaseModel):
                id: int


            class Created(KafkaPublisher[Order]):
                topic = "KAFKA_PACKAGE"
        """,
        "state.py": """
            import json, os
            from pathlib import Path

            records = []


            def record(phase, **values):
                row = dict(phase=phase, pid=os.getpid(), **values)
                records.append(row)
                if path := os.getenv("EVENT_KAFKA_LOG"):
                    with Path(path).open("a") as file:
                        file.write(json.dumps(row) + "\\n")
        """,
        "subscribers.py": """
            from faststream import StreamMessage
            from papilio_tasks.apps.events.subscribers.kafka import (
                KafkaSubscriber,
            )
            from .publishers import Created, Order
            from .state import record
            from uuid import uuid4


            class Session:
                def __init__(self, message: StreamMessage):
                    self.message = message
                    self.id = uuid4().hex


            class Finance(KafkaSubscriber[Order]):
                publisher = Created
                group_id = "KAFKA_PACKAGE-finance"

                def __init__(self, session: Session):
                    self.session = session

                async def run(self, event: Order) -> None:
                    record(
                        "run",
                        role=type(self).__name__,
                        value=event.id,
                        typed=isinstance(event, Order),
                        session=self.session.id,
                    )


            class Notify(Finance):
                group_id = "KAFKA_PACKAGE-notify"
        """,
    }
    for file, code in files.items():
        (folder / file).write_text(
            textwrap.dedent(code).replace("KAFKA_PACKAGE", name)
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


async def test_discovery_dto_decorator_hooks_and_release(package, monkeypatch):
    monkeypatch.setenv("TEST_KAFKA_URL", "localhost:9092")
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
        async with TestKafkaBroker(app.registrar.broker.native):
            assert await create() is result
        assert {r["role"] for r in check_records(state.records, 2)} == {
            "Finance",
            "Notify",
        }
        assert [r["topic"] for r in state.records if r["phase"] == "sent"] == [
            pub.Created.topic
        ]
        assert dict(vars(pub.Created)) == before
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await pub.Created.publish(pub.Order(id=1))


@pytest.fixture
async def kafka_topic(package):
    url = os.getenv("TEST_KAFKA_URL")
    if not url:
        pytest.skip("Requires isolated Kafka")
    admin = AIOKafkaAdminClient(bootstrap_servers=url)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(package, num_partitions=2, replication_factor=1)]
        )
        yield url
    finally:
        try:
            await admin.delete_topics([package])
        finally:
            await admin.close()


async def test_live_discovery_groups_hooks_and_dto(package, kafka_topic):
    pub = importlib.import_module(package + ".publishers")
    sub = importlib.import_module(package + ".subscribers")
    services = importlib.import_module(package + ".providers")
    state = importlib.import_module(package + ".state")
    producer = registry(kafka_topic)
    producer_app = create_app(
        registrar=producer,
        publishers=[package],
        providers=[services.Services()],
        publish_hooks=services.Sending,
    )
    workers = [registry(kafka_topic) for _ in range(2)]
    for reg in workers:
        reg.subscriber(
            sub.Finance,
            auto_offset_reset="earliest",
            session_timeout_ms=6000,
            heartbeat_interval_ms=1000,
        )
    workers[0].subscriber(sub.Notify, auto_offset_reset="earliest")
    apps = [
        create_app(
            registrar=reg,
            providers=[services.Services()],
            subscribe_hooks=services.Receiving,
        )
        for reg in workers
    ]
    try:
        await asyncio.gather(*(app.start() for app in apps))
        # Bounded test polling waits for native group assignment.
        async with asyncio.timeout(30):
            while True:
                assigned = [
                    next(
                        s
                        for s in reg.broker.native.subscribers
                        if s.group_id == sub.Finance.group_id
                    ).consumer.assignment()
                    for reg in workers
                ]
                if all(assigned) and assigned[0].isdisjoint(assigned[1]):
                    break
                await asyncio.sleep(0.05)
        assert assigned[0] | assigned[1] == {
            TopicPartition(package, 0),
            TopicPartition(package, 1),
        }
        await producer_app.connect()
        receipts = await asyncio.gather(
            *(
                pub.Created.publish(pub.Order(id=i), partition=i % 2)
                for i in range(4)
            )
        )
        assert all(r.topic == package for r in receipts)
        # Observe scope finalization as well as handler delivery.
        async with asyncio.timeout(20):
            while sum(r["phase"] == "closed" for r in state.records) < 8:
                await asyncio.sleep(0.05)
        runs = check_records(state.records, 8)
        assert {
            role: sorted(r["value"] for r in runs if r["role"] == role)
            for role in ("Finance", "Notify")
        } == {"Finance": [0, 1, 2, 3], "Notify": [0, 1, 2, 3]}
        assert sum(r["phase"] == "sent" for r in state.records) == 4
    finally:
        await asyncio.gather(
            producer_app.stop(), *(app.stop() for app in apps)
        )
    with pytest.raises(RuntimeError, match="not registered"):
        bindings.get(pub.Created)


async def test_live_cli_discovery_and_shutdown(package, kafka_topic, tmp_path):
    log = tmp_path / "events.jsonl"
    env = {
        **os.environ,
        "EVENT_KAFKA_LOG": str(log),
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
        deadline = time.monotonic() + 30
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
        producer = registry(kafka_topic)
        app = create_app(registrar=producer, publishers=[package])
        pub = importlib.import_module(package + ".publishers")
        try:
            await asyncio.to_thread(
                wait_for,
                lambda rows: any(r["phase"] == "ready" for r in rows),
                worker,
                output,
            )
            await app.connect()
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
            await app.stop()

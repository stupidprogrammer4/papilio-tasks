import asyncio
import importlib
import json
import os
import signal
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.nats")
pytest.importorskip("dishka_faststream")

import test_events_nats as nats_tests
from dishka import Provider, Scope
from faststream import AckPolicy, StreamMessage
from faststream.nats import JStream, PubAck, PullSub
from faststream.nats import NatsBroker as NativeNatsBroker
from hook_support import handler
from nats.js.api import ConsumerConfig
from nats.js.errors import NoStreamResponseError
from test_events_nats import check_records

from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.jetstream import JetPublisher
from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
from papilio_tasks.apps.events.subscribers.jetstream import JetSubscriber
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks

nats_package = nats_tests.package


def registry(url="nats://localhost:4222"):
    return JetRegistrar(NatsBroker(NativeNatsBroker(url, logger=None)))


def declarations(name="orders"):
    class Created(JetPublisher[int]):
        subject = name + ".created"
        stream = JStream(name)

    class Finance(JetSubscriber[int]):
        publisher = Created

        async def run(self, event: int) -> None:
            pass

    return Created, Finance


@pytest.fixture(autouse=True)
def isolated_publishers(monkeypatch):
    monkeypatch.setattr(bindings, "_senders", {})


@pytest.fixture
def nats_url():
    url = os.getenv("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    return url


@pytest.mark.parametrize("override", [False, True])
async def test_inheritance_precedence_and_mutable_isolation(
    monkeypatch, override
):
    Created, Parent = declarations()
    Parent.subject = "orders.*"
    Parent.queue = "finance"
    Parent.durable = "finance"
    Parent.pull_sub = PullSub(batch_size=4, timeout=0.2)
    Parent.config = ConsumerConfig(ack_wait=0.5, max_deliver=3)
    Parent.ack_policy = AckPolicy.NACK_ON_ERROR

    class Finance(Parent):
        pass

    reg = registry()
    spy = Mock(wraps=reg.broker.subscriber)
    monkeypatch.setattr(reg.broker, "subscriber", spy)
    options = (
        dict(
            subject="orders.other",
            queue="",
            durable="other",
            pull_sub=False,
            config=ConsumerConfig(max_deliver=5),
            ack_policy=AckPolicy.MANUAL,
        )
        if override
        else {}
    )
    before = deepcopy(Parent.config)
    subjects = list(Created.stream.subjects)
    reg.publisher(Created)
    with pytest.warns(RuntimeWarning):
        reg.subscriber(Finance, **options)
    kwargs = spy.call_args.kwargs
    assert spy.call_args.args == ("orders.other" if override else "orders.*",)
    assert kwargs["queue"] == ("" if override else "finance")
    assert kwargs["durable"] == ("other" if override else "finance")
    assert kwargs["ack_policy"] == (
        AckPolicy.MANUAL if override else Parent.ack_policy
    )
    assert kwargs["config"].max_deliver == (5 if override else 3)
    assert kwargs["config"] is not Parent.config
    assert kwargs["stream"] is not Created.stream
    assert kwargs["no_reply"] is True
    if override:
        assert kwargs["pull_sub"] is False
        assert kwargs["config"] is not options["config"]
    else:
        assert kwargs["pull_sub"] is not Parent.pull_sub
        assert kwargs["pull_sub"].batch_size == 4
    assert Parent.config == before
    assert Created.stream.subjects == subjects
    assert Parent.pull_sub.batch_size == 4
    await create_app(registrar=reg).stop()


async def test_subclass_reset_and_native_defaults(monkeypatch):
    Created, Parent = declarations()
    Parent.subject = "orders.*"
    Parent.durable = "finance"
    Parent.pull_sub = True
    Parent.config = ConsumerConfig(max_deliver=2)
    Parent.ack_policy = AckPolicy.MANUAL

    class Finance(Parent):
        subject = None
        durable = None
        pull_sub = False
        config = None
        ack_policy = None

    reg = registry()
    spy = Mock(wraps=reg.broker.subscriber)
    monkeypatch.setattr(reg.broker, "subscriber", spy)
    reg.subscriber(Finance)
    assert spy.call_args.args == (Created.subject,)
    options = spy.call_args.kwargs
    assert options["durable"] is options["config"] is None
    assert options["pull_sub"] is False
    assert "ack_policy" not in options
    assert options["queue"] == ""
    await create_app(registrar=reg).stop()


@pytest.mark.parametrize(
    "field,value",
    [
        ("subject", ""),
        ("subject", 123),
        ("queue", 123),
        ("durable", 123),
        ("pull_sub", "pull"),
        ("config", {}),
        ("ack_policy", "manual"),
        ("publisher", object),
    ],
)
def test_invalid_subscriber_before_registration(field, value):
    _, Finance = declarations()
    setattr(Finance, field, value)
    reg = registry()
    with pytest.raises((TypeError, ValueError)):
        reg.subscriber(Finance)
    assert not reg.broker.native.subscribers


@pytest.mark.parametrize(
    "field,value",
    [
        ("subject", ""),
        ("stream", "orders"),
        ("stream", JStream("")),
    ],
)
def test_invalid_publisher_before_registration(field, value):
    Created, _ = declarations()
    setattr(Created, field, value)
    reg = registry()
    with pytest.raises((TypeError, ValueError)):
        reg.publisher(Created)
    assert not reg.broker.native.publishers


async def test_native_puback_and_error_identity(monkeypatch):
    Created, _ = declarations()
    reg = registry()
    ack = PubAck(stream="orders", seq=1)
    send = AsyncMock(return_value=ack)
    monkeypatch.setattr(
        reg.broker, "publisher", lambda *a, **kw: SimpleNamespace(publish=send)
    )
    sent, failed = [], []

    async def after(event):
        sent.append(event)

    async def error(event):
        failed.append(event)

    hooks = PublishHooks(
        after_send=(handler(after),), on_error=(handler(error),)
    )
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: hooks, provides=PublishHooks)
    reg.publisher(Created)
    app = create_app(
        registrar=reg, providers=[provider], publish_hooks=PublishHooks
    )
    try:
        assert await Created.publish(1) is ack
        assert sent[0].result is ack
        assert sent[0].call.meta == {
            "subject": Created.subject,
            "stream": "orders",
        }
        failure = ConnectionError("offline")
        send.side_effect = failure
        with pytest.raises(ConnectionError) as caught:
            await Created.publish(2)
        assert caught.value is failed[0].error is failure
    finally:
        await app.stop()


@pytest.mark.parametrize("mode", ["push", "pull", "batch"])
async def test_live_retention_typed_consumption_ack_and_publish_errors(
    nats_url, mode
):
    Created, Base = declarations("jet_" + uuid4().hex)
    received, sent, errors = [], [], []
    done = asyncio.Event()

    class Reader(Base):
        def __init__(self, message: StreamMessage):
            self.message = message

        async def run(self, event: int) -> None:
            assert isinstance(event, int)
            received.append(event)
            await self.message.ack_sync()
            if len(received) == 4:
                done.set()

    class Batch(Base):
        async def run(self, event: list[int]) -> None:
            assert all(isinstance(item, int) for item in event)
            received.extend(event)
            if len(received) == 4:
                done.set()

    async def after(event):
        sent.append(event)

    async def error(event):
        errors.append(event)

    hooks = PublishHooks(
        after_send=(handler(after),), on_error=(handler(error),)
    )
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: hooks, provides=PublishHooks)
    provider.provide(Reader)
    provider.provide(Batch)
    producer, consumer = registry(nats_url), registry(nats_url)
    producer.publisher(Created)
    options = (
        {"queue": "reader"}
        if mode == "push"
        else {
            "durable": "reader",
            "pull_sub": PullSub(
                batch=mode == "batch", batch_size=2, timeout=0.2
            ),
        }
    )
    if mode != "batch":
        options["ack_policy"] = AckPolicy.MANUAL
    consumer.subscriber(Batch if mode == "batch" else Reader, **options)
    sender = create_app(
        registrar=producer, providers=[provider], publish_hooks=PublishHooks
    )
    worker = create_app(registrar=consumer, providers=[provider])
    client = await producer.broker.connect()
    js = client.jetstream()
    name = Created.stream.name
    try:
        await js.add_stream(name=name, subjects=[Created.subject])
        result = {"id": 0, "extra": True}

        @publish(Created, select=lambda row: row["id"])
        async def create():
            return result

        assert await create() is result
        acks = await asyncio.gather(*(Created.publish(i) for i in range(1, 4)))
        assert all(
            isinstance(ack, PubAck) and ack.stream == name for ack in acks
        )
        assert sorted(ack.seq for ack in acks) == [2, 3, 4]
        assert not received
        await worker.start()
        await asyncio.wait_for(done.wait(), 10)
        # Bounded observation: automatic batch ACK follows handler completion.
        async with asyncio.timeout(10):
            while (
                info := await js.consumer_info(name, "reader")
            ).num_ack_pending:
                await asyncio.sleep(0.02)
        assert info.num_pending == 0
        assert sorted(received) == list(range(4))
        assert len(sent) == 4 and all(
            isinstance(e.result, PubAck) for e in sent
        )
        await worker.stop()
        await js.delete_stream(name)
        with pytest.raises(NoStreamResponseError) as caught:
            await Created.publish(5, timeout=0.2)
        assert errors[0].error is caught.value
        assert len(sent) == 4
    finally:
        await worker.stop()
        await sender.stop()
    assert client.is_closed


async def test_live_native_redelivery_and_error_hooks(nats_url):
    Created, Base = declarations("retry_" + uuid4().hex)
    attempts, errors = [], []
    succeeded = asyncio.Event()
    failure = ValueError("first attempt")

    class Reader(Base):
        durable = "reader"
        pull_sub = PullSub(timeout=0.2)
        config = ConsumerConfig(ack_wait=0.2, max_deliver=2)
        ack_policy = AckPolicy.NACK_ON_ERROR

        async def run(self, event: int) -> None:
            attempts.append(event)
            if len(attempts) == 1:
                raise failure
            succeeded.set()

    async def error(event):
        errors.append(event)

    hooks = SubscribeHooks(on_error=(handler(error),))
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    provider.provide(lambda: hooks, provides=SubscribeHooks)
    reg = registry(nats_url)
    reg.publisher(Created)
    reg.subscriber(Reader)
    app = create_app(
        registrar=reg, providers=[provider], subscribe_hooks=SubscribeHooks
    )
    try:
        await app.start()
        js = reg.broker.native.connection.jetstream()
        await Created.publish(42)
        await asyncio.wait_for(succeeded.wait(), 10)
        # Observe server acknowledgement after the successful handler returns.
        async with asyncio.timeout(10):
            while (
                info := await js.consumer_info(Created.stream.name, "reader")
            ).num_ack_pending:
                await asyncio.sleep(0.02)
        assert attempts == [42, 42]
        assert errors[0].error is failure and errors[0].stage == "run"
        assert (
            info.config.max_deliver == 2 and info.delivered.consumer_seq == 2
        )
        await js.delete_stream(Created.stream.name)
    finally:
        await app.stop()


@pytest.fixture
def package(nats_package, tmp_path):
    # Reuse the Core fixture's modular services; select JetStream declarations
    # before any module is imported, as a user's independent app would do.
    folder = tmp_path / nats_package
    replacements = {
        "publishers.py": [
            ("publishers.nats", "publishers.jetstream"),
            ("NatsPublisher", "JetPublisher"),
            (
                "from pydantic",
                "from faststream.nats import JStream\nfrom pydantic",
            ),
            (
                'subject = "',
                f'stream = JStream("{nats_package}")\n    subject = "',
            ),
        ],
        "subscribers.py": [
            ("subscribers.nats", "subscribers.jetstream"),
            ("NatsSubscriber", "JetSubscriber"),
            (
                "from faststream import",
                "from faststream.nats import PullSub\nfrom faststream import",
            ),
            (
                'queue = "finance"',
                'durable = "finance"\n    pull_sub = PullSub(timeout=0.2)',
            ),
            ('queue = ""', 'durable = "notify"'),
        ],
        "entry.py": [
            ("registry.nats", "registry.jetstream"),
            ("NatsRegistrar", "JetRegistrar"),
        ],
        "providers.py": [("result=event.result,", "result=event.result.seq,")],
    }
    for file, edits in replacements.items():
        path = folder / file
        code = path.read_text()
        for old, new in edits:
            code = code.replace(old, new)
        path.write_text(code)
    return nats_package


async def test_live_cli_shared_durable_independent_consumer_and_cleanup(
    package, nats_url, tmp_path
):
    path = tmp_path / "worker.jsonl"
    log = tmp_path / "worker.log"
    env = {
        **os.environ,
        "EVENT_NATS_LOG": str(path),
        "PYTHONPATH": os.pathsep.join(
            (
                str(tmp_path),
                str(Path(__file__).resolve().parents[1]),
                os.getenv("PYTHONPATH", ""),
            )
        ),
    }
    command = [
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
    entry = importlib.import_module(package + ".entry")
    pub = importlib.import_module(package + ".publishers")
    sub = importlib.import_module(package + ".subscribers")
    services = importlib.import_module(package + ".providers")
    state = importlib.import_module(package + ".state")
    producer = registry(nats_url)
    sender = create_app(registrar=producer, publishers=[package])
    local = registry(nats_url)
    local.subscriber(sub.Finance)
    worker = create_app(
        registrar=local,
        providers=[services.Services()],
        subscribe_hooks=services.Receiving,
    )
    # The CLI app discovers both publishers and subscribers.
    assert callable(entry.build)
    client = await producer.broker.connect()
    js = client.jetstream()
    process = None

    def rows():
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    def wait_for(check):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if check():
                return
            if process.poll() is not None:
                break
            time.sleep(0.02)
        pytest.fail(log.read_text())

    try:
        await js.add_stream(name=package, subjects=[package + ".>"])
        await worker.start()
        # Establish consumption before the competing worker starts.
        await pub.Created.publish(pub.Order(id=0))
        async with asyncio.timeout(10):
            while not any(r["phase"] == "closed" for r in state.records):
                await asyncio.sleep(0.02)
        with log.open("w") as output:
            process = subprocess.Popen(
                command,
                env=env,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            await asyncio.to_thread(
                wait_for, lambda: any(r["phase"] == "ready" for r in rows())
            )
            await asyncio.gather(
                *(pub.Created.publish(pub.Order(id=i)) for i in range(1, 21))
            )
            # Both durable consumers must catch up; ACK follows scope cleanup.
            async with asyncio.timeout(15):
                while (
                    sum(r["phase"] == "closed" for r in state.records)
                    + sum(r["phase"] == "closed" for r in rows())
                ) < 42:
                    await asyncio.sleep(0.02)
            process.send_signal(signal.SIGTERM)
            assert await asyncio.to_thread(process.wait, timeout=15) == 0
        local_runs = check_records(
            state.records, sum(r["phase"] == "run" for r in state.records)
        )
        remote_rows = rows()
        remote_runs = check_records(
            remote_rows, sum(r["phase"] == "run" for r in remote_rows)
        )
        finance = [r for r in remote_runs if r["role"] == "Finance"]
        assert finance, "Remote worker should share the finance durable"
        assert sorted(r["value"] for r in local_runs + finance) == list(
            range(21)
        )
        assert sorted(
            r["value"] for r in remote_runs if r["role"] == "Notify"
        ) == list(range(21))
        assert sum(r["phase"] == "app_closed" for r in remote_rows) == 1
        await worker.stop()
        ack = await pub.Created.publish(pub.Order(id=99))
        assert isinstance(ack, PubAck)
        assert (await js.consumer_info(package, "finance")).num_pending == 1
        assert (await js.consumer_info(package, "notify")).num_pending == 1
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            await asyncio.to_thread(process.wait, timeout=5)
        await worker.stop()
        await js.delete_stream(package)
        await sender.stop()


def test_optional_import_isolation():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'redis', 'aio_pika', 'aiokafka',
                   'confluent_kafka', 'aiomqtt'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
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

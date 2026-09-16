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
from faststream.redis import PubSub, TestRedisBroker
from faststream.redis import RedisBroker as NativeRedisBroker
from hook_support import handler

from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.channels import ChannelPublisher
from papilio_tasks.apps.events.registry.channels import ChannelRegistrar
from papilio_tasks.apps.events.subscribers.channels import ChannelSubscriber
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker
from papilio_tasks.tools.hooks.publish import PublishHooks


def registry(url="redis://localhost:6379"):
    return ChannelRegistrar(RedisBroker(NativeRedisBroker(url, logger=None)))


def declarations(name="orders"):
    class Created(ChannelPublisher[dict]):
        channel = name

    class Finance(ChannelSubscriber[dict]):
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
    Parent.channel = PubSub(configured) if configured else None

    class Finance(Parent):
        pass

    if reset:
        Finance.channel = None
    selected = PubSub(override) if override else None
    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    before = dict(vars(Finance))
    reg.subscriber(Finance, channel=selected)
    native = reg.broker.native.subscribers[0]
    assert native.channel.name == expected
    assert native.channel.pattern == ("*" in expected)
    assert dict(vars(Finance)) == before
    configured_route = selected or Finance.channel
    if configured_route is not None:
        configured_route.polling_interval = 10
        assert native.channel.polling_interval != 10
    assert not reg.has_publisher(Created)
    connect.assert_not_awaited()
    assert not hasattr(Finance, "group_id")
    assert not hasattr(Finance, "ack_policy")
    app = create_app(registrar=reg)
    await app.stop()


@pytest.mark.parametrize(
    "invalid",
    ["publisher", "name", "subscriber", "channel", "empty", "handler"],
)
def test_invalid_configuration_precedes_native_registration(invalid):
    reg = registry()
    Created, Finance = declarations()
    if invalid == "publisher":
        operation = partial(reg.publisher, Publisher)
    elif invalid == "name":
        Created.channel = ""
        operation = partial(reg.publisher, Created)
    elif invalid == "subscriber":
        operation = partial(reg.subscriber, Subscriber)
    elif invalid == "channel":
        operation = partial(reg.subscriber, Finance, channel="orders")
    elif invalid == "empty":
        selected = PubSub("orders")
        selected.name = ""
        operation = partial(reg.subscriber, Finance, channel=selected)
    else:

        async def untyped(self, event):
            pass

        Finance.run = untyped
        operation = partial(reg.subscriber, Finance)
    with pytest.raises((ValueError, TypeError)):
        operation()
    assert not reg.broker.native.publishers
    assert not reg.broker.native.subscribers


@pytest.mark.parametrize("count", [0, 3])
async def test_publish_count_error_hooks_and_binding_ownership(
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
        assert observed[0].call.meta == {"channel": "orders"}
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
from papilio_tasks.apps.events.registry.channels import ChannelRegistrar
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
    name = "channels_app_" + uuid4().hex
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
            from papilio_tasks.apps.events.registry.channels import (
                ChannelRegistrar,
            )
            from papilio_tasks.apps.events.application import create_app
            from .providers import Services, Sending, Receiving, Resource
            from .state import record
            from .subscribers import Finance, Notify


            def build():
                reg = ChannelRegistrar(
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
                    publishers=["CHANNEL_PACKAGE"],
                    subscribers=["CHANNEL_PACKAGE"],
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
                    record(
                        "sent",
                        channel=event.call.meta["channel"],
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
            from papilio_tasks.apps.events.publishers.channels import (
                ChannelPublisher,
            )


            class Order(BaseModel):
                id: int


            class Created(ChannelPublisher[Order]):
                channel = "CHANNEL_PACKAGE.orders"
        """,
        "state.py": """
            import json, os
            from pathlib import Path

            records = []


            def record(phase, **values):
                row = dict(phase=phase, pid=os.getpid(), **values)
                records.append(row)
                if path := os.getenv("EVENT_CHANNELS_LOG"):
                    with Path(path).open("a") as file:
                        file.write(json.dumps(row) + "\\n")
        """,
        "subscribers.py": """
            from faststream.redis import PubSub
            from faststream import StreamMessage
            from papilio_tasks.apps.events.subscribers.channels import (
                ChannelSubscriber,
            )
            from .publishers import Created, Order
            from .state import record
            from uuid import uuid4


            class Session:
                def __init__(self, message: StreamMessage):
                    self.message = message
                    self.id = uuid4().hex


            class Finance(ChannelSubscriber[Order]):
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


            class Notify(Finance):
                channel = PubSub(
                    "CHANNEL_PACKAGE.*",
                    pattern=True,
                    polling_interval=0.05,
                )
        """,
    }
    for file, code in files.items():
        (folder / file).write_text(
            textwrap.dedent(code).replace("CHANNEL_PACKAGE", name)
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
    assert len(app.registrar.broker.native.subscribers) == 2
    assert state.records == []
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
            r["channel"] for r in state.records if r["phase"] == "sent"
        ] == [pub.Created.channel]
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


async def test_live_fanout_patterns_counts_disconnect_and_no_replay(
    package, redis_url
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
    client = await producer.broker.connect()

    def consumer(cls):
        reg = registry(redis_url)
        reg.subscriber(cls)
        return create_app(
            registrar=reg,
            providers=[services.Services()],
            subscribe_hooks=services.Receiving,
        )

    exact, pattern = consumer(sub.Finance), consumer(sub.Notify)
    later = None
    try:
        assert await pub.Created.publish(pub.Order(id=0)) == 0
        await asyncio.gather(exact.start(), pattern.start())
        assert (
            await pub.Created.publish(
                pub.Order(id=1), headers={"origin": "test"}
            )
            == 2
        )
        assert (
            await pub.Created.publish(
                pub.Order(id=2), channel=package + ".prices"
            )
            == 1
        )
        # Bounded observation of handler-scope finalization on actual Redis.
        async with asyncio.timeout(10):
            while sum(r["phase"] == "closed" for r in state.records) < 3:
                await asyncio.sleep(0.02)
        await asyncio.gather(exact.stop(), pattern.stop())
        assert await pub.Created.publish(pub.Order(id=3)) == 0
        later = consumer(sub.Finance)
        await later.start()
        assert await pub.Created.publish(pub.Order(id=4)) == 1
        async with asyncio.timeout(10):
            while sum(r["phase"] == "closed" for r in state.records) < 4:
                await asyncio.sleep(0.02)
        await later.stop()
        runs = check_records(state.records, 4)
        assert {
            role: sorted(r["value"] for r in runs if r["role"] == role)
            for role in ("Finance", "Notify")
        } == {"Finance": [1, 4], "Notify": [1, 2]}
        assert all(
            r["headers"]["origin"] == "test" for r in runs if r["value"] == 1
        )
        assert [
            r["result"] for r in state.records if r["phase"] == "sent"
        ] == [0, 2, 1, 0, 1]
        assert await client.exists(pub.Created.channel) == 0
    finally:
        await asyncio.gather(exact.stop(), pattern.stop())
        if later is not None:
            await later.stop()
        await producer_app.stop()


async def test_live_cli_two_processes_receive_broadcast(
    package, redis_url, tmp_path
):
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

    def wait_for(index, check):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if check(records(index)):
                return
            if workers[index].poll() is not None:
                break
            time.sleep(0.05)
        outputs[index].flush()
        outputs[index].seek(0)
        pytest.fail(outputs[index].read())

    workers, outputs = [], []
    producer = registry(redis_url)
    app = create_app(registrar=producer, publishers=[package])
    pub = importlib.import_module(package + ".publishers")
    try:
        for index in range(2):
            output = (tmp_path / f"worker-{index}.log").open("w+")
            outputs.append(output)
            workers.append(
                subprocess.Popen(
                    cmd,
                    env={**base_env, "EVENT_CHANNELS_LOG": str(paths[index])},
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )
        await asyncio.gather(
            *(
                asyncio.to_thread(
                    wait_for,
                    i,
                    lambda rows: any(r["phase"] == "ready" for r in rows),
                )
                for i in range(2)
            )
        )
        await app.connect()
        assert await pub.Created.publish(pub.Order(id=42)) == 4
        await asyncio.gather(
            *(
                asyncio.to_thread(
                    wait_for,
                    i,
                    lambda rows: (
                        sum(r["phase"] == "closed" for r in rows) == 2
                    ),
                )
                for i in range(2)
            )
        )
        for worker in workers:
            worker.send_signal(signal.SIGTERM)
        assert await asyncio.gather(
            *(asyncio.to_thread(w.wait, timeout=15) for w in workers)
        ) == [0, 0]
        for index in range(2):
            runs = check_records(records(index), 2)
            assert {r["role"] for r in runs} == {"Finance", "Notify"}
            assert all(r["value"] == 42 for r in runs)
            assert sum(r["phase"] == "app_closed" for r in records(index)) == 1
        assert await pub.Created.publish(pub.Order(id=43)) == 0
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

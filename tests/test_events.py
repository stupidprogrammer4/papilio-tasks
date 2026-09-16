import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("dishka_faststream")
pytest.importorskip("faststream.rabbit")

from dishka import Provider, Scope, provide
from faststream import StreamMessage
from faststream.rabbit import RabbitBroker as NativeRabbitBroker
from faststream.rabbit import TestRabbitBroker
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.rabbit import (
    ExchangeType,
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker


class Order(BaseModel):
    id: int


@pytest.fixture(autouse=True)
def isolated_event_bindings(monkeypatch):
    monkeypatch.setattr(bindings, "_senders", {})


def registry(url=None):
    native = (
        NativeRabbitBroker(url, logger=None)
        if url
        else NativeRabbitBroker(logger=None)
    )
    return RabbitRegistrar(RabbitBroker(native))


def event(name="orders"):
    class Created(RabbitPublisher[Order]):
        exchange = RabbitExchange(name, type=ExchangeType.TOPIC)
        routing_key = "order.created"

    return Created


async def test_message_di_payload_fanout_and_cleanup():
    reg = registry()
    Created = event()
    calls, closed = [], []

    class Service:
        def __init__(self, message: StreamMessage):
            self.message = message

    class Finance(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue("finance")

        def __init__(self, service: Service):
            self.service = service

        async def run(self, event: Order) -> None:
            calls.append(("finance", event, self.service.message.headers))

    class Notify(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue("notify")

        def __init__(self, service: Service):
            self.service = service

        async def run(self, event: Order) -> None:
            calls.append(("notify", event, self.service.message.headers))

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def service(
            self, message: StreamMessage
        ) -> AsyncIterator[Service]:
            value = Service(message)
            try:
                yield value
            finally:
                closed.append(value)

        finance = provide(Finance, scope=Scope.REQUEST)
        notify = provide(Notify, scope=Scope.REQUEST)

    before = dict(vars(Created))
    reg.publisher(Created)
    reg.subscriber(Finance)
    reg.subscriber(Notify)
    app = create_app(registrar=reg, providers=[Dependencies()])
    try:
        async with TestRabbitBroker(reg.broker.native):
            await Created.publish(Order(id=7), headers={"origin": "test"})
        assert sorted(calls, key=lambda v: v[0]) == [
            ("finance", Order(id=7), {"origin": "test"}),
            ("notify", Order(id=7), {"origin": "test"}),
        ]
        assert len(closed) == 2 and closed[0] is not closed[1]
        assert Finance.queue.routing_key == ""
        assert dict(vars(Created)) == before
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await Created.publish(Order(id=8))


async def test_producer_only_native_result_error_and_owner_release(
    monkeypatch,
):
    reg, other = registry(), registry()
    Created, Other = event(), event("other")
    reg.publisher(Created)
    other.publisher(Other)
    app = create_app(registrar=reg)
    other_app = create_app(registrar=other)
    connect = AsyncMock()
    monkeypatch.setattr(reg.broker.native, "connect", connect)
    try:
        await app.connect()
        connect.assert_awaited_once()
        result = object()
        sender = AsyncMock(return_value=result)
        monkeypatch.setitem(bindings._senders, Created, (reg, sender))
        assert await Created.publish(Order(id=1), correlation_id="x") is result
        sender.assert_awaited_once_with(Order(id=1), correlation_id="x")
        sender.side_effect = ConnectionError("offline")
        with pytest.raises(ConnectionError, match="offline"):
            await Created.publish(Order(id=2))
        await app.stop()
        assert bindings.get(Other)
        with pytest.raises(RuntimeError, match="closed"):
            await app.start()
        await app.stop()
    finally:
        await asyncio.gather(app.stop(), other_app.stop())


async def test_stop_error_still_closes_container_and_releases(monkeypatch):
    reg = registry()
    Created = event()
    reg.publisher(Created)
    app = create_app(registrar=reg)
    closed = []

    class Resource:
        pass

    class Resources(Provider):
        @provide(scope=Scope.APP)
        async def resource(self) -> AsyncIterator[Resource]:
            try:
                yield Resource()
            finally:
                closed.append(True)

    # Use an actual APP resource to verify cleanup on a native stop failure.
    await app.stop()
    reg = registry()
    reg.publisher(Created)
    app = create_app(registrar=reg, providers=[Resources()])
    await app.container.get(Resource)
    monkeypatch.setattr(
        reg.broker.native, "stop", AsyncMock(side_effect=OSError("stop"))
    )
    with pytest.raises(OSError, match="stop"):
        await app.stop()
    assert closed == [True]
    with pytest.raises(RuntimeError, match="not registered"):
        await Created.publish(Order(id=1))


async def test_registration_conflicts_and_timing():
    reg, other = registry(), registry()
    Created = event()
    reg.publisher(Created)
    with pytest.raises(ValueError, match="already registered"):
        other.publisher(Created)
    Conflicting = event()
    Conflicting.exchange = RabbitExchange("orders", type=ExchangeType.FANOUT)
    with pytest.raises(ValueError, match="Conflicting exchange"):
        reg.publisher(Conflicting)

    class Finance(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue("finance")

        async def run(self, event: Order) -> None:
            pass

    reg.subscriber(Finance)
    with pytest.raises(ValueError, match="already registered"):
        reg.subscriber(Finance)

    class BadQueue(Finance):
        queue = RabbitQueue("finance", arguments={"x-max-length": 10})

    with pytest.raises(ValueError, match="Conflicting queue"):
        reg.subscriber(BadQueue)

    class BadRoute(Finance):
        queue = RabbitQueue("other", routing_key="different")

    with pytest.raises(ValueError, match="routing_key"):
        reg.subscriber(BadRoute)
    app = create_app(registrar=reg)
    try:
        with pytest.raises(RuntimeError, match="before creating"):
            reg.publisher(event("late"))
        with pytest.raises(RuntimeError, match="before creating"):
            create_app(registrar=reg)
    finally:
        await app.stop()
        other.close()


async def test_handler_error_releases_message_scope():
    reg = registry()
    Created = event()
    closed = []

    class Failure(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue("failed")

        async def run(self, event: Order) -> None:
            raise ValueError("business error")

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def handler(self) -> AsyncIterator[Failure]:
            try:
                yield Failure()
            finally:
                closed.append(True)

    reg.publisher(Created)
    reg.subscriber(Failure)
    app = create_app(registrar=reg, providers=[Dependencies()])
    try:
        async with TestRabbitBroker(reg.broker.native):
            with pytest.raises(ValueError, match="business error"):
                await Created.publish(Order(id=1))
        assert closed == [True]
    finally:
        await app.stop()


def test_pure_events_imports():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'faststream', 'dishka',
                   'dishka_faststream', 'aio_pika'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events import Publisher, Subscriber
from papilio_tasks.apps.events.publishers import bindings
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=15)


async def test_live_separate_producer_worker_and_independent_queues():
    url = os.getenv("TEST_RABBIT_URL")
    if not url:
        pytest.skip("Requires isolated RabbitMQ")
    name = "papilio-app-events-" + uuid4().hex
    Created = event(name)
    producer, worker = registry(url), registry(url)
    received = {"finance": [], "notify": []}
    closed = []
    done = asyncio.Event()

    class Finance(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue(name + "-finance")

        async def run(self, event: Order) -> None:
            received["finance"].append(event.id)

    class Notify(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue(name + "-notify")

        async def run(self, event: Order) -> None:
            received["notify"].append(event.id)

    def cleanup(role):
        closed.append(role)
        if len(closed) == 6:
            done.set()

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def finance(self) -> AsyncIterator[Finance]:
            try:
                yield Finance()
            finally:
                cleanup("finance")

        @provide(scope=Scope.REQUEST)
        async def notify(self) -> AsyncIterator[Notify]:
            try:
                yield Notify()
            finally:
                cleanup("notify")

    producer.publisher(Created)
    worker.subscriber(Finance)
    worker.subscriber(Notify)
    producer_app = create_app(registrar=producer)
    worker_app = create_app(registrar=worker, providers=[Dependencies()])
    try:
        await worker_app.start()
        await producer_app.connect()
        await asyncio.gather(*(Created.publish(Order(id=i)) for i in range(3)))
        await asyncio.wait_for(done.wait(), timeout=15)
        assert {k: sorted(v) for k, v in received.items()} == {
            "finance": [0, 1, 2],
            "notify": [0, 1, 2],
        }
    finally:
        await worker_app.stop()
        try:
            connection = await producer.broker.connect()
            channel = await connection.channel()
            await asyncio.gather(
                channel.queue_delete(Finance.queue.name),
                channel.queue_delete(Notify.queue.name),
            )
            await channel.exchange_delete(name)
        finally:
            await producer_app.stop()


async def test_failed_assembly_releases_publisher_and_broker_has_one_owner():
    reg = registry()
    Created = event()
    reg.publisher(Created)

    class Missing:
        pass

    class Service:
        def __init__(self, missing: Missing):
            pass

    class Invalid(Provider):
        service = provide(Service, scope=Scope.REQUEST)

    from dishka.exceptions import GraphMissingFactoryError

    with pytest.raises(GraphMissingFactoryError):
        create_app(registrar=reg, providers=[Invalid()])
    with pytest.raises(RuntimeError, match="not registered"):
        await Created.publish(Order(id=1))
    replacement = registry()
    replacement.publisher(Created)
    app = create_app(registrar=replacement)
    try:
        duplicate = RabbitRegistrar(replacement.broker)
        with pytest.raises(ValueError, match="already has"):
            duplicate.publisher(event("late"))
        with pytest.raises(ValueError, match="already has"):
            create_app(registrar=duplicate)
    finally:
        await app.stop()
    # A stopped native broker retains its DI middleware and callbacks; create
    # a fresh broker/application instead of attaching another container to it.
    with pytest.raises(ValueError, match="already has"):
        create_app(registrar=RabbitRegistrar(replacement.broker))


async def test_dataclass_payload():
    from dataclasses import dataclass

    @dataclass
    class Payload:
        id: int

    class Changed(RabbitPublisher[Payload]):
        exchange = RabbitExchange("change")
        routing_key = "changed"

    seen = []

    class Receiver(RabbitSubscriber[Payload]):
        publisher = Changed
        queue = RabbitQueue("receive")

        async def run(self, event: Payload) -> None:
            seen.append(event)

    class Dependencies(Provider):
        receiver = provide(Receiver, scope=Scope.REQUEST)

    reg = registry()
    reg.publisher(Changed)
    reg.subscriber(Receiver)
    app = create_app(registrar=reg, providers=[Dependencies()])
    try:
        async with TestRabbitBroker(reg.broker.native):
            await Changed.publish(Payload(9))
        assert seen == [Payload(9)]
    finally:
        await app.stop()


def test_reject_invalid_handlers_before_registering():
    reg = registry()
    Created = event()

    class Invalid(RabbitSubscriber[Order]):
        publisher = Created
        queue = RabbitQueue("invalid")

        async def run(self, event):
            pass

    with pytest.raises(TypeError, match="annotate"):
        reg.subscriber(Invalid)
    assert not reg.broker.native.subscribers

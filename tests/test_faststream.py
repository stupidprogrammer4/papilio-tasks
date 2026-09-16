import asyncio
import os
import subprocess
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.rabbit")

from aio_pika.exceptions import ChannelPreconditionFailed, DeliveryError
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitMessage, TestRabbitBroker
from faststream.rabbit import RabbitBroker as NativeRabbitBroker

from papilio_tasks.infra.faststream.brokers.backends.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)


async def test_configuration_has_no_io_and_lifecycle_reuses_native(
    monkeypatch,
):
    native = NativeRabbitBroker(logger=None)
    connect = AsyncMock(return_value=object())
    start, stop = AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "connect", connect)
    monkeypatch.setattr(native, "start", start)
    monkeypatch.setattr(native, "stop", stop)
    broker = RabbitBroker(native)
    queue = RabbitQueue("finance", routing_key="order.created")
    exchange = RabbitExchange("events", type=ExchangeType.TOPIC)
    broker.subscriber(queue, exchange, ack_policy=AckPolicy.ACK)
    broker.publisher(exchange=exchange, routing_key="order.created")
    connect.assert_not_awaited()
    start.assert_not_awaited()
    stop.assert_not_awaited()
    assert broker.native is native
    assert await broker.connect() is connect.return_value
    start.assert_not_awaited()
    await broker.start()
    await broker.stop()
    connect.assert_awaited_once_with()
    start.assert_awaited_once_with()
    stop.assert_awaited_once_with()


async def test_native_publisher_and_subscriber_preserve_payload_and_headers():
    broker = RabbitBroker(NativeRabbitBroker(logger=None))
    exchange = RabbitExchange("events", type=ExchangeType.TOPIC)
    calls = []

    @broker.subscriber(RabbitQueue("finance", routing_key="order.*"), exchange)
    async def finance(body: dict, message: RabbitMessage):
        calls.append((body, message.headers, message.correlation_id))

    publisher = broker.publisher(
        exchange=exchange, routing_key="order.created", headers={"v": "1"}
    )
    async with TestRabbitBroker(broker.native):
        await publisher.publish({"id": 7}, correlation_id="first")
        await broker.publish(
            {"id": 8},
            exchange=exchange,
            routing_key="order.updated",
            headers={"v": "2"},
            correlation_id="second",
        )
    assert calls == [
        ({"id": 7}, {"v": "1"}, "first"),
        ({"id": 8}, {"v": "2"}, "second"),
    ]


async def test_publish_returns_native_confirmation_and_propagates_error(
    monkeypatch,
):
    native = NativeRabbitBroker(logger=None)
    broker = RabbitBroker(native)
    confirmation = object()
    send = AsyncMock(return_value=confirmation)
    monkeypatch.setattr(native, "publish", send)
    assert (
        await broker.publish("body", routing_key="order.created")
        is confirmation
    )
    error = ConnectionError("disconnected")
    send.side_effect = error
    with pytest.raises(ConnectionError) as caught:
        await broker.publish("body")
    assert caught.value is error
    assert (
        send.await_count == 2
    )  # One attempt per explicit call, no hidden retry.


def test_optional_import_boundaries():
    code = """
import importlib.abc, sys
blocked = {'taskiq', 'taskiq_aio_pika', 'dishka', 'redis', 'aiokafka',
           'confluent_kafka', 'nats', 'aiomqtt', 'papilio_tasks.apps'}
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p+'.') for p in blocked):
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker
from faststream.rabbit import RabbitBroker as Native
assert RabbitBroker(Native(logger=None)).native is not None
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=15)
    bare = code.replace(
        "'taskiq', 'taskiq_aio_pika'",
        "'faststream', 'aio_pika', 'taskiq', 'taskiq_aio_pika'",
    ).split("from papilio_tasks.infra.faststream.brokers.backends.rabbit")[0]
    bare += "from papilio_tasks.infra.faststream.brokers.base import Broker\n"
    subprocess.run([sys.executable, "-c", bare], check=True, timeout=15)


async def test_live_routing_declaration_and_competing_consumers():
    url = os.getenv("TEST_RABBIT_URL")
    if not url:
        pytest.skip("Requires isolated RabbitMQ")
    name = "papilio-events-" + uuid4().hex
    exchange = RabbitExchange(name, type=ExchangeType.TOPIC)
    queues = [
        RabbitQueue(name + "-" + role, routing_key=key)
        for role, key in (
            ("finance", "order.created"),
            ("notification", "order.*"),
            ("commerce", "order.created"),
            ("unmatched", "price.changed"),
        )
    ]
    producer = RabbitBroker(
        NativeRabbitBroker(
            url, logger=None, default_channel=Channel(on_return_raises=True)
        )
    )
    worker = RabbitBroker(NativeRabbitBroker(url, logger=None))
    competitor = RabbitBroker(NativeRabbitBroker(url, logger=None))
    count = 12
    received = {
        "finance": [],
        "notification": [],
        "commerce": [],
        "unmatched": [],
    }
    workers = set()
    finished = asyncio.Event()

    def handler(role, instance):
        async def consume(body: dict, message: RabbitMessage):
            received[role].append((body["id"], message.headers["origin"]))
            if role == "finance":
                workers.add(instance)
            # Exercise the native message ack, without waiting for a background
            # automatic acknowledgement after the test completion signal.
            await message.ack()
            if all(
                len(received[r]) == count
                for r in ("finance", "notification", "commerce")
            ):
                finished.set()

        return consume

    for role, queue in zip(received, queues):
        worker.subscriber(queue, exchange)(handler(role, "first"))
    competitor.subscriber(queues[0], exchange)(handler("finance", "second"))
    publisher = producer.publisher(
        exchange=exchange, routing_key="order.created"
    )
    try:
        connection = await producer.connect()
        channel = await connection.channel()
        declared_exchange = await producer.declare_exchange(exchange)
        declared_queue = await producer.declare_queue(queues[0])
        assert declared_exchange.name == exchange.name
        assert declared_queue.name == queues[0].name
        assert (
            await channel.get_exchange(exchange.name, ensure=True)
        ).name == name
        assert (
            await channel.get_queue(queues[0].name, ensure=True)
        ).name == queues[0].name
        await worker.start()
        await competitor.start()
        # Neither publication path starts the producer as a subscriber worker.
        await publisher.publish({"id": 0}, headers={"origin": "publisher"})
        await asyncio.gather(
            *(
                producer.publish(
                    {"id": i},
                    exchange=exchange,
                    routing_key="order.created",
                    headers={"origin": "direct"},
                )
                for i in range(1, count)
            )
        )
        await asyncio.wait_for(finished.wait(), timeout=15)
        expected = [(0, "publisher")] + [
            (i, "direct") for i in range(1, count)
        ]
        assert sorted(received["finance"]) == expected
        assert sorted(received["notification"]) == expected
        assert sorted(received["commerce"]) == expected
        assert received["unmatched"] == []
        assert workers == {"first", "second"}
        with pytest.raises(DeliveryError):
            await producer.publish(
                "unroutable",
                exchange=exchange,
                routing_key="unknown",
                mandatory=True,
            )
        # Bypass the native declaration cache to exercise the server.
        await channel.declare_queue(name + "-conflict", durable=True)
        with pytest.raises(
            ChannelPreconditionFailed, match="inequivalent arg"
        ):
            await producer.declare_queue(
                RabbitQueue(
                    name + "-conflict",
                    durable=True,
                    arguments={"x-max-length": 10},
                )
            )
    finally:
        await asyncio.gather(worker.stop(), competitor.stop())
        try:
            # Use a fresh channel: an invalid declaration closes its channel.
            connection = await producer.connect()
            channel = await connection.channel()
            await asyncio.gather(
                *(channel.queue_delete(q.name) for q in queues)
            )
            await channel.queue_delete(name + "-conflict")
            await channel.exchange_delete(exchange.name)
        finally:
            await producer.stop()

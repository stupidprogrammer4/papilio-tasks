import asyncio
import os
import subprocess
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.kafka")

from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from faststream import AckPolicy
from faststream.kafka import KafkaBroker as NativeKafkaBroker
from faststream.kafka import KafkaMessage, RecordMetadata, TestKafkaBroker

from papilio_tasks.infra.faststream.brokers.backends.kafka import KafkaBroker


async def test_configuration_and_lifecycle_reuse_native(monkeypatch):
    native = NativeKafkaBroker(logger=None)
    connect = AsyncMock(return_value=object())
    start, stop = AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "connect", connect)
    monkeypatch.setattr(native, "start", start)
    monkeypatch.setattr(native, "stop", stop)
    broker = KafkaBroker(native)
    subscriber = broker.subscriber(
        "orders", "returns", group_id="finance", ack_policy=AckPolicy.MANUAL
    )
    publisher = broker.publisher(
        "orders", partition=1, headers={"origin": "api"}
    )
    assert subscriber in native.subscribers
    assert publisher in native.publishers
    assert subscriber.ack_policy is AckPolicy.MANUAL
    connect.assert_not_awaited()
    start.assert_not_awaited()
    assert broker.native is native
    assert await broker.connect() is connect.return_value
    await broker.start()
    await broker.stop()
    connect.assert_awaited_once_with()
    start.assert_awaited_once_with()
    stop.assert_awaited_once_with()


@pytest.mark.parametrize("batch", [False, True])
async def test_native_outcome_options_and_errors(monkeypatch, batch):
    native = NativeKafkaBroker(logger=None)
    broker = KafkaBroker(native)
    pending = asyncio.get_running_loop().create_future()
    send = AsyncMock(return_value=pending)
    method = "publish_batch" if batch else "publish"
    monkeypatch.setattr(native, method, send)
    messages = ({"id": 1}, {"id": 2}) if batch else ({"id": 1},)
    options = dict(partition=1, headers={"origin": "api"}, no_confirm=True)
    result = await getattr(broker, method)(
        *messages, topic="orders", **options
    )
    assert result is pending and not pending.done()
    if batch:
        send.assert_awaited_once_with(*messages, topic="orders", **options)
    else:
        send.assert_awaited_once_with(*messages, "orders", **options)
    pending.set_result("receipt")
    assert await result == "receipt"
    error = ConnectionError("offline")
    send.side_effect = error
    with pytest.raises(ConnectionError) as caught:
        await getattr(broker, method)(*messages, topic="orders")
    assert caught.value is error
    assert send.await_count == 2


async def test_native_single_and_batch_routes():
    broker = KafkaBroker(NativeKafkaBroker(logger=None))
    single, batches = [], []

    @broker.subscriber("orders")
    async def receive(body: dict, message: KafkaMessage):
        single.append((body, message.headers))

    @broker.subscriber("imports", batch=True)
    async def receive_batch(body: list[dict]):
        batches.extend(body)

    publisher = broker.publisher("orders", headers={"origin": "api"})
    batch_publisher = broker.publisher("imports", batch=True)
    async with TestKafkaBroker(broker.native):
        await publisher.publish({"id": 1})
        await broker.publish({"id": 2}, "orders", headers={"origin": "worker"})
        await broker.publish_batch({"id": 3}, {"id": 4}, topic="imports")
        await batch_publisher.publish({"id": 5}, {"id": 6})
    assert [item[0] for item in single] == [{"id": 1}, {"id": 2}]
    assert [item[1]["origin"] for item in single] == ["api", "worker"]
    assert batches == [{"id": i} for i in range(3, 7)]


def test_optional_import_boundaries():
    code = """
import importlib.abc, sys
blocked = {'taskiq', 'dishka', 'dishka_faststream', 'aio_pika', 'redis',
           'confluent_kafka', 'nats', 'papilio_tasks.apps'}
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p+'.') for p in blocked):
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.infra.faststream.brokers.backends.kafka import KafkaBroker
from faststream.kafka import KafkaBroker as Native
assert KafkaBroker(Native()).native is not None
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


async def test_live_single_batch_metadata_and_groups():
    url = os.getenv("TEST_KAFKA_URL")
    if not url:
        pytest.skip("Requires isolated Kafka")
    topic = "papilio-" + uuid4().hex
    admin = AIOKafkaAdminClient(bootstrap_servers=url)
    await admin.start()
    await admin.create_topics(
        [NewTopic(topic, num_partitions=2, replication_factor=1)]
    )
    producer = KafkaBroker(NativeKafkaBroker(url, logger=None))
    workers = [
        KafkaBroker(NativeKafkaBroker(url, logger=None)) for _ in range(3)
    ]
    received = [[], [], []]
    completed = asyncio.Event()

    def record(index, body, raw):
        received[index].append(
            (body["seq"], raw.partition, raw.key, dict(raw.headers))
        )
        if len(received[0]) + len(received[1]) == 7 and len(received[2]) == 7:
            completed.set()

    def consumer(index):
        async def receive(body: dict, message: KafkaMessage):
            record(index, body, message.raw_message)

        return receive

    options = dict(
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.ACK,
        session_timeout_ms=6000,
        heartbeat_interval_ms=1000,
    )
    subscriptions = [
        workers[i].subscriber(topic, group_id=topic + "-shared", **options)
        for i in range(2)
    ]
    subscriptions[0](consumer(0))
    subscriptions[1](consumer(1))

    @workers[2].subscriber(
        topic, group_id=topic + "-independent", batch=True, **options
    )
    async def receive_batch(body: list[dict], message: KafkaMessage):
        for item, raw in zip(body, message.raw_message, strict=True):
            record(2, item, raw)

    try:
        await asyncio.gather(*(worker.start() for worker in workers))
        # Await native group assignment before publication to exercise sharing
        # without counting deliveries during initial consumer-group rebalances.
        async with asyncio.timeout(30):
            while True:
                assignments = [s.consumer.assignment() for s in subscriptions]
                if all(assignments) and assignments[0].isdisjoint(
                    assignments[1]
                ):
                    break
                await asyncio.sleep(0.05)
        assert assignments[0] | assignments[1] == {
            TopicPartition(topic, 0),
            TopicPartition(topic, 1),
        }
        builder = await producer.connect()
        assert callable(builder)
        assert not producer.native.subscribers
        confirmed = await producer.publish(
            {"seq": 0},
            topic,
            key=b"order",
            partition=0,
            headers={"origin": "api"},
        )
        assert isinstance(confirmed, RecordMetadata)
        assert (confirmed.topic, confirmed.partition) == (topic, 0)
        pending = await producer.publish(
            {"seq": 1}, topic, partition=0, no_confirm=True
        )
        assert isinstance(pending, asyncio.Future)
        assert (await pending).topic == topic
        batch = await producer.publish_batch(
            {"seq": 2}, {"seq": 3}, topic=topic, partition=1
        )
        assert isinstance(batch, RecordMetadata) and batch.partition == 1
        batch_publisher = producer.publisher(topic, batch=True, partition=1)
        await batch_publisher.publish({"seq": 4}, {"seq": 5})
        publisher = producer.publisher(topic, partition=0)
        await publisher.publish({"seq": 6})
        await asyncio.wait_for(completed.wait(), timeout=30)
        combined = received[0] + received[1]
        assert sorted(row[0] for row in combined) == list(range(7))
        assert sorted(row[0] for row in received[2]) == list(range(7))
        assert received[0] and received[1]
        assert {r[1] for r in received[0]}.isdisjoint(
            {r[1] for r in received[1]}
        )
        first = next(row for row in combined if row[0] == 0)
        assert first[1:3] == (0, b"order")
        assert first[3]["origin"] == b"api"
    finally:
        await asyncio.gather(
            producer.stop(), *(worker.stop() for worker in workers)
        )
        try:
            await admin.delete_topics([topic])
        finally:
            await admin.close()

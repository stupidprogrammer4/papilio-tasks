import asyncio
import importlib
import os
from collections.abc import AsyncIterator
from contextlib import aclosing
from uuid import uuid4

import pytest
from dishka import Provider, Scope
from pydantic import BaseModel
from taskiq import AckableMessage
from taskiq.receiver import Receiver

from papilio_tasks.apps.projections.application import create_broker


class Payload(BaseModel):
    value: int


@pytest.mark.parametrize("kind", ["nats", "kafka"])
async def test_projection_single_batch_payload_failure_and_native_delivery(
    kind,
):
    url = os.getenv(f"TEST_{kind.upper()}_URL")
    redis_url = os.getenv("TEST_REDIS_URL")
    if not url or not redis_url:
        pytest.skip("Requires isolated Redis and " + kind)
    pytest.importorskip("taskiq_redis")
    from redis.asyncio import Redis
    from taskiq_redis import RedisAsyncResultBackend

    name = "papilio-delivery-" + uuid4().hex
    target = name + "-products"
    backend = importlib.import_module(
        "papilio_tasks.apps.projections.backends." + kind
    )
    registrars = importlib.import_module(
        "papilio_tasks.apps.projections.registry." + kind
    )
    if kind == "nats":
        plugin = pytest.importorskip("taskiq_nats")
        prefix = "Nats"
        transport = backend.JetStreamBroker(
            plugin.PullBasedJetStreamBroker(
                url,
                subject=target,
                stream_name=name,
                durable=name,
                pull_consume_timeout=1,
            )
        )
    else:
        plugin = pytest.importorskip("taskiq_aio_kafka")
        from kafka.admin import NewTopic

        prefix = "Kafka"
        native = plugin.AioKafkaBroker(
            url,
            kafka_topic=name,
            kafka_topics=[NewTopic(target, 1, 1)],
            delete_topic_on_shutdown=True,
        )
        native.configure_consumer(group_id=name, auto_offset_reset="earliest")
        transport = backend.KafkaBroker(native)
    projection = getattr(backend, prefix + "Projection")
    direct = getattr(backend, prefix + "Direct")
    registrar = getattr(registrars, prefix + "Registrar")
    opened, closed, writes = [], [], []

    class Session:
        pass

    async def session() -> AsyncIterator[Session]:
        current = Session()
        opened.append(current)
        try:
            yield current
        finally:
            closed.append(current)

    class Product(projection[Payload, str, int]):
        def __init__(self, session: Session):
            super().__init__()
            self.session = session

        async def read(self, payload: Payload, *, factor: int = 2) -> Payload:
            return Payload(value=payload.value * factor)

        async def transform(self, data: Payload) -> str:
            return str(data.value)

        async def write(self, data: str) -> int:
            if data == "-2":
                raise ValueError("write unavailable")
            writes.append(("single", data))
            return int(data)

    class Products(direct[list[int], int]):
        def __init__(self, session: Session):
            super().__init__()
            self.session = session

        async def read(self, ids: list[int]) -> list[int]:
            return ids

        async def write(self, data: list[int]) -> int:
            writes.append(("batch", data))
            return sum(data)

    if kind == "kafka":
        Product.topic = target
        Products.topic = target
    registry = registrar(transport)
    registry.include(Product, name="products.single")
    registry.include(Products, name="products.batch")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(session, provides=Session)
    provider.provide(Product)
    provider.provide(Products)
    native = transport.native
    native.with_result_backend(
        RedisAsyncResultBackend(redis_url, prefix_str=name, result_ex_time=60)
    )
    broker = create_broker(registrar=registry, providers=[provider])
    broker.is_worker_process = True
    try:
        await broker.startup()
        handles = await asyncio.gather(
            Product.enqueue(Payload(value=4), factor=3),
            Products.enqueue([5, 6]),
            Product.enqueue(Payload(value=-1)),
        )
        assert opened == []
        receiver = Receiver(broker)
        messages_by_id = {}
        async with aclosing(broker.listen()) as messages:
            async with asyncio.timeout(20):
                # Consume a bounded native stream. Each callback must finish
                # before checking its scope and acknowledgement outcome.
                for _ in handles:
                    delivery = await anext(messages)
                    assert isinstance(delivery, AckableMessage) == (
                        kind == "nats"
                    )
                    data = (
                        delivery.data
                        if isinstance(delivery, AckableMessage)
                        else delivery
                    )
                    wire = broker.formatter.loads(data)
                    messages_by_id[wire.task_id] = wire
                    await receiver.callback(delivery)
        results = await asyncio.gather(
            *(handle.wait_result(timeout=5) for handle in handles)
        )
        assert results[0].return_value == 12 and not results[0].is_err
        assert results[1].return_value == 11 and not results[1].is_err
        assert results[2].is_err
        assert isinstance(results[2].error, ValueError)
        assert str(results[2].error) == "write unavailable"
        assert messages_by_id[handles[0].task_id].args == [{"value": 4}]
        assert messages_by_id[handles[0].task_id].kwargs == {"factor": 3}
        assert messages_by_id[handles[1].task_id].args == [[5, 6]]
        assert ("single", "12") in writes and ("batch", [5, 6]) in writes
        assert len(writes) == 2
        assert opened == closed and len(closed) == 3
        assert len({id(item) for item in closed}) == 3
        assert broker.state.dishka_container_registry == {}
        if kind == "nats":
            await native.client.flush()
            info = await native.js.consumer_info(name, name)
            assert info.num_ack_pending == 0
    finally:
        try:
            if kind == "nats" and native.client.is_connected:
                await native.js.delete_stream(name)
        finally:
            await broker.shutdown()
            async with Redis.from_url(redis_url) as redis:
                keys = [key async for key in redis.scan_iter(match=name + "*")]
                if keys:
                    await redis.delete(*keys)


async def test_jetstream_redelivers_unacked_task_after_consumer_restart():
    url = os.getenv("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    plugin = pytest.importorskip("taskiq_nats")
    from nats.js.api import ConsumerConfig

    from papilio_tasks.infra.taskiq.brokers.backends.nats import (
        JetStreamBroker,
    )

    name = "papilio-recovery-" + uuid4().hex

    def transport():
        return JetStreamBroker(
            plugin.PullBasedJetStreamBroker(
                url,
                subject=name,
                stream_name=name,
                durable=name,
                consumer_config=ConsumerConfig(
                    durable_name=name, ack_wait=0.1
                ),
                pull_consume_timeout=1,
            )
        )

    first, second = transport(), transport()

    async def run(value: int) -> int:
        return value

    task = first.register(run, name="recover")
    second.register(run, name="recover")
    try:
        await first.native.startup()
        handle = await task.kiq(17)
        async with aclosing(first.native.listen()) as messages:
            async with asyncio.timeout(10):
                delivery = await anext(messages)
        initial = first.native.formatter.loads(delivery.data)
        assert initial.task_id == handle.task_id
        # Simulate a lost worker before ack; the next connection uses the same
        # native durable rather than republishing the job or resetting offsets.
        await first.native.shutdown()
        await second.native.startup()
        async with aclosing(second.native.listen()) as messages:
            async with asyncio.timeout(10):
                repeated = await anext(messages)
                message = second.native.formatter.loads(repeated.data)
                assert message.task_id == initial.task_id
                assert message.args == [17]
                await repeated.ack()
        await second.native.client.flush()
        info = await second.native.js.consumer_info(name, name)
        assert info.num_ack_pending == 0
        assert info.num_pending == 0
    finally:
        if first.native.client.is_connected:
            await first.native.shutdown()
        try:
            if second.native.client.is_connected:
                await second.native.js.delete_stream(name)
        finally:
            await second.native.shutdown()

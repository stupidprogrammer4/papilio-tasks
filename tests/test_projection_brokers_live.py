import asyncio
import os
from contextlib import aclosing
from uuid import uuid4

import pytest
from dishka import Provider, Scope
from hook_support import handler
from pydantic import BaseModel
from taskiq.receiver import Receiver

from papilio_tasks.apps.projections import Direct, Hooks, Projection
from papilio_tasks.apps.projections.application import create_broker
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.tools.hooks.publish import PublishHooks


class Payload(BaseModel):
    value: int


@pytest.mark.parametrize("kind", ["redis", "rabbit"])
@pytest.mark.parametrize(
    "failure", [None, "write", "after_write", "error_hook"]
)
async def test_projection_live_transport_hooks_results_and_ack(kind, failure):
    redis_url = os.getenv("TEST_REDIS_URL")
    rabbit_url = os.getenv("TEST_RABBIT_URL")
    if not redis_url or (kind == "rabbit" and not rabbit_url):
        pytest.skip("Requires isolated Redis and selected broker service")
    pytest.importorskip("taskiq_redis")
    from redis.asyncio import Redis
    from taskiq_redis import RedisAsyncResultBackend

    prefix = f"papilio-projection-{uuid4().hex}"
    if kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from taskiq_aio_pika import Exchange, Queue

        from papilio_tasks.infra.taskiq.brokers.backends.rabbit import (
            RabbitBroker,
        )

        def transport():
            return RabbitBroker(
                rabbit_url,
                queues=[Queue(name=prefix)],
                exchange=Exchange(name=prefix),
                dead_letter_queue=Queue(name=prefix + "-dead"),
            )
    else:
        from papilio_tasks.infra.taskiq.brokers.backends.redis import (
            RedisStreamBroker,
        )

        def transport():
            return RedisStreamBroker(
                redis_url,
                queue_name=prefix,
                consumer_group_name=prefix,
                consumer_id="0",
            )

    producer, consumer = transport(), transport()
    for broker in (producer, consumer):
        broker.native.with_result_backend(
            RedisAsyncResultBackend(
                redis_url,
                prefix_str=prefix + "-results",
                result_ex_time=60,
            )
        )
    assert producer.native is not consumer.native
    opened, closed, calls, writes, errors = [], [], [], [], []
    published, publish_closed = [], []

    async def publication_hooks():
        async def sent(event):
            published.append(event)

        try:
            yield PublishHooks(after_send=(handler(sent),))
        finally:
            publish_closed.append(True)

    class Session:
        def __init__(self, marker):
            self.marker = marker
            self.closed = False

    def record(session, phase):
        assert not session.closed
        calls.append((session.marker, phase))

    def hooks(session, scope):
        async def read(event):
            record(session, scope + ":read")

        async def transform(event):
            record(session, scope + ":transform")

        async def write(event):
            record(session, scope + ":write")
            if (
                scope == "shared"
                and event.data == "39"
                and failure == "after_write"
            ):
                raise RuntimeError("notification failed")

        async def error(event):
            record(session, scope + ":error")
            errors.append((event.stage, str(event.error), event.call.args))
            if scope == "shared" and failure == "error_hook":
                raise RuntimeError("error reporting failed")

        return dict(
            after_read=(handler(read),),
            after_transform=(handler(transform),),
            after_write=(handler(write),),
            on_error=(handler(error),),
        )

    class AppHooks(Hooks[object, object, object]):
        def __init__(self, session: Session):
            super().__init__(**hooks(session, "shared"))

    class Product(Projection[Payload, str, int]):
        def __init__(self, session: Session):
            super().__init__(hooks=Hooks(**hooks(session, "local")))
            self.session = session

        async def read(self, payload: Payload, *, factor: int = 2) -> Payload:
            record(self.session, "read")
            return Payload(value=payload.value * factor)

        async def transform(self, data: Payload) -> str:
            record(self.session, "transform")
            return str(data.value)

        async def write(self, data: str) -> int:
            record(self.session, "write")
            writes.append(data)
            if data == "39" and failure in ("write", "error_hook"):
                raise ValueError("write failed")
            return int(data)

    class Products(Direct[list[int], int]):
        def __init__(self, session: Session):
            super().__init__(hooks=Hooks(**hooks(session, "local")))
            self.session = session

        async def read(self, ids: list[int]) -> list[int]:
            record(self.session, "read")
            return ids

        async def write(self, data: list[int]) -> int:
            record(self.session, "write")
            writes.append(data)
            return sum(data)

    async def session():
        current = Session(len(opened))
        opened.append(current)
        try:
            yield current
        finally:
            current.closed = True
            closed.append(current)

    publisher = Registrar(producer, hooks=AppHooks)
    registry = Registrar(consumer, hooks=AppHooks)
    single_task = publisher.include(Product, name="products.single")
    batch_task = publisher.include(Products, name="products.batch")
    single, batch = Product, Products
    # Only one Papilio class binding per process. Simulate a remote worker with
    # native registrations on its independent transport, not a second binding.
    consumer.register(
        single_task.original_func,
        name=single_task.task_name,
        labels=single_task.labels,
    )
    consumer.register(
        batch_task.original_func,
        name=batch_task.task_name,
        labels=batch_task.labels,
    )
    assert (
        single_task.labels["queue_name"]
        == batch_task.labels["queue_name"]
        == prefix
    )
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(session, provides=Session)
    provider.provide(Product)
    provider.provide(Products)
    provider.provide(AppHooks)
    producer_provider = Provider(scope=Scope.REQUEST)
    producer_provider.provide(publication_hooks, provides=PublishHooks)
    assert (
        create_broker(
            registrar=publisher,
            providers=[producer_provider],
            publish_hooks=PublishHooks,
        )
        is producer.native
    )
    assert (
        create_broker(registrar=registry, providers=[provider])
        is consumer.native
    )
    consumer.native.is_worker_process = True
    assert opened == []
    try:
        await consumer.native.startup()
        await producer.native.startup()
        handles = [
            await single.enqueue({"value": 4}),
            await batch.enqueue([5, 6]),
            await single.enqueue(Payload(value=13), factor=3),
        ]
        assert [event.task_id for event in published] == [
            handle.task_id for handle in handles
        ]
        assert publish_closed == [True] * 3
        assert opened == []  # Publishing resolved no worker services.
        receiver = Receiver(consumer.native)
        wire = []
        async with aclosing(consumer.native.listen()) as messages:
            async with asyncio.timeout(15):
                for _ in handles:
                    delivery = await anext(messages)
                    message = consumer.native.formatter.loads(delivery.data)
                    wire.append(
                        (message.task_name, message.args, message.kwargs)
                    )
                    await receiver.callback(delivery)
        results = [await handle.wait_result(timeout=5) for handle in handles]
        assert wire == [
            ("products.single", [{"value": 4}], {}),
            ("products.batch", [[5, 6]], {}),
            ("products.single", [{"value": 13}], {"factor": 3}),
        ]
        assert results[0].return_value == 8 and not results[0].is_err
        assert results[1].return_value == 11 and not results[1].is_err
        if failure:
            error = results[2].error
            expected = RuntimeError if failure == "after_write" else ValueError
            assert results[2].is_err and isinstance(error, expected)
            assert str(error) == (
                "notification failed"
                if failure == "after_write"
                else "write failed"
            )
            assert errors[0][0] == (
                "after_write" if failure == "after_write" else "write"
            )
        else:
            assert not results[2].is_err and results[2].return_value == 39
            assert errors == []
        assert writes == ["8", [5, 6], "39"]
        assert opened == closed and len(opened) == 3
        assert all(item.closed for item in opened)
        assert consumer.native.state.dishka_container_registry == {}
        stages = [
            "read",
            "shared:read",
            "local:read",
            "transform",
            "shared:transform",
            "local:transform",
            "write",
            "shared:write",
            "local:write",
        ]
        assert [phase for marker, phase in calls if marker == 0] == stages
        assert [phase for marker, phase in calls if marker == 1] == [
            phase for phase in stages if phase != "transform"
        ]
        if failure in ("write", "error_hook"):
            stages = stages[:7] + ["shared:error"]
            if failure == "write":
                stages.append("local:error")
        elif failure == "after_write":
            stages = stages[:8] + ["shared:error", "local:error"]
        assert [phase for marker, phase in calls if marker == 2] == stages
        if kind == "rabbit":
            async with consumer.native.write_conn.channel() as channel:
                queue = await channel.get_queue(prefix)
                assert queue.declaration_result.message_count == 0
                assert await queue.get(fail=False) is None
        else:
            async with Redis.from_url(redis_url) as redis:
                assert (await redis.xpending(prefix, prefix))["pending"] == 0
    finally:
        try:
            if kind == "rabbit" and consumer.native.write_conn is not None:
                async with consumer.native.write_conn.channel() as channel:
                    await channel.queue_delete(prefix)
                    await channel.queue_delete(prefix + "-dead")
                    await channel.exchange_delete(prefix)
        finally:
            await producer.native.shutdown()
            await consumer.native.shutdown()
            async with Redis.from_url(redis_url) as redis:
                keys = [
                    key async for key in redis.scan_iter(match=prefix + "*")
                ]
                if keys:
                    await redis.delete(*keys)

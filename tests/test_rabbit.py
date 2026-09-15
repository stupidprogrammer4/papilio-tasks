import asyncio
import os
from contextlib import aclosing
from uuid import uuid4

import pytest

pytest.importorskip("taskiq_aio_pika")
from taskiq_aio_pika import Exchange, Queue, QueueType  # noqa: E402

from papilio_tasks.schedulers import Registrar, Scheduler  # noqa: E402
from papilio_tasks.schedulers.backends.rabbit import (  # noqa: E402
    RabbitScheduler,
)
from papilio_tasks.schedulers.infra.brokers.backends.rabbit import (  # noqa: E402
    RabbitBroker,
)
from papilio_tasks.schedulers.registry.rabbit import (  # noqa: E402
    RabbitRegistrar,
)


async def run(value: int = 1) -> int:
    return value


def test_rabbit_registration_uses_queue_config_and_preserves_labels():
    queue = Queue(name="reports", routing_key="reports.daily")
    broker = RabbitBroker(
        "amqp://guest:guest@localhost/",
        queues=[queue],
        label_for_routing="route",
    )
    assert broker.get_queue("reports") == queue
    first = broker.register(run, name="first")
    second_queue = Queue(name="mail", type=QueueType.CLASSIC, max_priority=5)
    assert broker.add_queue(second_queue) == second_queue
    labels = {"name": "mail metadata", "priority": 3}
    second = broker.register(run, name="second", queue="mail", labels=labels)
    assert first.labels == {"route": "reports.daily"}
    assert second.labels == {**labels, "route": "mail"}
    assert "route" not in labels
    assert broker.register(run, name="default").labels == first.labels
    with pytest.raises(KeyError):
        broker.get_queue("missing")
    with pytest.raises(KeyError):
        broker.register(run, name="unknown", queue="missing")
    with pytest.raises(ValueError, match="conflicts"):
        broker.register(run, name="conflict", labels={"route": "mail"})
    assert broker.native.find_task("conflict") is None
    assert (
        broker.add_queue(Queue(name="reports", routing_key="reports.daily"))
        is queue
    )
    with pytest.raises(ValueError, match="Conflicting queue"):
        broker.add_queue(Queue(name="reports", durable=False))
    assert broker.get_queue("reports") is queue
    with pytest.raises(ValueError, match="empty"):
        broker.add_queue(Queue(name=""))


@pytest.mark.skipif(
    not os.getenv("TEST_RABBIT_URL"), reason="No test RabbitMQ"
)
async def test_rabbit_live_declares_routes_consumes_and_acks_both_queues():
    prefix = f"papilio-{uuid4().hex}"
    first = Queue(name=f"{prefix}-first", routing_key=f"{prefix}.first")
    second = Queue(name=f"{prefix}-second")
    broker = RabbitBroker(
        os.environ["TEST_RABBIT_URL"],
        queues=[first],
        exchange=Exchange(name=prefix),
        dead_letter_queue=Queue(name=f"{prefix}-dead"),
        label_for_routing="route",
    )
    task_a = broker.register(run, name="first")
    broker.add_queue(second)
    task_b = broker.register(run, name="second", queue=second.name)
    broker.native.is_worker_process = True
    await broker.native.startup()
    try:
        with pytest.raises(RuntimeError, match="before"):
            broker.add_queue(Queue(name="late"))
        await task_a.kiq(11)
        await task_b.kiq(22)
        # Use a fresh channel: RobustChannel caches earlier declarations.
        async with broker.native.write_conn.channel() as channel:
            for config in (first, second):
                queue = await channel.get_queue(config.name)
                assert queue.declaration_result.message_count == 1
        received = {}
        async with aclosing(broker.native.listen()) as messages:
            async with asyncio.timeout(10):
                for _ in range(2):
                    delivery = await anext(messages)
                    message = broker.native.formatter.loads(delivery.data)
                    received[message.task_name] = message.args
                    await delivery.ack()
        assert received == {"first": [11], "second": [22]}
        async with broker.native.write_conn.channel() as channel:
            for config in (first, second):
                queue = await channel.get_queue(config.name)
                assert await queue.get(fail=False) is None
    finally:
        for config in (first, second):
            await broker.native.write_channel.queue_delete(config.name)
        await broker.native.write_channel.queue_delete(f"{prefix}-dead")
        await broker.native.write_channel.exchange_delete(prefix)
        await broker.native.shutdown()


def test_rabbit_scheduler_queues_sharing_override_and_backend_boundary():
    broker = RabbitBroker("amqp://guest:guest@localhost/")
    registrar = RabbitRegistrar(broker)
    shared = Queue(name="reports", routing_key="reports.daily")

    class Report(RabbitScheduler):
        queue = shared

        async def run(self):
            pass

    class Mail(RabbitScheduler):
        queue = shared

        async def run(self):
            pass

    registrar.include(Report, name="report")
    registrar.include(Mail, name="mail")
    assert Report.task().labels["queue_name"] == "reports.daily"
    assert Mail.task().labels == Report.task().labels
    assert broker.get_queue("reports") == shared

    class Override(Report):
        pass

    registrar.include(Override, name="override", queue=Queue(name="separate"))
    assert Override.queue is shared
    assert Override.task().labels["queue_name"] == "separate"

    class ByName(Report):
        pass

    registrar.include(ByName, queue=broker.get_queue("separate"))
    assert ByName.queue is shared
    assert ByName.task().labels["queue_name"] == "separate"

    class Conflict(Report):
        queue = Queue(name="reports", durable=False)

    with pytest.raises(ValueError, match="Conflicting queue"):
        registrar.include(Conflict)
    with pytest.raises(TypeError, match="memory scheduler"):
        Registrar().include(Conflict)

    class Plain(Scheduler):
        async def run(self):
            pass

    with pytest.raises(TypeError, match="rabbit scheduler"):
        registrar.include(Plain)

    for index in range(300):
        cls = type(f"Job{index}", (Report,), {})
        registrar.include(cls, name=f"module{index}.report")
    assert len(broker.native.local_task_registry) == 304
    assert broker.get_queue("reports") == shared


@pytest.mark.skipif(
    not os.getenv("TEST_RABBIT_URL"), reason="No test RabbitMQ"
)
async def test_rabbit_subset_consumption_and_explicit_declaration():
    prefix = f"papilio-{uuid4().hex}"
    first, second, direct = (
        Queue(name=f"{prefix}-{suffix}") for suffix in ("a", "b", "direct")
    )
    broker = RabbitBroker(
        os.environ["TEST_RABBIT_URL"],
        queues=[first, second],
        exchange=Exchange(name=prefix),
        dead_letter_queue=Queue(name=f"{prefix}-dead"),
    )
    with pytest.raises(RuntimeError, match="Start"):
        await broker.declare_queue(direct)
    with pytest.raises(KeyError):
        broker.consume("unknown")
    broker.consume(second.name)

    class First(RabbitScheduler):
        queue = first

        async def run(self, value: int):
            return value

    class Second(First):
        queue = second

    registrar = RabbitRegistrar(broker)
    task_a = registrar.include(First, name="a")
    task_b = registrar.include(Second, name="b")
    broker.native.is_worker_process = True
    await broker.native.startup()
    try:
        queue = await broker.declare_queue(direct)
        assert queue.name == direct.name
        assert (await broker.declare_queue(direct)).name == direct.name
        with pytest.raises(KeyError):
            broker.get_queue(direct.name)
        # The explicit declaration also creates the routing binding.
        from aio_pika import Message

        exchange = await broker.native.write_channel.get_exchange(prefix)
        await exchange.publish(Message(b"direct"), routing_key=direct.name)
        received = await queue.get()
        assert received.body == b"direct"
        await received.ack()
        await task_a.kiq(11)
        await task_b.kiq(22)
        async with aclosing(broker.native.listen()) as messages:
            delivery = await asyncio.wait_for(anext(messages), timeout=10)
            assert (
                broker.native.formatter.loads(delivery.data).task_name == "b"
            )
            await delivery.ack()
        async with broker.native.write_conn.channel() as channel:
            unread = await channel.get_queue(first.name)
            assert unread.declaration_result.message_count == 1
        # A server-side definition conflict must surface, not be swallowed.
        from aio_pika.exceptions import ChannelPreconditionFailed

        with pytest.raises(ChannelPreconditionFailed):
            await broker.declare_queue(Queue(name=direct.name, durable=False))
    finally:
        async with broker.native.write_conn.channel() as channel:
            for config in (first, second, direct):
                await channel.queue_delete(config.name)
            await channel.queue_delete(f"{prefix}-dead")
            await channel.exchange_delete(prefix)
        await broker.native.shutdown()

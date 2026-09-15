import asyncio
import os
from contextlib import aclosing
from uuid import uuid4

import pytest

pytest.importorskip("taskiq_redis")
from redis.asyncio import Redis  # noqa: E402

from papilio_tasks.schedulers.backends.redis import (  # noqa: E402
    RedisScheduler,
)
from papilio_tasks.schedulers.infra.brokers.backends.redis import (  # noqa: E402
    RedisStreamBroker,
)
from papilio_tasks.schedulers.infra.queues.redis import (
    RedisQueue,  # noqa: E402
)
from papilio_tasks.schedulers.registry.redis import (  # noqa: E402
    RedisRegistrar,
)


async def run(value: int = 1) -> int:
    return value


def test_redis_registration_and_stream_configuration():
    broker = RedisStreamBroker("redis://localhost", queue_name="reports")
    first = broker.register(run, name="first")
    assert broker.add_queue(RedisQueue("mail")) == RedisQueue("mail")
    assert broker.get_queue("mail") == RedisQueue("mail")
    assert broker.get_queue("reports") == RedisQueue("reports")
    assert broker.native.additional_streams == {}
    labels = {"name": "metadata"}
    second = broker.register(run, name="second", queue="mail", labels=labels)
    assert first.labels == {"queue_name": "reports"}
    assert second.labels == {**labels, "queue_name": "mail"}
    assert "queue_name" not in labels
    assert broker.register(run, name="default").labels == first.labels
    with pytest.raises(KeyError):
        broker.get_queue("missing")
    with pytest.raises(KeyError):
        broker.register(run, name="missing", queue="missing")
    with pytest.raises(ValueError, match="conflicts"):
        broker.register(run, name="conflict", labels={"queue_name": "mail"})
    assert broker.native.find_task("conflict") is None
    for name in ("reports", "mail"):
        existing = broker.get_queue(name)
        assert broker.add_queue(RedisQueue(name)) is existing
        with pytest.raises(ValueError, match="Conflicting queue"):
            broker.add_queue(RedisQueue(name, read_id="0"))
        assert broker.get_queue(name) is existing
    with pytest.raises(ValueError, match="empty"):
        broker.add_queue(RedisQueue(""))


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="No test Redis")
async def test_redis_live_routes_consumes_and_acks_each_stream():
    prefix = f"papilio-{uuid4().hex}"
    first, second = f"{prefix}-first", f"{prefix}-second"
    url = os.environ["TEST_REDIS_URL"]
    broker = RedisStreamBroker(
        url,
        queue_name=first,
        additional_streams={second: ">"},
        consumer_group_name=prefix,
        consumer_id="0",
    )
    task_a = broker.register(run, name="first")
    broker.add_queue(RedisQueue(second))
    task_b = broker.register(run, name="second", queue=second)
    # Explicit consumer_id=0 must retain work queued before group creation.
    await task_a.kiq(11)
    await task_b.kiq(22)
    await broker.native.startup()
    async with Redis.from_url(url) as redis:
        try:
            assert await redis.xlen(first) == 1
            assert await redis.xlen(second) == 1
            received = {}
            async with aclosing(broker.native.listen()) as messages:
                async with asyncio.timeout(10):
                    for _ in range(2):
                        delivery = await anext(messages)
                        message = broker.native.formatter.loads(delivery.data)
                        received[message.task_name] = message.args
                        await delivery.ack()
            assert received == {"first": [11], "second": [22]}
            for stream in (first, second):
                assert (await redis.xpending(stream, prefix))["pending"] == 0
        finally:
            await broker.native.shutdown()
            await redis.delete(first, second)


def test_redis_scheduler_queue_sharing_override_and_settings():
    broker = RedisStreamBroker(
        "redis://localhost",
        consumer_group_name="group",
        additional_streams={"reports": ">"},
    )
    registrar = RedisRegistrar(broker)

    class Report(RedisScheduler):
        queue = RedisQueue("reports")

        async def run(self):
            pass

    class Other(Report):
        pass

    registrar.include(Report, name="report")
    registrar.include(Other, name="other")
    assert (
        Report.task().labels
        == Other.task().labels
        == {"queue_name": "reports"}
    )
    assert broker.native.consumer_group_name == "group"

    class Override(Report):
        pass

    registrar.include(Override, queue=RedisQueue("other", read_id="0"))
    assert Override.queue == RedisQueue("reports")
    assert broker.get_queue("other").read_id == "0"

    class ByName(Report):
        pass

    registrar.include(ByName, queue=broker.get_queue("other"))
    assert ByName.queue == RedisQueue("reports")
    assert ByName.task().labels["queue_name"] == "other"

    class Conflict(Report):
        queue = RedisQueue("reports", read_id="0")

    with pytest.raises(ValueError, match="Conflicting queue"):
        registrar.include(Conflict)
    with pytest.raises(KeyError):
        registrar.include(Conflict, queue=broker.get_queue("missing"))
    assert broker.native.queue_name == "taskiq"
    assert broker.native.additional_streams == {"reports": ">"}


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="No test Redis")
async def test_redis_subset_consumption_and_explicit_declaration():
    from redis.exceptions import ResponseError

    prefix = f"papilio-{uuid4().hex}"
    first, second, direct = [f"{prefix}-{s}" for s in ("a", "b", "direct")]
    broker = RedisStreamBroker(
        os.environ["TEST_REDIS_URL"],
        queue_name=second,
        consumer_group_name=prefix,
        consumer_id="0",
    )

    class First(RedisScheduler):
        queue = RedisQueue(first)

        async def run(self, value: int):
            return value

    class Second(First):
        queue = RedisQueue(second)

    registrar = RedisRegistrar(broker)
    task_a = registrar.include(First, name="a")
    task_b = registrar.include(Second, name="b")
    assert task_a.labels["queue_name"] == first
    assert broker.register(run, name="default").labels == {
        "queue_name": second
    }
    await broker.native.startup()
    async with Redis.from_url(os.environ["TEST_REDIS_URL"]) as redis:
        try:
            spec = RedisQueue(direct)
            assert await broker.declare_queue(spec) == spec
            assert await broker.declare_queue(spec) == spec
            assert (await redis.xinfo_groups(direct))[0][
                "name"
            ] == prefix.encode()
            with pytest.raises(KeyError):
                broker.get_queue(direct)
            await task_a.kiq(11)
            await task_b.kiq(22)
            async with aclosing(broker.native.listen()) as messages:
                delivery = await asyncio.wait_for(anext(messages), timeout=10)
                message = broker.native.formatter.loads(delivery.data)
                assert message.task_name == "b"
                await delivery.ack()
            assert await redis.xlen(first) == 1
            assert await redis.xinfo_groups(first) == []
            assert (await redis.xpending(second, prefix))["pending"] == 0
            await redis.set(f"{prefix}-invalid", "not a stream")
            with pytest.raises(ResponseError):
                await broker.declare_queue(RedisQueue(f"{prefix}-invalid"))
        finally:
            await broker.native.shutdown()
            await redis.delete(first, second, direct, f"{prefix}-invalid")

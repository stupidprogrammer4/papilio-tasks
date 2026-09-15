import asyncio
import importlib
import os
import subprocess
import sys
from contextlib import aclosing
from uuid import uuid4

import pytest
from taskiq.receiver import Receiver


@pytest.mark.parametrize(
    "module,names,dependency",
    [
        ("brokers.base", ("Broker",), "taskiq"),
        ("brokers.contracts.base", ("BrokerContract",), "taskiq"),
        (
            "brokers.contracts.rabbit",
            ("RabbitContract",),
            "taskiq_aio_pika",
        ),
        (
            "brokers.contracts.redis",
            ("RedisStreamContract",),
            "taskiq_redis",
        ),
        ("brokers.backends.memory", ("MemoryBroker",), "taskiq"),
        (
            "brokers.backends.rabbit",
            ("RabbitBroker",),
            "taskiq_aio_pika",
        ),
        (
            "brokers.backends.redis",
            ("RedisStreamBroker",),
            "taskiq_redis",
        ),
        (
            "queues.rabbit",
            ("RabbitQueue", "declare_queue"),
            "taskiq_aio_pika",
        ),
        ("queues.redis", ("RedisQueue",), "taskiq"),
    ],
)
def test_legacy_imports_keep_object_identity(module, names, dependency):
    pytest.importorskip(dependency)
    old = importlib.import_module("papilio_tasks.schedulers.infra." + module)
    shared = importlib.import_module("papilio_tasks.infra.taskiq." + module)
    for name in names:
        assert getattr(old, name) is getattr(shared, name)


@pytest.mark.parametrize("kind", ["memory", "rabbit", "redis"])
def test_shared_infra_is_independent_of_apps_and_other_backends(kind):
    if kind != "memory":
        pytest.importorskip(
            "taskiq_aio_pika" if kind == "rabbit" else "taskiq_redis"
        )
    code = """
import asyncio
import importlib
import importlib.abc
import sys

kind = sys.argv[1]
blocked = {'papilio', 'dishka', 'faststream',
           'papilio_tasks.schedulers', 'papilio_tasks.projections',
           'papilio_tasks.events'}
if kind != 'rabbit': blocked.add('taskiq_aio_pika')
if kind != 'redis': blocked.update({'taskiq_redis', 'redis'})
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())

module = importlib.import_module(
    'papilio_tasks.infra.taskiq.brokers.backends.' + kind
)
if kind == 'memory':
    broker = module.MemoryBroker(await_inplace=True)
elif kind == 'rabbit':
    broker = module.RabbitBroker('amqp://guest:guest@localhost/')
else:
    broker = module.RedisStreamBroker('redis://localhost')

async def add(value: int) -> int:
    return value + 1
task = broker.register(add, name='add')
assert broker.native.find_task('add') is task
async def execute():
    await broker.native.startup()
    try:
        result = await (await task.kiq(4)).get_result()
        assert not result.is_err and result.return_value == 5
    finally:
        await broker.native.shutdown()
if kind == 'memory': asyncio.run(execute())
"""
    result = subprocess.run(
        [sys.executable, "-c", code, kind],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
async def test_distinct_tasks_execute_from_one_live_queue(kind):
    url = os.getenv(
        "TEST_RABBIT_URL" if kind == "rabbit" else "TEST_REDIS_URL"
    )
    if not url:
        pytest.skip("No test " + kind)
    name = "papilio-shared-" + uuid4().hex
    if kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from taskiq_aio_pika import Exchange

        from papilio_tasks.infra.taskiq.brokers.backends.rabbit import (
            RabbitBroker,
        )
        from papilio_tasks.infra.taskiq.queues.rabbit import RabbitQueue

        broker = RabbitBroker(
            url,
            queues=[RabbitQueue(name=name)],
            exchange=Exchange(name=name),
            dead_letter_queue=RabbitQueue(name=name + "-dead"),
        )
    else:
        pytest.importorskip("taskiq_redis")
        from papilio_tasks.infra.taskiq.brokers.backends.redis import (
            RedisStreamBroker,
        )

        broker = RedisStreamBroker(
            url, queue_name=name, consumer_group_name=name, consumer_id="0"
        )

    seen = []

    async def one(id: int):
        seen.append(("one", id))

    async def batch(ids: list[int], *, tag: str):
        seen.append(("batch", ids, tag))

    first = broker.register(one, name="products.one")
    second = broker.register(batch, name="products.batch")
    assert first.labels["queue_name"] == second.labels["queue_name"] == name
    broker.native.is_worker_process = True
    await broker.native.startup()
    try:
        await first.kiq(42)
        await second.kiq([43, 44], tag="sync")
        receiver = Receiver(broker.native, max_async_tasks=2)
        async with aclosing(broker.native.listen()) as messages:
            async with asyncio.timeout(10):
                for _ in range(2):
                    await receiver.callback(
                        await anext(messages), raise_err=True
                    )
        assert len(seen) == 2
        assert ("one", 42) in seen
        assert ("batch", [43, 44], "sync") in seen
    finally:
        try:
            if kind == "rabbit":
                async with broker.native.write_conn.channel() as channel:
                    await channel.queue_delete(name)
                    await channel.queue_delete(name + "-dead")
                    await channel.exchange_delete(name)
            else:
                from redis.asyncio import Redis

                async with Redis.from_url(url) as redis:
                    await redis.delete(name)
        finally:
            await broker.native.shutdown()

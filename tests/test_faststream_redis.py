import asyncio
import os
import subprocess
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.redis")

from faststream.redis import ListSub, PubSub, StreamSub, TestRedisBroker
from faststream.redis import RedisBroker as NativeRedisBroker

from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker


async def test_native_registration_and_lifecycle(monkeypatch):
    native = NativeRedisBroker(logger=None)
    broker = RedisBroker(native)
    connect = AsyncMock(return_value=object())
    start, stop = AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "connect", connect)
    monkeypatch.setattr(native, "start", start)
    monkeypatch.setattr(native, "stop", stop)
    assert broker.native is native
    channel = broker.subscriber(PubSub("orders"))
    stream = broker.subscriber(stream=StreamSub("history"))
    queue = broker.publisher(list=ListSub("pending"))
    assert channel in native.subscribers and stream in native.subscribers
    assert queue in native.publishers
    connect.assert_not_awaited()
    assert await broker.connect() is connect.return_value
    await broker.start()
    await broker.stop()
    connect.assert_awaited_once_with()
    start.assert_awaited_once_with()
    stop.assert_awaited_once_with()


@pytest.mark.parametrize("batch", [False, True])
async def test_native_options_result_and_error_fidelity(monkeypatch, batch):
    native = NativeRedisBroker(logger=None)
    broker = RedisBroker(native)
    value = 2 if batch else b"10-0"
    sender = AsyncMock(return_value=value)
    method = "publish_batch" if batch else "publish"
    monkeypatch.setattr(native, method, sender)
    messages = ({"id": 1}, {"id": 2}) if batch else ({"id": 1},)
    options = {"list": "orders"} if batch else {"stream": "orders"}
    options["headers"] = {"origin": "test"}
    options["correlation_id"] = "request-1"
    assert await getattr(broker, method)(*messages, **options) is value
    if batch:
        sender.assert_awaited_once_with(*messages, **options)
    else:
        sender.assert_awaited_once_with(*messages, None, **options)
    error = ConnectionError("offline")
    sender.side_effect = error
    with pytest.raises(ConnectionError) as caught:
        await getattr(broker, method)(*messages, **options)
    assert caught.value is error and sender.await_count == 2


async def test_native_channel_list_and_stream_routes():
    broker = RedisBroker(NativeRedisBroker(logger=None))
    observed = []

    @broker.subscriber("channel")
    async def channel(event: int):
        observed.append(("channel", event))

    @broker.subscriber(list=ListSub("list", batch=True))
    async def queue(event: list[int]):
        observed.append(("list", event))

    @broker.subscriber(stream=StreamSub("stream"))
    async def stream(event: int):
        observed.append(("stream", event))

    publisher = broker.publisher(stream="stream")
    async with TestRedisBroker(broker.native):
        await broker.publish(1, "channel")
        await broker.publish_batch(2, 3, list="list")
        await publisher.publish(4)
    assert observed == [("channel", 1), ("list", [2, 3]), ("stream", 4)]


def test_optional_import_isolation():
    script = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'dishka', 'dishka_faststream', 'aio_pika',
                   'aiokafka', 'confluent_kafka', 'nats'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from faststream.redis import RedisBroker as Native
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker
assert RedisBroker(Native()).native is not None
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


async def test_live_modes_metadata_and_connection_reuse():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires isolated Redis")
    prefix = "events-infra-" + uuid4().hex
    broker = RedisBroker(NativeRedisBroker(url, logger=None))
    observed = []
    done = asyncio.Event()

    def record(mode, value):
        observed.append((mode, value))
        if len(observed) == 3:
            done.set()

    @broker.subscriber(prefix + "-channel")
    async def channel(event: int):
        record("channel", event)

    @broker.subscriber(list=prefix + "-list")
    async def queue(event: int):
        record("list", event)

    @broker.subscriber(stream=StreamSub(prefix + "-stream", last_id="0-0"))
    async def stream(event: int):
        record("stream", event)

    client = await broker.connect()
    try:
        assert await broker.connect() is client
        await broker.start()
        sent = await asyncio.gather(
            broker.publish(1, prefix + "-channel"),
            broker.publish(2, list=prefix + "-list"),
            broker.publish(3, stream=prefix + "-stream"),
        )
        await asyncio.wait_for(done.wait(), 10)
        assert sorted(observed) == [("channel", 1), ("list", 2), ("stream", 3)]
        assert isinstance(sent[0], int) and sent[0] >= 1
        assert isinstance(sent[1], int)
        assert isinstance(sent[2], bytes)
        rows = await client.xrange(prefix + "-stream")
        assert rows[0][0] == sent[2]
        assert await broker.publish_batch(4, 5, list=prefix + "-batch") == 2
        assert await client.llen(prefix + "-batch") == 2
    finally:
        await broker.stop()
        # Reuse the same native connection pool for test-key cleanup.
        try:
            await client.delete(
                prefix + "-list", prefix + "-stream", prefix + "-batch"
            )
        finally:
            await client.aclose()

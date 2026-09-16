import asyncio
import os
import subprocess
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.nats")

from faststream import AckPolicy
from faststream.nats import JStream, NatsMessage, PubAck, PullSub
from faststream.nats import NatsBroker as NativeNatsBroker
from faststream.nats.message import NatsMessage as Response
from nats.errors import NoRespondersError

from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


async def test_registration_and_shared_lifecycle(monkeypatch):
    native = NativeNatsBroker(logger=None)
    broker = NatsBroker(native)
    connect, start, stop = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "connect", connect)
    monkeypatch.setattr(native, "start", start)
    monkeypatch.setattr(native, "stop", stop)
    core = broker.subscriber("orders", queue="finance")
    stream = JStream("history", subjects=["history.*"])
    push = broker.subscriber("history.push", stream=stream)
    pull = broker.subscriber("history.pull", stream=stream, pull_sub=PullSub())
    publisher = broker.publisher("history.push", stream=stream)
    assert all(sub in native.subscribers for sub in (core, push, pull))
    assert publisher in native.publishers
    assert broker.native is native
    connect.assert_not_awaited()
    assert await broker.connect() is connect.return_value
    await broker.start()
    await broker.stop()
    connect.assert_awaited_once_with()
    start.assert_awaited_once_with()
    stop.assert_awaited_once_with()


@pytest.mark.parametrize("operation", ["publish", "request"])
async def test_options_native_result_and_exception_identity(
    monkeypatch, operation
):
    native = NativeNatsBroker(logger=None)
    broker = NatsBroker(native)
    result = object()
    sender = AsyncMock(return_value=result)
    monkeypatch.setattr(native, operation, sender)
    options = dict(
        headers={"origin": "test"},
        correlation_id="test-id",
        stream="history",
        timeout=2,
    )
    assert (
        await getattr(broker, operation)({"id": 1}, "orders", **options)
        is result
    )
    sender.assert_awaited_once_with({"id": 1}, "orders", **options)
    failure = ConnectionError("offline")
    sender.side_effect = failure
    with pytest.raises(ConnectionError) as caught:
        await getattr(broker, operation)({"id": 1}, "orders", **options)
    assert caught.value is failure and sender.await_count == 2


@pytest.mark.parametrize("mode", ["adapter", "root"])
def test_optional_import_isolation(mode):
    script = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'dishka', 'dishka_faststream', 'aio_pika',
                   'aiokafka', 'confluent_kafka', 'redis', 'aiomqtt'}
        if sys.argv[1] == 'root':
            blocked.update({'nats', 'faststream'})
        if fullname.split('.')[0] in blocked:
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
if sys.argv[1] == 'root':
    import papilio_tasks.infra.faststream.brokers
    from papilio_tasks.cli.main import main
    main(['--help'])
else:
    from faststream.nats import NatsBroker as Native
    from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
    assert NatsBroker(Native()).native is not None
"""
    run = subprocess.run(
        [sys.executable, "-c", script, mode],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert run.returncode == 0, run.stderr


@pytest.fixture
def nats_url():
    url = os.getenv("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream enabled")
    return url


async def test_live_core_broadcast_queue_groups_and_connection_reuse(nats_url):
    subject = "events." + uuid4().hex
    sender = NatsBroker(NativeNatsBroker(nats_url, logger=None))
    workers = [
        NatsBroker(NativeNatsBroker(nats_url, logger=None)) for _ in range(3)
    ]
    received = [[], [], []]
    done = asyncio.Event()

    def handler(index):
        async def consume(event: int, msg: NatsMessage):
            received[index].append(
                (event, msg.headers["origin"], msg.correlation_id)
            )
            if (
                len(received[0]) + len(received[1]) == 40
                and len(received[2]) == 40
            ):
                done.set()

        return consume

    for index, worker in enumerate(workers):
        worker.subscriber(
            subject, queue="finance" if index < 2 else "", no_reply=True
        )(handler(index))
    client = await sender.connect()
    try:
        assert await sender.connect() is client
        # Core does not retain messages sent before subscriptions are active.
        assert await sender.publish(-1, subject) is None
        await client.flush()
        await asyncio.gather(*(worker.start() for worker in workers))
        worker_clients = [worker.native.connection for worker in workers]
        await asyncio.gather(
            *(worker.native.connection.flush() for worker in workers)
        )
        results = await asyncio.gather(
            *(
                sender.publish(
                    i,
                    subject,
                    headers={"origin": "test"},
                    correlation_id="test-id",
                )
                for i in range(40)
            )
        )
        assert results == [None] * 40
        await asyncio.wait_for(done.wait(), 10)
        assert received[0] and received[1]
        assert sorted(row[0] for row in received[0] + received[1]) == list(
            range(40)
        )
        assert sorted(row[0] for row in received[2]) == list(range(40))
        assert all(
            row[1:] == ("test", "test-id")
            for group in received
            for row in group
        )
    finally:
        await asyncio.gather(
            sender.stop(), *(worker.stop() for worker in workers)
        )
    assert client.is_closed
    assert all(connection.is_closed for connection in worker_clients)


@pytest.mark.parametrize("mode", ["push", "pull", "batch"])
async def test_live_jetstream_backlog_puback_and_ack(nats_url, mode):
    name = "events_" + uuid4().hex
    subject = name + ".orders"
    sender = NatsBroker(NativeNatsBroker(nats_url, logger=None))
    consumer = NatsBroker(NativeNatsBroker(nats_url, logger=None))
    stream = JStream(name, subjects=[subject], declare=False)
    publisher = sender.publisher(subject, stream=stream)
    received, batch_sizes = [], []
    done = asyncio.Event()

    async def one(event: int, msg: NatsMessage):
        received.append(event)
        assert msg.headers["origin"] == "test"
        await msg.ack_sync()
        if len(received) == 4:
            done.set()

    async def batch(event: list[int]):
        received.extend(event)
        batch_sizes.append(len(event))
        if len(received) == 4:
            done.set()

    options = dict(stream=stream, no_reply=True)
    if mode == "push":
        options["queue"] = "reader"
    if mode != "push":
        options["durable"] = "reader"
        options["pull_sub"] = PullSub(
            batch=mode == "batch", batch_size=2, timeout=0.2
        )
    if mode != "batch":
        options["ack_policy"] = AckPolicy.MANUAL
    consumer.subscriber(subject, **options)(batch if mode == "batch" else one)
    client = await sender.connect()
    js = client.jetstream()
    try:
        await js.add_stream(name=name, subjects=[subject])
        ack = await publisher.publish(0, headers={"origin": "test"})
        assert isinstance(ack, PubAck) and ack.stream == name and ack.seq == 1
        sent = await asyncio.gather(
            *(
                sender.publish(
                    i, subject, stream=name, headers={"origin": "test"}
                )
                for i in range(1, 4)
            )
        )
        assert all(isinstance(item, PubAck) for item in sent)
        assert sorted(item.seq for item in sent) == [2, 3, 4]
        assert (await js.stream_info(name)).state.messages == 4
        assert received == []
        await consumer.start()
        await asyncio.wait_for(done.wait(), 10)
        # Bounded observation: native automatic batch ACK follows the handler.
        async with asyncio.timeout(10):
            while (
                info := await js.consumer_info(name, "reader")
            ).num_ack_pending:
                await asyncio.sleep(0.02)
        assert info.num_pending == 0
        assert sorted(received) == list(range(4))
        assert (await js.stream_info(name)).state.messages == 4
        if mode == "batch":
            assert sum(batch_sizes) == 4 and max(batch_sizes) <= 2
    finally:
        await consumer.stop()
        try:
            await js.delete_stream(name)
        finally:
            await sender.stop()
    assert client.is_closed


@pytest.mark.parametrize("jetstream", [False, True])
async def test_live_request_reply_and_native_no_responders(
    nats_url, jetstream
):
    subject = "rpc_" + uuid4().hex
    server = NatsBroker(NativeNatsBroker(nats_url, logger=None))
    sender = NatsBroker(NativeNatsBroker(nats_url, logger=None))
    observed = []
    config = {"stream": JStream(subject)} if jetstream else {}

    @server.subscriber(subject, **config)
    async def double(event: int, msg: NatsMessage):
        observed.append((msg.headers["origin"], msg.correlation_id))
        return event * 2

    client = await sender.connect()
    try:
        await server.start()
        await server.native.connection.flush()
        options = {"stream": subject} if jetstream else {}
        reply = await sender.request(
            21,
            subject,
            timeout=3,
            headers={"origin": "test"},
            correlation_id="rpc-id",
            **options,
        )
        assert isinstance(reply, Response)
        assert await reply.decode() == 42
        assert observed == [("test", "rpc-id")]
        with pytest.raises(NoRespondersError):
            await sender.request(1, subject + ".missing", timeout=1)
    finally:
        await server.stop()
        try:
            if jetstream:
                await client.jetstream().delete_stream(subject)
        finally:
            await sender.stop()

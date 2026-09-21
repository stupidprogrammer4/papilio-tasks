import asyncio
from unittest.mock import AsyncMock

import pytest
from faststream import FastStream

from papilio_tasks.apps.events.rabbit import create_app as rabbit_app
from papilio_tasks.apps.lifecycle import Producers
from papilio_tasks.apps.projections.redis import create_app as projection_app
from papilio_tasks.apps.schedulers.redis import create_app as scheduler_app
from papilio_tasks.apps.taskiq import RedisSettings


@pytest.mark.parametrize(
    "factory,source_count", [(scheduler_app, 2), (projection_app, 1)]
)
async def test_assembly_is_lazy_and_beat_uses_worker_retry_source(
    factory, source_count
):
    # An unreachable address must be safe until startup.
    settings = RedisSettings(
        url="redis://127.0.0.1:1/0",
        queue_name="test-jobs",
        schedule_prefix="test-schedules",
        consumer_group="test-workers",
        result_prefix="test-results",
    )
    dependency = AsyncMock()
    app = factory(settings, producers=[dependency])
    try:
        assert app.broker.queue_name == "test-jobs"
        assert app.broker.consumer_group_name == "test-workers"
        assert len(app.scheduler.sources) == source_count
        assert (
            app.scheduler.sources[0]
            is app.broker.state.papilio_retry_source.native
        )
        dependency.connect.assert_not_awaited()
        dependency.stop.assert_not_awaited()
    finally:
        await app.stop()


@pytest.mark.parametrize(
    "field,value",
    [
        ("url", ""),
        ("queue_name", ""),
        ("schedule_prefix", ""),
        ("result_ttl", 0),
    ],
)
def test_invalid_redis_configuration_fails_before_assembly(field, value):
    options = dict(
        url="redis://localhost",
        queue_name="jobs",
        schedule_prefix="jobs-schedule",
    )
    options[field] = value
    with pytest.raises(ValueError):
        RedisSettings(**options)


async def test_startup_failure_cancels_connections_before_closing():
    started, cancelled = asyncio.Event(), asyncio.Event()
    order = []

    async def pending():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def failed():
        await started.wait()
        raise ConnectionError("offline")

    async def close_pending():
        assert cancelled.is_set()
        order.append("pending")

    async def close_failed():
        order.append("failed")

    pending_app = AsyncMock(
        connect=AsyncMock(side_effect=pending),
        stop=AsyncMock(side_effect=close_pending),
    )
    failed_app = AsyncMock(
        connect=AsyncMock(side_effect=failed),
        stop=AsyncMock(side_effect=close_failed),
    )
    group = Producers(pending_app, failed_app)
    with pytest.raises(ExceptionGroup):
        async with group():
            pytest.fail("Must not start serving")
    assert order == ["failed", "pending"]
    await group.stop()
    pending_app.stop.assert_awaited_once()
    failed_app.stop.assert_awaited_once()


async def test_shutdown_failure_still_closes_other_connections():
    first = AsyncMock()
    second = AsyncMock(
        stop=AsyncMock(side_effect=RuntimeError("close failed"))
    )
    with pytest.raises(RuntimeError, match="close failed"):
        async with Producers(first, second)():
            first.connect.assert_awaited_once()
            second.connect.assert_awaited_once()
    first.stop.assert_awaited_once()
    second.stop.assert_awaited_once()


def test_duplicate_producer_rejected():
    producer = AsyncMock()
    with pytest.raises(ValueError, match="only be registered once"):
        Producers(producer, producer)


async def test_rabbit_connect_does_not_start_consumers_or_dependencies(
    monkeypatch,
):
    dependency = AsyncMock()
    app = rabbit_app("amqp://guest:guest@localhost/", producers=[dependency])
    connect = AsyncMock()
    start = AsyncMock()
    monkeypatch.setattr(app.registrar.broker, "connect", connect)
    monkeypatch.setattr(FastStream, "start", start)
    monkeypatch.setattr(FastStream, "stop", AsyncMock())
    await app.connect()
    connect.assert_awaited_once()
    start.assert_not_awaited()
    dependency.connect.assert_not_awaited()
    await app.stop()
    dependency.stop.assert_not_awaited()


async def test_rabbit_failed_consumer_start_closes_dependencies(monkeypatch):
    dependency = AsyncMock()
    app = rabbit_app("amqp://guest:guest@localhost/", producers=[dependency])
    monkeypatch.setattr(
        FastStream, "start", AsyncMock(side_effect=ConnectionError("offline"))
    )
    stop = AsyncMock()
    monkeypatch.setattr(FastStream, "stop", stop)
    with pytest.raises(ConnectionError, match="offline"):
        await app.start()
    dependency.connect.assert_awaited_once()
    dependency.stop.assert_awaited_once()
    stop.assert_awaited_once()
    await app.stop()
    stop.assert_awaited_once()


@pytest.mark.parametrize("worker", [False, True])
async def test_dependencies_start_only_in_worker_process(monkeypatch, worker):
    dependency = AsyncMock()
    app = scheduler_app(
        RedisSettings(
            url="redis://127.0.0.1:1/0",
            queue_name="jobs",
            schedule_prefix="schedule",
        ),
        producers=[dependency],
    )
    monkeypatch.setattr(app.broker, "_declare_consumer_group", AsyncMock())
    app.broker.is_worker_process = worker
    try:
        await app.connect()
        assert dependency.connect.await_count == int(worker)
    finally:
        await app.stop()
    assert dependency.stop.await_count == int(worker)

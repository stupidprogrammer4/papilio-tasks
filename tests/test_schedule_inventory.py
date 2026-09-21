import asyncio
import os
from datetime import UTC, datetime, timedelta
from pickle import UnpicklingError
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from taskiq import ScheduledTask
from taskiq.serializers import JSONSerializer

from papilio_tasks.apps.schedulers.redis import RedisSettings, create_app
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_REDIS_URL"), reason="No test Redis"
)


@pytest.fixture
async def inventory():
    prefix = "test:inventory:" + uuid4().hex
    url = os.environ["TEST_REDIS_URL"]
    app = create_app(
        RedisSettings(
            url=url,
            queue_name=prefix + ":queue",
            schedule_prefix=prefix + ":schedules",
            result_prefix=prefix + ":results",
        )
    )
    async with Redis.from_url(url) as redis:
        try:
            yield app, redis, prefix
        finally:
            keys = await redis.keys(prefix + "*")
            if keys:
                await redis.delete(*keys)
            await app.stop()


def record(id, **timing):
    return ScheduledTask(
        schedule_id=id,
        task_name="inventory.test",
        args=[1],
        kwargs={"name": "طلا"},
        labels={},
        **timing,
    )


async def test_inventory_includes_future_past_cron_interval_and_labels(
    inventory,
):
    app, redis, prefix = inventory
    now = datetime.now(UTC)
    rows = [
        record("future", time=now + timedelta(days=2)),
        record("past", time=now - timedelta(days=2)),
        record("cron", cron="* * * * *"),
        record("interval", interval=30),
    ]
    await asyncio.gather(*(app.source.add_schedule(row) for row in rows))

    @app.broker.task(schedule=[{"interval": 60, "schedule_id": "declared"}])
    async def declared():
        pass

    keys = await redis.keys(prefix + "*")
    before = dict(
        zip(
            keys,
            await asyncio.gather(*(redis.dump(key) for key in keys)),
            strict=True,
        )
    )
    listed = await app.list_schedules()
    assert {row.schedule_id for row in listed} == {
        "future",
        "past",
        "cron",
        "interval",
        "declared",
    }
    assert next(
        row for row in listed if row.schedule_id == "future"
    ).kwargs == {"name": "طلا"}
    keys = await redis.keys(prefix + "*")
    after = dict(
        zip(
            keys,
            await asyncio.gather(*(redis.dump(key) for key in keys)),
            strict=True,
        )
    )
    assert before == after
    feed = await app.source.get_schedules()
    assert "future" not in {row.schedule_id for row in feed}
    assert {row.schedule_id for row in await app.source.list_schedules()} == {
        row.schedule_id for row in rows
    }


async def test_inventory_empty_and_malformed_data_is_not_hidden(inventory):
    app, redis, prefix = inventory
    assert await app.list_schedules() == []
    await redis.set(app.settings.schedule_prefix + ":data:broken", b"broken")
    with pytest.raises(UnpicklingError):
        await app.list_schedules()


async def test_inventory_batches_deduplicates_scan_and_handles_deleted_rows(
    inventory, monkeypatch
):
    app, redis, prefix = inventory
    rows = [record("one", interval=10), record("two", interval=20)]
    await asyncio.gather(*(app.source.add_schedule(row) for row in rows))
    base = app.settings.schedule_prefix + ":data:"
    pages = iter(
        [
            (1, [(base + "one").encode(), (base + "gone").encode()]),
            (0, [(base + "one").encode(), (base + "two").encode()]),
        ]
    )
    calls = []

    async def scan(self, cursor=0, **kwargs):
        calls.append(cursor)
        return next(pages)

    monkeypatch.setattr(Redis, "scan", scan)
    found = await app.source.list_schedules()
    assert {row.schedule_id for row in found} == {"one", "two"}
    assert len(found) == 2 and calls == [0, 1]


@pytest.mark.parametrize("decode", [False, True])
async def test_inventory_honors_custom_serializer_and_namespace(
    inventory, decode
):
    app, redis, prefix = inventory
    source = RedisSource(
        app.settings.url, prefix=prefix + ":json", serializer=JSONSerializer()
    )
    try:
        await source.add_schedule(record("custom", interval=10))
        found = await source.list_schedules()
        assert [row.schedule_id for row in found] == ["custom"]
        assert found[0].kwargs == {"name": "طلا"}
        assert await app.source.list_schedules() == []
    finally:
        await source.native.shutdown()


async def test_inventory_source_cleanup_runs_if_broker_stop_fails(
    inventory, monkeypatch
):
    app, redis, prefix = inventory
    close = AsyncMock()
    monkeypatch.setattr(
        app.broker,
        "shutdown",
        AsyncMock(side_effect=RuntimeError("stop failed")),
    )
    monkeypatch.setattr(app.source.native, "shutdown", close)
    try:
        with pytest.raises(RuntimeError, match="stop failed"):
            await app.stop()
        close.assert_awaited_once()
    finally:
        monkeypatch.undo()

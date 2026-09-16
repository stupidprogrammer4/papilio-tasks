import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

pytest.importorskip("taskiq_redis")
from redis.asyncio import Redis  # noqa: E402
from taskiq_redis import ListRedisScheduleSource  # noqa: E402

from papilio_tasks.apps.schedulers import Registrar, Scheduler  # noqa: E402
from papilio_tasks.infra.taskiq.sources.backends.redis import (  # noqa: E402
    RedisSource,
)
from papilio_tasks.infra.taskiq.sources.contracts.base import (  # noqa: E402
    MutableSourceContract,
)


def test_redis_source_imports_without_network_brokers():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq_aio_pika', 'dishka', 'papilio', 'faststream'}
        if fullname.split('.')[0] in blocked:
            raise ImportError(fullname)

sys.meta_path.insert(0, Block())
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource
source = RedisSource('redis://localhost', prefix='jobs', buffer_size=10)
broker_module = 'papilio_tasks.infra.taskiq.brokers.backends.redis'
assert broker_module not in sys.modules
assert not hasattr(source, 'get_schedule')
assert not hasattr(source, 'replace_schedule')
""",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="No test Redis")
async def test_redis_native_schedules_payload_deletion_and_namespace():
    url = os.environ["TEST_REDIS_URL"]
    prefix = "papilio-source-" + uuid4().hex
    source: MutableSourceContract = RedisSource(url, prefix=prefix)
    other = RedisSource(url, prefix=prefix + "-other")
    assert isinstance(source.native, ListRedisScheduleSource)

    class Report(Scheduler):
        async def run(self, value: int, *, factor: int):
            return value * factor

    Registrar().include(
        Report, name="report", labels={"queue_name": "reports", "priority": 3}
    )

    await source.native.startup()
    await other.native.startup()
    async with Redis.from_url(url) as redis:
        try:
            at = datetime.now(UTC) - timedelta(seconds=2)
            timed = await Report.at(source, at, 7, factor=2)
            cron = await Report.cron(source, "* * * * *", 8, factor=3)
            interval = await Report.every(
                source, timedelta(minutes=1), 9, factor=4
            )
            future = await Report.at(
                source, at + timedelta(days=1), 10, factor=5
            )
            created = [timed, cron, interval, future]
            assert len({item.schedule_id for item in created}) == 4
            schedules = await source.get_schedules()
            assert {item.schedule_id for item in schedules} == {
                timed.schedule_id,
                cron.schedule_id,
                interval.schedule_id,
            }
            for item in (timed, cron, interval):
                stored = next(
                    s for s in schedules if s.schedule_id == item.schedule_id
                )
                assert stored == item.task
                assert stored.labels == {
                    "queue_name": "reports",
                    "priority": "3",
                }
            assert await other.get_schedules() == []
            # Same schedule ID in another namespace must remain independent.
            await other.add_schedule(cron.task)
            await source.delete_schedule(cron.schedule_id)
            await source.delete_schedule(cron.schedule_id)
            assert (await other.get_schedules()) == [cron.task]
            assert cron.schedule_id not in {
                s.schedule_id for s in await source.get_schedules()
            }
            await timed.unschedule()
            await interval.unschedule()
            await future.unschedule()
            assert await source.get_schedules() == []
        finally:
            await source.native.shutdown()
            await other.native.shutdown()
            # Native list-source shutdown does not close its pool in 1.2.3.
            # Explicit test cleanup, not an adapter lifecycle override.
            await source.native._connection_pool.disconnect()
            await other.native._connection_pool.disconnect()
            keys = [key async for key in redis.scan_iter(match=prefix + "*")]
            if keys:
                await redis.delete(*keys)

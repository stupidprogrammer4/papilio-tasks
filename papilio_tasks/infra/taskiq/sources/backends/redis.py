from typing import Any

from redis.asyncio import Redis
from taskiq import ScheduledTask
from taskiq.compat import model_validate
from taskiq_redis import ListRedisScheduleSource

from ..base import MutableSource


class _InventorySource(ListRedisScheduleSource):
    async def shutdown(self) -> None:
        await self._connection_pool.disconnect()
        await super().shutdown()

    async def list_schedules(self) -> list[ScheduledTask]:
        """Read stored payloads without advancing the timing feed."""
        schedules: dict[str, ScheduledTask] = {}
        async with Redis(connection_pool=self._connection_pool) as redis:
            cursor = 0
            # Each SCAN cursor depends on the previous response.
            while True:
                cursor, keys = await redis.scan(
                    cursor, match=f"{self._prefix}:data:*", count=500
                )
                if keys:
                    payloads = await redis.mget(keys)
                    for payload in payloads:
                        if payload is not None:
                            if isinstance(payload, str):
                                encoder = self._connection_pool.get_encoder()
                                payload = payload.encode(encoder.encoding)
                            schedule = model_validate(
                                ScheduledTask, self._serializer.loadb(payload)
                            )
                            schedules[schedule.schedule_id] = schedule
                if cursor == 0:
                    break
        return list(schedules.values())


class RedisSource(MutableSource):
    """Shared schedules using Taskiq's native Redis list source.

    Options are passed to ListRedisScheduleSource. Add is not an upsert;
    this source does not offer lookup by ID or atomic replacement.
    """

    _native: _InventorySource

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(_InventorySource(url, **options))

    @property
    def native(self) -> ListRedisScheduleSource:
        return self._native

    async def list_schedules(self) -> list[ScheduledTask]:
        """List all stored schedules, including future one-shot jobs.

        Concurrent changes are not an atomic snapshot. Entries deleted while
        reading are omitted; malformed payloads and backend errors propagate.
        """
        schedules = await self._native.list_schedules()
        return schedules

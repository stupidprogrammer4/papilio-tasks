from typing import Any

from taskiq_redis import ListRedisScheduleSource

from ..base import MutableSource


class RedisSource(MutableSource):
    """Shared schedules using Taskiq's native Redis list source.

    Options are passed to ListRedisScheduleSource. Add is not an upsert;
    this source does not offer lookup by ID or atomic replacement.
    """

    _native: ListRedisScheduleSource

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(ListRedisScheduleSource(url, **options))

    @property
    def native(self) -> ListRedisScheduleSource:
        return self._native

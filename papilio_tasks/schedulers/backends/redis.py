from typing import ClassVar

from ..base import Scheduler
from ..infra.brokers.backends.redis import RedisStreamBroker
from ..infra.queues.redis import RedisQueue

__all__ = [
    "RedisQueue",
    "RedisScheduler",
    "RedisStreamBroker",
]


class RedisScheduler(Scheduler):
    _backend = "redis"
    queue: ClassVar[RedisQueue | None] = None

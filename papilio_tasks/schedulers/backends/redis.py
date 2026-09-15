from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.redis import RedisStreamBroker
from papilio_tasks.infra.taskiq.queues.redis import RedisQueue

from ..base import Scheduler

__all__ = [
    "RedisQueue",
    "RedisScheduler",
    "RedisStreamBroker",
]


class RedisScheduler(Scheduler):
    _backend = "redis"
    queue: ClassVar[RedisQueue | None] = None

from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.redis import RedisStreamBroker
from papilio_tasks.infra.taskiq.queues.redis import RedisQueue

from ..base import Direct, Projection

__all__ = ["RedisDirect", "RedisProjection", "RedisQueue", "RedisStreamBroker"]


class RedisProjection[T, D, R](Projection[T, D, R]):
    _backend = "redis"
    queue: ClassVar[RedisQueue | None] = None


class RedisDirect[T, R](RedisProjection[T, T, R], Direct[T, R]):
    """Redis projection using the shared identity transform."""

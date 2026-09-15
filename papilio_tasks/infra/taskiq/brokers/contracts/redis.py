from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_redis import RedisStreamBroker

from ...queues.redis import RedisQueue
from .base import BrokerContract


class RedisStreamContract(BrokerContract, Protocol):
    @property
    def native(self) -> RedisStreamBroker: ...

    def add_queue(self, queue: RedisQueue) -> RedisQueue:
        """Reuse matching destinations; do not change worker subscriptions."""
        ...

    def get_queue(self, name: str) -> RedisQueue: ...

    async def declare_queue(self, queue: RedisQueue) -> RedisQueue: ...

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        queue: str | None = None,
    ) -> AsyncTaskiqDecoratedTask: ...

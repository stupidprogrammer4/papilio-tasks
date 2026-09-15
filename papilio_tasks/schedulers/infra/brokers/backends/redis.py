from collections.abc import Awaitable, Callable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_redis import RedisStreamBroker as NativeRedisStreamBroker

from ...queues.redis import RedisQueue
from ..base import Broker
from ..contracts.redis import RedisStreamContract


class RedisStreamBroker(Broker, RedisStreamContract):
    """Configure Redis streams and consumer groups before native startup."""

    _native: NativeRedisStreamBroker

    @property
    def native(self) -> NativeRedisStreamBroker:
        return self._native

    def __init__(
        self,
        url: str,
        *,
        queue_name: str = "taskiq",
        additional_streams: dict[str, str | int] | None = None,
        **options: Any,
    ) -> None:
        if not queue_name:
            raise ValueError("Queue name cannot be empty")
        super().__init__(
            NativeRedisStreamBroker(
                url,
                queue_name=queue_name,
                additional_streams=additional_streams,
                **options,
            )
        )
        self._queues = {
            queue_name: RedisQueue(queue_name),
            **{
                name: RedisQueue(name, read_id)
                for name, read_id in self.native.additional_streams.items()
            },
        }

    def add_queue(self, queue: RedisQueue) -> RedisQueue:
        """Register a destination without changing worker subscriptions."""
        existing = self._queues.get(queue.name)
        if existing is not None:
            if existing != queue:
                raise ValueError(f"Conflicting queue: {queue.name}")
            return existing
        self._queues[queue.name] = queue
        return queue

    def get_queue(self, name: str) -> RedisQueue:
        return self._queues[name]

    async def declare_queue(self, queue: RedisQueue) -> RedisQueue:
        """Create a stream/group using the native broker's group settings."""
        async with Redis(connection_pool=self.native.connection_pool) as conn:
            try:
                await conn.xgroup_create(
                    queue.name,
                    self.native.consumer_group_name,
                    id=self.native.consumer_id,
                    mkstream=self.native.mkstream,
                )
            except ResponseError as error:
                if not str(error).startswith("BUSYGROUP"):
                    raise
        return queue

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        queue: str | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        target = self.get_queue(
            self.native.queue_name if queue is None else queue
        )
        metadata = dict(labels or {})
        if "queue_name" in metadata and metadata["queue_name"] != target.name:
            raise ValueError("Routing label conflicts with the selected queue")
        metadata["queue_name"] = target.name
        return super().register(task, name=name, labels=metadata)

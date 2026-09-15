from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from aio_pika.abc import AbstractChannel, AbstractQueue
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_aio_pika import AioPikaBroker, Queue

from ...queues.rabbit import declare_queue
from ..base import Broker
from ..contracts.rabbit import RabbitContract


class _Rabbit(AioPikaBroker):
    """Keep publish destinations while narrowing native read subscriptions."""

    consume_names: tuple[str, ...] | None = None

    async def _declare_queues(
        self, channel: AbstractChannel
    ) -> list[tuple[AbstractQueue, dict[str, Any]]]:
        queues = await super()._declare_queues(channel)
        if channel is self.read_channel and self.consume_names is not None:
            return [q for q in queues if q[0].name in self.consume_names]
        return queues

    async def declare_queue(self, queue: Queue) -> AbstractQueue:
        if self.write_channel is None:
            raise RuntimeError("Start the broker before declaring a queue")
        return await declare_queue(
            self.write_channel,
            queue,
            exchange=self._exchange.name,
            dead_letter=(
                self._dead_letter_queue.routing_key
                or self._dead_letter_queue.name
            ),
            delayed_exchange=(
                self._delayed_message_exchange.name
                if self._delayed_message_exchange_plugin
                else None
            ),
        )


class RabbitBroker(Broker, RabbitContract):
    """Configure RabbitMQ task queues before native startup."""

    _native: _Rabbit

    @property
    def native(self) -> _Rabbit:
        return self._native

    def __init__(
        self,
        url: str,
        *,
        queues: Sequence[Queue] | None = None,
        label_for_routing: str = "queue_name",
        **options: Any,
    ) -> None:
        if queues is None:
            queues = [Queue()]
        if not queues:
            raise ValueError("Configure at least one default queue")
        super().__init__(
            _Rabbit(
                url,
                task_queues=[],
                label_for_routing=label_for_routing,
                **options,
            )
        )
        self._queues: dict[str, Queue] = {}
        self._routing_label = label_for_routing
        for queue in queues:
            self.add_queue(queue)
        self._default_queue = queues[0].name

    def add_queue(self, queue: Queue) -> Queue:
        """Add or reuse queue config; startup performs declaration I/O."""
        existing = self._queues.get(queue.name)
        if existing is not None:
            if existing != queue:
                raise ValueError(f"Conflicting queue: {queue.name}")
            return existing
        if self.native.write_channel is not None:
            raise RuntimeError("Add queues before broker startup")
        if not queue.name:
            raise ValueError("Queue name cannot be empty")
        self.native.with_queue(queue)
        self._queues[queue.name] = queue
        return queue

    def get_queue(self, name: str) -> Queue:
        return self._queues[name]

    def consume(self, *names: str) -> None:
        """Select worker queues before startup; preserve destinations."""
        if self.native.write_channel is not None:
            raise RuntimeError("Select queues before broker startup")
        if not names:
            raise ValueError("Select at least one queue")
        for name in names:
            self.get_queue(name)
        self.native.consume_names = names

    async def declare_queue(self, queue: Queue) -> AbstractQueue:
        """Declare/bind on the server; this does not change subscriptions."""
        return await self.native.declare_queue(queue)

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        queue: str | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        target = self.get_queue(
            self._default_queue if queue is None else queue
        )
        routing_key = target.routing_key or target.name
        metadata = dict(labels or {})
        if (
            self._routing_label in metadata
            and metadata[self._routing_label] != routing_key
        ):
            raise ValueError("Routing label conflicts with the selected queue")
        metadata[self._routing_label] = routing_key
        return super().register(task, name=name, labels=metadata)

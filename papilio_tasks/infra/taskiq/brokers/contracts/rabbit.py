from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from aio_pika.abc import AbstractQueue
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_aio_pika import AioPikaBroker, Queue

from .base import BrokerContract


class RabbitContract(BrokerContract, Protocol):
    @property
    def native(self) -> AioPikaBroker: ...

    def add_queue(self, queue: Queue) -> Queue:
        """Reuse identical config; reject conflicting settings."""
        ...

    def get_queue(self, name: str) -> Queue: ...

    def consume(self, *names: str) -> None: ...

    async def declare_queue(self, queue: Queue) -> AbstractQueue: ...

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        queue: str | None = None,
    ) -> AsyncTaskiqDecoratedTask: ...

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_aio_kafka import AioKafkaBroker

from .base import BrokerContract


class KafkaContract(BrokerContract, Protocol):
    @property
    def native(self) -> AioKafkaBroker: ...

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        topic: str | None = None,
    ) -> AsyncTaskiqDecoratedTask: ...

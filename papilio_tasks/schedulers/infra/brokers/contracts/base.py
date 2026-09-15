from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask


class BrokerContract(Protocol):
    @property
    def native(self) -> AsyncBroker: ...

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
    ) -> AsyncTaskiqDecoratedTask: ...

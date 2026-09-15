from collections.abc import Awaitable, Callable
from inspect import iscoroutinefunction
from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

from .contracts.base import BrokerContract


class Broker(BrokerContract):
    """Shared callable registration on a native Taskiq broker."""

    def __init__(self, native: AsyncBroker) -> None:
        self._native = native

    @property
    def native(self) -> AsyncBroker:
        return self._native

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        if not name:
            raise ValueError("Task name cannot be empty")
        if not iscoroutinefunction(task):
            raise TypeError("Expected an async callable")
        if self.native.find_task(name) is not None:
            raise ValueError(f"Task name already registered: {name}")
        registered = self.native.register_task(task, task_name=name)
        # Keep metadata keys separate from native registration arguments too.
        registered.labels.update(labels or {})
        return registered

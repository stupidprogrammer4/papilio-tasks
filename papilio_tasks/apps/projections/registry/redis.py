from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.redis import (
    RedisStreamContract,
)
from papilio_tasks.tools.hooks.projection import Hooks

from ..backends.redis import RedisProjection, RedisQueue
from ..base import Projection
from .base import Registrar


class RedisRegistrar(Registrar):
    _backend = "redis"
    broker: RedisStreamContract

    def __init__(
        self,
        broker: RedisStreamContract,
        *,
        hooks: type[Hooks[object, object, object]] | None = None,
    ) -> None:
        super().__init__(broker, hooks=hooks)

    def include[T, D, R](
        self,
        cls: type[Projection[T, D, R]],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
        queue: RedisQueue | None = None,
    ) -> AsyncTaskiqDecoratedTask[Any, R]:
        if not isinstance(cls, type) or not issubclass(cls, RedisProjection):
            raise TypeError("Include a RedisProjection class")
        execute, name, labels = self._prepare(cls, name, labels)
        selected = cls.queue if queue is None else queue
        if selected is not None:
            if not isinstance(selected, RedisQueue):
                raise TypeError("Expected RedisQueue")
            self.broker.add_queue(selected)
        task = self.broker.register(
            execute,
            name=name,
            labels=labels,
            queue=selected.name if selected is not None else None,
        )
        bindings.add(cls, task)
        return task

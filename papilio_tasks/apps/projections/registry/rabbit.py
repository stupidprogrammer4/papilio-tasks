from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.rabbit import RabbitContract
from papilio_tasks.tools.hooks.projection import Hooks

from ..backends.rabbit import RabbitProjection, RabbitQueue
from ..base import Projection
from .base import Registrar


class RabbitRegistrar(Registrar):
    _backend = "rabbit"
    broker: RabbitContract

    def __init__(
        self,
        broker: RabbitContract,
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
        queue: RabbitQueue | None = None,
    ) -> AsyncTaskiqDecoratedTask[Any, R]:
        if not isinstance(cls, type) or not issubclass(cls, RabbitProjection):
            raise TypeError("Include a RabbitProjection class")
        execute, name, labels = self._prepare(cls, name, labels)
        selected = cls.queue if queue is None else queue
        if selected is not None:
            if not isinstance(selected, RabbitQueue):
                raise TypeError("Expected RabbitQueue")
            self.broker.add_queue(selected)
        task = self.broker.register(
            execute,
            name=name,
            labels=labels,
            queue=selected.name if selected is not None else None,
        )
        bindings.add(cls, task)
        return task

from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.kafka import KafkaContract
from papilio_tasks.tools.hooks.projection import Hooks

from ..backends.kafka import KafkaProjection
from ..base import Projection
from .base import Registrar


class KafkaRegistrar(Registrar):
    _backend = "kafka"
    broker: KafkaContract

    def __init__(
        self,
        broker: KafkaContract,
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
        topic: str | None = None,
    ) -> AsyncTaskiqDecoratedTask[Any, R]:
        if not isinstance(cls, type) or not issubclass(cls, KafkaProjection):
            raise TypeError("Include a KafkaProjection class")
        execute, name, labels = self._prepare(cls, name, labels)
        task = self.broker.register(
            execute,
            name=name,
            labels=labels,
            topic=cls.topic if topic is None else topic,
        )
        bindings.add(cls, task)
        return task

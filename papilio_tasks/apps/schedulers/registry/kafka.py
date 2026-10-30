from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.kafka import KafkaContract

from ..backends.kafka import KafkaScheduler
from .base import Registrar


class KafkaRegistrar(Registrar[KafkaScheduler]):
    _backend = "kafka"
    broker: KafkaContract

    def __init__(self, broker: KafkaContract) -> None:
        super().__init__(broker)

    def include(
        self,
        cls: type[KafkaScheduler],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
        topic: str | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        execute, name = self._prepare(cls, name)
        labels = self._labels(cls, labels)
        task = self.broker.register(
            execute,
            name=name,
            labels=labels,
            topic=cls.topic if topic is None else topic,
        )
        bindings.add(cls, task)
        return task

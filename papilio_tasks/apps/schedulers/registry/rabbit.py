from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.rabbit import RabbitContract
from papilio_tasks.infra.taskiq.queues.rabbit import RabbitQueue
from papilio_tasks.infra.taskiq.retry import retry_labels

from ..backends.rabbit import RabbitScheduler
from .base import Registrar


class RabbitRegistrar(Registrar[RabbitScheduler]):
    _backend = "rabbit"
    broker: RabbitContract

    def __init__(self, broker: RabbitContract) -> None:
        super().__init__(broker)

    def include(
        self,
        cls: type[RabbitScheduler],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
        queue: RabbitQueue | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        execute, name = self._prepare(cls, name)
        labels = retry_labels(self.broker.native, cls.retry, labels)
        selected = cls.queue if queue is None else queue
        if selected is not None:
            self.broker.add_queue(selected)
        task = self.broker.register(
            execute,
            name=name,
            labels=labels,
            queue=selected.name if selected is not None else None,
        )
        bindings.add(cls, task)
        return task

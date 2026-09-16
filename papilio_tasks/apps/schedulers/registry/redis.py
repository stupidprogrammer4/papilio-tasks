from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.contracts.redis import (
    RedisStreamContract,
)
from papilio_tasks.infra.taskiq.queues.redis import RedisQueue
from papilio_tasks.infra.taskiq.retry import retry_labels

from ..backends.redis import RedisScheduler
from .base import Registrar


class RedisRegistrar(Registrar[RedisScheduler]):
    _backend = "redis"
    broker: RedisStreamContract

    def __init__(self, broker: RedisStreamContract) -> None:
        super().__init__(broker)

    def include(
        self,
        cls: type[RedisScheduler],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
        queue: RedisQueue | None = None,
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

from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.infra.taskiq.queues.rabbit import RabbitQueue

from ..base import Scheduler

__all__ = ["RabbitBroker", "RabbitQueue", "RabbitScheduler"]


class RabbitScheduler(Scheduler):
    _backend = "rabbit"
    queue: ClassVar[RabbitQueue | None] = None

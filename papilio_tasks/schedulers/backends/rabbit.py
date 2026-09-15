from typing import ClassVar

from ..base import Scheduler
from ..infra.brokers.backends.rabbit import RabbitBroker
from ..infra.queues.rabbit import RabbitQueue

__all__ = ["RabbitBroker", "RabbitQueue", "RabbitScheduler"]


class RabbitScheduler(Scheduler):
    _backend = "rabbit"
    queue: ClassVar[RabbitQueue | None] = None

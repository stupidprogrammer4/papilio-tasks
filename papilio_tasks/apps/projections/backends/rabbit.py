from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.infra.taskiq.queues.rabbit import RabbitQueue

from ..base import Direct, Projection

__all__ = ["RabbitBroker", "RabbitDirect", "RabbitProjection", "RabbitQueue"]


class RabbitProjection[T, D, R](Projection[T, D, R]):
    _backend = "rabbit"
    queue: ClassVar[RabbitQueue | None] = None


class RabbitDirect[T, R](RabbitProjection[T, T, R], Direct[T, R]):
    """Rabbit projection using the shared identity transform."""

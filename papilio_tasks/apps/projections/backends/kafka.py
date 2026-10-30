from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.kafka import KafkaBroker

from ..base import Direct, Projection

__all__ = ["KafkaBroker", "KafkaDirect", "KafkaProjection"]


class KafkaProjection[T, D, R](Projection[T, D, R]):
    _backend = "kafka"
    topic: ClassVar[str | None] = None


class KafkaDirect[T, R](KafkaProjection[T, T, R], Direct[T, R]):
    """Kafka projection using the shared identity transform."""

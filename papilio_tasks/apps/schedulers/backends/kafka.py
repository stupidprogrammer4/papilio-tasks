from typing import ClassVar

from papilio_tasks.infra.taskiq.brokers.backends.kafka import KafkaBroker

from ..base import Scheduler

__all__ = ["KafkaBroker", "KafkaScheduler"]


class KafkaScheduler(Scheduler):
    _backend = "kafka"
    topic: ClassVar[str | None] = None

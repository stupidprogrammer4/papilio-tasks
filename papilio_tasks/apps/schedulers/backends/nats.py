from papilio_tasks.infra.taskiq.brokers.backends.nats import JetStreamBroker

from ..base import Scheduler

__all__ = ["JetStreamBroker", "NatsScheduler"]


class NatsScheduler(Scheduler):
    """A job using its native broker's JetStream subject and durable."""

    _backend = "nats"

from papilio_tasks.infra.taskiq.brokers.backends.nats import JetStreamBroker

from ..base import Direct, Projection

__all__ = ["JetStreamBroker", "NatsDirect", "NatsProjection"]


class NatsProjection[T, D, R](Projection[T, D, R]):
    _backend = "nats"


class NatsDirect[T, R](NatsProjection[T, T, R], Direct[T, R]):
    """JetStream projection using the shared identity transform."""

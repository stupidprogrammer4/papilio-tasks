from taskiq_nats import PullBasedJetStreamBroker

from ..base import Broker
from ..contracts.nats import JetStreamContract


class JetStreamBroker(Broker, JetStreamContract):
    """Register tasks on a caller-configured native pull JetStream broker.

    The native broker owns its subject, stream, durable consumer and lifecycle.
    Registration does not open connections or change subscriptions.
    """

    _native: PullBasedJetStreamBroker

    def __init__(self, native: PullBasedJetStreamBroker) -> None:
        if not isinstance(native, PullBasedJetStreamBroker):
            raise TypeError("Expected a native PullBasedJetStreamBroker")
        super().__init__(native)

    @property
    def native(self) -> PullBasedJetStreamBroker:
        return self._native

from typing import Protocol

from taskiq_nats import PullBasedJetStreamBroker

from .base import BrokerContract


class JetStreamContract(BrokerContract, Protocol):
    @property
    def native(self) -> PullBasedJetStreamBroker: ...

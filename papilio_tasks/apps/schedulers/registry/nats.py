from papilio_tasks.infra.taskiq.brokers.contracts.nats import (
    JetStreamContract,
)

from ..backends.nats import NatsScheduler
from .base import Registrar


class NatsRegistrar(Registrar[NatsScheduler]):
    _backend = "nats"
    broker: JetStreamContract

    def __init__(self, broker: JetStreamContract) -> None:
        super().__init__(broker)

from papilio_tasks.infra.taskiq.brokers.contracts.nats import (
    JetStreamContract,
)
from papilio_tasks.tools.hooks.projection import Hooks

from .base import Registrar


class NatsRegistrar(Registrar):
    _backend = "nats"
    broker: JetStreamContract

    def __init__(
        self,
        broker: JetStreamContract,
        *,
        hooks: type[Hooks[object, object, object]] | None = None,
    ) -> None:
        super().__init__(broker, hooks=hooks)

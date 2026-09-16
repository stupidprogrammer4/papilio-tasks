from typing import Any, Protocol

from faststream.nats import NatsBroker, PubAck
from faststream.nats.message import NatsMessage
from faststream.nats.publisher.usecase import LogicPublisher
from faststream.nats.subscriber.usecases.basic import LogicSubscriber
from nats.aio.client import Client

from .base import BrokerContract


class NatsContract(BrokerContract[Client], Protocol):
    @property
    def native(self) -> NatsBroker: ...

    def subscriber(
        self, subject: str = "", **options: Any
    ) -> LogicSubscriber[Any]: ...

    def publisher(self, subject: str, **options: Any) -> LogicPublisher: ...

    async def publish(
        self, message: Any, subject: str, **options: Any
    ) -> PubAck | None: ...

    async def request(
        self, message: Any, subject: str, **options: Any
    ) -> NatsMessage: ...

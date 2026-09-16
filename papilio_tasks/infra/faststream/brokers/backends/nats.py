from typing import Any

from faststream.nats import NatsBroker as NativeNatsBroker
from faststream.nats import PubAck
from faststream.nats.message import NatsMessage
from faststream.nats.publisher.usecase import LogicPublisher
from faststream.nats.subscriber.usecases.basic import LogicSubscriber
from nats.aio.client import Client

from ..base import Broker
from ..contracts.nats import NatsContract


class NatsBroker(Broker[Client], NatsContract):
    """Delegate NATS operations to one caller-configured native broker."""

    _native: NativeNatsBroker

    def __init__(self, native: NativeNatsBroker) -> None:
        super().__init__(native)

    @property
    def native(self) -> NativeNatsBroker:
        return self._native

    def subscriber(
        self, subject: str = "", **options: Any
    ) -> LogicSubscriber[Any]:
        return self.native.subscriber(subject, **options)

    def publisher(self, subject: str, **options: Any) -> LogicPublisher:
        return self.native.publisher(subject, **options)

    async def publish(
        self, message: Any, subject: str, **options: Any
    ) -> PubAck | None:
        return await self.native.publish(message, subject, **options)

    async def request(
        self, message: Any, subject: str, **options: Any
    ) -> NatsMessage:
        return await self.native.request(message, subject, **options)

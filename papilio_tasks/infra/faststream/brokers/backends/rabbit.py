from typing import Any

from aio_pika import RobustConnection, RobustExchange, RobustQueue
from aiormq.abc import ConfirmationFrameType
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from faststream.rabbit import RabbitBroker as NativeRabbitBroker
from faststream.rabbit.publisher import RabbitPublisher
from faststream.rabbit.subscriber import RabbitSubscriber
from faststream.rabbit.types import AioPikaSendableMessage

from ..base import Broker
from ..contracts.rabbit import RabbitContract

__all__ = ["ExchangeType", "RabbitBroker", "RabbitExchange", "RabbitQueue"]


class RabbitBroker(Broker[RobustConnection], RabbitContract):
    """Use native Rabbit operations and settings without owning app policy."""

    _native: NativeRabbitBroker

    def __init__(self, native: NativeRabbitBroker) -> None:
        super().__init__(native)

    @property
    def native(self) -> NativeRabbitBroker:
        return self._native

    def subscriber(
        self,
        queue: RabbitQueue | str,
        exchange: RabbitExchange | str | None = None,
        **options: Any,
    ) -> RabbitSubscriber:
        return self.native.subscriber(queue, exchange, **options)

    def publisher(
        self,
        queue: RabbitQueue | str = "",
        exchange: RabbitExchange | str | None = None,
        *,
        routing_key: str = "",
        **options: Any,
    ) -> RabbitPublisher:
        return self.native.publisher(
            queue, exchange, routing_key=routing_key, **options
        )

    async def publish(
        self,
        message: AioPikaSendableMessage = None,
        queue: RabbitQueue | str = "",
        exchange: RabbitExchange | str | None = None,
        *,
        routing_key: str = "",
        **options: Any,
    ) -> ConfirmationFrameType | None:
        return await self.native.publish(
            message, queue, exchange, routing_key=routing_key, **options
        )

    async def declare_queue(self, queue: RabbitQueue) -> RobustQueue:
        return await self.native.declare_queue(queue)

    async def declare_exchange(
        self, exchange: RabbitExchange
    ) -> RobustExchange:
        return await self.native.declare_exchange(exchange)

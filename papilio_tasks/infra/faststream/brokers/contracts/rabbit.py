from typing import Any, Protocol

from aio_pika import RobustConnection, RobustExchange, RobustQueue
from aiormq.abc import ConfirmationFrameType
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.publisher import RabbitPublisher
from faststream.rabbit.subscriber import RabbitSubscriber
from faststream.rabbit.types import AioPikaSendableMessage

from .base import BrokerContract


class RabbitContract(BrokerContract[RobustConnection], Protocol):
    @property
    def native(self) -> RabbitBroker: ...

    def subscriber(
        self,
        queue: RabbitQueue | str,
        exchange: RabbitExchange | str | None = None,
        **options: Any,
    ) -> RabbitSubscriber: ...

    def publisher(
        self,
        queue: RabbitQueue | str = "",
        exchange: RabbitExchange | str | None = None,
        *,
        routing_key: str = "",
        **options: Any,
    ) -> RabbitPublisher: ...

    async def publish(
        self,
        message: AioPikaSendableMessage = None,
        queue: RabbitQueue | str = "",
        exchange: RabbitExchange | str | None = None,
        *,
        routing_key: str = "",
        **options: Any,
    ) -> ConfirmationFrameType | None: ...

    async def declare_queue(self, queue: RabbitQueue) -> RobustQueue: ...

    async def declare_exchange(
        self, exchange: RabbitExchange
    ) -> RobustExchange: ...

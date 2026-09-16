from typing import ClassVar

from faststream.rabbit import ExchangeType, RabbitExchange

from .base import Publisher

__all__ = ["ExchangeType", "RabbitExchange", "RabbitPublisher"]


class RabbitPublisher[T](Publisher[T]):
    exchange: ClassVar[RabbitExchange]
    routing_key: ClassVar[str]

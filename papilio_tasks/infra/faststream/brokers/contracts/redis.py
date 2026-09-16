from typing import Any, Protocol

from faststream.redis import PubSub, RedisBroker
from faststream.redis.publisher.usecase import LogicPublisher
from faststream.redis.subscriber.usecases.basic import LogicSubscriber
from redis.asyncio import Redis

from .base import BrokerContract


class RedisContract(BrokerContract[Redis], Protocol):
    @property
    def native(self) -> RedisBroker: ...

    def subscriber(
        self, channel: PubSub | str | None = None, **options: Any
    ) -> LogicSubscriber: ...

    def publisher(
        self, channel: PubSub | str | None = None, **options: Any
    ) -> LogicPublisher: ...

    async def publish(
        self, message: Any = None, channel: str | None = None, **options: Any
    ) -> int | bytes: ...

    async def publish_batch(
        self, *messages: Any, list: str, **options: Any
    ) -> int: ...

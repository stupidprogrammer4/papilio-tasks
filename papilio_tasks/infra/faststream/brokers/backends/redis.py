from typing import Any

from faststream.redis import PubSub
from faststream.redis import RedisBroker as NativeRedisBroker
from faststream.redis.publisher.usecase import LogicPublisher
from faststream.redis.subscriber.usecases.basic import LogicSubscriber
from redis.asyncio import Redis

from ..base import Broker
from ..contracts.redis import RedisContract


class RedisBroker(Broker[Redis], RedisContract):
    """Delegate Redis operations to one caller-configured native broker."""

    _native: NativeRedisBroker

    def __init__(self, native: NativeRedisBroker) -> None:
        super().__init__(native)

    @property
    def native(self) -> NativeRedisBroker:
        return self._native

    def subscriber(
        self, channel: PubSub | str | None = None, **options: Any
    ) -> LogicSubscriber:
        return self.native.subscriber(channel, **options)

    def publisher(
        self, channel: PubSub | str | None = None, **options: Any
    ) -> LogicPublisher:
        return self.native.publisher(channel, **options)

    async def publish(
        self, message: Any = None, channel: str | None = None, **options: Any
    ) -> int | bytes:
        return await self.native.publish(message, channel, **options)

    async def publish_batch(
        self, *messages: Any, list: str, **options: Any
    ) -> int:
        return await self.native.publish_batch(*messages, list=list, **options)

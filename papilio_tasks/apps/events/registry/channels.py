from copy import deepcopy
from typing import Any

from faststream.redis import PubSub

from papilio_tasks.infra.faststream.brokers.contracts.redis import (
    RedisContract,
)

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.channels import ChannelPublisher
from ..subscribers.base import Subscriber
from ..subscribers.channels import ChannelSubscriber
from .base import Registrar


class ChannelRegistrar(Registrar):
    broker: RedisContract

    def __init__(self, broker: RedisContract) -> None:
        super().__init__(broker)

    def _channel(self, cls: type[Publisher[Any]]) -> str:
        if not isinstance(cls, type) or not issubclass(cls, ChannelPublisher):
            raise TypeError("Register a ChannelPublisher class")
        if not isinstance(cls.channel, str) or not cls.channel:
            raise ValueError("Publisher requires a named Redis channel")
        return cls.channel

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        channel = self._channel(cls)
        bindings.check(cls)
        sender = self.broker.publisher(channel, **options)
        bindings.add(
            cls, self, self.sender(cls, sender.publish, {"channel": channel})
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        channel: PubSub | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, ChannelSubscriber):
            raise TypeError("Register a ChannelSubscriber class")
        name = self._channel(cls.publisher)
        selected = cls.channel if channel is None else channel
        if selected is None:
            selected = PubSub(name)
        if not isinstance(selected, PubSub):
            raise TypeError("Expected PubSub or None")
        if not selected.name:
            raise ValueError("Subscriber requires a named Redis channel")
        handler = self.handler(cls)
        self.broker.subscriber(deepcopy(selected), no_reply=True, **options)(
            handler
        )
        self._subscribers.add(cls)

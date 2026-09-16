from copy import deepcopy
from typing import Any

from faststream import AckPolicy
from faststream.redis import StreamSub

from papilio_tasks.infra.faststream.brokers.contracts.redis import (
    RedisContract,
)

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.streams import StreamPublisher
from ..subscribers.base import Subscriber
from ..subscribers.streams import StreamSubscriber
from .base import Registrar


class StreamRegistrar(Registrar):
    broker: RedisContract

    def __init__(self, broker: RedisContract) -> None:
        super().__init__(broker)

    def _stream(self, cls: type[Publisher[Any]]) -> str:
        if not isinstance(cls, type) or not issubclass(cls, StreamPublisher):
            raise TypeError("Register a StreamPublisher class")
        if not isinstance(cls.stream, str) or not cls.stream:
            raise ValueError("Publisher requires a named Redis stream")
        return cls.stream

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        stream = self._stream(cls)
        bindings.check(cls)
        sender = self.broker.publisher(stream=stream, **options)
        bindings.add(
            cls, self, self.sender(cls, sender.publish, {"stream": stream})
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        stream: StreamSub | None = None,
        ack_policy: AckPolicy | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, StreamSubscriber):
            raise TypeError("Register a StreamSubscriber class")
        name = self._stream(cls.publisher)
        selected = cls.stream if stream is None else stream
        if not isinstance(selected, StreamSub):
            raise TypeError("Expected StreamSub")
        if selected.name != name:
            raise ValueError(
                "Subscriber stream conflicts with publisher route"
            )
        policy = cls.ack_policy if ack_policy is None else ack_policy
        if policy is not None:
            if not isinstance(policy, AckPolicy):
                raise TypeError("Expected AckPolicy or None")
            options["ack_policy"] = policy
        handler = self.handler(cls)
        self.broker.subscriber(
            stream=deepcopy(selected), no_reply=True, **options
        )(handler)
        self._subscribers.add(cls)

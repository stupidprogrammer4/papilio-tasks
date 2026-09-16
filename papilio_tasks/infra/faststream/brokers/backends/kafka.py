from asyncio import Future
from collections.abc import Callable
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import RecordMetadata
from faststream.kafka import KafkaBroker as NativeKafkaBroker
from faststream.kafka.publisher import BatchPublisher, DefaultPublisher
from faststream.kafka.subscriber.usecase import LogicSubscriber

from ..base import Broker
from ..contracts.kafka import KafkaContract


class KafkaBroker(Broker[Callable[..., AIOKafkaConsumer]], KafkaContract):
    """Delegate Kafka operations to one caller-configured native broker."""

    _native: NativeKafkaBroker

    def __init__(self, native: NativeKafkaBroker) -> None:
        super().__init__(native)

    @property
    def native(self) -> NativeKafkaBroker:
        return self._native

    def subscriber(self, *topics: str, **options: Any) -> LogicSubscriber[Any]:
        return self.native.subscriber(*topics, **options)

    def publisher(
        self, topic: str, **options: Any
    ) -> DefaultPublisher | BatchPublisher:
        return self.native.publisher(topic, **options)

    async def publish(
        self, message: Any, topic: str = "", **options: Any
    ) -> RecordMetadata | Future[RecordMetadata]:
        return await self.native.publish(message, topic, **options)

    async def publish_batch(
        self, *messages: Any, topic: str = "", **options: Any
    ) -> RecordMetadata | Future[RecordMetadata]:
        return await self.native.publish_batch(
            *messages, topic=topic, **options
        )

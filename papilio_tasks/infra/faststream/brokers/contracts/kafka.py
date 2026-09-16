from asyncio import Future
from collections.abc import Callable
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import RecordMetadata
from faststream.kafka import KafkaBroker
from faststream.kafka.publisher import BatchPublisher, DefaultPublisher
from faststream.kafka.subscriber.usecase import LogicSubscriber

from .base import BrokerContract


class KafkaContract(BrokerContract[Callable[..., AIOKafkaConsumer]], Protocol):
    @property
    def native(self) -> KafkaBroker: ...

    def subscriber(
        self, *topics: str, **options: Any
    ) -> LogicSubscriber[Any]: ...

    def publisher(
        self, topic: str, **options: Any
    ) -> DefaultPublisher | BatchPublisher: ...

    async def publish(
        self, message: Any, topic: str = "", **options: Any
    ) -> RecordMetadata | Future[RecordMetadata]: ...

    async def publish_batch(
        self, *messages: Any, topic: str = "", **options: Any
    ) -> RecordMetadata | Future[RecordMetadata]: ...

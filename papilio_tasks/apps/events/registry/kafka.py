from typing import Any

from faststream import AckPolicy

from papilio_tasks.infra.faststream.brokers.contracts.kafka import (
    KafkaContract,
)

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.kafka import KafkaPublisher
from ..subscribers.base import Subscriber
from ..subscribers.kafka import KafkaSubscriber
from .base import Registrar


class KafkaRegistrar(Registrar):
    broker: KafkaContract

    def __init__(self, broker: KafkaContract) -> None:
        super().__init__(broker)

    def _topic(self, cls: type[Publisher[Any]]) -> str:
        if not isinstance(cls, type) or not issubclass(cls, KafkaPublisher):
            raise TypeError("Register a KafkaPublisher class")
        topic = cls.topic
        if not isinstance(topic, str) or not topic:
            raise ValueError("Publisher requires a named Kafka topic")
        return topic

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        topic = self._topic(cls)
        bindings.check(cls)
        sender = self.broker.publisher(topic, **options)
        bindings.add(
            cls, self, self.sender(cls, sender.publish, {"topic": topic})
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        ack_policy: AckPolicy | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, KafkaSubscriber):
            raise TypeError("Register a KafkaSubscriber class")
        topic = self._topic(cls.publisher)
        if not isinstance(cls.group_id, str) or not cls.group_id:
            raise ValueError("Subscriber requires a named Kafka group_id")
        policy = cls.ack_policy if ack_policy is None else ack_policy
        if policy is not None:
            if not isinstance(policy, AckPolicy):
                raise TypeError("Expected AckPolicy or None")
            options["ack_policy"] = policy
        handler = self.handler(cls)
        self.broker.subscriber(
            topic, group_id=cls.group_id, no_reply=True, **options
        )(handler)
        self._subscribers.add(cls)

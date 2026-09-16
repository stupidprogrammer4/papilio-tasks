from typing import Any

from papilio_tasks.infra.faststream.brokers.contracts.nats import NatsContract

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.nats import NatsPublisher
from ..subscribers.base import Subscriber
from ..subscribers.nats import NatsSubscriber
from .base import Registrar


class NatsRegistrar(Registrar):
    broker: NatsContract

    def __init__(self, broker: NatsContract) -> None:
        super().__init__(broker)

    def _subject(self, cls: type[Publisher[Any]]) -> str:
        if not isinstance(cls, type) or not issubclass(cls, NatsPublisher):
            raise TypeError("Register a NatsPublisher class")
        if not isinstance(cls.subject, str) or not cls.subject:
            raise ValueError("Publisher requires a named NATS subject")
        return cls.subject

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        subject = self._subject(cls)
        bindings.check(cls)
        sender = self.broker.publisher(subject, **options)
        bindings.add(
            cls, self, self.sender(cls, sender.publish, {"subject": subject})
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        subject: str | None = None,
        queue: str | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, NatsSubscriber):
            raise TypeError("Register a NatsSubscriber class")
        name = self._subject(cls.publisher)
        selected = cls.subject if subject is None else subject
        if selected is None:
            selected = name
        if not isinstance(selected, str) or not selected:
            raise ValueError("Subscriber requires a named NATS subject")
        group = cls.queue if queue is None else queue
        if not isinstance(group, str):
            raise TypeError("Expected a NATS queue group string")
        handler = self.handler(cls)
        self.broker.subscriber(
            selected, queue=group, no_reply=True, **options
        )(handler)
        self._subscribers.add(cls)

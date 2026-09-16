from copy import deepcopy
from typing import Any

from faststream import AckPolicy
from faststream.nats import JStream, PullSub
from nats.js.api import ConsumerConfig

from papilio_tasks.infra.faststream.brokers.contracts.nats import NatsContract

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.jetstream import JetPublisher
from ..subscribers.base import Subscriber
from ..subscribers.jetstream import JetSubscriber
from .base import Registrar


class JetRegistrar(Registrar):
    broker: NatsContract

    def __init__(self, broker: NatsContract) -> None:
        super().__init__(broker)

    def _route(self, cls: type[Publisher[Any]]) -> tuple[str, JStream]:
        if not isinstance(cls, type) or not issubclass(cls, JetPublisher):
            raise TypeError("Register a JetPublisher class")
        if not isinstance(cls.subject, str) or not cls.subject:
            raise ValueError("Publisher requires a named NATS subject")
        if not isinstance(cls.stream, JStream):
            raise TypeError("Expected JStream")
        if not cls.stream.name:
            raise ValueError("Publisher requires a named JetStream stream")
        return cls.subject, cls.stream

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        subject, stream = self._route(cls)
        bindings.check(cls)
        sender = self.broker.publisher(
            subject, stream=deepcopy(stream), **options
        )
        bindings.add(
            cls,
            self,
            self.sender(
                cls,
                sender.publish,
                {"subject": subject, "stream": stream.name},
            ),
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        subject: str | None = None,
        queue: str | None = None,
        durable: str | None = None,
        pull_sub: bool | PullSub | None = None,
        config: ConsumerConfig | None = None,
        ack_policy: AckPolicy | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, JetSubscriber):
            raise TypeError("Register a JetSubscriber class")
        name, stream = self._route(cls.publisher)
        selected = cls.subject if subject is None else subject
        if selected is None:
            selected = name
        if not isinstance(selected, str) or not selected:
            raise ValueError("Subscriber requires a named NATS subject")
        group = cls.queue if queue is None else queue
        if not isinstance(group, str):
            raise TypeError("Expected a NATS queue group string")
        consumer = cls.durable if durable is None else durable
        if consumer is not None and not isinstance(consumer, str):
            raise TypeError("Expected a durable name or None")
        pull = cls.pull_sub if pull_sub is None else pull_sub
        if not isinstance(pull, (bool, PullSub)):
            raise TypeError("Expected bool or PullSub")
        settings = cls.config if config is None else config
        if settings is not None and not isinstance(settings, ConsumerConfig):
            raise TypeError("Expected ConsumerConfig or None")
        policy = cls.ack_policy if ack_policy is None else ack_policy
        if policy is not None:
            if not isinstance(policy, AckPolicy):
                raise TypeError("Expected AckPolicy or None")
            options["ack_policy"] = policy
        handler = self.handler(cls)
        self.broker.subscriber(
            selected,
            stream=deepcopy(stream),
            queue=group,
            durable=consumer,
            pull_sub=deepcopy(pull),
            config=deepcopy(settings),
            no_reply=True,
            **options,
        )(handler)
        self._subscribers.add(cls)

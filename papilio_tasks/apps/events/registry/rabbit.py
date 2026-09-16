from copy import deepcopy
from typing import Any, cast

from faststream import AckPolicy
from faststream.rabbit import RabbitExchange, RabbitQueue
from faststream.rabbit.schemas.queue import ClassicQueueArgs

from papilio_tasks.infra.faststream.brokers.contracts.rabbit import (
    RabbitContract,
)

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.rabbit import RabbitPublisher
from ..subscribers.base import Subscriber
from ..subscribers.rabbit import RabbitSubscriber
from .base import Registrar


class RabbitRegistrar(Registrar):
    broker: RabbitContract

    def __init__(self, broker: RabbitContract) -> None:
        super().__init__(broker)
        self._exchanges: dict[str, RabbitExchange] = {}
        self._queues: dict[str, RabbitQueue] = {}

    def _exchange(self, cls: type[Publisher[Any]]) -> RabbitExchange:
        if not isinstance(cls, type) or not issubclass(cls, RabbitPublisher):
            raise TypeError("Register a RabbitPublisher class")
        exchange = cls.exchange
        if not isinstance(exchange, RabbitExchange) or not exchange.name:
            raise ValueError("Publisher requires a named RabbitExchange")
        if not isinstance(cls.routing_key, str):
            raise TypeError("Publisher routing_key must be a string")
        existing = self._exchanges.get(exchange.name)
        if existing is not None and (
            existing != exchange
            or existing.declare != exchange.declare
            or existing.bind_to != exchange.bind_to
            or existing.bind_arguments != exchange.bind_arguments
            or existing.routing_key != exchange.routing_key
            or existing.robust != exchange.robust
            or existing.timeout != exchange.timeout
        ):
            raise ValueError(f"Conflicting exchange: {exchange.name}")
        return deepcopy(exchange)

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, RabbitPublisher):
            raise TypeError("Register a RabbitPublisher class")
        bindings.check(cls)
        exchange = self._exchange(cls)
        sender = self.broker.publisher(
            exchange=exchange, routing_key=cls.routing_key, **options
        )
        bindings.add(
            cls,
            self,
            self.sender(
                cls,
                sender.publish,
                {"exchange": exchange.name, "routing_key": cls.routing_key},
            ),
        )
        self._publishers.add(cls)
        self._exchanges[exchange.name] = exchange

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        ack_policy: AckPolicy | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, RabbitSubscriber):
            raise TypeError("Register a RabbitSubscriber class")
        policy = cls.ack_policy if ack_policy is None else ack_policy
        if policy is not None:
            if not isinstance(policy, AckPolicy):
                raise TypeError("Expected AckPolicy or None")
            options["ack_policy"] = policy
        handler = self.handler(cls)
        exchange = self._exchange(cls.publisher)
        queue = cls.queue
        if not isinstance(queue, RabbitQueue) or not queue.name:
            raise ValueError("Subscriber requires a named RabbitQueue")
        if (
            queue.routing_key
            and queue.routing_key != cls.publisher.routing_key
        ):
            raise ValueError(
                "Queue routing_key conflicts with publisher route"
            )
        existing = self._queues.get(queue.name)
        if existing is not None and (
            existing != queue
            or existing.declare != queue.declare
            or existing.robust != queue.robust
            or existing.timeout != queue.timeout
            or existing.bind_arguments != queue.bind_arguments
        ):
            raise ValueError(f"Conflicting queue: {queue.name}")
        selected = RabbitQueue(
            queue.name,
            durable=queue.durable,
            exclusive=queue.exclusive,
            declare=queue.declare,
            auto_delete=queue.auto_delete,
            arguments=cast(ClassicQueueArgs, deepcopy(queue.arguments)),
            timeout=queue.timeout,
            robust=queue.robust,
            bind_arguments=deepcopy(queue.bind_arguments),
            routing_key=cls.publisher.routing_key,
        )
        self.broker.subscriber(selected, exchange, no_reply=True, **options)(
            handler
        )
        self._subscribers.add(cls)
        self._queues[queue.name] = selected
        self._exchanges[exchange.name] = exchange

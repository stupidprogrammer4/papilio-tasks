from copy import deepcopy
from typing import Any

from faststream.redis import ListSub

from papilio_tasks.infra.faststream.brokers.contracts.redis import (
    RedisContract,
)

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.lists import ListPublisher
from ..subscribers.base import Subscriber
from ..subscribers.lists import ListSubscriber
from .base import Registrar


class ListRegistrar(Registrar):
    broker: RedisContract

    def __init__(self, broker: RedisContract) -> None:
        super().__init__(broker)

    def _list(self, cls: type[Publisher[Any]]) -> str:
        if not isinstance(cls, type) or not issubclass(cls, ListPublisher):
            raise TypeError("Register a ListPublisher class")
        if not isinstance(cls.list, str) or not cls.list:
            raise ValueError("Publisher requires a named Redis list")
        return cls.list

    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None:
        self.check_open()
        name = self._list(cls)
        bindings.check(cls)
        sender = self.broker.publisher(list=name, **options)
        bindings.add(
            cls, self, self.sender(cls, sender.publish, {"list": name})
        )
        self._publishers.add(cls)

    def subscriber(
        self,
        cls: type[Subscriber[Any]],
        *,
        list: ListSub | None = None,
        **options: Any,
    ) -> None:
        self.check_open()
        if not isinstance(cls, type) or not issubclass(cls, ListSubscriber):
            raise TypeError("Register a ListSubscriber class")
        name = self._list(cls.publisher)
        selected = cls.list if list is None else list
        if selected is None:
            selected = ListSub(name)
        if not isinstance(selected, ListSub):
            raise TypeError("Expected ListSub or None")
        if selected.name != name:
            raise ValueError("Subscriber list conflicts with publisher route")
        handler = self.handler(cls)
        self.broker.subscriber(
            list=deepcopy(selected), no_reply=True, **options
        )(handler)
        self._subscribers.add(cls)

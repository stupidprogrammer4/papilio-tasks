from abc import ABC, abstractmethod
from collections.abc import Mapping
from inspect import isabstract, iscoroutinefunction, signature
from types import MappingProxyType
from typing import Any, get_type_hints
from weakref import WeakSet

from dishka import AsyncContainer
from dishka_faststream import FromDishka, inject

from papilio_tasks.infra.faststream.brokers.contracts.base import (
    BrokerContract,
)
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import (
    SubscribeCall,
    SubscribeHooks,
)
from papilio_tasks.tools.hooks.subscribe import run as run_hooks

from ..publishers import bindings
from ..publishers.base import Publisher
from ..publishers.contracts import Sender
from ..publishers.send import send
from ..subscribers.base import Subscriber

_owners: WeakSet[object] = WeakSet()


class Registrar(ABC):
    def __init__(self, broker: BrokerContract[Any]) -> None:
        self.broker = broker
        self.attached = False
        self.closed = False
        self.container: AsyncContainer | None = None
        self.publish_hooks: type[PublishHooks] | None = None
        self.subscribe_hooks: type[SubscribeHooks] | None = None
        self._publishers: set[type[Publisher[Any]]] = set()
        self._subscribers: set[type[Subscriber[Any]]] = set()

    @abstractmethod
    def publisher(self, cls: type[Publisher[Any]], **options: Any) -> None: ...

    @abstractmethod
    def subscriber(
        self, cls: type[Subscriber[Any]], **options: Any
    ) -> None: ...

    def has_publisher(self, cls: type[Publisher[Any]]) -> bool:
        return cls in self._publishers

    def has_subscriber(self, cls: type[Subscriber[Any]]) -> bool:
        return cls in self._subscribers

    def check_open(self) -> None:
        if self.closed or self.attached:
            raise RuntimeError(
                "Register events before creating the application"
            )
        if self.broker.native in _owners:
            raise ValueError("Broker already has an Events application")

    def attach(self) -> None:
        self.check_open()
        _owners.add(self.broker.native)
        self.attached = True

    def sender(
        self,
        cls: type[Publisher[Any]],
        native: Sender,
        meta: Mapping[str, object],
    ) -> Sender:
        metadata = MappingProxyType(dict(meta))

        async def execute(event: Any, **options: Any) -> Any:
            return await send(
                cls,
                event,
                options,
                native,
                container=self.container,
                hooks=self.publish_hooks,
                meta=metadata,
            )

        return execute

    def handler(self, cls: type[Subscriber[Any]]) -> Any:
        self.check_open()
        if cls in self._subscribers:
            raise ValueError(
                f"Subscriber already registered: {cls.__qualname__}"
            )
        run = cls.run
        params = list(signature(run).parameters.values())
        if (
            isabstract(cls)
            or not iscoroutinefunction(run)
            or len(params) != 2
            or params[1].name != "event"
            or params[1].kind
            not in (params[1].POSITIONAL_ONLY, params[1].POSITIONAL_OR_KEYWORD)
        ):
            raise TypeError("Subscriber must implement async run(self, event)")
        hints = get_type_hints(run)
        if "event" not in hints:
            raise TypeError("Subscriber.run must annotate event with its DTO")

        async def execute(
            event: Any, *, _subscriber: Any, _scope: AsyncContainer
        ) -> None:
            if self.subscribe_hooks is None:
                await _subscriber.run(event)
                return
            hooks = await _scope.get(self.subscribe_hooks)
            await run_hooks(
                SubscribeCall(f"{cls.__module__}.{cls.__qualname__}", event),
                _subscriber.run,
                hooks,
            )

        execute.__annotations__ = {
            "event": hints["event"],
            "_subscriber": FromDishka[cls],
            "_scope": FromDishka[AsyncContainer],
            "return": None,
        }
        return inject(execute)

    def close(self) -> None:
        self.closed = True
        bindings.release(self)

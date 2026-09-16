from collections.abc import Sequence
from typing import Any, cast

from dishka import AsyncContainer, Provider, make_async_container
from dishka_faststream import FastStreamProvider, setup_dishka

from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks

from .publishers.base import Publisher
from .registry.base import Registrar
from .subscribers.base import Subscriber


class Application:
    """Own broker and DI lifecycle; connect publishes, start also consumes."""

    def __init__(
        self, registrar: Registrar, container: AsyncContainer
    ) -> None:
        self.registrar = registrar
        self.container = container
        self._closed = False

    async def connect(self) -> None:
        if self._closed:
            raise RuntimeError("Application is closed")
        await self.registrar.broker.connect()

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("Application is closed")
        await self.registrar.broker.start()

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.registrar.broker.stop()
        finally:
            try:
                await self.container.close()
            finally:
                self.registrar.close()


def create_app(
    *,
    registrar: Registrar,
    providers: Sequence[Provider] = (),
    publishers: Sequence[str] = (),
    subscribers: Sequence[str] = (),
    publish_hooks: type[PublishHooks] | None = None,
    subscribe_hooks: type[SubscribeHooks] | None = None,
) -> Application:
    """Discover selected event modules and attach message-scoped DI.

    Paths are package roots containing publishers.py/subscribers.py or matching
    packages. Providers remain explicit. Existing manual registration wins.
    Assembly opens no connections; callers start and stop the returned app.
    """
    registrar.check_open()
    native = registrar.broker.native
    try:
        for selected, base, name in (
            (publish_hooks, PublishHooks, "publish_hooks"),
            (subscribe_hooks, SubscribeHooks, "subscribe_hooks"),
        ):
            if selected is not None and (
                not isinstance(selected, type)
                or not issubclass(selected, base)
            ):
                raise TypeError(
                    f"{name} must be a {base.__name__} class or None"
                )
        senders = Bootstrapper(publishers).classes("publishers", Publisher)
        receivers = Bootstrapper(subscribers).classes(
            "subscribers", Subscriber
        )
        for cls in senders:
            if not registrar.has_publisher(cls):
                registrar.publisher(cls)
        for cls in receivers:
            if not registrar.has_subscriber(cls):
                registrar.subscriber(cls)
        container = make_async_container(FastStreamProvider(), *providers)
        # The shared lifecycle protocol excludes native DI middleware methods.
        setup_dishka(container, broker=cast(Any, native))
        registrar.container = container
        registrar.publish_hooks = publish_hooks
        registrar.subscribe_hooks = subscribe_hooks
    except Exception:
        registrar.close()
        raise
    registrar.attach()
    return Application(registrar, container)

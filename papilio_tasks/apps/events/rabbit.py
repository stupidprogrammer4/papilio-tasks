"""Configured RabbitMQ event applications."""

from collections.abc import Sequence

from dishka import Provider
from faststream.rabbit import RabbitBroker as NativeRabbitBroker

from papilio_tasks.apps.lifecycle import Producer
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks

from .application import Application
from .application import create_app as assemble
from .registry.rabbit import RabbitRegistrar


def create_app(
    url: str,
    *,
    providers: Sequence[Provider] = (),
    publishers: Sequence[str] = (),
    subscribers: Sequence[str] = (),
    producers: Sequence[Producer] = (),
    publish_hooks: type[PublishHooks] | None = None,
    subscribe_hooks: type[SubscribeHooks] | None = None,
) -> Application:
    """Build an event app; connect publishes and start also consumes."""
    return assemble(
        registrar=RabbitRegistrar(RabbitBroker(NativeRabbitBroker(url))),
        providers=providers,
        publishers=publishers,
        subscribers=subscribers,
        producers=producers,
        publish_hooks=publish_hooks,
        subscribe_hooks=subscribe_hooks,
    )

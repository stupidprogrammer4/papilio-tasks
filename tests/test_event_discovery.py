import importlib
import sys
import textwrap
from uuid import uuid4

import pytest

pytest.importorskip("dishka_faststream")
pytest.importorskip("faststream.rabbit")

from dishka import Provider, Scope
from faststream.rabbit import RabbitBroker as NativeRabbitBroker
from faststream.rabbit import TestRabbitBroker

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker


@pytest.fixture
def package(tmp_path, monkeypatch):
    name = "event_app_" + uuid4().hex
    monkeypatch.syspath_prepend(str(tmp_path))

    def write(path, content=""):
        target = tmp_path / name / path
        target.parent.mkdir(parents=True, exist_ok=True)
        for parent in (target.parent, *target.parent.parents):
            if parent == tmp_path:
                break
            (parent / "__init__.py").touch()
        target.write_text(textwrap.dedent(content).replace("APP", name))
        importlib.invalidate_caches()

    write("__init__.py")
    write(
        "orders/publishers.py",
        """
        from papilio_tasks.apps.events.publishers.rabbit import (
            RabbitPublisher,
            RabbitExchange,
            ExchangeType,
        )
        class Created(RabbitPublisher[dict]):
            exchange = RabbitExchange("orders", type=ExchangeType.TOPIC)
            routing_key = "order.created"
        Alias = Created
        """,
    )
    write(
        "finance/subscribers.py",
        """
        from faststream import StreamMessage
        from APP.orders.publishers import Created
        from papilio_tasks.apps.events.subscribers.rabbit import (
            RabbitSubscriber,
            RabbitQueue,
        )
        seen = []
        constructed = []
        class Service:
            def __init__(self, message: StreamMessage):
                self.message = message
                constructed.append(self)
        class Finance(RabbitSubscriber[dict]):
            publisher = Created
            queue = RabbitQueue("finance.orders")
            def __init__(self, service: Service):
                self.service = service
            async def run(self, event: dict) -> None:
                seen.append((event, self.service.message.headers))
        Alias = Finance
        class Abstract(RabbitSubscriber[dict]):
            pass
        """,
    )
    yield name, write
    for module in tuple(sys.modules):
        if module == name or module.startswith(name + "."):
            del sys.modules[module]


def registry():
    return RabbitRegistrar(RabbitBroker(NativeRabbitBroker(logger=None)))


def provider(module):
    result = Provider(scope=Scope.REQUEST)
    result.provide(module.Service)
    result.provide(module.Finance)
    return result


async def test_discovery_packages_aliases_abstract_and_imported_classes(
    package,
):
    name, write = package
    write(
        "orders/publishers.py",
        """
        from abc import ABC, abstractmethod
        from papilio_tasks.apps.events.publishers.rabbit import (
            RabbitPublisher,
            RabbitExchange,
        )
        class Abstract(RabbitPublisher[dict], ABC):
            @abstractmethod
            def unused(self): pass
        class Created(Abstract):
            exchange = RabbitExchange("orders")
            routing_key = "order.created"
            def unused(self): pass
        Alias = Created
        """,
    )
    write(
        "notifications/subscribers/mail.py",
        """
        from APP.finance.subscribers import Finance
        from papilio_tasks.apps.events.subscribers.rabbit import (
            RabbitQueue,
        )
        class Mail(Finance):
            queue = RabbitQueue("mail.orders")
        """,
    )
    write("unrelated.py", "raise AssertionError('unrelated module imported')")
    reg = registry()
    app = create_app(
        registrar=reg,
        publishers=[name, name + ".orders", name],
        subscribers=[name, name + ".notifications", name],
    )
    try:
        finance = importlib.import_module(name + ".finance.subscribers")
        mail = importlib.import_module(
            name + ".notifications.subscribers.mail"
        )
        assert reg.has_publisher(finance.Created)
        assert reg.has_subscriber(finance.Finance)
        assert reg.has_subscriber(mail.Mail)
        assert len(reg.broker.native.publishers) == 1
        assert len(reg.broker.native.subscribers) == 2
        assert finance.constructed == []
    finally:
        await app.stop()


async def test_manual_options_survive_discovery_and_message_uses_provider(
    package,
):
    name, _ = package
    module = importlib.import_module(name + ".finance.subscribers")
    reg = registry()
    reg.publisher(module.Created, headers={"origin": "manual"})
    reg.subscriber(module.Finance, decoder=lambda _: {"id": 9})
    app = create_app(
        registrar=reg,
        providers=[provider(module)],
        publishers=[name + ".orders"],
        subscribers=[name + ".finance"],
    )
    try:
        assert module.constructed == []
        assert len(reg.broker.native.publishers) == 1
        assert len(reg.broker.native.subscribers) == 1
        async with TestRabbitBroker(reg.broker.native):
            await module.Created.publish({"id": 1})
        assert module.seen == [({"id": 9}, {"origin": "manual"})]
        assert len(module.constructed) == 1
    finally:
        await app.stop()


async def test_producer_roots_do_not_import_consumer_package(package):
    name, write = package
    write("finance/__init__.py", "raise AssertionError('consumer imported')")
    reg = registry()
    app = create_app(registrar=reg, publishers=[name + ".orders"])
    try:
        assert name + ".finance" not in sys.modules
        assert len(reg.broker.native.publishers) == 1
        assert not reg.broker.native.subscribers
    finally:
        await app.stop()


async def test_subscriber_discovery_does_not_bind_imported_publisher(package):
    name, _ = package
    module = importlib.import_module(name + ".finance.subscribers")
    reg = registry()
    app = create_app(
        registrar=reg,
        subscribers=[name + ".finance"],
        providers=[provider(module)],
    )
    try:
        assert reg.has_subscriber(module.Finance)
        assert not reg.has_publisher(module.Created)
        with pytest.raises(RuntimeError, match="not registered"):
            await module.Created.publish({"id": 1})
        async with TestRabbitBroker(reg.broker.native):
            await reg.broker.publish(
                {"id": 3},
                exchange=module.Created.exchange,
                routing_key=module.Created.routing_key,
            )
        assert module.seen == [({"id": 3}, {})]
    finally:
        await app.stop()


@pytest.mark.parametrize("kind", ["publishers", "subscribers"])
def test_wrong_backend_rejected_and_partial_bindings_released(package, kind):
    name, write = package
    if kind == "publishers":
        write(
            "wrong/publishers.py",
            """
            from papilio_tasks.apps.events import Publisher
            class Wrong(Publisher[dict]): pass
            """,
        )
    else:
        write(
            "wrong/subscribers.py",
            """
            from papilio_tasks.apps.events import Subscriber
            from papilio_tasks.apps.events.subscribers.rabbit import (
                RabbitQueue,
            )
            from APP.orders.publishers import Created
            class Wrong(Subscriber[dict]):
                publisher = Created
                queue = RabbitQueue("looks-like-rabbit")
                async def run(self, event: dict) -> None: pass
            """,
        )
    module = importlib.import_module(name + ".orders.publishers")
    reg = registry()
    roots = {"publishers": [name + ".orders"]}
    roots.setdefault(kind, []).append(name + ".wrong")
    with pytest.raises(TypeError, match="Register a Rabbit"):
        create_app(registrar=reg, **roots)
    with pytest.raises(RuntimeError, match="not registered"):
        bindings.get(module.Created)
    with pytest.raises(RuntimeError, match="before creating"):
        reg.publisher(module.Created)


async def test_another_owners_sender_is_not_skipped_or_released(package):
    name, _ = package
    owner, other = registry(), registry()
    app = create_app(registrar=owner, publishers=[name + ".orders"])
    module = importlib.import_module(name + ".orders.publishers")
    sender = bindings.get(module.Created)
    try:
        with pytest.raises(ValueError, match="already registered"):
            create_app(registrar=other, publishers=[name + ".orders"])
        assert bindings.get(module.Created) is sender
        assert not other.broker.native.publishers
    finally:
        await app.stop()


@pytest.mark.parametrize("nested", [False, True])
def test_import_errors_preserved_and_manual_binding_cleaned(package, nested):
    name, write = package
    module = importlib.import_module(name + ".orders.publishers")
    reg = registry()
    reg.publisher(module.Created)
    missing = "absent_event_dependency_" + uuid4().hex
    root = missing
    if nested:
        write("broken/subscribers.py", f"import {missing}")
        root = name + ".broken"
    with pytest.raises(ModuleNotFoundError) as caught:
        create_app(registrar=reg, subscribers=[root])
    assert caught.value.name == missing
    with pytest.raises(RuntimeError, match="not registered"):
        bindings.get(module.Created)


async def test_empty_roots_remain_manual_and_bad_provider_graph_releases(
    package,
):
    name, _ = package
    reg = registry()
    app = create_app(registrar=reg)
    try:
        assert name + ".orders.publishers" not in sys.modules
        assert not reg.broker.native.publishers
    finally:
        await app.stop()

    class Missing:
        pass

    class Service:
        def __init__(self, missing: Missing):
            pass

    invalid = Provider(scope=Scope.REQUEST)
    invalid.provide(Service)
    from dishka.exceptions import GraphMissingFactoryError

    with pytest.raises(GraphMissingFactoryError):
        create_app(
            registrar=registry(),
            providers=[invalid],
            publishers=[name + ".orders"],
        )
    module = importlib.import_module(name + ".orders.publishers")
    with pytest.raises(RuntimeError, match="not registered"):
        bindings.get(module.Created)

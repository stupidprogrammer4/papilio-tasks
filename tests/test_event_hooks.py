import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("dishka_faststream")
pytest.importorskip("faststream.rabbit")

from dishka import Provider, Scope, provide
from faststream.rabbit import RabbitBroker as NativeRabbitBroker
from faststream.rabbit import TestRabbitBroker
from hook_support import handler

import papilio_tasks.apps.events as events
from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.rabbit import (
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.tools.hooks import Handler, Hook
from papilio_tasks.tools.hooks.publish import (
    Published,
    PublishError,
    PublishHooks,
)
from papilio_tasks.tools.hooks.subscribe import (
    SubscribeCall,
    SubscribeFailed,
    SubscribeHooks,
    run,
)


def registry(url=None):
    native = (
        NativeRabbitBroker(url, logger=None)
        if url
        else NativeRabbitBroker(logger=None)
    )
    return RabbitRegistrar(RabbitBroker(native))


def event(name="orders"):
    class Created(RabbitPublisher[dict]):
        exchange = RabbitExchange(name)
        routing_key = "created"

    return Created


def supply(kind, value):
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: value, provides=kind)
    return provider


@pytest.mark.parametrize(
    "outcome", ["ok", "send", "after", "cancel", "resolve"]
)
async def test_producer_native_none_errors_cancellation_cleanup(
    monkeypatch, outcome
):
    reg = registry()
    Created = event()
    error = ValueError("primary")
    hook_error = RuntimeError("hook failed")
    observed, closed = [], []
    native = AsyncMock(return_value=None)
    monkeypatch.setattr(
        reg.broker, "publisher", lambda **_: SimpleNamespace(publish=native)
    )
    reg.publisher(Created)

    async def after(sent):
        observed.append(sent)
        if outcome == "after":
            raise error

    async def on_error(failed):
        observed.append(failed)
        raise hook_error

    async def hooks():
        try:
            if outcome == "resolve":
                raise error
            yield PublishHooks(
                after_send=(handler(after),), on_error=(handler(on_error),)
            )
        finally:
            closed.append(True)

    providers = Provider(scope=Scope.REQUEST)
    providers.provide(hooks, provides=PublishHooks)
    app = create_app(
        registrar=reg, providers=[providers], publish_hooks=PublishHooks
    )
    try:
        if outcome in ("send", "cancel"):
            native.side_effect = (
                error if outcome == "send" else asyncio.CancelledError()
            )
        if outcome == "ok":
            assert await Created.publish({"id": 1}) is None
            assert observed[0].result is None
        elif outcome == "after":
            with pytest.raises(PublishError) as caught:
                await Created.publish({"id": 1})
            assert caught.value.result is None
            assert caught.value.__cause__ is error
        elif outcome == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await Created.publish({"id": 1})
        else:
            with pytest.raises(ValueError) as caught:
                await Created.publish({"id": 1})
            assert caught.value is error
            if outcome == "send":
                assert error.__cause__ is hook_error
        assert closed == [True]
        assert native.await_count == (0 if outcome == "resolve" else 1)
        assert len(observed) == (0 if outcome in ("cancel", "resolve") else 1)
    finally:
        await app.stop()


@pytest.mark.parametrize("outcome", ["ok", "send", "cancel", "resolve"])
async def test_cleanup_failure_preserves_primary_and_return(
    monkeypatch, outcome
):
    reg = registry()
    Created = event()
    error = LookupError("primary")
    native = AsyncMock(return_value=None)
    monkeypatch.setattr(
        reg.broker, "publisher", lambda **_: SimpleNamespace(publish=native)
    )
    reg.publisher(Created)

    class Resource:
        pass

    async def resource():
        try:
            yield Resource()
        finally:
            raise RuntimeError("cleanup")

    def hooks(resource: Resource):
        if outcome == "resolve":
            raise error
        return PublishHooks()

    providers = Provider(scope=Scope.REQUEST)
    providers.provide(resource, provides=Resource)
    providers.provide(hooks, provides=PublishHooks)
    app = create_app(
        registrar=reg, providers=[providers], publish_hooks=PublishHooks
    )
    try:
        if outcome == "send":
            native.side_effect = error
        if outcome == "cancel":
            native.side_effect = asyncio.CancelledError()
        expected = (
            PublishError
            if outcome == "ok"
            else asyncio.CancelledError
            if outcome == "cancel"
            else LookupError
        )
        with pytest.raises(expected) as caught:
            await Created.publish({})
        if outcome == "ok":
            assert caught.value.result is None
        elif outcome != "cancel":
            assert caught.value is error
        assert native.await_count == (0 if outcome == "resolve" else 1)
    finally:
        await app.stop()


async def test_publish_collection_applies_to_all_senders_in_only_its_app(
    monkeypatch,
):
    first, other = registry(), registry()
    Created, Updated, Unhooked = event(), event("updated"), event("elsewhere")
    native = AsyncMock(return_value="receipt")
    monkeypatch.setattr(
        first.broker, "publisher", lambda **_: SimpleNamespace(publish=native)
    )
    monkeypatch.setattr(
        other.broker, "publisher", lambda **_: SimpleNamespace(publish=native)
    )
    first.publisher(Created)
    first.publisher(Updated)
    other.publisher(Unhooked)
    observed, opened, closed = [], [], []

    class Audit(Hook[Published]):
        async def run(self, sent: Published) -> None:
            observed.append(sent)

    class Hooks(PublishHooks):
        def __init__(self, audit: Audit):
            super().__init__(after_send=(Handler(audit),))

    async def audit():
        instance = Audit()
        opened.append(instance)
        try:
            yield instance
        finally:
            closed.append(instance)

    providers = Provider(scope=Scope.REQUEST)
    providers.provide(audit, provides=Audit)
    providers.provide(Hooks)
    app = create_app(
        registrar=first, providers=[providers], publish_hooks=Hooks
    )
    other_app = create_app(registrar=other)
    try:
        await asyncio.gather(
            Created.publish({"id": 1}, headers={"v": "1"}),
            Updated.publish({"id": 2}),
        )
        assert await Unhooked.publish({}) == "receipt"
        assert len(observed) == len(opened) == len(closed) == 2
        call = next(
            x.call for x in observed if x.call.meta["exchange"] == "orders"
        )
        assert call.sender.endswith(".Created")
        assert call.args == ({"id": 1},)
        assert call.kwargs == {"headers": {"v": "1"}}
        assert call.meta == {"exchange": "orders", "routing_key": "created"}
        with pytest.raises(TypeError):
            call.meta["exchange"] = "changed"
        with pytest.raises(TypeError):
            call.kwargs["headers"] = {}
    finally:
        await asyncio.gather(app.stop(), other_app.stop())
    with pytest.raises(RuntimeError, match="not registered"):
        await Created.publish({})


async def test_subscriber_and_hooks_share_message_scope_for_all_handlers():
    reg = registry()
    Created = event()
    observed, closed = [], []

    class Resource:
        pass

    class Audit(Hook[SubscribeCall]):
        def __init__(self, resource: Resource):
            self.resource = resource

        async def run(self, call: SubscribeCall) -> None:
            observed.append((call.subscriber, self.resource))

    class Hooks(SubscribeHooks):
        def __init__(self, audit: Audit):
            super().__init__(
                before_run=(Handler(audit),), after_run=(Handler(audit),)
            )

    class Finance(RabbitSubscriber[dict]):
        publisher = Created
        queue = RabbitQueue("finance")

        def __init__(self, resource: Resource):
            self.resource = resource

        async def run(self, event: dict) -> None:
            observed.append(("run", self.resource))

    class Notify(Finance):
        queue = RabbitQueue("notify")

    class Providers(Provider):
        scope = Scope.REQUEST
        audit = provide(Audit)
        hooks = provide(Hooks)
        finance = provide(Finance)
        notify = provide(Notify)

        @provide
        async def resource(self) -> AsyncIterator[Resource]:
            instance = Resource()
            try:
                yield instance
            finally:
                closed.append(instance)

    reg.publisher(Created)
    reg.subscriber(Finance)
    reg.subscriber(Notify)
    app = create_app(
        registrar=reg, providers=[Providers()], subscribe_hooks=Hooks
    )
    try:
        async with TestRabbitBroker(reg.broker.native):
            await Created.publish({"id": 1})
        assert len(closed) == 2 and closed[0] is not closed[1]
        for resource in closed:
            calls = [name for name, dep in observed if dep is resource]
            assert len(calls) == 3
            assert calls[0] == calls[2] and calls[1] == "run"
    finally:
        await app.stop()


@pytest.mark.parametrize("stage", ["before_run", "run", "after_run"])
@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_subscriber_stage_order_error_policy(stage, policy):
    calls = []
    error = ValueError("primary")

    async def before(call):
        calls.append("before")
        if stage == "before_run":
            raise error

    async def execute(data):
        calls.append("run")
        if stage == "run":
            raise error

    async def after(call):
        calls.append("after")
        if stage == "after_run":
            raise error

    async def on_error(failure: SubscribeFailed):
        calls.append("error")
        assert failure.stage == stage and failure.error is error
        raise RuntimeError("failed error hook")

    hooks = SubscribeHooks(
        before_run=(handler(before, policy),),
        after_run=(handler(after, policy),),
        on_error=(handler(on_error),),
    )
    if policy == "continue" and stage != "run":
        await run(SubscribeCall("handler", {}), execute, hooks)
        assert calls == ["before", "run", "after"]
    else:
        with pytest.raises(ValueError) as caught:
            await run(SubscribeCall("handler", {}), execute, hooks)
        assert caught.value is error
        assert str(error.__cause__) == "failed error hook"
        assert (
            calls
            == {
                "before_run": ["before", "error"],
                "run": ["before", "run", "error"],
                "after_run": ["before", "run", "after", "error"],
            }[stage]
        )


async def test_subscriber_cancellation_is_not_a_run_error():
    async def execute(data):
        raise asyncio.CancelledError()

    on_error = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await run(
            SubscribeCall("handler", {}),
            execute,
            SubscribeHooks(on_error=(handler(on_error),)),
        )
    on_error.assert_not_awaited()


@pytest.mark.parametrize(
    "name,invalid",
    [
        ("publish_hooks", PublishHooks()),
        ("subscribe_hooks", PublishHooks),
        ("subscribe_hooks", SubscribeHooks()),
    ],
)
def test_invalid_collection_rejected_before_discovery(name, invalid):
    with pytest.raises(TypeError, match=name):
        create_app(
            registrar=registry(),
            publishers=["missing_root_must_not_import"],
            **{name: invalid},
        )


async def test_live_send_success_is_separate_from_subscriber_failure():
    import os
    from uuid import uuid4

    url = os.getenv("TEST_RABBIT_URL")
    if not url:
        pytest.skip("Requires isolated RabbitMQ")
    name = "papilio-hooks-" + uuid4().hex
    Created = event(name)
    producer, worker = registry(url), registry(url)
    sent, send_errors, completed, failures = [], [], [], []
    done = asyncio.Event()

    class Finance(RabbitSubscriber[dict]):
        publisher = Created
        queue = RabbitQueue(name + "-finance")

        async def run(self, event: dict) -> None:
            raise ValueError("business failure")

    class Notify(RabbitSubscriber[dict]):
        publisher = Created
        queue = RabbitQueue(name + "-notify")

        async def run(self, event: dict) -> None:
            pass

    async def after_send(result):
        sent.append(result)

    async def send_error(failure):
        send_errors.append(failure)

    async def after_run(call):
        completed.append(call)
        if failures:
            done.set()

    async def run_error(failure):
        failures.append(failure)
        if completed:
            done.set()

    providers = Provider(scope=Scope.REQUEST)
    providers.provide(Finance)
    providers.provide(Notify)
    producer.publisher(Created)
    worker.subscriber(Finance)
    worker.subscriber(Notify)
    producer_app = create_app(
        registrar=producer,
        publish_hooks=PublishHooks,
        providers=[
            supply(
                PublishHooks,
                PublishHooks(
                    after_send=(handler(after_send),),
                    on_error=(handler(send_error),),
                ),
            )
        ],
    )
    worker_app = create_app(
        registrar=worker,
        subscribe_hooks=SubscribeHooks,
        providers=[
            providers,
            supply(
                SubscribeHooks,
                SubscribeHooks(
                    after_run=(handler(after_run),),
                    on_error=(handler(run_error),),
                ),
            ),
        ],
    )
    try:
        await worker_app.start()
        await producer_app.connect()
        receipt = await Created.publish({"id": 7})
        await asyncio.wait_for(done.wait(), timeout=15)
        assert len(sent) == 1 and sent[0].result is receipt
        assert send_errors == []
        assert len(completed) == 1
        assert completed[0].subscriber.endswith(".Notify")
        assert len(failures) == 1
        assert failures[0].call.subscriber.endswith(".Finance")
        assert failures[0].stage == "run"
        assert str(failures[0].error) == "business failure"
        assert failures[0].call.data == {"id": 7}
    finally:
        await worker_app.stop()
        try:
            connection = await producer.broker.connect()
            channel = await connection.channel()
            await asyncio.gather(
                channel.queue_delete(Finance.queue.name),
                channel.queue_delete(Notify.queue.name),
            )
            await channel.exchange_delete(name)
        finally:
            await producer_app.stop()


async def test_missing_subscriber_dependency_does_not_emit_run_error():
    reg = registry()
    Created = event()
    observed = AsyncMock()

    class Unprovided(RabbitSubscriber[dict]):
        publisher = Created
        queue = RabbitQueue("unprovided")

        async def run(self, event: dict) -> None:
            raise AssertionError("must not run")

    reg.publisher(Created)
    reg.subscriber(Unprovided)
    app = create_app(
        registrar=reg,
        subscribe_hooks=SubscribeHooks,
        providers=[
            supply(
                SubscribeHooks,
                SubscribeHooks(
                    on_error=(handler(observed),),
                ),
            )
        ],
    )
    try:
        async with TestRabbitBroker(reg.broker.native):
            with pytest.raises(Exception, match="Unprovided"):
                await Created.publish({})
        observed.assert_not_awaited()
    finally:
        await app.stop()


async def test_decorator_uses_publication_hooks_once(monkeypatch):
    # Runtime imports must not replace the public decorator with a module.
    assert events.publish is publish and callable(publish)
    reg = registry()
    Created = event()
    native = AsyncMock(return_value="receipt")
    monkeypatch.setattr(
        reg.broker, "publisher", lambda **_: SimpleNamespace(publish=native)
    )
    observed = []
    result = {"id": 7}

    async def after(sent):
        observed.append(sent)
        raise ValueError("audit failed")

    @publish(Created, select=lambda result: {"order_id": result["id"]})
    async def create():
        return result

    reg.publisher(Created)
    app = create_app(
        registrar=reg,
        publish_hooks=PublishHooks,
        providers=[
            supply(PublishHooks, PublishHooks(after_send=(handler(after),)))
        ],
    )
    try:
        with pytest.raises(PublishError) as caught:
            await create()
        assert caught.value.result == "receipt"
        assert str(caught.value.__cause__) == "audit failed"
        native.assert_awaited_once_with({"order_id": 7})
        assert len(observed) == 1 and observed[0].result == "receipt"
    finally:
        await app.stop()

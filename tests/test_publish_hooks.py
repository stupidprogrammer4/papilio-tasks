import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope
from hook_support import handler
from taskiq import AsyncTaskiqTask, TaskiqMiddleware
from taskiq.exceptions import SendTaskError

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.projections.application import create_broker
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.tools.hooks import Handler, Hook
from papilio_tasks.tools.hooks.publish import (
    Published,
    PublishError,
    PublishHooks,
)


@asynccontextmanager
async def application(monkeypatch, *, shared=None, local=None, providers=()):
    class Job(Direct[int, int]):
        publish_hooks = local

        def __init__(self):
            raise AssertionError("Producer must not construct the projection")

        async def read(self, id: int) -> int:
            return id

        async def write(self, data: int) -> int:
            return data

    registry = Registrar()
    task = registry.include(Job, name="product")
    broker = create_broker(
        registrar=registry, providers=providers, publish_hooks=shared
    )
    send = AsyncMock()
    monkeypatch.setattr(broker, "kick", send)
    try:
        yield SimpleNamespace(job=Job, task=task, broker=broker, send=send)
    finally:
        await broker.shutdown()


def supply(cls, instance):
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(lambda: instance, provides=cls)
    return provider


async def test_providers_scope_order_payload_and_cleanup(monkeypatch):
    opened, closed, calls = [], [], []

    class Service:
        pass

    async def service():
        instance = Service()
        opened.append(instance)
        try:
            yield instance
        finally:
            closed.append(instance)

    class Audit(Hook[Published]):
        def __init__(self, service: Service):
            self.service = service

        async def run(self, event: Published) -> None:
            assert self.service not in closed
            calls.append((self.service, event))

    class Shared(PublishHooks):
        def __init__(self, audit: Audit):
            super().__init__(after_send=(Handler(audit),))

    class Local(PublishHooks):
        def __init__(self, audit: Audit):
            super().__init__(after_send=(Handler(audit),))

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(service, provides=Service)
    provider.provide(Audit)
    provider.provide(Shared)
    provider.provide(Local)
    async with application(
        monkeypatch, shared=Shared, local=Local, providers=[provider]
    ) as app:
        assert opened == []
        first = await app.job.enqueue(id=42)
        second = await app.job.enqueue(43)
        assert isinstance(first, AsyncTaskiqTask)
        assert len(opened) == 2 and opened == closed
        assert [dep for dep, _ in calls] == [opened[0]] * 2 + [opened[1]] * 2
        assert [event.task_id for _, event in calls] == [first.task_id] * 2 + [
            second.task_id
        ] * 2
        event = calls[0][1]
        assert event.call.task_name == "product"
        assert event.call.projection.endswith(".Job")
        assert event.call.args == () and event.call.kwargs == {"id": 42}
        with pytest.raises(TypeError):
            event.call.kwargs["id"] = 0
        assert app.send.await_count == 2


@pytest.mark.parametrize("phase", ["after_send", "on_error"])
@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_hook_order_failure_policy_and_primary_error(
    monkeypatch, phase, policy
):
    calls = []
    hook_error = ValueError("audit failed")

    async def first(event):
        calls.append(("shared", event))
        raise hook_error

    async def next_shared(event):
        calls.append(("shared-next", event))

    async def local(event):
        calls.append(("local", event))

    class Shared(PublishHooks):
        pass

    class Local(PublishHooks):
        pass

    providers = [
        supply(
            Shared,
            Shared(**{phase: (handler(first, policy), handler(next_shared))}),
        ),
        supply(Local, Local(**{phase: (handler(local),)})),
    ]
    async with application(
        monkeypatch, shared=Shared, local=Local, providers=providers
    ) as app:
        if phase == "on_error":
            send_error = ConnectionError("offline")
            app.send.side_effect = send_error
            with pytest.raises(SendTaskError) as caught:
                await app.job.enqueue(42)
            assert caught.value is calls[0][1].error
            assert caught.value.__cause__ is (
                hook_error if policy == "raise" else send_error
            )
        elif policy == "raise":
            with pytest.raises(PublishError) as caught:
                await app.job.enqueue(42)
            assert caught.value.task_id == calls[0][1].task_id
            assert caught.value.__cause__ is hook_error
        else:
            result = await app.job.enqueue(42)
            assert result.task_id == calls[0][1].task_id
        assert [name for name, _ in calls] == (
            ["shared"]
            if policy == "raise"
            else ["shared", "shared-next", "local"]
        )
        app.send.assert_awaited_once()


@pytest.mark.parametrize("failure", ["none", "send", "after_send", "cancel"])
async def test_cleanup_failure_preserves_outcome(monkeypatch, failure):
    events = []
    primary = ValueError("primary")

    class Service:
        pass

    async def service():
        try:
            yield Service()
        finally:
            raise RuntimeError("cleanup failed")

    class Hooks(PublishHooks):
        def __init__(self, dep: Service):
            async def after(event):
                events.append(event)
                if failure == "after_send":
                    raise primary
                if failure == "cancel":
                    raise asyncio.CancelledError()

            async def error(event):
                events.append(event)

            super().__init__(
                after_send=(handler(after),), on_error=(handler(error),)
            )

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(service, provides=Service)
    provider.provide(Hooks)
    async with application(
        monkeypatch, shared=Hooks, providers=[provider]
    ) as app:
        if failure == "send":
            app.send.side_effect = primary
            with pytest.raises(SendTaskError) as caught:
                await app.job.enqueue(42)
            assert caught.value is events[0].error
        elif failure == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await app.job.enqueue(42)
        else:
            with pytest.raises(PublishError) as caught:
                await app.job.enqueue(42)
            assert caught.value.task_id == events[0].task_id
            if failure == "after_send":
                assert caught.value.__cause__ is primary
            else:
                assert isinstance(caught.value.__cause__, ExceptionGroup)
                assert (
                    str(caught.value.__cause__.exceptions[0])
                    == "cleanup failed"
                )
        assert len(events) == 1
        app.send.assert_awaited_once()


async def test_resolution_failure_prevents_publication(monkeypatch):
    async def fail(event):
        raise AssertionError("No error hooks before successful resolution")

    class Local(PublishHooks):
        pass

    class Shared(PublishHooks):
        pass

    async with application(
        monkeypatch,
        shared=Shared,
        local=Local,
        providers=[supply(Shared, Shared(on_error=(handler(fail),)))],
    ) as app:
        with pytest.raises(Exception, match="Local"):
            await app.job.enqueue(42)
        app.send.assert_not_awaited()


@pytest.mark.parametrize("stage", ["send", "hook"])
async def test_cancellation_propagates_without_error_hooks(monkeypatch, stage):
    closed = []

    async def cancelled(event):
        raise asyncio.CancelledError()

    async def unexpected(event):
        raise AssertionError("Cancellation must not invoke error hooks")

    class Hooks(PublishHooks):
        pass

    async def hooks():
        try:
            yield Hooks(
                after_send=(handler(cancelled),),
                on_error=(handler(unexpected),),
            )
        finally:
            closed.append(True)

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(hooks, provides=Hooks)
    async with application(
        monkeypatch, shared=Hooks, providers=[provider]
    ) as app:
        if stage == "send":
            app.send.side_effect = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await app.job.enqueue(42)
        assert closed == [True]
        app.send.assert_awaited_once()


async def test_native_post_send_error_is_not_proof_of_non_delivery(
    monkeypatch,
):
    seen = []
    error = ValueError("native post_send failed")

    class Native(TaskiqMiddleware):
        async def post_send(self, message):
            raise error

    async def failed(event):
        seen.append(event)

    async def unexpected(event):
        raise AssertionError("kiq did not return")

    hooks = PublishHooks(
        on_error=(handler(failed),), after_send=(handler(unexpected),)
    )
    async with application(
        monkeypatch,
        shared=PublishHooks,
        providers=[supply(PublishHooks, hooks)],
    ) as app:
        app.broker.add_middlewares(Native())
        with pytest.raises(ValueError) as caught:
            await app.job.enqueue(42)
        assert caught.value is error and seen[0].error is error
        app.send.assert_awaited_once()


async def test_project_delegates_once_and_bypasses_prepublication_errors(
    monkeypatch,
):
    events = []

    async def after(event):
        events.append(event)

    hooks = PublishHooks(after_send=(handler(after),))
    async with application(
        monkeypatch,
        local=PublishHooks,
        providers=[supply(PublishHooks, hooks)],
    ) as app:

        @app.job.project(select=lambda result: {"id": result})
        async def save(id):
            if id < 0:
                raise ValueError("service")
            return id

        assert await save(42) == 42
        assert len(events) == 1 and events[0].call.kwargs == {"id": 42}
        with pytest.raises(ValueError, match="service"):
            await save(-1)

        @app.job.project(select=lambda result: 1 / 0)
        async def invalid():
            return 42

        with pytest.raises(ZeroDivisionError):
            await invalid()
        assert len(events) == 1
        app.send.assert_awaited_once()


async def test_apps_use_their_own_collections_and_raw_kiq_bypasses_hooks(
    monkeypatch,
):
    events = []

    async def first(event):
        events.append("first")

    async def second(event):
        events.append("second")

    async with (
        application(
            monkeypatch,
            shared=PublishHooks,
            providers=[
                supply(
                    PublishHooks, PublishHooks(after_send=(handler(first),))
                )
            ],
        ) as one,
        application(
            monkeypatch,
            shared=PublishHooks,
            providers=[
                supply(
                    PublishHooks, PublishHooks(after_send=(handler(second),))
                )
            ],
        ) as two,
    ):
        await one.job.enqueue(1)
        await two.job.enqueue(2)
        await one.task.kiq(3)
        assert events == ["first", "second"]


async def test_no_hooks_support_bare_registry_and_hooks_require_container(
    monkeypatch,
):
    class Job(Direct[int, int]):
        async def read(self, id: int) -> int:
            return id

        async def write(self, data: int) -> int:
            return data

    class Hooked(Job):
        publish_hooks = PublishHooks

    registry = Registrar()
    registry.include(Job)
    registry.include(Hooked)
    send = AsyncMock()
    monkeypatch.setattr(registry.broker.native, "kick", send)
    assert isinstance(await Job.enqueue(42), AsyncTaskiqTask)
    with pytest.raises(RuntimeError, match="create_broker"):
        await Hooked.enqueue(42)
    send.assert_awaited_once()


def test_invalid_shared_collection_is_rejected():
    with pytest.raises(TypeError, match="PublishHooks class"):
        create_broker(publish_hooks=PublishHooks())

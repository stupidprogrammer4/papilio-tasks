import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
from dishka import Provider, Scope, make_async_container, provide
from hook_support import handler

from papilio_tasks.apps.projections import Direct, Failure, Hooks, Written
from papilio_tasks.tools.hooks import Handler, Hook


@dataclass
class Session:
    app: str
    closed: bool = False


class AuditHook(Hook[Written[object, object]]):
    def __init__(self, session: Session):
        self.session = session

    async def run(self, event: Written[object, object]) -> None:
        assert not self.session.closed
        assert event.call.kwargs["app"] == self.session.app
        assert event.data == event.call.args[0]
        assert event.result is None


class Audit(Hooks[object, object, object]):
    def __init__(self, hook: AuditHook):
        super().__init__(after_write=(Handler(hook),))
        self.session = hook.session


class Job(Direct[int, None]):
    def __init__(self, session: Session):
        async def check(event: Written[int, None]) -> None:
            assert not session.closed
            assert event.data == event.call.args[0]
            assert event.result is None

        super().__init__(hooks=Hooks(after_write=(handler(check),)))
        self.session = session

    async def read(self, id: int, *, app: str) -> int:
        assert self.session.app == app
        return id

    async def write(self, data: int) -> None:
        assert not self.session.closed


class AppProvider(Provider):
    scope = Scope.REQUEST
    job = provide(Job)
    audit = provide(Audit)
    audit_hook = provide(AuditHook)

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.sessions = []

    @provide
    async def session(self) -> AsyncIterator[Session]:
        session = Session(self.name)
        self.sessions.append(session)
        try:
            yield session
        finally:
            session.closed = True


async def test_normal_providers_share_request_scope_and_isolate_apps():
    left, right = AppProvider("left"), AppProvider("right")
    a, b = make_async_container(left), make_async_container(right)
    try:
        for _ in range(2):
            async with a() as request_a, b() as request_b:
                job_a, audit_a = (
                    await request_a.get(Job),
                    await request_a.get(Audit),
                )
                job_b, audit_b = (
                    await request_b.get(Job),
                    await request_b.get(Audit),
                )
                assert job_a.session is audit_a.session
                assert job_b.session is audit_b.session
                assert audit_a.session is not audit_b.session
                await asyncio.gather(
                    job_a._run(audit_a, (1,), {"app": "left"}),
                    job_b._run(audit_b, (2,), {"app": "right"}),
                )
            assert audit_a.session.closed
            assert audit_b.session.closed
        assert len(left.sessions) == len(right.sessions) == 2
        assert left.sessions[0] is not left.sessions[1]
    finally:
        await a.close()
        await b.close()


@pytest.mark.parametrize(
    "phase", ["after_read", "after_transform", "after_write", "on_error"]
)
@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_cancellation_in_pipeline_hooks_propagates(phase, policy):
    errors = []

    class Product(Direct[int, None]):
        async def read(self):
            if phase == "on_error":
                raise ValueError("read failed")
            return 42

        async def write(self, data):
            pass

    async def cancel(event):
        raise asyncio.CancelledError

    async def report(event: Failure):
        errors.append(event)

    attachments = {phase: (handler(cancel, failure=policy),)}
    if phase != "on_error":
        attachments["on_error"] = (handler(report),)
    with pytest.raises(asyncio.CancelledError):
        await Product(hooks=Hooks(**attachments)).run()
    assert errors == []


async def test_none_write_result_and_continued_notification_do_not_replay():
    written, notifications = [], []

    class Product(Direct[int, None]):
        async def read(self):
            return 42

        async def write(self, data):
            written.append(data)

    async def broken(event):
        assert event.result is None
        assert event.data == 42
        raise RuntimeError("notification failed")

    async def next_hook(event):
        notifications.append(event.data)

    job = Product(hooks=Hooks(after_write=(handler(next_hook),)))
    assert (
        await job._run(
            Hooks(after_write=(handler(broken, failure="continue"),)), (), {}
        )
        is None
    )
    assert notifications == written == [42]


async def test_same_shared_hooks_cover_heterogeneous_projections():
    observed = []

    async def collect(event):
        observed.append((event.data, event.result))

    class Number(Direct[int, int]):
        async def read(self):
            return 42

        async def write(self, data):
            return data

    class Text(Direct[str, None]):
        async def read(self):
            return "product"

        async def write(self, data):
            pass

    shared = Hooks(after_write=(handler(collect),))
    assert await Number()._run(shared, (), {}) == 42
    assert await Text()._run(shared, (), {}) is None
    assert observed == [(42, 42), ("product", None)]


async def test_shared_hooks_are_not_stored_on_reused_projection():
    calls = []

    class Product(Direct[int, int]):
        async def read(self, id):
            await asyncio.sleep(0)
            return id

        async def write(self, data):
            return data

    def configured(app):
        async def after(event):
            calls.append((app, event.call.args, event.data))

        return Hooks(after_read=(handler(after),))

    job = Product()
    assert await asyncio.gather(
        job._run(configured("a"), (1,), {}),
        job._run(configured("b"), (2,), {}),
    ) == [1, 2]
    assert sorted(calls) == [("a", (1,), 1), ("b", (2,), 2)]
    assert await job.run(3) == 3
    assert len(calls) == 2

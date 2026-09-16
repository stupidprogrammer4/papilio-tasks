import asyncio

import pytest
from hook_support import handler

from papilio_tasks.tools.hooks import Handler, Hook, emit


@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_hook_failure_policy_and_registration_order(policy, caplog):
    calls = []
    failure = ValueError("cannot report")

    async def first(value):
        calls.append(("first", value))
        raise failure

    async def second(value):
        calls.append(("second", value))

    hooks = (handler(first, failure=policy), handler(second))
    if policy == "raise":
        with pytest.raises(ValueError) as caught:
            await emit(hooks, 42)
        assert caught.value is failure
        assert calls == [("first", 42)]
    else:
        await emit(hooks, 42)
        assert calls == [("first", 42), ("second", 42)]
        assert caplog.records[-1].exc_info[1] is failure


@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_hook_cancellation_is_never_swallowed(policy):
    calls = []

    async def cancelled(value):
        raise asyncio.CancelledError

    async def following(value):
        calls.append(value)

    with pytest.raises(asyncio.CancelledError):
        await emit(
            (handler(cancelled, failure=policy), handler(following)), 42
        )
    assert calls == []


def test_invalid_failure_policy_is_rejected():
    async def callback(value):
        pass

    with pytest.raises(ValueError, match="failure"):
        handler(callback, failure="ignore")


def test_hook_requires_run():
    class Missing(Hook[int]):
        pass

    with pytest.raises(TypeError, match="run"):
        Missing()


async def test_same_hook_has_independent_attachment_policies():
    calls = []

    class Report(Hook[int]):
        async def run(self, event: int) -> None:
            calls.append(event)
            raise ValueError("report failed")

    report = Report()
    await emit((Handler(report, failure="continue"),), 1)
    with pytest.raises(ValueError, match="report failed"):
        await emit((Handler(report),), 2)
    assert calls == [1, 2]


async def test_next_hook_waits_for_previous_to_finish():
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def first(event):
        calls.append("first:start")
        started.set()
        await release.wait()
        calls.append("first:end")

    async def second(event):
        calls.append("second")

    task = asyncio.create_task(emit((handler(first), handler(second)), 42))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert calls == ["first:start"]
        release.set()
        await asyncio.wait_for(task, timeout=1)
        assert calls == ["first:start", "first:end", "second"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_generic_hooks_do_not_import_application_or_runtime():
    import subprocess
    import sys

    code = """
import asyncio
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'dishka', 'redis',
                   'papilio_tasks.apps.projections',
                   'papilio_tasks.tools.hooks.projection',
                   'papilio_tasks.infra'}
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.tools.hooks import Handler, Hook, emit
class Report(Hook[int]):
    async def run(self, event: int) -> None:
        assert event == 42
asyncio.run(emit((Handler(Report()),), 42))
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)

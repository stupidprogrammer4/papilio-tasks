import asyncio
import inspect
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from papilio_tasks.apps.events import Publisher, publish
from papilio_tasks.apps.events.publishers import bindings


@dataclass
class Order:
    id: int


@dataclass
class Payload:
    order_id: int


@pytest.fixture
def sender():
    class Created(Publisher[Payload]):
        pass

    owner = object()
    native = AsyncMock(return_value="native receipt")
    yield Created, owner, native
    bindings.release(owner)


async def test_result_selection_late_registration_signature_and_method(sender):
    Created, owner, native = sender
    calls = []
    expected = Order(7)

    def select(result):
        assert result is expected
        calls.append("select")
        return Payload(result.id)

    class Service:
        @publish(Created, select=select, headers={"origin": "order"})
        async def create(self, id: int, *, enabled: bool = True) -> Order:
            """Create an order."""
            assert id == 7 and enabled
            calls.append("function")
            return expected

    assert Service.create.__name__ == "create"
    assert Service.create.__doc__ == "Create an order."
    assert inspect.signature(Service.create) == inspect.signature(
        Service.create.__wrapped__
    )
    bindings.add(Created, owner, native)
    assert await Service().create(7) is expected
    assert calls == ["function", "select"]
    native.assert_awaited_once_with(Payload(7), headers={"origin": "order"})


@pytest.mark.parametrize("stage", ["function", "select", "send"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_error_and_cancellation_boundaries(sender, stage, cancel):
    Created, owner, native = sender
    bindings.add(Created, owner, native)
    error = asyncio.CancelledError() if cancel else ValueError(stage)
    selected = []

    def select(result):
        selected.append(result)
        if stage == "select":
            raise error
        return Payload(result.id)

    @publish(Created, select=select)
    async def create() -> Order:
        if stage == "function":
            raise error
        return Order(2)

    if stage == "send":
        native.side_effect = error
    with pytest.raises(type(error)) as caught:
        await create()
    assert caught.value is error
    assert native.await_count == (1 if stage == "send" else 0)
    assert len(selected) == (0 if stage == "function" else 1)


def test_reject_synchronous_function(sender):
    Created, _, _ = sender
    with pytest.raises(TypeError, match="async function"):
        publish(Created, select=lambda result: Payload(result))(lambda: 1)


async def test_missing_registration_is_reported_at_invocation(sender):
    Created, _, native = sender
    calls = []

    @publish(Created, select=lambda result: Payload(result.id))
    async def create() -> Order:
        calls.append("function")
        return Order(1)

    with pytest.raises(RuntimeError, match="not registered"):
        await create()
    assert calls == ["function"]
    native.assert_not_awaited()


def test_decorator_definition_is_pure_without_optional_dependencies():
    code = """
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'taskiq', 'dishka', 'faststream', 'dishka_faststream', 'aio_pika'
        } or fullname.startswith('papilio_tasks.infra'):
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.publishers import Publisher as PublicPublisher
from papilio_tasks.apps.events.subscribers import (
    Subscriber as PublicSubscriber,
)
assert PublicPublisher is Publisher and PublicSubscriber is Subscriber
class Created(Publisher[int]): pass
@publish(Created, select=lambda result: result)
async def create(id: int) -> int: return id
assert create.__name__ == 'create'
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_decorator_type_contract(tmp_path):
    root = Path(__file__).resolve().parents[1]
    fixture = root / "tests/typing/event_decorator.py"
    config = tmp_path / "pyrightconfig.json"
    config.write_text(
        json.dumps(
            {
                "include": [
                    os.path.relpath(fixture, tmp_path),
                    os.path.relpath(
                        root / "papilio_tasks/apps/events/publishers/base.py",
                        tmp_path,
                    ),
                ],
                "exclude": [],
                "extraPaths": [str(root)],
                "pythonVersion": "3.13",
                "typeCheckingMode": "strict",
            }
        )
    )
    result = subprocess.run(
        [sys.executable, "-m", "pyright", "-p", str(config), "--outputjson"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = json.loads(result.stdout)
    expected = {
        i
        for i, line in enumerate(fixture.read_text().splitlines())
        if "# error" in line
    }
    assert report["summary"]["errorCount"] == len(expected), report
    assert report["summary"]["warningCount"] == 0, report
    diagnostics = report["generalDiagnostics"]
    assert all(Path(d["file"]) == fixture for d in diagnostics), report
    assert {d["range"]["start"]["line"] for d in diagnostics} == expected

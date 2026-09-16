import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from hook_support import handler

from papilio_tasks.apps.projections import (
    Direct,
    Hooks,
    Projection,
    ProjectionContract,
)


async def test_single_projection_transforms_data_and_preserves_result():
    calls = []
    row = {"id": 42, "title": "Product"}
    document = {"id": 42, "name": "Product", "index": "products"}
    receipt = object()

    class Product(Projection[tuple[dict, str], dict, object]):
        async def read(self, id: int, *, index: str):
            calls.append("read")
            return row, index

        async def transform(self, data):
            record, index = data
            calls.append("transform")
            return {
                "id": record["id"],
                "name": record["title"],
                "index": index,
            }

        async def write(self, data):
            assert data == document
            calls.append("write")
            return receipt

    def hooks(scope):
        async def read(event):
            assert event.data == (row, "products")
            assert event.call.args == (42,)
            assert event.call.kwargs == {"index": "products"}
            assert event.call.projection.endswith(".Product")
            calls.append(scope + ":read")

        async def transformed(event):
            assert event.data == document
            calls.append(scope + ":transform")

        async def written(event):
            assert event.data == document
            assert event.result is receipt
            calls.append(scope + ":write")
            return "ignored"

        return Hooks(
            after_read=(handler(read),),
            after_transform=(handler(transformed),),
            after_write=(handler(written),),
        )

    job = Product(hooks=hooks("local"))
    assert (
        await job._run(hooks("shared"), (42,), {"index": "products"})
        is receipt
    )
    assert calls == [
        "read",
        "shared:read",
        "local:read",
        "transform",
        "shared:transform",
        "local:transform",
        "write",
        "shared:write",
        "local:write",
    ]
    calls.clear()
    assert await job.run(42, index="products") is receipt
    assert calls == [
        "read",
        "local:read",
        "transform",
        "local:transform",
        "write",
        "local:write",
    ]


@pytest.mark.parametrize("ids", [[42, 43], []])
async def test_batch_uses_one_read_and_write_with_identity_transform(ids):
    calls = []
    rows = [{"id": id} for id in ids]
    receipt = {"written": len(rows)}

    class Products(Direct[list[dict[str, int]], dict[str, int]]):
        async def read(self, ids: list[int], **options):
            calls.append(("read", ids, options))
            return rows

        async def write(self, data):
            assert data is rows
            calls.append("write")
            return receipt

    options = {
        "shared": "x",
        "args": 1,
        "kwargs": 2,
        "stage": "draft",
        "action": "replace",
        "data": "products",
    }
    assert await Products().run(ids=ids, **options) is receipt
    assert calls == [("read", ids, options), "write"]


@pytest.mark.parametrize("policy", ["raise", "continue"])
@pytest.mark.parametrize("scope", ["shared", "local"])
@pytest.mark.parametrize(
    "failed",
    [
        "read",
        "after_read",
        "transform",
        "after_transform",
        "write",
        "after_write",
    ],
)
async def test_failure_stops_operations_but_hooks_can_continue(
    failed, scope, policy, caplog
):
    calls, errors = [], []
    failure = ValueError("stage failed")
    stages = [
        "read",
        "after_read",
        "transform",
        "after_transform",
        "write",
        "after_write",
    ]

    def record(stage):
        calls.append(stage)
        if stage == failed:
            raise failure

    class Job(Projection[int, str, bool]):
        async def read(self, id):
            record("read")
            return id

        async def transform(self, data):
            record("transform")
            return str(data)

        async def write(self, data):
            record("write")
            return True

    def callback(stage):
        async def receive(event):
            record(stage)

        return handler(receive, failure=policy)

    async def report(event):
        errors.append(event)

    configured = Hooks(
        after_read=(callback("after_read"),),
        after_transform=(callback("after_transform"),),
        after_write=(callback("after_write"),),
        on_error=(handler(report),),
    )
    job = Job(hooks=configured if scope == "local" else None)
    shared = configured if scope == "shared" else None
    if policy == "continue" and failed.startswith("after_"):
        assert await job._run(shared, (42,), {}) is True
        assert calls == stages
        assert errors == []
        assert "stage failed" in caplog.text
    else:
        with pytest.raises(ValueError) as caught:
            await job._run(shared, (42,), {})
        assert caught.value is failure
        assert calls == stages[: stages.index(failed) + 1]
        assert len(errors) == 1
        assert errors[0].error is failure
        assert errors[0].stage == failed
        assert errors[0].call.args == (42,)


@pytest.mark.parametrize("policy", ["raise", "continue"])
async def test_error_hook_failure_keeps_original_partial_write_error(policy):
    written, errors = [], []
    failure = ValueError("second item failed")
    handler_failure = RuntimeError("error reporting failed")

    class Products(Direct[list[int], None]):
        async def read(self):
            return [42, 43]

        async def write(self, data):
            written.append(data[0])
            raise failure

    async def broken(event):
        errors.append("shared")
        assert event.error is failure
        assert event.stage == "write"
        raise handler_failure

    async def report(event):
        errors.append("local")
        assert event.error is failure

    job = Products(hooks=Hooks(on_error=(handler(report),)))
    with pytest.raises(ValueError) as caught:
        await job._run(
            Hooks(on_error=(handler(broken, failure=policy),)), (), {}
        )
    assert caught.value is failure
    assert caught.value.__cause__ is (
        handler_failure if policy == "raise" else None
    )
    assert errors == (["shared"] if policy == "raise" else ["shared", "local"])
    assert written == [42]


async def test_concurrent_runs_and_cancellation_keep_execution_state_local():
    started, release = asyncio.Event(), asyncio.Event()
    written, errors, completed = [], [], []

    class Product(Direct[int, int]):
        async def read(self, id: int):
            if id == 42:
                started.set()
                await release.wait()
            return id

        async def write(self, data):
            written.append(data)
            return data

    async def report(event):
        errors.append(event)

    async def finish(event):
        completed.append((event.call.args, event.data, event.result))

    job = Product(
        hooks=Hooks(
            on_error=(handler(report),), after_write=(handler(finish),)
        )
    )
    first = asyncio.create_task(job.run(42))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert await job.run(43) == 43
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    assert written == [43]
    assert errors == []
    release.set()
    assert await job.run(42) == 42
    assert completed == [((43,), 43, 43), ((42,), 42, 42)]


def test_old_override_api_removed():
    for cls in (Projection, ProjectionContract):
        for name in ("after_read", "after_write", "on_error", "_step"):
            assert not hasattr(cls, name)


def test_required_operations_and_optional_conversion():
    with pytest.raises(TypeError):
        Projection[int, str, bool]()

    class ReadOnly(Direct[int, None]):
        async def read(self):
            return 42

    with pytest.raises(TypeError, match="write"):
        ReadOnly()

    class WriteOnly(Direct[int, None]):
        async def write(self, data):
            pass

    with pytest.raises(TypeError, match="read"):
        WriteOnly()

    class MissingConversion(Projection[int, str, bool]):
        async def read(self):
            return 42

        async def write(self, data):
            return bool(data)

    with pytest.raises(TypeError, match="transform"):
        MissingConversion()


@pytest.mark.parametrize("values", [[], [1, 2, 3]])
async def test_converted_batch_is_passed_once_between_stages(values):
    calls = []

    class Batch(Projection[list[int], list[str], int]):
        async def read(self) -> list[int]:
            calls.append("read")
            return values

        async def transform(self, data: list[int]) -> list[str]:
            assert data is values
            calls.append("transform")
            return [str(value) for value in data]

        async def write(self, data: list[str]) -> int:
            calls.append("write")
            assert data == [str(value) for value in values]
            return len(data)

    assert await Batch().run() == len(values)
    assert calls == ["read", "transform", "write"]


def test_projection_type_contracts(tmp_path):
    root = Path(__file__).resolve().parents[1]
    fixture = root / "tests" / "typing" / "projections.py"
    config = tmp_path / "pyrightconfig.json"
    config.write_text(
        json.dumps(
            {
                "include": [
                    os.path.relpath(fixture, tmp_path),
                    os.path.relpath(
                        root / "papilio_tasks/tools/hooks", tmp_path
                    ),
                    # Keep this strict fixture on the dependency-free core.
                    # Optional Taskiq adapters follow the project type config.
                    *(
                        os.path.relpath(
                            root / "papilio_tasks/apps/projections" / name,
                            tmp_path,
                        )
                        for name in ("__init__.py", "base.py", "contracts.py")
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
        index
        for index, line in enumerate(fixture.read_text().splitlines())
        if "# error" in line
    }
    diagnostics = report["generalDiagnostics"]
    assert report["summary"]["filesAnalyzed"] == 9, report
    assert report["summary"]["errorCount"] == len(expected), report
    assert report["summary"]["warningCount"] == 0, report
    assert all(Path(d["file"]) == fixture for d in diagnostics), report
    assert {d["range"]["start"]["line"] for d in diagnostics} == expected


def test_direct_execution_requires_no_runtime_or_database_dependencies():
    code = """
import asyncio
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'taskiq_redis', 'taskiq_aio_pika', 'dishka',
                   'redis', 'aio_pika', 'sqlalchemy', 'elasticsearch',
                   'faststream', 'papilio', 'papilio_tasks.apps.schedulers',
                   'papilio_tasks.infra'}
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.apps.projections import Direct
class Product(Direct[int, int]):
    async def read(self, id: int): return id
    async def write(self, data): return data
@Product.project(select=lambda result: {'id': result})
async def save(): return 42
assert save.__name__ == 'save'
assert asyncio.run(Product().run(42)) == 42
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr

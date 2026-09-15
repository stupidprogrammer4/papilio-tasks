import inspect
import subprocess
import sys

import pytest
from pydantic import BaseModel
from taskiq import InMemoryBroker

from papilio_tasks.schedulers.infra.brokers.backends.memory import MemoryBroker
from papilio_tasks.schedulers.infra.brokers.base import Broker


async def test_broker_registers_plain_callable_without_scheduler_or_dishka():
    broker = MemoryBroker(await_inplace=True)

    async def add(a: int, b: int) -> int:
        return a + b

    task = broker.register(add, name="add")
    await broker.native.startup()
    try:
        assert (await (await task.kiq(2, 3)).get_result()).return_value == 5
        with pytest.raises(ValueError, match="already registered"):
            broker.register(add, name="add")
        with pytest.raises(ValueError, match="empty"):
            broker.register(add, name="")
        with pytest.raises(TypeError, match="async"):
            broker.register(lambda: 1, name="sync")
        broker.native.register_task(add, task_name="external")
        with pytest.raises(ValueError, match="already registered"):
            broker.register(add, name="external")
    finally:
        await broker.native.shutdown()


class Payload(BaseModel):
    value: int


async def test_native_signature_payload_and_metadata_are_preserved():
    broker = Broker(InMemoryBroker(await_inplace=True))

    async def double(payload: Payload, *, factor: int = 2) -> int:
        return payload.value * factor

    labels = {"name": "metadata", "task_name": "also metadata", "func": "x"}
    task = broker.register(double, name="double", labels=labels)
    other = broker.register(double, name="other", labels=labels)
    assert inspect.signature(task) == inspect.signature(double)
    assert task.labels == labels
    assert task.task_name == "double"
    labels["name"] = "changed"
    task.labels["func"] = "changed"
    assert other.labels == {
        "name": "metadata",
        "task_name": "also metadata",
        "func": "x",
    }
    await broker.native.startup()
    try:
        result = await (await task.kiq({"value": 3}, factor=4)).get_result()
        assert not result.is_err
        assert result.return_value == 12
    finally:
        await broker.native.shutdown()


def test_memory_does_not_offer_queue_operations():
    broker = MemoryBroker()
    assert not hasattr(broker, "add_queue")
    assert not hasattr(broker, "get_queue")

    async def run():
        pass

    with pytest.raises(TypeError, match="queue"):
        broker.register(run, name="run", queue="reports")


def test_common_imports_do_not_require_optional_backends_or_dishka():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys

class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'taskiq_aio_pika', 'taskiq_redis', 'dishka', 'papilio'
        }:
            raise ImportError(fullname)

sys.meta_path.insert(0, BlockOptional())
from papilio_tasks.schedulers.infra.brokers.base import Broker
from papilio_tasks.schedulers.infra.brokers.backends.memory import MemoryBroker
MemoryBroker()
from papilio_tasks.schedulers.infra.sources.backends.memory import MemorySource
from papilio_tasks.schedulers.infra.sources.base import Source, MutableSource
from papilio_tasks.schedulers.infra.sources.contracts.memory import (
    MemorySourceContract,
)
MemorySource()
""",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

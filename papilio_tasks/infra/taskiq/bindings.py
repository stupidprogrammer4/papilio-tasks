"""Internal class-to-task bindings for synchronous application assembly."""

from typing import Any

from taskiq import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask

_tasks: dict[type, AsyncTaskiqDecoratedTask[Any, Any]] = {}


def check(cls: type) -> None:
    if cls in _tasks:
        raise ValueError(f"Class already registered: {cls.__qualname__}")


def add(cls: type, task: AsyncTaskiqDecoratedTask[Any, Any]) -> None:
    check(cls)
    _tasks[cls] = task


def get(cls: type) -> AsyncTaskiqDecoratedTask[Any, Any]:
    try:
        return _tasks[cls]
    except KeyError:
        raise RuntimeError(
            f"Class is not included: {cls.__qualname__}"
        ) from None


def release(broker: AsyncBroker) -> None:
    for cls, task in tuple(_tasks.items()):
        if task.broker is broker:
            del _tasks[cls]

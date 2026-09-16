from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from functools import wraps
from inspect import iscoroutinefunction
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from papilio_tasks.tools.hooks import emit
from papilio_tasks.tools.hooks.projection import (
    Call,
    Data,
    Failure,
    Hooks,
    Stage,
    Written,
)

from .contracts import ProjectionContract

if TYPE_CHECKING:
    from taskiq import AsyncTaskiqTask
    from taskiq.decor import AsyncTaskiqDecoratedTask


class Projection[T, D, R](ProjectionContract[T, D, R], ABC):
    """Read T, transform to D, then write and return R.

    Write owns commit/partial-failure semantics. Hooks observe data by
    reference; the pipeline does not copy data, retry or roll back writes.
    """

    def __init__(self, *, hooks: Hooks[T, D, R] | None = None) -> None:
        self.hooks: Hooks[T, D, R] = hooks if hooks is not None else Hooks()

    @abstractmethod
    async def read(self, *args: Any, **kwargs: Any) -> T: ...

    @abstractmethod
    async def transform(self, data: T) -> D: ...

    @abstractmethod
    async def write(self, data: D) -> R: ...

    @classmethod
    def task(cls) -> AsyncTaskiqDecoratedTask[Any, R]:
        from papilio_tasks.infra.taskiq import bindings

        return bindings.get(cls)

    @classmethod
    async def enqueue(cls, *args: Any, **kwargs: Any) -> AsyncTaskiqTask[R]:
        """Send read arguments without constructing a Projection instance."""
        return await cls.task().kiq(*args, **kwargs)

    @classmethod
    def project[**P, S](
        cls, *, select: Callable[[Any], Mapping[str, Any]]
    ) -> Callable[
        [Callable[P, Awaitable[S]]], Callable[P, Coroutine[Any, Any, S]]
    ]:
        """Select keyword arguments after success, enqueue, return the result.

        Defining the decorator does not require a task runtime or registration.
        Submission happens through enqueue; this does not imply a DB commit.
        """

        def decorate(
            func: Callable[P, Awaitable[S]],
        ) -> Callable[P, Coroutine[Any, Any, S]]:
            if not iscoroutinefunction(func):
                raise TypeError("project requires an async function")

            @wraps(func)
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> S:
                result = await func(*args, **kwargs)
                await cls.enqueue(**select(result))
                return result

            return wrapped

        return decorate

    async def run(self, *args: Any, **kwargs: Any) -> R:
        """Run independently with this Projection's local hooks only."""
        return await self._run(None, args, kwargs)

    async def _run(
        self,
        shared: Hooks[object, object, object] | None,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        """Callers own resolution and scope of shared hooks."""
        common: Hooks[object, object, object] = (
            shared if shared is not None else Hooks()
        )
        local = self.hooks
        call = Call(
            projection=f"{type(self).__module__}.{type(self).__qualname__}",
            args=args,
            kwargs=MappingProxyType(kwargs.copy()),
        )
        stage: Stage = "read"
        try:
            data = await self.read(*args, **kwargs)
            stage = "after_read"
            read = Data(call, data)
            await emit(common.after_read, Data[object](call, data))
            await emit(local.after_read, read)

            stage = "transform"
            converted = await self.transform(data)
            stage = "after_transform"
            transformed = Data(call, converted)
            await emit(common.after_transform, Data[object](call, converted))
            await emit(local.after_transform, transformed)

            stage = "write"
            result = await self.write(converted)
            stage = "after_write"
            written = Written(call, converted, result)
            await emit(
                common.after_write,
                Written[object, object](call, converted, result),
            )
            await emit(local.after_write, written)
            return result
        except Exception as error:
            failure = Failure(call, stage, error)
            try:
                await emit(common.on_error, failure)
                await emit(local.on_error, failure)
            except Exception as hook_error:
                raise error from hook_error
            raise


class Direct[T, R](Projection[T, T, R]):
    """Read and write the same data type without copying or converting it."""

    async def transform(self, data: T) -> T:
        return data

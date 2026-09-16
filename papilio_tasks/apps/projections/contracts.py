from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.retry import Retry

if TYPE_CHECKING:
    from taskiq import AsyncTaskiqTask
    from taskiq.decor import AsyncTaskiqDecoratedTask


class ProjectionContract[T, D, R](Protocol):
    retry: ClassVar[Retry | None]
    publish_hooks: ClassVar[type[PublishHooks] | None]

    async def read(self, *args: Any, **kwargs: Any) -> T: ...

    async def transform(self, data: T) -> D: ...

    async def write(self, data: D) -> R: ...

    async def run(self, *args: Any, **kwargs: Any) -> R: ...

    @classmethod
    def task(cls) -> AsyncTaskiqDecoratedTask[Any, R]: ...

    @classmethod
    async def enqueue(
        cls, *args: Any, **kwargs: Any
    ) -> AsyncTaskiqTask[R]: ...

    @classmethod
    def project[**P, S](
        cls, *, select: Callable[[Any], Mapping[str, Any]]
    ) -> Callable[
        [Callable[P, Awaitable[S]]], Callable[P, Coroutine[Any, Any, S]]
    ]: ...

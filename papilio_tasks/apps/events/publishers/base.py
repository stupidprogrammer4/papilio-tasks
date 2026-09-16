from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any

from .bindings import get


class Publisher[T]:
    """Declare an event route; the application owns its runtime sender."""

    @classmethod
    async def publish(cls, event: T, **options: Any) -> Any:
        return await get(cls)(event, **options)


def publish[T, **P, R](
    publisher: type[Publisher[T]],
    *,
    select: Callable[[Any], T],
    **options: Any,
) -> Callable[
    [Callable[P, Awaitable[R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    """Publish selected output after success and return the original result."""

    def decorate(
        func: Callable[P, Awaitable[R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        if not iscoroutinefunction(func):
            raise TypeError("publish requires an async function")

        @wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            result = await func(*args, **kwargs)
            await publisher.publish(select(result), **options)
            return result

        return wrapped

    return decorate

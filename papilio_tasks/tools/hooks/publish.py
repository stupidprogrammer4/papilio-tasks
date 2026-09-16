"""In-process publication hooks; a send error does not prove non-delivery."""

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any

from .runner import Handler, emit


@dataclass(frozen=True)
class PublishCall:
    sender: str
    args: tuple[object, ...]
    kwargs: Mapping[str, object]
    meta: Mapping[str, object]


@dataclass(frozen=True)
class Published[R]:
    call: PublishCall
    result: R


@dataclass(frozen=True)
class PublishFailed:
    call: PublishCall
    error: Exception


@dataclass(frozen=True, kw_only=True)
class PublishHooks:
    after_send: tuple[Handler[Published[Any]], ...] = ()
    on_error: tuple[Handler[PublishFailed], ...] = ()


class PublishError(Exception):
    """Native publication returned, but a hook or scope cleanup failed."""

    def __init__(self, result: object) -> None:
        self.result = result
        super().__init__("Post-publication work failed")


async def publish[C, R](
    call: PublishCall,
    send: Callable[[], Awaitable[R]],
    scope: AbstractAsyncContextManager[C],
    resolve: Callable[[C], Awaitable[PublishHooks]],
) -> R:
    """Observe one send, preserving its outcome through hooks and cleanup."""
    result: R | None = None
    sent = False  # None is a valid native return, not evidence of failure.
    primary: BaseException | None = None
    try:
        async with scope as context:
            try:
                hooks = await resolve(context)
                try:
                    result = await send()
                    sent = True
                except Exception as error:
                    try:
                        await emit(hooks.on_error, PublishFailed(call, error))
                    except Exception as hook_error:
                        raise error from hook_error
                    raise
                await emit(hooks.after_send, Published(call, result))
            except BaseException as error:
                primary = error
                raise
    except Exception as error:
        failure = primary if primary is not None else error
        if sent and isinstance(failure, Exception):
            raise PublishError(result) from failure
        if failure is not error:
            raise failure from error
        raise
    return result

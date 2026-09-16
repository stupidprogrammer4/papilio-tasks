"""Observe subscriber execution, independently of sending and broker ack."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from .runner import Handler, emit


@dataclass(frozen=True)
class SubscribeCall[T]:
    subscriber: str
    data: T


@dataclass(frozen=True)
class SubscribeFailed[T]:
    call: SubscribeCall[T]
    stage: Literal["before_run", "run", "after_run"]
    error: Exception


@dataclass(frozen=True, kw_only=True)
class SubscribeHooks:
    before_run: tuple[Handler[SubscribeCall[Any]], ...] = ()
    after_run: tuple[Handler[SubscribeCall[Any]], ...] = ()
    on_error: tuple[Handler[SubscribeFailed[Any]], ...] = ()


async def run[T](
    call: SubscribeCall[T],
    execute: Callable[[T], Awaitable[None]],
    hooks: SubscribeHooks,
) -> None:
    stage: Literal["before_run", "run", "after_run"] = "before_run"
    try:
        await emit(hooks.before_run, call)
        stage = "run"
        await execute(call.data)
        stage = "after_run"
        await emit(hooks.after_run, call)
    except Exception as error:
        try:
            await emit(hooks.on_error, SubscribeFailed(call, stage, error))
        except Exception as hook_error:
            raise error from hook_error
        raise

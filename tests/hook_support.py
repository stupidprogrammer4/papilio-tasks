"""Adapt inline test assertions to the Hook.run contract."""

from collections.abc import Awaitable, Callable
from typing import Literal

from papilio_tasks.tools.hooks import Handler, Hook


class Callback[T](Hook[T]):
    def __init__(self, callback: Callable[[T], Awaitable[None]]) -> None:
        self.callback = callback

    async def run(self, event: T) -> None:
        await self.callback(event)


def handler[T](
    callback: Callable[[T], Awaitable[None]],
    failure: Literal["raise", "continue"] = "raise",
) -> Handler[T]:
    return Handler(Callback(callback), failure=failure)

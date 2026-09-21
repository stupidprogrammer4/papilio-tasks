"""Compose producer connections without starting consumers."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Protocol


class Producer(Protocol):
    async def connect(self) -> None: ...
    async def stop(self) -> None: ...


class Producers:
    """An ASGI lifespan and a connection group for worker dependencies."""

    def __init__(self, *producers: Producer) -> None:
        if len({id(item) for item in producers}) != len(producers):
            raise ValueError("A producer may only be registered once")
        self.producers = producers
        self._stack: AsyncExitStack | None = None

    async def connect(self) -> None:
        if self._stack is not None:
            raise RuntimeError("Producer group is already connected")
        async with AsyncExitStack() as stack:
            for producer in self.producers:
                stack.push_async_callback(producer.stop)
            async with asyncio.TaskGroup() as startup:
                for producer in self.producers:
                    startup.create_task(producer.connect())
            self._stack = stack.pop_all()

    async def stop(self) -> None:
        stack, self._stack = self._stack, None
        if stack is not None:
            await stack.aclose()

    @asynccontextmanager
    async def __call__(self, app: object = None) -> AsyncIterator[None]:
        await self.connect()
        try:
            yield
        finally:
            await self.stop()

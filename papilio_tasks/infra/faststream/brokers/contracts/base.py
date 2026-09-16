from typing import Protocol


class Lifecycle[C](Protocol):
    """Native broker lifecycle; C is its connection type."""

    async def connect(self) -> C: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class BrokerContract[C](Lifecycle[C], Protocol):
    @property
    def native(self) -> Lifecycle[C]: ...

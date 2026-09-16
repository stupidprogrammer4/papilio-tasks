from .contracts.base import BrokerContract, Lifecycle


class Broker[C](BrokerContract[C]):
    """Delegate lifecycle to one caller-configured native broker."""

    def __init__(self, native: Lifecycle[C]) -> None:
        self._native = native

    @property
    def native(self) -> Lifecycle[C]:
        return self._native

    async def connect(self) -> C:
        """Connect for publication without starting subscribers."""
        return await self.native.connect()

    async def start(self) -> None:
        """Connect and start the configured subscribers."""
        await self.native.start()

    async def stop(self) -> None:
        await self.native.stop()

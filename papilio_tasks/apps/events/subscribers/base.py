from abc import ABC, abstractmethod


class Subscriber[T](ABC):
    """Providers construct subscribers and their dependencies per scope."""

    @abstractmethod
    async def run(self, event: T) -> None: ...

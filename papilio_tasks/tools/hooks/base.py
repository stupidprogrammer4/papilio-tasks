from abc import ABC, abstractmethod


class Hook[T](ABC):
    """Application-owned behavior with provider-supplied dependencies."""

    @abstractmethod
    async def run(self, event: T) -> None: ...

from typing import Any

from taskiq import InMemoryBroker

from ..base import Broker


class MemoryBroker(Broker):
    """Taskiq's in-process broker for tests and local development."""

    def __init__(self, **options: Any) -> None:
        super().__init__(InMemoryBroker(**options))

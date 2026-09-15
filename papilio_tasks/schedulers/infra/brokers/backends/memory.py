"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.brokers.backends.memory import (
    MemoryBroker,
)

__all__ = ["MemoryBroker"]

"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.brokers.contracts.redis import (
    RedisStreamContract,
)

__all__ = ["RedisStreamContract"]

"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.queues.redis import (
    RedisQueue,
)

__all__ = ["RedisQueue"]

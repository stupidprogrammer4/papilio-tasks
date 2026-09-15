"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.queues.rabbit import (
    RabbitQueue,
    declare_queue,
)

__all__ = ["RabbitQueue", "declare_queue"]

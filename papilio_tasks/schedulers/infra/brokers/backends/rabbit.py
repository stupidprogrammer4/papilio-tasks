"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.brokers.backends.rabbit import (
    RabbitBroker,
)

__all__ = ["RabbitBroker"]

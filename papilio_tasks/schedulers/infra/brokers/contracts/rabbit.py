"""Compatibility imports for the shared Taskiq infrastructure."""

from papilio_tasks.infra.taskiq.brokers.contracts.rabbit import (
    RabbitContract,
)

__all__ = ["RabbitContract"]

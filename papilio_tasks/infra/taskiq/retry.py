"""Adapt retry settings and manage the worker's schedule source."""

from collections.abc import Iterable
from typing import Any

from taskiq import AsyncBroker, TaskiqEvents, TaskiqMessage, TaskiqState
from taskiq.middlewares import SimpleRetryMiddleware, SmartRetryMiddleware

from papilio_tasks.tools.retry import Retry

from .sources.contracts.base import MutableSourceContract


class _Retry(SmartRetryMiddleware):
    def make_delay(self, message: TaskiqMessage, retries: int) -> float:
        # Native `delay` also delays RabbitMQ publication, including attempt 1.
        if "retry_delay" in message.labels:
            return float(message.labels["retry_delay"])
        return super().make_delay(message, retries)


def retry_labels(
    broker: AsyncBroker,
    policy: Retry | None,
    labels: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge a class policy without overriding explicit task metadata."""
    metadata = dict(labels or {})
    if policy is None:
        return metadata
    if not isinstance(policy, Retry):
        raise TypeError("retry must be Retry or None")
    if (
        "papilio_retry_source" in broker.state
        and broker.state.papilio_retry_source is None
    ):
        raise ValueError("Select retry_source before including a retry job")
    settings = {
        "retry_on_error": True,
        "max_retries": policy.attempts,
        "retry_delay": policy.delay,
        "types_of_exceptions": policy.errors,
    }
    reserved = {"_retries", "delay"} & metadata.keys()
    if reserved:
        names = ", ".join(sorted(reserved))
        raise ValueError(
            f"Retry labels conflict with the class policy: {names}"
        )
    for key, value in settings.items():
        if key in metadata and metadata[key] != value:
            raise ValueError(
                f"Retry label conflicts with the class policy: {key}"
            )
    metadata.update(settings)
    return metadata


def validate_retry(
    broker: AsyncBroker,
    source: MutableSourceContract | None,
    policies: Iterable[Retry | None],
) -> None:
    """Check discovered and pre-included jobs before changing the broker."""
    required = False
    for policy in policies:
        if policy is None:
            continue
        if not isinstance(policy, Retry):
            raise TypeError("retry must be Retry or None")
        required = True

    if source is None:
        required = required or any(
            "retry_delay" in task.labels
            and str(task.labels.get("retry_on_error", False)).lower() == "true"
            for task in broker.local_task_registry.values()
        )
        if required:
            raise ValueError("Retry jobs require an explicit retry_source")
        return

    if not isinstance(source, MutableSourceContract):
        raise TypeError("retry_source must be a writable source")
    for middleware in broker.middlewares:
        if isinstance(
            middleware, (SimpleRetryMiddleware, SmartRetryMiddleware)
        ):
            raise ValueError("Broker already has a retry middleware")


def setup_retry(
    broker: AsyncBroker, source: MutableSourceContract | None
) -> None:
    """Install native retries; worker events own this process's source."""
    broker.state.papilio_retry_source = source
    if source is None:
        return
    broker.add_middlewares(
        _Retry(schedule_source=source.native, default_retry_label=False)
    )
    started = False

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def start_source(state: TaskiqState) -> None:
        nonlocal started
        if started or broker.is_scheduler_process:
            return
        await source.native.startup()
        started = True

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    async def stop_source(state: TaskiqState) -> None:
        nonlocal started
        if not started:
            return
        started = False
        await source.native.shutdown()

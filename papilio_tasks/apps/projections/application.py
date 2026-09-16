"""Assemble Projection tasks with application-owned providers and discovery."""

from collections.abc import Sequence

from dishka import Provider, make_async_container
from dishka.integrations.taskiq import TaskiqProvider, setup_dishka
from taskiq import AsyncBroker, TaskiqEvents, TaskiqScheduler, TaskiqState

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.retry import setup_retry, validate_retry
from papilio_tasks.infra.taskiq.sources.contracts.base import (
    MutableSourceContract,
    SourceContract,
)
from papilio_tasks.tools.bootstrap import Bootstrapper

from .base import Projection
from .registry import Registrar


def create_broker(
    *,
    registrar: Registrar | None = None,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
    retry_source: MutableSourceContract | None = None,
) -> AsyncBroker:
    """Discover projections and attach one container to the native broker.

    Providers own Projection and hook construction. Assembly opens no network
    resources. Callers own native broker startup/shutdown; shutdown also closes
    the container. Pre-included tasks are retained.
    Retry policies require an explicit writable source, also consumed by beat.
    """
    registry = registrar if registrar is not None else Registrar()
    broker = registry.broker.native
    if "papilio_container" in broker.state:
        raise ValueError("Broker already has a Papilio application")

    try:
        classes = Bootstrapper(modules).classes("projections", Projection)
        validate_retry(broker, retry_source, (cls.retry for cls in classes))
        for cls in classes:
            registry.include(cls)
        container = make_async_container(TaskiqProvider(), *providers)
        setup_dishka(container, broker)
        broker.state.papilio_container = container

        @broker.on_event(
            TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN
        )
        async def close_container(state: TaskiqState) -> None:
            try:
                await container.close()
            finally:
                bindings.release(broker)

        setup_retry(broker, retry_source)
    except Exception:
        bindings.release(broker)
        raise

    return broker


def create_beat(
    broker: AsyncBroker, *, sources: Sequence[SourceContract]
) -> TaskiqScheduler:
    """Build native beat with caller-selected sources; open no resources.

    Include the broker's retry_source explicitly. In a separate beat process,
    construct a source pointing at the same shared store, then assemble there.
    """
    native_sources = [source.native for source in sources]
    if "papilio_retry_source" in broker.state:
        retry_source = broker.state.papilio_retry_source
        if retry_source is not None and not any(
            source is retry_source.native for source in native_sources
        ):
            raise ValueError("Beat sources must include the retry_source")
    return TaskiqScheduler(broker, sources=native_sources)

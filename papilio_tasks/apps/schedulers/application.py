"""Assemble scheduler applications from classes and ordinary providers."""

from collections.abc import Sequence
from typing import Any

from dishka import Provider, make_async_container
from dishka.integrations.taskiq import TaskiqProvider, setup_dishka
from taskiq import AsyncBroker, TaskiqEvents, TaskiqScheduler, TaskiqState

from papilio_tasks.infra.taskiq.sources.contracts.base import SourceContract
from papilio_tasks.tools.bootstrap import Bootstrapper

from .base import Scheduler
from .registry import Registrar


def create_broker(
    *,
    registrar: Registrar[Any] | None = None,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
) -> AsyncBroker:
    """Register discovered schedulers and wire per-job Dishka injection.

    Providers own scheduler construction and scope. Assembly performs no
    network I/O. The returned native broker owns application startup/shutdown.
    """
    registry = registrar if registrar is not None else Registrar()
    broker = registry.broker.native
    if "papilio_container" in broker.state:
        raise ValueError("Broker already has a Papilio application")

    classes = Bootstrapper(modules).classes("schedulers", Scheduler)
    container = make_async_container(TaskiqProvider(), *providers)
    for cls in classes:
        registry.include(cls)
    setup_dishka(container, broker)
    broker.state.papilio_container = container

    @broker.on_event(
        TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN
    )
    async def close_container(state: TaskiqState) -> None:
        await container.close()

    return broker


def create_beat(
    broker: AsyncBroker, *, sources: Sequence[SourceContract]
) -> TaskiqScheduler:
    """Reuse an assembled broker and caller-selected schedule sources.

    The native scheduler CLI owns source startup/shutdown. Construction opens
    no resources and does not create instances or register schedulers again.
    """
    return TaskiqScheduler(
        broker, sources=[source.native for source in sources]
    )

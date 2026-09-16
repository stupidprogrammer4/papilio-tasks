"""Assemble Projection tasks with application-owned providers and discovery."""

from collections.abc import Sequence

from dishka import Provider, make_async_container
from dishka.integrations.taskiq import TaskiqProvider, setup_dishka
from taskiq import AsyncBroker, TaskiqEvents, TaskiqState

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.tools.bootstrap import Bootstrapper

from .base import Projection
from .registry import Registrar


def create_broker(
    *,
    registrar: Registrar | None = None,
    providers: Sequence[Provider] = (),
    modules: Sequence[str] = (),
) -> AsyncBroker:
    """Discover projections and attach one container to the native broker.

    Providers own Projection and hook construction. Assembly opens no network
    resources. Callers own native broker startup/shutdown; shutdown also closes
    the container. Pre-included tasks are retained.
    """
    registry = registrar if registrar is not None else Registrar()
    broker = registry.broker.native
    if "papilio_container" in broker.state:
        raise ValueError("Broker already has a Papilio application")

    try:
        classes = Bootstrapper(modules).classes("projections", Projection)
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
    except Exception:
        bindings.release(broker)
        raise

    return broker

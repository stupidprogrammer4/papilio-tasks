"""Resolve producer hooks around one native publication."""

from types import MappingProxyType
from typing import Any

from dishka import AsyncContainer
from taskiq import AsyncTaskiqTask

from papilio_tasks.tools.hooks.publish import (
    PublishCall,
    PublishHooks,
    publish,
)

from .contracts import ProjectionContract


async def enqueue[T, D, R](
    cls: type[ProjectionContract[T, D, R]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> AsyncTaskiqTask[R]:
    task = cls.task()
    state = task.broker.state
    shared_type = (
        state.papilio_publish_hooks
        if "papilio_publish_hooks" in state
        else None
    )
    local_type = cls.publish_hooks
    if shared_type is None and local_type is None:
        return await task.kiq(*args, **kwargs)
    if local_type is not None and (
        not isinstance(local_type, type)
        or not issubclass(local_type, PublishHooks)
    ):
        raise TypeError("publish_hooks must be a PublishHooks class or None")
    if "papilio_container" not in state:
        raise RuntimeError("Publication hooks require create_broker providers")

    container: AsyncContainer = state.papilio_container
    call = PublishCall(
        f"{cls.__module__}.{cls.__qualname__}",
        args,
        MappingProxyType(kwargs.copy()),
        MappingProxyType({"task_name": task.task_name}),
    )

    async def resolve(scope: AsyncContainer) -> PublishHooks:
        shared = (
            await scope.get(shared_type)
            if shared_type is not None
            else PublishHooks()
        )
        local = (
            await scope.get(local_type)
            if local_type is not None
            else PublishHooks()
        )
        return PublishHooks(
            after_send=shared.after_send + local.after_send,
            on_error=shared.on_error + local.on_error,
        )

    async def send() -> AsyncTaskiqTask[R]:
        return await task.kiq(*args, **kwargs)

    return await publish(call, send, container(), resolve)

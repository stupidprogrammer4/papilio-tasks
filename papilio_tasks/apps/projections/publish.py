"""Resolve producer hooks around one native publication."""

from types import MappingProxyType
from typing import Any

from dishka import AsyncContainer
from taskiq import AsyncTaskiqTask

from papilio_tasks.tools.hooks import emit
from papilio_tasks.tools.hooks.publish import (
    PublishCall,
    Published,
    PublishError,
    PublishFailed,
    PublishHooks,
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
        task.task_name,
        args,
        MappingProxyType(kwargs.copy()),
    )
    result: AsyncTaskiqTask[R] | None = None
    primary: BaseException | None = None
    try:
        async with container() as scope:
            try:
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
                try:
                    result = await task.kiq(*args, **kwargs)
                except Exception as error:
                    failure = PublishFailed(call, error)
                    try:
                        await emit(shared.on_error, failure)
                        await emit(local.on_error, failure)
                    except Exception as hook_error:
                        raise error from hook_error
                    raise
                published = Published(call, result.task_id)
                await emit(shared.after_send, published)
                await emit(local.after_send, published)
            except BaseException as error:
                # Keep send/setup errors and cancellation if scope exit fails.
                primary = error
                raise
    except Exception as error:
        failure = primary if primary is not None else error
        if result is not None and isinstance(failure, Exception):
            raise PublishError(result.task_id) from failure
        if failure is not error:
            raise failure from error
        raise
    return result

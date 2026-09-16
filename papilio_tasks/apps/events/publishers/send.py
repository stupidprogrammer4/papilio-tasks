"""Provide app-wide producer hooks around the registered native sender."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from dishka import AsyncContainer

from papilio_tasks.tools.hooks.publish import (
    PublishCall,
    PublishHooks,
    publish,
)

from .contracts import Sender


async def send(
    cls: type,
    event: Any,
    options: dict[str, Any],
    sender: Sender,
    *,
    container: AsyncContainer | None,
    hooks: type[PublishHooks] | None,
    meta: Mapping[str, object],
) -> Any:
    if hooks is None:
        return await sender(event, **options)
    if container is None:
        raise RuntimeError("Publication hooks require create_app providers")
    call = PublishCall(
        f"{cls.__module__}.{cls.__qualname__}",
        (event,),
        MappingProxyType(options.copy()),
        meta,
    )

    async def resolve(scope: AsyncContainer) -> PublishHooks:
        return await scope.get(hooks)

    async def native() -> Any:
        return await sender(event, **options)

    return await publish(call, native, container(), resolve)

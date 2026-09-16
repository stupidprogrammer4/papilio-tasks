"""Run resolved hooks with the policy selected for each attachment."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from .base import Hook

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Handler[T]:
    hook: Hook[T]
    failure: Literal["raise", "continue"] = "raise"

    def __post_init__(self) -> None:
        if self.failure not in ("raise", "continue"):
            raise ValueError("Hook failure must be 'raise' or 'continue'")


async def emit[T](handlers: Sequence[Handler[T]], event: T) -> None:
    # Explicit exception to the no-await-in-loops rule: hooks must finish in
    # registration order, and a failure may prevent the next hook from running.
    for handler in handlers:
        try:
            await handler.hook.run(event)
        except Exception:
            if handler.failure == "raise":
                raise
            logger.exception("Hook failed; continuing: %r", handler.hook)

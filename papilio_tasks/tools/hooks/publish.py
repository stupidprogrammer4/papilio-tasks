"""In-process publication hooks; a send error does not prove non-delivery."""

from collections.abc import Mapping
from dataclasses import dataclass

from .runner import Handler


@dataclass(frozen=True)
class PublishCall:
    projection: str
    task_name: str
    args: tuple[object, ...]
    kwargs: Mapping[str, object]


@dataclass(frozen=True)
class Published:
    call: PublishCall
    task_id: str


@dataclass(frozen=True)
class PublishFailed:
    call: PublishCall
    error: Exception


@dataclass(frozen=True, kw_only=True)
class PublishHooks:
    after_send: tuple[Handler[Published], ...] = ()
    on_error: tuple[Handler[PublishFailed], ...] = ()


class PublishError(Exception):
    """Native publication returned, but a hook or scope cleanup failed."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"Post-publication work failed for task {task_id}")

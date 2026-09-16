"""Typed, in-process payloads and explicit Projection hook attachments."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from .runner import Handler

type Stage = Literal[
    "read",
    "after_read",
    "transform",
    "after_transform",
    "write",
    "after_write",
]


@dataclass(frozen=True)
class Call:
    projection: str
    args: tuple[object, ...]
    kwargs: Mapping[str, object]


@dataclass(frozen=True)
class Data[T]:
    call: Call
    data: T


@dataclass(frozen=True)
class Written[D, R]:
    call: Call
    data: D
    result: R


@dataclass(frozen=True)
class Failure:
    call: Call
    stage: Stage
    error: Exception


@dataclass(frozen=True, kw_only=True)
class Hooks[T, D, R]:
    after_read: tuple[Handler[Data[T]], ...] = ()
    after_transform: tuple[Handler[Data[D]], ...] = ()
    after_write: tuple[Handler[Written[D, R]], ...] = ()
    on_error: tuple[Handler[Failure], ...] = ()

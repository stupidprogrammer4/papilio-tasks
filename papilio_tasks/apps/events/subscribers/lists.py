from typing import Any, ClassVar

from faststream.redis import ListSub

from ..publishers.lists import ListPublisher
from .base import Subscriber

__all__ = ["ListSub", "ListSubscriber"]


class ListSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[ListPublisher[Any]]]
    list: ClassVar[ListSub | None] = None

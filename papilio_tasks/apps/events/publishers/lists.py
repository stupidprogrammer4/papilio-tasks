from typing import ClassVar

from .base import Publisher


class ListPublisher[T](Publisher[T]):
    list: ClassVar[str]

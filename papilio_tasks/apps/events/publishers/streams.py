from typing import ClassVar

from .base import Publisher


class StreamPublisher[T](Publisher[T]):
    stream: ClassVar[str]

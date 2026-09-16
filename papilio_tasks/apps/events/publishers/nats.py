from typing import ClassVar

from .base import Publisher


class NatsPublisher[T](Publisher[T]):
    subject: ClassVar[str]

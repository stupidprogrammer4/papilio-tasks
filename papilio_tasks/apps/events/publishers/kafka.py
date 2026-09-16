from typing import ClassVar

from .base import Publisher


class KafkaPublisher[T](Publisher[T]):
    topic: ClassVar[str]

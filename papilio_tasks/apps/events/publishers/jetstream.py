from typing import ClassVar

from faststream.nats import JStream

from .base import Publisher


class JetPublisher[T](Publisher[T]):
    subject: ClassVar[str]
    stream: ClassVar[JStream]

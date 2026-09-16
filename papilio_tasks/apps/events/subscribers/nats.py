from typing import Any, ClassVar

from ..publishers.nats import NatsPublisher
from .base import Subscriber


class NatsSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[NatsPublisher[Any]]]
    subject: ClassVar[str | None] = None
    queue: ClassVar[str] = ""

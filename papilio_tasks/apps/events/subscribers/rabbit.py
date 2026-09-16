from typing import Any, ClassVar

from faststream import AckPolicy
from faststream.rabbit import RabbitQueue

from ..publishers.rabbit import RabbitPublisher
from .base import Subscriber

__all__ = ["RabbitQueue", "RabbitSubscriber"]


class RabbitSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[RabbitPublisher[Any]]]
    queue: ClassVar[RabbitQueue]
    ack_policy: ClassVar[AckPolicy | None] = None

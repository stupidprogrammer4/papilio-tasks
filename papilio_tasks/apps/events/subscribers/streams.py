from typing import Any, ClassVar

from faststream import AckPolicy
from faststream.redis import StreamSub

from ..publishers.streams import StreamPublisher
from .base import Subscriber

__all__ = ["StreamSub", "StreamSubscriber"]


class StreamSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[StreamPublisher[Any]]]
    stream: ClassVar[StreamSub]
    ack_policy: ClassVar[AckPolicy | None] = None

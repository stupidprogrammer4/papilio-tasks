from typing import Any, ClassVar

from faststream.redis import PubSub

from ..publishers.channels import ChannelPublisher
from .base import Subscriber

__all__ = ["ChannelSubscriber", "PubSub"]


class ChannelSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[ChannelPublisher[Any]]]
    channel: ClassVar[PubSub | None] = None

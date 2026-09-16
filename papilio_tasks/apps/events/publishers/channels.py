from typing import ClassVar

from .base import Publisher


class ChannelPublisher[T](Publisher[T]):
    channel: ClassVar[str]

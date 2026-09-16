from typing import Any, ClassVar

from faststream import AckPolicy
from faststream.nats import PullSub
from nats.js.api import ConsumerConfig

from ..publishers.jetstream import JetPublisher
from .base import Subscriber


class JetSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[JetPublisher[Any]]]
    subject: ClassVar[str | None] = None
    queue: ClassVar[str] = ""
    durable: ClassVar[str | None] = None
    pull_sub: ClassVar[bool | PullSub] = False
    config: ClassVar[ConsumerConfig | None] = None
    ack_policy: ClassVar[AckPolicy | None] = None

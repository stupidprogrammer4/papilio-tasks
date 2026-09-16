from typing import Any, ClassVar

from faststream import AckPolicy

from ..publishers.kafka import KafkaPublisher
from .base import Subscriber


class KafkaSubscriber[T](Subscriber[T]):
    publisher: ClassVar[type[KafkaPublisher[Any]]]
    group_id: ClassVar[str]
    ack_policy: ClassVar[AckPolicy | None] = None

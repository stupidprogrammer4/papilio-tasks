from collections.abc import Awaitable, Callable
from typing import Any

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq_aio_kafka import AioKafkaBroker

from ..base import Broker
from ..contracts.kafka import KafkaContract


class KafkaBroker(Broker, KafkaContract):
    """Register tasks and select native publication topics.

    Configure declarations and worker topics on the supplied AioKafkaBroker.
    Selecting a publication topic does not change worker subscriptions.
    """

    _native: AioKafkaBroker

    def __init__(self, native: AioKafkaBroker) -> None:
        if not isinstance(native, AioKafkaBroker):
            raise TypeError("Expected a native AioKafkaBroker")
        super().__init__(native)

    @property
    def native(self) -> AioKafkaBroker:
        return self._native

    def register(
        self,
        task: Callable[..., Awaitable[Any]],
        *,
        name: str,
        labels: dict[str, Any] | None = None,
        topic: str | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        metadata = dict(labels or {})
        if topic is not None:
            if not isinstance(topic, str) or not topic:
                raise ValueError("Topic must be a nonempty string")
            key = self.native.task_topic_label
            if key in metadata and metadata[key] != topic:
                raise ValueError("Routing label conflicts with the topic")
            metadata[key] = topic
        return super().register(task, name=name, labels=metadata)

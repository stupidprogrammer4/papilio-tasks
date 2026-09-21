"""Shared native Taskiq application configuration and lifecycle."""

from dataclasses import dataclass

from taskiq import AsyncBroker, TaskiqEvents, TaskiqScheduler, TaskiqState

from .lifecycle import Producers


@dataclass(frozen=True, kw_only=True)
class RedisSettings:
    url: str
    queue_name: str
    schedule_prefix: str
    consumer_group: str = "taskiq"
    consumer_id: str = "0"
    result_prefix: str = "taskiq_result"
    result_ttl: int = 86400

    def __post_init__(self) -> None:
        if not all((self.url, self.queue_name, self.schedule_prefix)):
            raise ValueError(
                "Redis URL, queue and schedule prefix are required"
            )
        if self.result_ttl <= 0:
            raise ValueError("Result TTL must be positive")


@dataclass(frozen=True)
class Application:
    """Expose native worker/beat objects and a producer connection."""

    broker: AsyncBroker
    scheduler: TaskiqScheduler

    async def connect(self) -> None:
        await self.broker.startup()

    async def stop(self) -> None:
        await self.broker.shutdown()


def worker_producers(broker: AsyncBroker, producers: Producers) -> None:
    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def start(state: TaskiqState) -> None:
        await producers.connect()

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    async def stop(state: TaskiqState) -> None:
        await producers.stop()

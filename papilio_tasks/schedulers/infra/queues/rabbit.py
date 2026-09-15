from typing import Any

from aio_pika.abc import AbstractChannel, AbstractQueue
from taskiq_aio_pika import Queue as RabbitQueue

__all__ = ["RabbitQueue", "declare_queue"]


async def declare_queue(
    channel: AbstractChannel,
    queue: RabbitQueue,
    *,
    exchange: str,
    dead_letter: str,
    delayed_exchange: str | None = None,
) -> AbstractQueue:
    """Declare a task queue with the same defaults as taskiq-aio-pika."""
    if not queue.name:
        raise ValueError("Queue name cannot be empty")
    if not queue.declare:
        return await channel.get_queue(queue.name, ensure=True)
    arguments: dict[str, Any] = {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter,
        "x-queue-type": queue.type.value,
    }
    if queue.max_priority is not None:
        arguments["x-max-priority"] = queue.max_priority
    arguments.update(queue.arguments)
    declared = await channel.declare_queue(
        name=queue.name,
        durable=queue.durable,
        exclusive=queue.exclusive,
        passive=queue.passive,
        auto_delete=queue.auto_delete,
        arguments=arguments,
        timeout=queue.timeout,
    )
    await declared.bind(
        exchange,
        routing_key=queue.routing_key or queue.name,
        arguments=queue.bind_arguments,
        timeout=queue.bind_timeout,
    )
    if delayed_exchange is not None:
        await declared.bind(
            delayed_exchange, routing_key=queue.routing_key or queue.name
        )
    return declared

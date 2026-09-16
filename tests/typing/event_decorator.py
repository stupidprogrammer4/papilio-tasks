from dataclasses import dataclass
from typing import assert_type

from papilio_tasks.apps.events import Publisher, publish


@dataclass
class Order:
    id: int


@dataclass
class Payload:
    order_id: int


class Created(Publisher[Payload]):
    pass


@publish(Created, select=lambda result: Payload(result.id))
async def create(id: int, *, name: str = "") -> Order:
    return Order(id)


class Service:
    @publish(Created, select=lambda result: Payload(result.id))
    async def create(self, id: int) -> Order:
        return Order(id)


async def check() -> None:
    assert_type(await create(1, name="order"), Order)
    assert_type(await Service().create(1), Order)
    await create("wrong")  # error
    await create(1, unknown=True)  # error
    await Service().create("wrong")  # error


@publish(Created, select=lambda result: "wrong")  # error
async def invalid_payload() -> Order:
    return Order(1)

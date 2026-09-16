"""Positive publication types and deliberately rejected calls."""

from dataclasses import dataclass
from typing import assert_type

from taskiq import AsyncTaskiqTask

from papilio_tasks.apps.projections import Direct


@dataclass
class Saved:
    id: int


class Product(Direct[int, str]):
    async def read(self, id: int) -> int:
        return id

    async def write(self, data: int) -> str:
        return str(data)


async def check() -> None:
    @Product.project(select=lambda result: {"id": result.id})
    async def create(title: str, *, active: bool = True) -> Saved:
        return Saved(1)

    assert_type(await create("a"), Saved)
    assert_type(await Product.enqueue(id=1), AsyncTaskiqTask[str])

    class Service:
        @Product.project(select=lambda result: {"id": result.id})
        async def save(self, title: str) -> Saved:
            return Saved(2)

    assert_type(await Service().save("b"), Saved)
    await create(123)  # error
    await create("a", active="yes")  # error
    await Service().save(3)  # error
    Product.project(select=lambda result: 1)  # error

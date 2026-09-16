"""Positive and deliberately invalid cases for the public Projection API."""

from typing import assert_type

from papilio_tasks.apps.projections import (
    Data,
    Direct,
    Failure,
    Hooks,
    Projection,
    ProjectionContract,
    Written,
)
from papilio_tasks.tools.hooks import Handler, Hook


class Converted(Projection[int, str, bool]):
    async def read(self, id: int) -> int:
        return id

    async def transform(self, data: int) -> str:
        return str(data)

    async def write(self, data: str) -> bool:
        return bool(data)


class Same(Direct[int, bool]):
    async def read(self, id: int) -> int:
        return id

    async def write(self, data: int) -> bool:
        return bool(data)


class Batch(Direct[list[int], None]):
    async def read(self, ids: list[int]) -> list[int]:
        return ids

    async def write(self, data: list[int]) -> None:
        pass


async def valid() -> None:
    job = Converted()
    assert_type(await job.read(42), int)
    assert_type(await job.transform(42), str)
    assert_type(await job.write("42"), bool)
    assert_type(await job.run(42), bool)
    assert_type(await Same().run(42), bool)
    assert_type(await Batch().run([1, 2]), None)


def compatible() -> ProjectionContract[int, str, bool]:
    return Converted()


def direct_contract() -> ProjectionContract[int, int, bool]:
    return Same()


class BadRead(Converted):
    async def read(self, id: int) -> str:  # error
        return str(id)


class BadTransform(Converted):
    async def transform(self, data: int) -> int:  # error
        return data


class BadWrite(Converted):
    async def write(self, data: int) -> bool:  # error
        return bool(data)


class BadDirect(Same):
    async def transform(self, data: int) -> str:  # error
        return str(data)


class Missing(Projection[int, str, bool]):
    async def read(self, id: int) -> int:
        return id

    async def write(self, data: str) -> bool:
        return bool(data)


Missing()  # error


async def wrong_result() -> str:
    return await Same().run(42)  # error


class ReadHook(Hook[Data[int]]):
    async def run(self, event: Data[int]) -> None:
        assert_type(event.data, int)


class WriteHook(Hook[Written[str, bool]]):
    async def run(self, event: Written[str, bool]) -> None:
        assert_type(event.data, str)
        assert_type(event.result, bool)


class ErrorHook(Hook[Failure]):
    async def run(self, event: Failure) -> None:
        assert_type(event.error, Exception)


class SharedRead(Hook[Data[object]]):
    async def run(self, event: Data[object]) -> None:
        assert_type(event.data, object)


local = Hooks[int, str, bool](
    after_read=(Handler(ReadHook()),),
    after_write=(Handler(WriteHook()),),
    on_error=(Handler(ErrorHook()),),
)
Converted(hooks=local)
Hooks[object, object, object](after_read=(Handler(SharedRead()),))
Hooks[str, str, bool](after_read=(Handler(ReadHook()),))  # error
Hooks[int, int, bool](after_write=(Handler(WriteHook()),))  # error
Hooks[object, object, object](after_read=(Handler(ReadHook()),))  # error
Same(hooks=local)  # error


class BadHook(ReadHook):
    async def run(self, event: Data[str]) -> None:  # error
        pass


class MissingHook(Hook[Failure]):
    pass


MissingHook()  # error


async def plain(event: Data[int]) -> None:
    pass


Handler[Data[int]](plain)  # error

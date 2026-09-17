import asyncio
import weakref
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import pytest
from taskiq import (
    InMemoryBroker,
    ScheduledTask,
    TaskiqMiddleware,
    TaskiqScheduler,
)
from taskiq.cli.scheduler.run import SchedulerLoop
from taskiq.exceptions import ScheduledTaskCancelledError

from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource


def schedule(**changes):
    values = dict(
        schedule_id="report",
        task_name="report",
        task_id="delivery",
        args=[{"ids": [1]}],
        kwargs={"meta": {"ids": [2]}},
        labels={"tag": {"ids": [3]}},
        cron="* * * * *",
    )
    values.update(changes)
    return ScheduledTask(**values)


async def test_public_reads_isolate_nested_input_and_output():
    original = schedule()
    source = MemorySource([original])
    original.args[0]["ids"].append(9)
    original.kwargs["meta"]["ids"].append(9)
    original.labels["tag"]["ids"].append(9)
    assert await source.get_schedule("report") == schedule()
    result = (await source.get_schedules())[0]
    result.args[0]["ids"].append(8)
    result.kwargs["meta"]["ids"].append(8)
    result.labels["tag"]["ids"].append(8)
    assert await source.get_schedule("report") == schedule()
    await source.replace_schedule(result)
    result.args[0]["ids"].append(7)
    assert (await source.get_schedule("report")).args == [{"ids": [1, 8]}]


class Payload:
    copies = 0

    def __deepcopy__(self, memo):
        type(self).copies += 1
        return type(self)()


async def test_refresh_skips_payload_and_dispatch_copies_only_due_job():
    source = MemorySource(
        [schedule(schedule_id=str(i), args=[Payload()]) for i in range(20)]
    )
    Payload.copies = 0
    first = await source.native.get_schedules()
    again = await source.native.get_schedules()
    assert len(first) == 20 and Payload.copies == 0
    assert first is not again
    assert all(a is b for a, b in zip(first, again, strict=True))
    assert all(
        not task.args and not task.kwargs and not task.labels for task in first
    )
    source.native.pre_send(first[0])
    assert Payload.copies == 1
    assert isinstance(first[0].args[0], Payload)
    await source.native.get_schedules()
    assert Payload.copies == 1


@pytest.mark.parametrize(
    "timing",
    [
        {"cron": "0 12 * * *", "cron_offset": timedelta(hours=3)},
        {"cron": None, "interval": timedelta(seconds=10)},
        {"cron": None, "time": datetime(2030, 1, 1, tzinfo=UTC)},
    ],
)
async def test_native_feed_preserves_timing_and_delivery_identity(timing):
    original = schedule(**timing)
    source = MemorySource([original])
    feed = (await source.native.get_schedules())[0]
    for field in (
        "task_name",
        "task_id",
        "schedule_id",
        "cron",
        "cron_offset",
        "interval",
        "time",
    ):
        assert getattr(feed, field) == getattr(original, field)


@pytest.mark.parametrize("change", ["replace", "delete", "readd"])
async def test_stale_native_feed_cannot_send_after_mutation(change):
    original = schedule()
    source = MemorySource([original])
    stale = (await source.native.get_schedules())[0]
    if change == "replace":
        await source.replace_schedule(original)
    else:
        await source.delete_schedule("report")
        if change == "readd":
            await source.add_schedule(original)
    with pytest.raises(ScheduledTaskCancelledError):
        source.native.pre_send(stale)
    if change != "delete":
        fresh = (await source.native.get_schedules())[0]
        source.native.pre_send(fresh)
        assert fresh.args == original.args


async def test_failed_replacement_preserves_both_stored_data_and_feed():
    source = MemorySource([schedule()])
    feed = (await source.native.get_schedules())[0]

    class Broken:
        def __deepcopy__(self, memo):
            raise ValueError("Cannot copy payload")

    with pytest.raises(ValueError, match="Cannot copy payload"):
        await source.replace_schedule(schedule(args=[Broken()]))
    assert await source.get_schedule("report") == schedule()
    source.native.pre_send(feed)
    assert feed.args == schedule().args


async def test_repeat_dispatch_retains_only_latest_payload_copy():
    source = MemorySource([schedule(args=[Payload()])])
    feed = (await source.native.get_schedules())[0]
    source.native.pre_send(feed)
    previous = weakref.ref(feed.args[0])
    # Synchronous sends isolate retention from broker-owned history.
    for _ in range(100):
        source.native.pre_send(feed)
        source.native.post_send(feed)
    assert previous() is None
    current = weakref.ref(feed.args[0])
    await source.delete_schedule("report")
    del feed
    assert current() is None
    assert await source.native.get_schedules() == []


@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError])
async def test_native_send_isolates_mutations_and_resets_after_failure(
    failure,
):
    class Mutate(TaskiqMiddleware):
        fail = failure

        def pre_send(self, message):
            message.args[0]["ids"].append(4)
            message.kwargs["meta"]["ids"].append(5)
            if self.fail is not None:
                raise self.fail()
            return message

    middleware = Mutate()
    broker = InMemoryBroker(await_inplace=True).with_middlewares(middleware)
    received = []

    @broker.task(task_name="report")
    async def report(value, meta):
        received.append((value["ids"][:], meta["ids"][:]))

    original = schedule(labels={"tag": "original"})
    source = MemorySource([original])
    beat = TaskiqScheduler(broker, [source.native])
    feed = (await source.native.get_schedules())[0]
    await broker.startup()
    try:
        if failure is not None:
            with pytest.raises(failure):
                await beat.on_ready(source.native, feed)
            assert received == []
            assert await source.get_schedule("report") == original
            middleware.fail = None
        await beat.on_ready(source.native, feed)
        await beat.on_ready(source.native, feed)
        assert received == [([1, 4], [2, 5]), ([1, 4], [2, 5])]
        assert await source.get_schedule("report") == original
    finally:
        await broker.shutdown()


async def test_real_beat_loop_dispatches_one_shot_and_preserves_future_job():
    broker = InMemoryBroker(await_inplace=True)
    received = []
    sent = asyncio.Event()

    @broker.task(task_name="report")
    async def report(value, meta):
        received.append((value, meta))

    now = datetime.now(UTC)
    due = schedule(cron=None, time=now - timedelta(seconds=1))
    future = schedule(
        schedule_id="later", cron=None, time=now + timedelta(days=1)
    )
    source = MemorySource([due, future])

    class Observed(TaskiqScheduler):
        async def on_ready(self, native, task):
            await super().on_ready(native, task)
            sent.set()

    beat = Observed(broker, [source.native])
    loop = SchedulerLoop(beat)
    await broker.startup()
    running = asyncio.create_task(
        loop.run(loop_interval=timedelta(milliseconds=10))
    )
    try:
        async with asyncio.timeout(3):
            await sent.wait()
        assert received == [({"ids": [1]}, {"ids": [2]})]
        assert await source.get_schedules() == [future]
        assert [
            task.schedule_id for task in await source.native.get_schedules()
        ] == ["later"]
    finally:
        running.cancel()
        with suppress(asyncio.CancelledError):
            await running
        await broker.shutdown()


@pytest.mark.parametrize("change", ["replace", "delete"])
async def test_edit_during_send_does_not_change_in_flight_payload(change):
    entered = asyncio.Event()
    resume = asyncio.Event()

    class Pause(TaskiqMiddleware):
        async def pre_send(self, message):
            entered.set()
            await resume.wait()
            return message

    broker = InMemoryBroker(await_inplace=True).with_middlewares(Pause())
    received = []

    @broker.task(task_name="report")
    async def report(value, meta):
        received.append(value)

    original = schedule(cron=None, time=datetime.now(UTC))
    replacement = schedule(args=[{"ids": [9]}])
    source = MemorySource([original])
    beat = TaskiqScheduler(broker, [source.native])
    old = (await source.native.get_schedules())[0]
    await broker.startup()
    sending = asyncio.create_task(beat.on_ready(source.native, old))
    try:
        async with asyncio.timeout(3):
            await entered.wait()
            if change == "replace":
                await source.replace_schedule(replacement)
            else:
                await source.delete_schedule("report")
            resume.set()
            await sending
        assert received == [{"ids": [1]}]
        expected = [replacement] if change == "replace" else []
        assert await source.get_schedules() == expected
        with pytest.raises(ScheduledTaskCancelledError):
            source.native.pre_send(old)
    finally:
        sending.cancel()
        with suppress(asyncio.CancelledError):
            await sending
        await broker.shutdown()

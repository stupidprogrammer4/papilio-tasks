import importlib
import subprocess
import sys
from types import SimpleNamespace

import pytest
from taskiq.schedule_sources import LabelScheduleSource

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.schedulers import Scheduler
from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.tools.retry import Retry


@pytest.fixture(params=["nats", "kafka"])
def backend(request):
    kind = request.param
    # Optional integrations must remain independently importable.
    native_module = pytest.importorskip(
        "taskiq_nats" if kind == "nats" else "taskiq_aio_kafka"
    )
    infra = importlib.import_module(
        "papilio_tasks.infra.taskiq.brokers.backends." + kind
    )
    scheduler = importlib.import_module(
        "papilio_tasks.apps.schedulers.backends." + kind
    )
    projection = importlib.import_module(
        "papilio_tasks.apps.projections.backends." + kind
    )
    scheduler_registry = importlib.import_module(
        "papilio_tasks.apps.schedulers.registry." + kind
    )
    projection_registry = importlib.import_module(
        "papilio_tasks.apps.projections.registry." + kind
    )
    if kind == "nats":
        native = native_module.PullBasedJetStreamBroker(
            "nats://localhost:4222", subject="reports", durable="reports"
        )
        adapter = infra.JetStreamBroker
        prefix = "Nats"
    else:
        # Only the external admin boundary is substituted. The native broker,
        # Taskiq registration, routing, schedules and registrars are real.
        native = native_module.AioKafkaBroker(
            "localhost:9092", kafka_admin_client=SimpleNamespace()
        )
        adapter = infra.KafkaBroker
        prefix = "Kafka"
    return SimpleNamespace(
        kind=kind,
        adapter=adapter,
        broker=adapter(native),
        scheduler=getattr(scheduler, prefix + "Scheduler"),
        direct=getattr(projection, prefix + "Direct"),
        projection=getattr(projection, prefix + "Projection"),
        scheduler_registry=getattr(scheduler_registry, prefix + "Registrar"),
        projection_registry=getattr(projection_registry, prefix + "Registrar"),
    )


async def run(value: int = 1) -> int:
    return value


def test_adapter_preserves_native_and_validates_registration(backend):
    broker = backend.broker
    assert backend.adapter(broker.native).native is broker.native
    with pytest.raises(TypeError, match="native"):
        backend.adapter(object())
    labels = {"name": "metadata", "task_name": "metadata too"}
    task = broker.register(run, name="reports.single", labels=labels)
    assert broker.native.find_task("reports.single") is task
    task.labels["name"] = "changed"
    assert labels["name"] == "metadata"
    with pytest.raises(ValueError, match="already registered"):
        broker.register(run, name="reports.single")
    with pytest.raises(ValueError, match="empty"):
        broker.register(run, name="")
    with pytest.raises(TypeError, match="async"):
        broker.register(lambda: 1, name="sync")
    assert not hasattr(broker, "add_queue")
    assert not hasattr(broker, "consume")


async def test_registrars_preserve_schedule_defaults_overrides_and_retry(
    backend,
):
    class Report(backend.scheduler):
        schedule = [{"interval": 20, "kwargs": {"value": 7}}]
        retry = Retry(attempts=2, delay=1, errors=(ConnectionError,))

        async def run(self, value: int):
            return value

    class Disabled(Report):
        pass

    registry = backend.scheduler_registry(backend.broker)
    task = registry.include(Report)
    disabled = registry.include(Disabled, labels={"schedule": []})
    source = LabelScheduleSource(backend.broker.native)
    await source.startup()
    schedules = await source.get_schedules()
    assert len(schedules) == 1
    assert schedules[0].task_name == task.task_name
    assert schedules[0].kwargs == {"value": 7}
    assert schedules[0].interval == 20
    assert task.labels["max_retries"] == 2
    assert task.labels["retry_delay"] == 1
    task.labels["schedule"][0]["kwargs"]["value"] = 9
    assert Report.schedule == [{"interval": 20, "kwargs": {"value": 7}}]
    assert disabled.labels["schedule"] == []
    with pytest.raises(ValueError, match="already registered"):
        registry.include(Report)
    with pytest.raises(TypeError, match="scheduler"):
        registry.include(Scheduler)


async def test_dynamic_schedules_remain_independent_of_transport(backend):
    class Report(backend.scheduler):
        async def run(self, value: int):
            return value

    backend.scheduler_registry(backend.broker).include(Report)
    source = MemorySource()
    interval = await Report.every(source, 20, value=7)
    cron = await Report.cron(source, "0 3 * * *", value=8)
    assert interval.schedule_id != cron.schedule_id
    stored = await source.get_schedules()
    assert {tuple(s.kwargs.items()) for s in stored} == {
        (("value", 7),),
        (("value", 8),),
    }
    await interval.unschedule()
    assert [s.schedule_id for s in await source.get_schedules()] == [
        cron.schedule_id
    ]


def test_projection_registration_preserves_contract_and_rejects_wrong_backend(
    backend,
):
    class Product(backend.direct[int, int]):
        async def read(self, value: int) -> int:
            return value

        async def write(self, data: int) -> int:
            return data

    registry = backend.projection_registry(backend.broker)
    registered = registry.include(Product, name="products.single")
    assert Product.task() is registered
    assert registered.original_func.__annotations__["value"] is int
    with pytest.raises(ValueError, match="already registered"):
        registry.include(Product)
    with pytest.raises(TypeError, match="Projection|backend"):
        registry.include(Direct)


@pytest.mark.parametrize("family", ["schedulers", "projections"])
def test_kafka_topic_selection_and_conflicts(backend, family):
    if backend.kind != "kafka":
        # Both fixture variants establish that NATS has no per-task routing.
        with pytest.raises(TypeError, match="topic"):
            backend.broker.register(run, name="invalid", topic="elsewhere")
        return

    if family == "schedulers":

        class First(backend.scheduler):
            topic = "reports"

            async def run(self, value: int):
                return value

        registry = backend.scheduler_registry(backend.broker)
    else:

        class First(backend.direct[int, int]):
            topic = "reports"

            async def read(self, value: int) -> int:
                return value

            async def write(self, data: int) -> int:
                return data

        registry = backend.projection_registry(backend.broker)

    class Override(First):
        pass

    class Conflict(First):
        pass

    class Empty(First):
        topic = ""

    labels = {"custom": "kept"}
    first = registry.include(First, labels=labels)
    second = registry.include(Override, topic="other")
    key = backend.broker.native.task_topic_label
    assert first.labels[key] == "reports"
    assert second.labels[key] == "other"
    assert labels == {"custom": "kept"}
    with pytest.raises(ValueError, match="conflicts"):
        registry.include(Conflict, labels={key: "different"})
    with pytest.raises(RuntimeError, match="not included"):
        Conflict.task()
    with pytest.raises(ValueError, match="nonempty"):
        registry.include(Empty)


def test_backend_imports_do_not_require_other_integrations(backend):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
import importlib.abc
import sys
kind = sys.argv[1]
blocked = {'papilio', 'faststream', 'taskiq_redis', 'taskiq_aio_pika'}
blocked.add('taskiq_aio_kafka' if kind == 'nats' else 'taskiq_nats')
blocked.add('kafka' if kind == 'nats' else 'nats')
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in blocked:
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
for family in ('schedulers', 'projections'):
    for part in ('backends', 'registry'):
        importlib.import_module('papilio_tasks.apps.' + family + '.' +
                                part + '.' + kind)
""",
            backend.kind,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr

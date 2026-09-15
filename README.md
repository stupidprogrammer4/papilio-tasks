# Papilio Tasks

Modular background jobs for Python 3.13+. The scheduler application uses Taskiq
and ordinary Dishka providers. Events and projections are planned applications;
they are not implemented yet.

## Installation

```bash
pip install 'papilio-tasks[scheduler]'         # in-process Memory default
pip install 'papilio-tasks[scheduler-rabbit]'  # RabbitMQ
pip install 'papilio-tasks[scheduler-redis]'   # Redis Streams
```

A bare `papilio-tasks` installation provides the lightweight CLI and discovery
core. It does not require Taskiq, Dishka, FastStream, Redis or RabbitMQ. Extras
select dependencies; the distribution includes all source files. Scheduler extras
do not install FastStream. Each future application will own its dependencies.

## A local job

```python
import asyncio

from dishka import Provider, Scope

from papilio_tasks.schedulers import Registrar, Scheduler
from papilio_tasks.schedulers.application import create_broker


class Report(Scheduler):
    async def run(self, value: int) -> int:
        return value * 2


provider = Provider(scope=Scope.REQUEST)
provider.provide(Report)
registrar = Registrar()
registrar.include(Report, name="reports.daily")
broker = create_broker(registrar=registrar, providers=[provider])


async def main():
    await broker.startup()
    try:
        execution = await Report.enqueue(3)
        result = await execution.wait_result(timeout=5)
        assert not result.is_err and result.return_value == 6
        assert await Report().run(3) == 6
    finally:
        await broker.shutdown()


asyncio.run(main())
```

`run` is a local method. `enqueue` publishes a task and returns its native Taskiq
handle. `Report.task()` exposes the registered native task and its scheduling
helpers. Registration does not construct the scheduler. Providers determine its
scope: REQUEST gives one instance per execution, APP permits shared instances.

`create_broker` creates the Dishka container and wires native Taskiq integration;
request scopes close on completion or failure, and broker shutdown closes the
application container. Normal module providers supply business dependencies and
scheduler classes. There is no `factory(cls)` or Binding to register. Supplied
provider graphs are validated at assembly; a scheduler missing from those
providers raises Dishka's missing-factory error when a worker resolves it.

## Modules and runnable paths

Keep each module's schedulers and providers together, for example:

```text
app/
├── tasks.py
└── modules/
    └── reports/
        ├── schedulers.py
        └── providers.py
```

Add `__init__.py` files for these application packages. The module definition can
use a backend-specific scheduler and an optional queue specification:

```python
# app/modules/reports/schedulers.py
from typing import ClassVar
from papilio_tasks.schedulers.backends.rabbit import (
    RabbitQueue,
    RabbitScheduler,
)


class Report(RabbitScheduler):
    queue: ClassVar[RabbitQueue | None] = RabbitQueue(
        name="reports", durable=True
    )

    async def run(self, value: int) -> int:
        return value * 2
```

```python
# app/modules/reports/providers.py
from dishka import Provider, Scope, provide
from .schedulers import Report


class Jobs(Provider):
    report = provide(Report, scope=Scope.REQUEST)
```

The application collects its providers and passes them to the factory. The
`modules` argument supplies dotted package roots for scheduler discovery:

```python
# app/tasks.py
from datetime import UTC, datetime, timedelta
from taskiq import ScheduledTask

from papilio_tasks.schedulers.application import create_beat, create_broker
from papilio_tasks.schedulers.backends.rabbit import RabbitBroker
from papilio_tasks.schedulers.infra.sources.backends.memory import MemorySource
from papilio_tasks.schedulers.registry.rabbit import RabbitRegistrar

from .modules.reports.providers import Jobs
from .modules.reports.schedulers import Report

registrar = RabbitRegistrar(RabbitBroker("amqp://guest:guest@localhost/"))
broker = create_broker(
    registrar=registrar,
    providers=[Jobs()],
    modules=["app.modules"],
)

# Example initial schedule, reconstructed when this module is imported.
source = MemorySource(
    [
        ScheduledTask(
            task_name=Report.task().task_name,
            labels=dict(Report.task().labels),
            args=[3],
            kwargs={},
            time=datetime.now(UTC) + timedelta(minutes=1),
        )
    ]
)
beat = create_beat(broker, sources=[source])
```

Run the two processes from your application directory:

```bash
papilio_tasks scheduler worker app.tasks:broker --workers 2
papilio_tasks scheduler beat app.tasks:beat
```

`python -m papilio_tasks` provides the same commands. Worker and beat options are
forwarded to Taskiq's native commands, including `--app-dir`, `--workers`,
`--skip-first-run` and native help. The CLI delegates process management and
normal shutdown to Taskiq. It loads only the selected application.

Discovery walks the chosen package roots for `schedulers.py` or `schedulers/`
packages, including nested files. Only concrete subclasses defined in each file
are included; imported classes and abstract bases are excluded. Roots and aliases
are deduplicated. Import errors inside application code propagate. Package
`__init__.py` files execute during discovery; keep their side effects minimal.
Providers are passed explicitly, never automatically discovered or instantiated.
All schedulers discovered for a registrar must match its backend.

Default task names are `module.qualified_class_name`. For a custom name, labels
or queue override, call `registrar.include(...)` explicitly and exclude that
scheduler's package from automatic discovery. One concrete class has one task
registration per process; re-inclusion or another application registering the
same class raises an error. `create_beat` reuses the assembled broker, so the
broker/beat entrypoints above do not register those classes twice in one process.

## Sources and lifecycle

`create_beat(broker, sources=[...])` returns a native TaskiqScheduler. It accepts
source adapters implementing `SourceContract`; wrap any native read-only source
with `Source(native_source)`. Source selection is explicit. The beat command owns
source startup/shutdown, and the native scheduler starts/stops its broker.
Factory calls perform no framework-managed network I/O or job execution.

Source contracts describe capabilities; they do not require every backend to
support lookup or atomic replacement:

| Contract | Operations |
| --- | --- |
| `SourceContract` | `native`, `get_schedules()` |
| `MutableSourceContract` | Above plus `add_schedule(schedule)`, `delete_schedule(id)` |
| `MemorySourceContract` | Above plus `get_schedule(id)`, `replace_schedule(schedule)` |

All schedules use Taskiq's `ScheduledTask`. Mutations return `None`; Memory lookup
returns a copy and raises `KeyError` for an unknown ID. Memory rejects duplicate
IDs on add, and replacement requires an existing ID with the same task name.
Its replacement is atomic within one event loop; a dispatch already past
`pre_send` can still publish the previous input.

`Source` implements native reading; `MutableSource` also delegates add/delete to a
native source known to support them. Use `Source(LabelScheduleSource(broker))`
for label-based schedules. These read-only adapters do not expose editing methods.

`MemorySource` lives in one process: changes in an API/worker process are not
visible in the separate beat process. The example above seeds the beat's memory
on each import; it is not durable.

For shared schedules, install `papilio-tasks[scheduler-redis]` and use
`RedisSource`. It uses Taskiq's `ListRedisScheduleSource`; constructor options such
as `prefix`, `buffer_size` and `serializer` are passed directly to that source.
The broker is independent: a RabbitMQ broker can use this Redis source too.

```python
from papilio_tasks.schedulers.infra.sources.backends.redis import RedisSource

source = RedisSource("redis://localhost:6379/0", prefix="jobs")
beat = create_beat(broker, sources=[source])


# In the producer, after Report has been registered:
async def schedule_report(when, report_id):
    return await Report.at(source, when, report_id)
```

Create one source per application/process, using the same Redis database and
prefix in the producer and beat. An ordinary Dishka APP provider can own the
instance; no source provider is registered automatically. Keep using native
startup/shutdown in the owning application; the beat CLI calls these for its
sources. This adapter preserves native lifecycle behavior, including upstream
limitations; it does not add pool management or startup-failure cleanup.

Use these classmethods after registering the scheduler:

| Method | Timing input |
| --- | --- |
| `await Report.at(source, when, *args, **kwargs)` | `datetime` |
| `await Report.cron(source, expression, *args, **kwargs)` | Cron string or Taskiq `CronSpec` (including its offset) |
| `await Report.every(source, interval, *args, **kwargs)` | Seconds as `int`, or `timedelta` |

Each delegates to Taskiq's corresponding `schedule_by_time`, `schedule_by_cron`
or `schedule_by_interval` helper. Pass an editable `MutableSourceContract` instance;
read-only sources are not accepted by the type contract. The source is explicit
on every call and is not stored on the scheduler class. Taskiq retains control of
argument serialization and timing behavior; these helpers do not add validation,
signature synthesis or per-run static argument types. Native helper keyword-name
collisions still apply. `Report.task()` remains available for advanced native
options such as custom schedule IDs through its kicker.

The returned object is Taskiq's `CreatedSchedule`: use `plan.schedule_id` to keep
its ID and `await plan.unschedule()` to delete it. `await Report.enqueue(...)`
still submits immediately without a source. Redis changes
become visible through beat's refresh cycle. Its `get_schedules()` returns the
native beat feed (recurring/current-time entries and native overdue handling),
not all future schedules. Redis has no `get_schedule` or `replace_schedule` API
here. Use fresh IDs when adding schedules; add is not an upsert, and duplicate-ID
semantics remain native. Deletion does not revoke a schedule already fetched by
beat or a job already sent. A general `reschedule` API is deferred.

Memory broker executes in the caller process; it cannot connect independent
worker and beat processes. Use RabbitMQ/Redis for that layout. Standalone
producers own `await broker.startup()` / `await broker.shutdown()` and must drain
outstanding local tasks before shutdown. Native Taskiq currently owns startup
failure behavior as well: a failure partway through beat source startup does not
provide automatic cleanup of already-started sources. Normal shutdown is tested.

## Backend queues

Rabbit definitions are under `schedulers.backends.rabbit`; Redis definitions are
under `schedulers.backends.redis`. Each has its own registrar under
`schedulers.registry`. The common Memory scheduler has no queue obligation.

```python
from papilio_tasks.schedulers.backends.redis import (
    RedisQueue,
    RedisScheduler,
    RedisStreamBroker,
)
from papilio_tasks.schedulers.registry.redis import RedisRegistrar


class Report(RedisScheduler):
    queue = RedisQueue("reports")

    async def run(self, value: int):
        return value * 2


registrar = RedisRegistrar(
    RedisStreamBroker(
        "redis://localhost:6379/0",
        queue_name="reports",
        consumer_group_name="jobs",
        consumer_id="0",
    )
)
registrar.include(Report)
# Pass registrar and Report's ordinary provider to create_broker before execution.
```

Queues may be shared. Compatible specifications are reused; conflicting options
for a name fail. `include(queue=spec)` overrides the class's queue without mutating
its ClassVar. Resolve a configured name explicitly with `broker.get_queue(name)`.
A scheduler without a queue uses the backend's default destination.

| Operation on the infra broker | Effect |
| --- | --- |
| `add_queue(spec)` | Configure/reuse a destination before startup |
| `get_queue(name)` | Return its specification |
| `await declare_queue(spec)` | Declare the destination on the server |
| `consume(*names)` (Rabbit only) | Select worker subscriptions before startup |
| `register(func, name=..., labels=...)` | Register a prepared async callable |

Explicit declaration does not change local routing or subscriptions. Rabbit
requires startup before direct declaration. Redis lazily uses its native pool;
its declaration creates a stream/group and retains an existing group's cursor.
Native Redis `consumer_id="$"` starts new groups at the stream end; use `"0"` to
include already-published messages. Redis read cursors are supplied through
`additional_streams`, separately from the group's starting ID. A destination's
`read_id` alone does not configure a worker subscription.
Redis startup/error policy is unchanged by this application/CLI work.

Rabbit consumes all configured destinations by default; `consume` selects a subset.
Redis selects worker streams at construction: `queue_name` is the main stream and
default send destination; `additional_streams={"mail": ">"}` adds read streams.
Redis `add_queue` and scheduler inclusion register send destinations without adding
worker subscriptions. To process a scheduler's dedicated stream, select it in the
worker broker's constructor. Constructor stream names are also available through
`get_queue`; additional-stream cursors are preserved in their specifications.
When migrating from Redis `consume`, put the first selected name in `queue_name`
and the remaining names/cursors in `additional_streams`. Give tasks an explicit
queue when their send destination differs from that main stream.
Configuration must finish before startup. Rabbit enforces that timing; Redis
currently relies on the caller. Network task results require a native result
backend if callers need to retrieve them.

## Signatures and boundaries

Task callbacks preserve `run` parameter kinds, defaults and annotations without
`self`. Dishka adds its internal keyword dependency; `_job` and `dishka_container`
are reserved and must not be sent as task kwargs. Native Taskiq handles payload
conversion. Its mixed untyped/typed and variadic conversion limitations still
apply; this adapter does not add a parser or make coercion strict. The convenience
`enqueue` method does not provide static per-run argument typing.

CLI implementations live together in `papilio_tasks/cli/`: `main.py` routes
commands and `scheduler.py` delegates worker/beat execution. The shared CLI/core
do not import application runtimes. Scheduler construction
lives in `schedulers/application.py`; registry owns class inclusion/injection;
infra owns callable registration, queue operations and source capabilities.
Events/projections remain unimplemented and expose no runnable placeholder command.

## Migration and development

- Install `scheduler`, `scheduler-rabbit` or `scheduler-redis` rather than relying
  on a bare installation or the old `rabbit`/`redis` extras.
- Import specialized scheduler classes from `schedulers.backends.rabbit` or
  `schedulers.backends.redis` instead of their former root modules.
- Replace registrar `factory=` with ordinary providers passed to `create_broker`.
- Use `papilio_tasks scheduler worker/beat` with the factory's native outputs.

Install development dependencies with
`pip install -e '.[dev,scheduler-rabbit,scheduler-redis]'`. Run `pytest`,
`ruff check .`, `ruff format --check .` and `pyright`. Set `TEST_RABBIT_URL` and
`TEST_REDIS_URL` to isolated services for live tests; resources have unique names
and are cleaned up. `TEST_PAPILIO_CLI` can select an installed CLI executable for
the worker/beat integration test. Tests establish behavior, not throughput,
network recovery or exactly-once execution.

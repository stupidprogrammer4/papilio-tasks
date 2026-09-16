<p align="center">
  <img src="docs/assets/logo.webp" alt="Papilio Tasks logo" width="320">
</p>

# Papilio Tasks

Modular background jobs for Python 3.13+. Schedulers and projections use Taskiq
and ordinary Dishka providers. Projections run read/transform/write pipelines
locally or through workers. The events application remains unimplemented.

## Installation

```bash
pip install 'papilio-tasks[scheduler]'         # in-process Memory default
pip install 'papilio-tasks[scheduler-rabbit]'  # RabbitMQ
pip install 'papilio-tasks[scheduler-redis]'   # Redis Streams
pip install 'papilio-tasks[projection]'        # in-process Memory default
pip install 'papilio-tasks[projection-rabbit]' # RabbitMQ
pip install 'papilio-tasks[projection-redis]'  # Redis Streams / retry source
```

A bare `papilio-tasks` installation provides the lightweight CLI and discovery
core. It does not require Taskiq, Dishka, FastStream, Redis or RabbitMQ. Extras
select dependencies; the distribution includes all source files. Scheduler and
Projection extras are independent and do not install FastStream. For RabbitMQ
transport with a Redis retry source, install both `projection-rabbit` and
`projection-redis`. Each application owns its dependencies.

## A local job

```python
import asyncio

from dishka import Provider, Scope

from papilio_tasks.apps.schedulers import Registrar, Scheduler
from papilio_tasks.apps.schedulers.application import create_broker


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
from papilio_tasks.apps.schedulers.backends.rabbit import (
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

from papilio_tasks.infra.taskiq.sources.backends.memory import MemorySource
from papilio_tasks.apps.schedulers.application import create_beat, create_broker
from papilio_tasks.apps.schedulers.backends.rabbit import RabbitBroker
from papilio_tasks.apps.schedulers.registry.rabbit import RabbitRegistrar

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

## Retry policies

Set an immutable policy on a scheduler class. It is inherited normally;
`retry = None` disables the class policy. `attempts` counts all executions,
including the first, and `delay` is a fixed delay in seconds before a retry.

```python
from typing import ClassVar

from papilio_tasks.tools.retry import Retry
from papilio_tasks.apps.schedulers import Scheduler


class Report(Scheduler):
    retry: ClassVar[Retry | None] = Retry(
        attempts=4, delay=10, errors=(ConnectionError, TimeoutError),
    )

    async def run(self, id: int):
        ...
```

Supply a writable retry source explicitly to `create_broker`. Use the same
source in `create_beat`; worker and beat processes must use the same storage
namespace. Source selection does not depend on the broker: RabbitMQ can use
a Redis schedule source. Keep the registrar and providers appropriate to your
scheduler classes:

```python
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource

source = RedisSource("redis://localhost:6379/0", prefix="jobs")
broker = create_broker(
    registrar=registrar, providers=providers, modules=["app.modules"],
    retry_source=source,
)
beat = create_beat(broker, sources=[source])
```

Manual inclusion and discovery apply the same policy. Missing/read-only retry
sources and conflicting retry labels are rejected. One Taskiq
`SmartRetryMiddleware` handles retries per broker; an existing retry middleware
cannot be combined with this managed setup. A small delay adapter keeps
`retry_delay` separate from RabbitMQ's publication `delay`, so the initial
submission is not delayed by the retry policy. Counts, exception filtering and
rescheduling use Taskiq's implementation, preserving the original task ID,
arguments and queue. The whole job runs again with a new Dishka request scope.

Worker startup/shutdown invokes the retry source's native lifecycle methods;
the beat CLI owns its process's source lifecycle. Factories open no connections.
Memory sources work only when execution and beat share the same in-process store.
Beat must be running to dispatch retries; its polling interval can make execution
later than the configured delay (`--update-interval` controls source refresh).
Source-specific cleanup semantics remain those of its native implementation.
This retries job execution, not failed producer publication. Publication hooks
observe submission separately; final-failure hooks remain pending. Projections use the same
policy and adapter through their own application factory, as described below.

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
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource

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

Rabbit definitions are under `apps.schedulers.backends.rabbit`; Redis definitions
are under `apps.schedulers.backends.redis`. Each has its own registrar under
`apps.schedulers.registry`. The common Memory scheduler has no queue obligation.

```python
from papilio_tasks.apps.schedulers.backends.redis import (
    RedisQueue,
    RedisScheduler,
    RedisStreamBroker,
)
from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar


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
commands and `taskiq.py` delegates worker/beat execution for both applications.
The CLI and shared discovery tool
do not import application runtimes. Scheduler construction
lives in `apps/schedulers/application.py`; registry owns class inclusion/injection.
Shared Taskiq transport code lives in `papilio_tasks/infra/taskiq/`: `brokers/`
owns callable registration and backend operations, and `queues/` owns queue
specifications and declaration helpers. These modules do not depend on scheduler
classes, projection classes or Dishka. Schedule sources are shared under
`infra/taskiq/sources/`.

Use the shared paths when working directly with transport infrastructure:

```python
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.infra.taskiq.brokers.backends.redis import RedisStreamBroker
from papilio_tasks.infra.taskiq.queues.rabbit import RabbitQueue
from papilio_tasks.infra.taskiq.queues.redis import RedisQueue
```

Import only the backend you installed. Scheduler convenience imports from
`apps.schedulers.backends.rabbit` and `.redis` also remain available.
Projection assembly is available through its application module, with independent
CLI commands and installation extras. Events remain unimplemented.

The package has three main responsibility groups:

- `apps/`: application bases, contracts, registrars and assembly.
- `infra/`: adapters for task runtimes, brokers, queues and sources.
- `tools/`: discovery, retry settings and hook execution/payloads.

Applications depend on tools and infrastructure; infrastructure may use independent
policy types from tools. Tools do not import apps, infrastructure or optional
runtimes. Both `apps` and `tools` have lightweight package initializers.
`cli/` remains the command entry point. `tools/retry.py` holds configuration;
`infra/taskiq/retry.py` adapts it to Taskiq. Events remain unimplemented.

## Local projections

`Projection[T, D, R]` runs `read → transform → write`. `read` returns `T`,
`transform` converts `T` to `D`, and `write` accepts `D` and returns `R`.
`run` returns that same `R`. All three operations are required and async.
Independent hooks can observe each completed operation and execution failures.

```python
from dataclasses import dataclass
from typing import Protocol, TypedDict

from papilio_tasks.tools.hooks import Handler, Hook
from papilio_tasks.tools.hooks.projection import Failure, Hooks
from papilio_tasks.apps.projections import Projection


@dataclass
class Row:
    id: int
    title: str


class Document(TypedDict):
    id: int
    name: str


class ErrorStore(Protocol):
    async def save(self, event: Failure) -> None: ...


class SaveError(Hook[Failure]):
    def __init__(self, sink: ErrorStore):
        self.sink = sink

    async def run(self, event: Failure) -> None:
        await self.sink.save(event)


class Product(Projection[Row, Document, int]):
    def __init__(self, source, target, errors: SaveError):
        super().__init__(
            hooks=Hooks(on_error=(Handler(errors, failure="continue"),)),
        )
        self.source = source
        self.target = target

    async def read(self, id: int) -> Row:
        return await self.source.get_by_id(id)

    async def transform(self, row: Row) -> Document:
        return {"id": row.id, "name": row.title}

    async def write(self, document: Document) -> int:
        return await self.target.upsert(document)
```

Here `source`, `target` and `sink` are application-owned dependencies. The sink
chooses how to serialize/store a failure; there is no framework error store.
Queries, transactions, commits and outbox behavior belong to the application.
Construct the projection with those dependencies, then `await projection.run(42)`.
Original arguments go to `read`; its output goes through `transform` to `write`.
If the destination needs per-call options, include them in the data passed
between these methods. A batch projection reads and writes a batch once;
the base neither splits work into individual tasks nor skips empty data.

When no conversion is needed, inherit `Direct[T, R]`. It supplies an identity
`transform(T) -> T`, preserving the original object; implement only `read` and
`write`. `Direct[list[Row], int]` reads and writes a whole list of rows.
Both forms conform to `ProjectionContract[T, D, R]` (`D = T` for `Direct`).
Arbitrary `run(*args, **kwargs)` arguments are forwarded dynamically to `read`;
static per-read argument checking is not provided.

### Hook attachments

`Hook[T]` is an abstract class with `async run(event: T) -> None`. It imposes no
constructor: providers supply application services and connections to concrete
hook classes. `Handler(hook, failure=...)` attaches a resolved instance with its
own failure policy; the same hook can be strict in one place and tolerant in
another. A hook provider supplies its construction recipe; selecting a handler activates
it at a particular phase. `emit` awaits each handler sequentially because order
and stopping after failure are required.

The generic contract and runner live in `tools/hooks/base.py` and `tools/hooks/runner.py`.
Projection payloads, stage names and collections live in `tools/hooks/projection.py`;
the generic runner does not depend on Projection, Dishka or Taskiq.

`Hooks[T, D, R]` contains ordered tuples of `Handler` attachments:

| Attachment | Hook input |
| --- | --- |
| `after_read` | `Data[T]`: invocation and read data |
| `after_transform` | `Data[D]`: invocation and converted data |
| `after_write` | `Written[D, R]`: invocation, written data and result |
| `on_error` | `Failure`: invocation, failing stage and original exception |

Payloads live in `papilio_tasks.tools.hooks.projection` and are also exported from
`papilio_tasks.apps.projections`. Every payload has a
`call` containing the qualified Projection class name, original `args` and a
read-only shallow copy of `kwargs`. These are in-process objects, not guaranteed
serializable messages or durable snapshots. Data and argument values remain
references; hooks should not mutate them. Hook `run` return values are ignored.
The collections and payload envelopes are frozen, and collections default to
empty tuples.

`failure="raise"` is the default: stop the current hook sequence and fail the
pipeline. `failure="continue"` logs that hook's exception and runs subsequent
hooks and operations. It does not send the tolerated failure to `on_error`.
`read`, `transform` and `write` errors always stop the pipeline and propagate
through `on_error`; error-hook return values cannot suppress them. If an error
hook also fails with `raise`, remaining error hooks stop, the original pipeline
exception remains primary and the hook exception is chained as its cause.
Error hooks are not invoked recursively. Cancellation always propagates directly.

A successful write is not undone or repeated by this pipeline when its after-hook
fails. In queued execution, propagating that hook failure to Taskiq may retry
the whole job and repeat the write; choose hook policy accordingly.
This core has no retry, rollback, connection management or CPU offloading.

### Shared hooks and dependency scopes

A common collection can serve heterogeneous projections:

```python
class AppHooks(Hooks[object, object, object]):
    def __init__(self, errors: SaveError):
        super().__init__(
            on_error=(Handler(errors, failure="continue"),),
        )
```

Register `SaveError`, `AppHooks` and the Projection through ordinary user
providers with the appropriate scopes. For example, a module provider can declare
`save_error = provide(SaveError, scope=Scope.REQUEST)` and
`app_hooks = provide(AppHooks, scope=Scope.REQUEST)`; the sink and other services
are supplied by the application's providers. Provider registration supplies construction recipes;
explicit attachment selects where hooks execute. A shared hook
receives `Data[object]` or `Written[object, object]`, while local hooks retain
concrete `T/D/R` types. A hook requiring `Row` cannot apply to every projection.

The core composition entry `_run(shared, args, kwargs)` accepts an already-resolved
shared collection. It runs shared hooks before local hooks at each phase,
without storing the shared collection on the Projection or changing class state.
The caller owns scope and dependency resolution. Tests exercise composition with
ordinary Dishka providers, separate app containers and request cleanup.

Public `projection.run(...)` is standalone and invokes local hooks only.
Queued execution resolves the Projection and the registrar-selected shared hooks
from the supplied providers in the same job scope. The core itself requires no
Taskiq, Dishka or database dependencies. App-managed manual execution and a
dedicated CLI remain subsequent work.

### Low-level task registration

`papilio_tasks.apps.projections.registry.Registrar` bridges the pipeline to Taskiq.
It uses the shared `MemoryBroker` by default and returns a native task:

```python
from papilio_tasks.apps.projections.registry import Registrar

registry = Registrar(hooks=AppHooks)
task = registry.include(
    Product, name="products.single", labels={"kind": "product"}
)
```

`include` does not construct projections, hooks or containers. It exposes `read`'s
arguments and `write`'s return annotation, injects the Projection and selected
shared hook collection, and calls the existing pipeline. `_job`, `_hooks` and
`dishka_container` are reserved injection parameter names. Registration metadata
is separate from task names. The class receives no `_task` attribute.

### Application and publication

`create_broker` wires the registrar and ordinary module providers into one Dishka
container. It returns the native broker and closes that container on native
client/worker shutdown. Construction performs no network I/O or service resolution.
Registration belongs to app assembly; business modules publish through the class:

```python
from papilio_tasks.apps.projections.application import create_broker

# MyProvider supplies Product, AppHooks and their dependencies.
broker = create_broker(registrar=registry, providers=[MyProvider()])

await broker.startup()
try:
    submitted = await Product.enqueue(id=42)
    completed = await submitted.wait_result(timeout=5)
    if completed.is_err:
        raise completed.error
    print(completed.return_value)
finally:
    await broker.shutdown()
```

This example uses the default in-process broker. For discovery, pass
`modules=["app.modules"]` to find concrete classes defined in `projections.py`
or `projections/` beneath those roots. Providers remain explicitly supplied by the
caller. Choose manual inclusion or discovery for each class; including the same
class or task name twice is an error. Custom task names do not affect class lookup.

`Product.enqueue(*args, **kwargs)` forwards `read` arguments to native
Taskiq publication and returns `AsyncTaskiqTask[R]`, not the write result. A batch
argument stays one message. Task names and backend labels come from registration;
publication adds no new broker or connection. `Product.task()` returns the native
task; `include` still returns that task as well. Sending does not construct the
worker's Projection, hooks or database services. No per-Projection Publisher or
registry dependency is needed inside a business module.

Both schedulers and projections use an internal process-local class-to-task map.
Each concrete class has at most one active binding per process, even across
different registrars/brokers. Different classes can share a queue, and separate
worker processes register their own copies of the class. Subclasses never inherit
a parent's binding. Registration does not write runtime attributes to user classes.

Application shutdown releases only its broker's bindings, including when container
cleanup fails. A later fresh broker may register the released class. Calls before
registration or after release fail clearly. Drain callers/jobs before shutdown;
live rebinding and restarting the same closed broker are not supported. Failed
application assembly releases that broker's bindings; discard the failed broker
and registrar because native registrations/queue side effects are not rolled back.
Low-level registration without `create_broker` leaves teardown to its caller:
`papilio_tasks.infra.taskiq.bindings.release(native_broker)` removes its bindings.

The calling app still owns producer startup/shutdown. Tests cover live
Redis/Rabbit transport through this factory and `enqueue`, including shared/local
hooks and job cleanup. Separate CLI process tests exercise a producer and worker,
plus an optional beat for retries. Retry policies and publication hooks are
selected explicitly. Use the `projection` / `projection-redis` /
`projection-rabbit` extras for runtime dependencies; pure Projection imports still
require none of them.

### Run Projection workers

For separate producer and worker processes, choose Redis or RabbitMQ transport;
the default Memory broker is only for in-process execution. For example, expose
your application's broker in `app/tasks.py`:

```python
from papilio_tasks.apps.projections.application import create_broker
from papilio_tasks.apps.projections.registry import Registrar
from papilio_tasks.infra.taskiq.brokers.backends.redis import RedisStreamBroker

from app.modules.products.projections import Product
from app.modules.products.providers import ProductProvider

registry = Registrar(RedisStreamBroker(
    "redis://localhost:6379/0", queue_name="products",
))
registry.include(Product)
broker = create_broker(registrar=registry, providers=[ProductProvider()])
```

Here Product has no retry policy. Run its worker:

```bash
papilio_tasks projection worker app.tasks:broker --workers 2
```

The producer imports the same `Product` through that assembled application,
starts its broker, calls `await Product.enqueue(...)`, and shuts its broker down
on application exit. Alternatively, assembly can discover classes from your
`modules` roots. Neither the CLI nor the factory chooses providers or transport
for you.

**Beat is optional.** With no retry, a producer and worker are sufficient, including
publication hooks. With a retry policy, configure an explicit shared source and
expose `beat = create_beat(broker, sources=[source])` as in the next example. Run
it separately:

```bash
papilio_tasks projection beat app.tasks:beat --update-interval 1
```

This dispatches retry attempts, including `delay=0`; it is not automatically
started by the worker. One beat can serve multiple projections on its broker and
sources. Retries remain undispatched while beat is absent; assembly validates the
source, not whether beat is alive. Deploy processes against the same source store
and prefix. Memory sources are not shared between separate processes.

Both commands delegate to native Taskiq and forward its arguments, including
`--app-dir`, worker concurrency options and beat refresh options. Root and
application help work without runtime dependencies; command execution checks the
selected application's extras. Run `papilio_tasks projection --help` for the
application overview and append `--help` to worker/beat for native options.

### Projection execution retry

Retry is opt-in: `Projection.retry` defaults to `None`. For ordinary queued
projections without retry, run the producer and worker; no retry source or beat
is required. Publication hooks also work without beat.

To enable retry, select a class policy, explicitly supply `retry_source`, and
run beat to dispatch scheduled attempts. Beat is not started automatically by
`create_broker`, `create_beat` or `enqueue`; `create_beat` only constructs its
configuration. One beat can serve multiple projections registered on its broker
and using its configured sources; there is no beat per Projection.

Set `retry: ClassVar[Retry | None]` on the Projection class, using
`papilio_tasks.tools.retry.Retry`. For example:

```python
from typing import ClassVar

from papilio_tasks.apps.projections import Direct
from papilio_tasks.tools.retry import Retry


class Product(Direct[int, int]):
    retry: ClassVar[Retry | None] = Retry(
        attempts=3, delay=10, errors=(ConnectionError, TimeoutError),
    )

    # Implement read and write as usual.
```

The policy is inherited; `retry = None` disables it. `attempts` includes the first
execution. Supply a writable source explicitly during Projection assembly:

```python
from papilio_tasks.apps.projections.application import create_beat, create_broker
from papilio_tasks.infra.taskiq.sources.backends.redis import RedisSource

# registry includes Product; providers supply Product and its dependencies.
source = RedisSource("redis://localhost:6379/0", prefix="projection-retries")
broker = create_broker(
    registrar=registry, providers=providers, retry_source=source,
)
beat = create_beat(broker, sources=[source])
```

Factories return native Taskiq objects and open no connections. Run the Projection
worker and optional beat commands with these objects. Each process assembles its
own broker/source pointing at the same shared
store and prefix. Within each assembly, beat must include the configured native
retry source. `MemorySource` is suitable only when worker and beat share a process.
Without a running beat, the initial queued attempt still executes, but any
scheduled retries remain undispatched. This also applies to `delay=0`: the
current retry path always writes the next attempt to the source. Beat later
publishes eligible attempts; the worker executes them. An attempt limit of one
or a nonmatching exception does not schedule another attempt.

Missing `retry_source` for a configured policy is rejected during assembly.
The factory does not check whether a separate beat process is running; running
and monitoring it is the application's deployment responsibility. Separate
processes must use shared source storage, not separate `MemorySource` instances.

Manual inclusion and discovery both validate the policy/source. Native Taskiq
SmartRetry preserves the task ID, arguments and queue, applies the exception
filter and attempt limit, and schedules the next attempt after the configured
delay. Initial publication is not delayed. Each attempt resolves a fresh Dishka
job scope and reruns **the entire read → transform → write pipeline**, including
shared/local hooks. A matching error in `after_write` can repeat an already
successful write; the application's write operation must tolerate repetition.
An error suppressed by a hook's `failure="continue"` does not trigger retry.

`on_error` hooks observe each failed attempt, not only final failure. The framework
does not provide rollback, exactly-once delivery or automatic outbox handling.
Standalone `projection.run(...)` and failed `enqueue` publication are not retried
by this execution policy.

### Publication hooks

Publication hooks run on the producer around `Projection.enqueue`, independently
of the worker's pipeline hooks. Select a local collection with
`publish_hooks: ClassVar[type[PublishHooks] | None]` on the Projection; select a
shared collection with `create_broker(publish_hooks=AppPublishHooks)`. Shared hooks
run before local hooks, in attachment order. Inheritance works normally;
`publish_hooks = None` disables the local selection without disabling shared hooks.

```python
from dishka import Provider, Scope, provide

from papilio_tasks.tools.hooks import Handler, Hook
from papilio_tasks.tools.hooks.publish import Published, PublishHooks


class RecordSend(Hook[Published]):
    def __init__(self, audit: AuditService):
        self.audit = audit

    async def run(self, event: Published) -> None:
        await self.audit.record(event.task_id)


class AppPublishHooks(PublishHooks):
    def __init__(self, record: RecordSend):
        super().__init__(after_send=(Handler(record),))


class PublishProvider(Provider):
    scope = Scope.REQUEST
    record = provide(RecordSend)
    hooks = provide(AppPublishHooks)


# Your other providers supply AuditService and worker dependencies.
broker = create_broker(
    registrar=registry,
    providers=[PublishProvider(), *providers],
    publish_hooks=AppPublishHooks,
)
```

Registering a hook with a provider supplies construction; selecting its collection
attaches it. Both shared and local collections resolve before publication in one
fresh Dishka request scope, closed after hooks finish. Only producer dependencies
are resolved; publishing does not construct the worker's Projection or its database
services. This scope is independent of the caller's HTTP request/transaction.
Without selected hooks, enqueue calls native publication directly with no scope.

`after_send` receives `Published(call, task_id)` after native `kiq` returns.
`on_error` receives `PublishFailed(call, error)` when native `kiq` raises.
`call` contains the qualified projection name, registered `task_name`, `args` and
a read-only shallow copy of `kwargs`; nested values remain references. These are
in-process observations, not durable or serialized message snapshots.

`Handler(..., failure="continue")` logs a hook error and continues. The default
`"raise"` stops remaining hooks. A failed error hook does not replace the original
send exception, and does not suppress or retry publication. Missing registration,
missing container and DI resolution failures occur before send and do not invoke
send-error hooks. Cancellation propagates directly, with scope cleanup.

If native publication returned but an after-send hook or scope cleanup failed,
`PublishError` carries the returned `task_id` and chains the original error. It
does not invoke send-error hooks or resend. Successful enqueue returns the native
task handle unchanged. A failed `kiq` is **not proof of non-delivery**: network
outcomes may be ambiguous, or native `post_send` middleware may fail after sending.
Neither a successful send nor its hook establishes worker completion.

`project` delegates to enqueue and invokes these hooks once. Service/selector
failures occur before enqueue and do not invoke them. Direct `task().kiq` calls
and Taskiq's automatic retries bypass these producer hooks. Applications own
outbox persistence, transactions and reconciliation; these hooks provide no
automatic delivery guarantee.

### Publish from a function result

`project` decorates an async function or method through its target Projection:

```python
@Product.project(select=lambda result: {"id": result.id})
async def create_product(title: str) -> Row:
    return await service.create(title)
```

It awaits the original function, calls the synchronous `select` with its result,
then awaits `Product.enqueue(**selected)` and returns the original result.
Decorator definition needs no registration or optional runtime imports; task lookup
happens at submission, after the function and selector have run. It does not
preflight broker availability or undo the function if sending fails.
`select` must return a mapping of keyword names for the target `read`. For a batch
projection with `read(ids=...)`, use
`select=lambda rows: {"ids": [row.id for row in rows]}`. Positional-only `read`
arguments can be supplied through direct `enqueue`, not this keyword selector.
The selector itself is never serialized. The decorator preserves the function's
signature and result type; selector contents and `read` arguments remain dynamically
typed. A typed named selector can check access to its own input fields.

Function failure prevents selection/publication. Selection errors, send failures
and cancellation propagate; there is no implicit retry or fire-and-forget task.
Successful publication is not worker completion. Returning from a service does
not establish a transaction commit, and this decorator does not make database
commit and message publication atomic. Outbox/transaction decisions remain owned
by the calling application.

Dishka resolves constructor parameters even when they have defaults. A Projection
inheriting the base constructor exposes optional `hooks` as a dependency. Supply
an explicit provider factory when using the empty local-hook configuration:

```python
from dishka import Provider, Scope, provide


class MyProvider(Provider):
    @provide(scope=Scope.REQUEST)
    def projection(self) -> MyProjection:
        return MyProjection()
```

For a Projection with its own constructor dependencies, register its class or an
ordinary factory as usual. The registrar never fills missing provider bindings.

## Migration and development

- Replace `registry.publisher(Product)` / `Publisher` with `Product.enqueue`,
  `Product.project` and `Product.task`; the Publisher module was removed.
  Projection now permits only one active registration per concrete class/process.
  Reusing the same class in concurrent apps or under multiple aliases is rejected.
  Scheduler class-facing methods remain; `_task` storage moved to internal bindings.
- Applications moved from `papilio_tasks.schedulers`, `.projections`, `.events`
  to `papilio_tasks.apps.schedulers`, `.projections`, `.events`.
- Shared hooks and the `Retry` policy moved to `papilio_tasks.tools.hooks` and
  `papilio_tasks.tools.retry`. Import `Bootstrapper` from
  `papilio_tasks.tools.bootstrap` instead of `papilio_tasks.core.bootstrap`.
  The old module paths were removed; update application imports.
- CLI commands, optional installation extras and native infrastructure paths
  remain unchanged.

- The old `Hook(callback, failure=...)` wrapper is now
  `Handler(hook_instance, failure=...)`; implement `Hook[T].run` on the instance.
  Import payloads from `papilio_tasks.tools.hooks.projection`; the former
  `papilio_tasks.projections.hooks` module was removed.

- Replace Projection overrides `after_read`, `after_write` and `on_error` with
  `Hook[T]` subclasses implementing `run`, attached through `Hooks`. Their
  inputs are now typed payloads, and
  failure continuation is selected on `Handler`, not by returning `True`.
  `_step` was removed. Subclasses with a constructor must call `super().__init__`
  (optionally passing `hooks=`).

- Specify `Projection[T, D, R]` and implement `transform`, or use `Direct[T, R]`
  for existing projections that only implement `read` and `write`.
- Replace imports from `papilio_tasks.schedulers.infra.*` with
  `papilio_tasks.infra.taskiq.*`; the old infrastructure package was removed.
- Install `scheduler`, `scheduler-rabbit` or `scheduler-redis` rather than relying
  on a bare installation or the old `rabbit`/`redis` extras.
- Import specialized scheduler classes from `apps.schedulers.backends.rabbit` or
  `apps.schedulers.backends.redis` instead of their former root modules.
- Replace registrar `factory=` with ordinary providers passed to `create_broker`.
- Use `papilio_tasks scheduler worker/beat` with the factory's native outputs.

Install development dependencies with
`pip install -e '.[dev,scheduler-rabbit,scheduler-redis]'`. Run `pytest`,
`ruff check .`, `ruff format --check .` and `pyright`. Set `TEST_RABBIT_URL` and
`TEST_REDIS_URL` to isolated services for live tests; resources have unique names
and are cleaned up. `TEST_PAPILIO_CLI` can select an installed CLI executable for
the worker/beat integration test. Tests establish behavior, not throughput,
network recovery or exactly-once execution.

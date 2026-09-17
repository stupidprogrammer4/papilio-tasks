<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stupidprogrammer4/papilio-tasks/main/docs/assets/logo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/stupidprogrammer4/papilio-tasks/main/docs/assets/logo-light.png">
    <img src="https://raw.githubusercontent.com/stupidprogrammer4/papilio-tasks/main/docs/assets/logo-light.png" alt="Papilio Tasks" width="560">
  </picture>
</p>
<p align="center"><em>Background jobs and scheduling for modular Python applications.</em></p>
<p align="center">
  <img src="https://img.shields.io/badge/python-3.13%2B-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python 3.13+">
  <a href="https://github.com/taskiq-python/taskiq"><img src="https://img.shields.io/badge/powered_by-Taskiq-D94A28?style=flat-square" alt="Powered by Taskiq"></a>
  <a href="https://github.com/ag2ai/faststream"><img src="https://img.shields.io/badge/powered_by-FastStream-009688?style=flat-square" alt="Powered by FastStream"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-202020?style=flat-square" alt="MIT License"></a>
</p>

---

Modular background jobs for Python 3.13+. Schedulers and projections use Taskiq
and ordinary Dishka providers. Projections run read/transform/write pipelines
locally or through workers. Events provide typed publishers and subscribers for RabbitMQ, Kafka, Redis and
NATS using FastStream and Dishka.

## Version 1 compatibility

The documented import paths, contracts, factory arguments and behavior form the
public API for the 1.x series. Internal binding maps, underscore-prefixed helpers
and third-party internals are not part of that compatibility commitment. Native
handles and backend options follow the installed Taskiq/FastStream dependencies.
See [release notes](https://github.com/stupidprogrammer4/papilio-tasks/blob/main/CHANGELOG.md)
for the initial scope and known limitations, and Migration and development below
for changes from development versions.

CI covers standard CPython 3.13 and 3.14 on Linux. Other operating systems,
free-threaded builds and prerelease Python versions are not currently validated.
`requires-python` states the minimum interpreter version; it does not establish
that future Python releases have already been tested.

## Installation

```bash
pip install 'papilio-tasks[scheduler]'         # in-process Memory default
pip install 'papilio-tasks[scheduler-rabbit]'  # RabbitMQ
pip install 'papilio-tasks[scheduler-redis]'   # Redis Streams
pip install 'papilio-tasks[projection]'        # in-process Memory default
pip install 'papilio-tasks[projection-rabbit]' # RabbitMQ
pip install 'papilio-tasks[projection-redis]'  # Redis Streams / retry source
pip install 'papilio-tasks[events-rabbit]'     # FastStream RabbitMQ events
pip install 'papilio-tasks[events-kafka]'      # FastStream Kafka events
pip install 'papilio-tasks[events-redis]'      # FastStream Redis Streams / PubSub / List events
pip install 'papilio-tasks[events-nats]'       # NATS Core and JetStream events
```

A bare `papilio-tasks` installation provides the lightweight CLI and discovery
core. It does not require Taskiq, Dishka, FastStream, Redis or RabbitMQ. Extras
select dependencies; the distribution includes all source files. Scheduler and
Projection extras are independent and do not install FastStream. For RabbitMQ
transport with a Redis retry source, install both `projection-rabbit` and
`projection-redis`. Each application owns its dependencies.

## FastStream infrastructure

`events-rabbit` installs FastStream's Rabbit and CLI extras and the Dishka
integration, independently of Taskiq. The infrastructure adapter does not import Dishka
or the Events app. The optional app layer is described below.

The adapter takes one already-configured native broker. It does not construct a
connection or client per operation. Its base implements `connect`, `start`, `stop`
and `native`; `RabbitContract` adds native `subscriber`, `publisher`, `publish`,
`declare_queue` and `declare_exchange` operations. Queue/exchange settings use
FastStream's own types. Additional keyword options go to the native method;
advanced options retain native runtime validation through `**options`.

```python
import asyncio

from faststream.rabbit import RabbitBroker as NativeRabbitBroker

from papilio_tasks.infra.faststream.brokers.backends.rabbit import (
    ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue,
)

broker = RabbitBroker(NativeRabbitBroker("amqp://guest:guest@localhost/"))
shutdown = asyncio.Event()
exchange = RabbitExchange("orders", type=ExchangeType.TOPIC, durable=True)

@broker.subscriber(
    RabbitQueue("finance", routing_key="order.created", durable=True),
    exchange,
)
async def finance(event: dict) -> None:
    print("finance", event)

@broker.subscriber(
    RabbitQueue("notifications", routing_key="order.created", durable=True),
    exchange,
)
async def notify(event: dict) -> None:
    print("notify", event)

publisher = broker.publisher(
    exchange=exchange, routing_key="order.created", persist=True,
)

async def serve() -> None:
    try:
        await broker.start()
        await publisher.publish({"order_id": 42})
        # Keep running until your application's shutdown signal.
        await shutdown.wait()  # An asyncio.Event owned by your application.
    finally:
        await broker.stop()
```

Registration only configures native objects. `connect()` opens a connection for
publication without starting subscribers; `start()` also starts consumption and
declares/binds configured subscriptions. A producer-only process should connect,
publish and stop; consumer queues/bindings must already exist for routing.
For explicit topology creation, call `declare_queue(RabbitQueue(...))` and
`declare_exchange(RabbitExchange(...))` after connecting. They return real native
server objects. Queue declaration alone does not bind it to an exchange; configure
a subscriber or bind the returned queue explicitly through its native API.

Distinct subscription queues each receive matching events; consumers of the same
queue compete for its messages. RabbitMQ controls routing and acknowledgements.
`publish` returns the native confirmation (or `None`), not a handler result.
Publisher confirmations do not mean subscribers finished processing. Native
errors propagate; to raise on returned unroutable messages, configure the native
broker with `default_channel=Channel(on_return_raises=True)` (import `Channel`
from `faststream.rabbit`) and publish with `mandatory=True`. The native channel
defaults to `on_return_raises=False`. The adapter adds no retry, default queue or
delivery guarantees.
Use `native` for further FastStream features. TestRabbitBroker is a testing utility,
not a production in-memory broker. Kafka infrastructure is also available below;
other backends for both runtimes remain planned.

### Kafka infrastructure

`events-kafka` installs FastStream's `aiokafka` integration. Import its adapter
explicitly; RabbitMQ, Redis and Taskiq are not required by this extra. The Events
layer also provides Kafka publisher/subscriber classes and a registrar;
see [Kafka Events](#kafka-events) for application setup.

```python
from faststream import AckPolicy
from faststream.kafka import KafkaBroker as NativeKafkaBroker

from papilio_tasks.infra.faststream.brokers.backends.kafka import KafkaBroker

broker = KafkaBroker(NativeKafkaBroker("localhost:9092"))

@broker.subscriber(
    "orders",
    group_id="finance",
    auto_offset_reset="earliest",
    ack_policy=AckPolicy.ACK,
)
async def finance(event: dict) -> None:
    print(event)

publisher = broker.publisher("orders")

async def send() -> None:
    try:
        await broker.connect()
        metadata = await publisher.publish({"order_id": 42}, key=b"customer-7")
        await broker.publish_batch(
            {"order_id": 43}, {"order_id": 44}, topic="orders", partition=0,
        )
    finally:
        await broker.stop()
```

`connect()` starts publication resources without starting subscribers. A consumer
process calls `start()`, waits for its application shutdown signal, then `stop()`.
Construction and registration perform no network I/O. Configure native settings
such as security and serialization on `NativeKafkaBroker`; the adapter reuses
that instance. Kafka's native `connect()` result is its consumer factory, not a
Rabbit-style connection object.

`KafkaContract` adds `subscriber(*topics, **options)`,
`publisher(topic, **options)`, `publish(message, topic, **options)` and
`publish_batch(*messages, topic=..., **options)` to the common lifecycle.
Publisher/subscriber factories return native objects, including batch variants.
For a native batch publisher, call `publisher.publish(message1, message2)`.
Single-message options include key and partition; batch options follow FastStream's
native signature. Options and errors are forwarded without a custom retry loop.

Default publication returns native `RecordMetadata`. With `no_confirm=True`,
awaiting `publish` or `publish_batch` returns the native Future; await that Future
if you need the eventual metadata or delivery error. Neither result means the
consumer completed its business work. Advanced keyword options are validated by
the native library, rather than exhaustively typed by this thin adapter.

Different consumer groups can consume the same records independently. Consumers
inside one group share partition assignments; this is Kafka's group model.
Topic creation, partition counts and retention remain deployment/native admin
responsibilities. The adapter adds no topic administration or Rabbit queue model.
Acknowledgement and offset behavior follow Kafka's native FastStream settings.

Live tests use `TEST_KAFKA_URL=localhost:9092` against an isolated Kafka service;
they create and delete uniquely named topics. They cover single/batch messages,
metadata, keys/headers/partitions, two consumers sharing a group, and a separate
batch consumer group. They are not a throughput or failure-recovery benchmark.

### Redis infrastructure

`events-redis` installs FastStream's native Redis integration. Import
`RedisBroker` from `papilio_tasks.infra.faststream.brokers.backends.redis` and
construct it with your configured `faststream.redis.RedisBroker`. This adapter
is separate from the Taskiq Redis backend. See [Redis Streams Events](#redis-streams-events),
[Redis Pub/Sub Events](#redis-pubsub-events) and [Redis List Events](#redis-list-events)
for app-level setup.

`RedisContract` exposes the shared `connect/start/stop/native` lifecycle plus
native `subscriber`, `publisher`, `publish` and `publish_batch`. Channel routes
accept `str` or `PubSub`; use `list=ListSub(...)` or `stream=StreamSub(...)` for
other subscription modes. Publisher/subscriber factories return native objects.
Construction and registration perform no network I/O. `connect()` returns the
native Redis client and repeated calls reuse it; the native broker owns its pool.

Publication preserves native results: Pub/Sub returns a subscriber count, List
publication returns list length, and Stream publication returns an entry ID.
`publish_batch(*messages, list="jobs")` is the native **List-only** operation.
Options, errors and native pipeline behavior pass through to FastStream; there
is no custom client per send, serialization format or retry loop. Advanced
options remain validated by the native library. Direct infra use bypasses Events
hooks and DI. Live tests require an isolated `TEST_REDIS_URL` and use unique keys.

### NATS infrastructure

Install `papilio-tasks[events-nats]`. `NatsBroker` wraps one caller-configured
`faststream.nats.NatsBroker`; native `JStream`, `PullSub` and consumer settings
remain available without replacement configuration classes.

```python
from faststream.nats import JStream, PullSub
from faststream.nats import NatsBroker as NativeNatsBroker

from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


broker = NatsBroker(NativeNatsBroker("nats://localhost:4222"))


@broker.subscriber("orders.preview")
async def preview(order: dict) -> dict:
    return {"id": order["id"], "accepted": True}


@broker.subscriber("orders.created", queue="finance", no_reply=True)
async def finance(order: dict) -> None:
    print(order["id"])


@broker.subscriber(
    "orders.history",
    stream=JStream("history", subjects=["orders.history"]),
    durable="archive",
    pull_sub=PullSub(batch_size=10),
    no_reply=True,
)
async def archive(order: dict) -> None:
    print(order["id"])
```

`NatsContract` exposes the common `connect/start/stop/native` lifecycle and native
`subscriber`, `publisher`, `publish` and `request`. Registration returns native
publisher/subscriber objects and performs no network I/O. `connect()` returns
and reuses the native NATS client for producer-only use; `start()` also starts
subscriptions and performs configured native stream declarations. Use `stop()`
in `finally` to close resources. A producer that only calls `connect()` needs
its target JetStream stream to exist already. `JStream(..., declare=False)` lets
the application use an externally provisioned stream.

With the server's JetStream enabled and the handlers above started:

- `await broker.publish({"id": 1}, "orders.created")` returns `None`.
- `await broker.publish({"id": 1}, "orders.history", stream="history")`
  returns native `PubAck`; this confirms storage, not successful handler execution.
- `response = await broker.request({"id": 1}, "orders.preview", timeout=3)`
  returns native `NatsMessage`; `await response.decode()` returns the reply body.
  Set `stream=` explicitly when requesting a JetStream handler's reply.

Core NATS delivers to active subscriptions. Subscribers in the same queue group
share work; independent subscriptions each receive a copy. Core does not retain
messages for disconnected subscribers or provide durable processing ACKs.
JetStream adds storage and consumer acknowledgement. Stream retention, durable
consumer identity, push/pull mode and delivery policy remain native configuration.
An ACK updates consumer state; whether an entry is removed depends on stream
retention policy. For typed batch delivery, use `PullSub(batch=True, ...)` and a
handler accepting `list[T]`. Core and JetStream are different delivery modes.

Headers, correlation IDs, reply subjects, timeouts and native errors pass through.
No custom queue declaration, serialization, recovery or retry loop is added.
Advanced operations are accessible through `broker.native`. Direct infra calls
do not run Events hooks or resolve app providers. For application integration,
see [NATS Core Events](#nats-core-events) and
[JetStream Events](#jetstream-events).

Live tests use an isolated `TEST_NATS_URL` with JetStream enabled. They cover Core
broadcast/queue groups, offline backlog with push/pull and batch consumers,
native `PubAck` and consumer ACK, request/reply, metadata and connection cleanup.
They do not establish cluster failover, restart durability or throughput.

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

Memory's public `get_schedule()` and `get_schedules()` return full independent
copies, including nested payloads. Its `native.get_schedules()` is an internal,
borrowed Beat feed: cached timing records start with empty payloads. Do not use
or mutate these records as public snapshots. Taskiq's `pre_send` loads a fresh
copy of the stored payload only for the due task. Replacing or deleting a schedule
invalidates its old feed record, even when a replacement has identical values.
Native send hooks require records obtained from that source's native feed.

This avoids copying every payload during Beat refresh. Public bulk reads still
copy all returned data, and each delivery pays for its own payload copy. Frequent
or simultaneous large deliveries still consume CPU. A cached feed record may
retain its latest delivery payload until replacement, deletion or source disposal;
repeated sends replace that copy rather than accumulating delivery history.

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

For large schedule sets or deployments with separate worker and Beat processes,
prefer `RedisSource` over `MemorySource`. Keep memory storage for tests and small
single-process setups. This recommendation concerns schedule storage; it does not
require using Redis as the task broker or guarantee higher execution throughput.

Measure refresh latency with your expected schedule count and payload sizes.
The current [taskiq-redis 1.2.3 source](https://github.com/taskiq-python/taskiq-redis/blob/1.2.3/taskiq_redis/list_schedule_source.py)
scans for overdue schedules on each refresh by default and reads all cron/interval
schedules. Redis does not eliminate serialization or large-payload processing
costs; these upstream scaling limits still apply.

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
CLI commands and installation extras. Events support RabbitMQ, Kafka, Redis and
NATS registration (see Events below).

The package has three main responsibility groups:

- `apps/`: application bases, contracts, registrars and assembly.
- `infra/`: adapters for task runtimes, brokers, queues and sources.
- `tools/`: discovery, retry settings and hook execution/payloads.

Applications depend on tools and infrastructure; infrastructure may use independent
policy types from tools. Tools do not import apps, infrastructure or optional
runtimes. Both `apps` and `tools` have lightweight package initializers.
`cli/` remains the command entry point. `tools/retry.py` holds configuration;
`infra/taskiq/retry.py` adapts it to Taskiq.

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
Taskiq, Dishka or database dependencies. App-managed manual execution remains
subsequent work; queued workers and retry beats have dedicated CLI commands below.

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

### Backend-specific Projection queues

Use `RabbitProjection[T, D, R]` or `RedisProjection[T, D, R]` with the matching
registrar to select queues per class. `RabbitDirect[T, R]` and `RedisDirect[T, R]`
reuse the identity transform from `Direct`. The pipeline, hooks, DI and retry
policy remain shared.

```python
from typing import ClassVar

from papilio_tasks.apps.projections.backends.rabbit import (
    RabbitBroker, RabbitDirect, RabbitQueue,
)
from papilio_tasks.apps.projections.registry.rabbit import RabbitRegistrar

products = RabbitQueue(name="products", durable=True)

class Product(RabbitDirect[int, int]):
    queue: ClassVar[RabbitQueue | None] = products

    async def read(self, id: int) -> int:
        return id  # Fetch from database A through injected services.

    async def write(self, data: int) -> int:
        return data  # Write to database B through injected services.

class Products(RabbitDirect[list[int], int]):
    queue: ClassVar[RabbitQueue | None] = products

    async def read(self, ids: list[int]) -> list[int]:
        return ids

    async def write(self, data: list[int]) -> int:
        return len(data)

transport = RabbitBroker("amqp://guest:guest@localhost/")
registry = RabbitRegistrar(transport)
registry.include(Product)
registry.include(Products)
transport.consume("products")  # Select queues for this worker.
# Pass registry and ordinary module providers to create_broker.
```

Single and batch tasks have distinct task names and share the same queue. Identical
queue specifications can be reused; conflicting settings for the same name fail
during inclusion. Rabbit routing keys come from `RabbitQueue.routing_key`; do not
override them through conflicting task labels.

Selection order is `include(..., queue=spec)` → inherited class `queue` → broker
default. An include override does not change the class. As with Schedulers,
`queue=None` on include means use the class setting; a class whose queue is `None`
uses the broker default. Strings are not queue specifications. Backend classes
require their matching registrar; plain `Projection`/`Direct` with the common
`Registrar` retain default-queue behavior.

For Redis, import `RedisProjection`, `RedisDirect`, `RedisQueue` and
`RedisStreamBroker` from `apps.projections.backends.redis`, and `RedisRegistrar`
from `apps.projections.registry.redis`. A `RedisQueue(name="products")` selects the
publication destination. Configure worker subscriptions explicitly using the
broker's `queue_name` and `additional_streams`; class queue selection does not
change subscriptions or consumer groups. Several projections can share a stream.

Inclusion registers queue configuration and a task without opening connections.
Rabbit's broker lifecycle or explicit `declare_queue` performs server declaration;
Redis retains its native stream lifecycle. Discovery via
`create_broker(registrar=registry, modules=[...])` uses the supplied backend registrar
and each class's queue. Install only the selected `projection-rabbit` or
`projection-redis` extra; neither backend imports the other.

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
        await self.audit.record(event.result.task_id)


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

`after_send` receives `Published(call, result)` after native `kiq` returns;
`result` is the unchanged task handle (`event.result.task_id`).
`on_error` receives `PublishFailed(call, error)` when native `kiq` raises.
`call.sender` identifies the qualified Projection class. `call.meta["task_name"]`
contains its registered Taskiq name; `call.args` and a read-only shallow copy of
`call.kwargs` describe the invocation. Metadata is also read-only; nested values
remain references. These are in-process observations, not durable or serialized
message snapshots.

`Handler(..., failure="continue")` logs a hook error and continues. The default
`"raise"` stops remaining hooks. A failed error hook does not replace the original
send exception, and does not suppress or retry publication. Missing registration,
missing container and DI resolution failures occur before send and do not invoke
send-error hooks. Cancellation propagates directly, with scope cleanup.

If native publication returned but an after-send hook or scope cleanup failed,
`PublishError.result` carries the returned task handle and chains the original
error (`error.result.task_id` retrieves its ID). It does not invoke send-error
hooks or resend. Successful enqueue returns the native
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

The [CI workflow](https://github.com/stupidprogrammer4/papilio-tasks/blob/main/.github/workflows/ci.yml) runs on pushes, pull requests and manual
dispatches using Python 3.13 and 3.14. It installs all supported backend extras, checks
style/types, runs the full suite against isolated Redis, RabbitMQ, Kafka and NATS
with JetStream, and builds the distributions. Skipped tests fail CI, so missing
backend dependencies or services cannot silently reduce coverage. Test reports
and service logs are retained as workflow artifacts; CI does not publish packages.

## Events

Use your existing Pydantic model or dataclass as the payload. A publisher declares
its backend route; each subscriber references that publisher and selects its
queue (Rabbit), Kafka consumer group, Redis configuration or NATS subject/group.
Services enter subscribers through ordinary Dishka providers. The first example uses RabbitMQ.

```python
import asyncio

from dishka import Provider, Scope, provide
from faststream.rabbit import RabbitBroker as NativeRabbitBroker
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.rabbit import (
    ExchangeType,
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker


class OrderData(BaseModel):
    order_id: int


class OrderCreated(RabbitPublisher[OrderData]):
    exchange = RabbitExchange("orders", type=ExchangeType.TOPIC)
    routing_key = "order.created"


class FinanceService:
    async def record_order(self, order_id: int) -> None:
        print("Record order", order_id)


class Finance(RabbitSubscriber[OrderData]):
    publisher = OrderCreated
    queue = RabbitQueue("finance.orders", durable=True)

    def __init__(self, service: FinanceService):
        self.service = service

    async def run(self, event: OrderData) -> None:
        await self.service.record_order(event.order_id)


class Dependencies(Provider):
    service = provide(FinanceService, scope=Scope.REQUEST)
    finance = provide(Finance, scope=Scope.REQUEST)


registry = RabbitRegistrar(
    RabbitBroker(NativeRabbitBroker("amqp://guest:guest@localhost/"))
)
registry.publisher(OrderCreated, persist=True)
registry.subscriber(Finance)
app = create_app(registrar=registry, providers=[Dependencies()])
shutdown = asyncio.Event()  # Set by your application's shutdown handler.


async def serve() -> None:
    try:
        await app.start()
        await OrderCreated.publish(OrderData(order_id=42))
        await shutdown.wait()
    finally:
        await app.stop()
```

A producer-only process registers its publishers, calls `create_app` without
subscriber providers and uses `await app.connect()` before publishing. The
exchange, consumer queues and bindings must already exist (provision them or start
the worker first). `connect()` does not start consumers or provision topology.
A worker registers subscribers and uses `await app.start()`. Registering a
subscriber reads its publisher's route; it does not register a producer binding
or construct any producer service. Workers can also register publishers when
needed. Assembly itself opens no connections.

The same event can have finance, notification and commerce subscribers: reference
`OrderCreated` from each class and give each a different queue. Replicas sharing
a queue compete for messages; they do not each receive a copy. This is native
RabbitMQ routing, not a Python loop calling all subscribers. A publisher requires
a named exchange. The queue's empty routing key is filled from the publisher
without modifying the class's queue; an explicitly different key is rejected.
Conflicting same-name topology definitions are rejected within one registrar;
RabbitMQ validates topology across processes.

Manual registration must finish before `create_app`; optional module discovery
runs during assembly (see below). Native options
such as `ack_policy`, `consume_args` and `channel` can be passed to
`registry.subscriber`; publication options such as `headers`, `message_id` and
`correlation_id` can be passed to `publish`. Additional options retain native
runtime validation. Subscriber `run(self, event: Payload)` must be async. Its
return is ignored; it does not automatically publish a reply. Dishka resolves
subscribers within the message scope and closes request resources on success or
failure. For message metadata in a provider, use the integration's `StreamMessage`
context; concrete `RabbitMessage` injection requires an explicit context provider.

Set a Rabbit subscriber's acknowledgement policy on the class to apply it during
module discovery as well as manual registration:

```python
from faststream import AckPolicy


class Finance(RabbitSubscriber[OrderData]):
    publisher = OrderCreated
    queue = RabbitQueue("finance.orders")
    ack_policy = AckPolicy.NACK_ON_ERROR

    async def run(self, event: OrderData) -> None:
        ...
```

`ack_policy` is an inherited `ClassVar[AckPolicy | None]`, defaulting to `None`.
Selection order is an explicit non-`None` registration argument, then the class
setting, then the native broker default. For example:

```python
registry.subscriber(Finance, ack_policy=AckPolicy.REJECT_ON_ERROR)
```

Manual registration before `create_app` is preserved by discovery. Passing
`ack_policy=None` at registration uses the class setting; it does not reset it.
A subclass can set its class policy to `None` to inherit the broker default
instead. Without any selection, Rabbit's native default is `REJECT_ON_ERROR`.
Policies must be FastStream `AckPolicy` values, not strings.

`NACK_ON_ERROR` allows redelivery of the whole handler, including when `run`
succeeds but an `after_run` hook raises. The hook's independent
`Handler(..., failure="continue")` setting can log that hook failure and continue.
Rejecting without requeue sends the message to a configured dead-letter exchange;
without one, it is discarded. Acknowledgement policy does not add a retry delay,
backoff or attempt budget.

`publish` returns the native publication result, not subscriber results. It does
not guarantee business processing or turn database commits and sends into a
transaction. Native errors propagate; this layer adds no retry policy. Optional
app-wide publication and subscriber hooks are described below.
Broker confirmation and unroutable-message behavior depend on the selected native
channel/options, just as in the infrastructure adapter.

Runtime bindings live outside user classes. A publisher class has one active
sender per process; a second registration fails rather than replacing it. Stop
releases only this app's bindings and closes its container even if broker shutdown
raises. Always call `stop()` in `finally`, including after failed startup. A stopped
app/native broker is not reassembled with another container: create a fresh native
broker and registrar. Use separate publisher subclasses for simultaneous producer
bindings to different brokers. Lifecycle calls can integrate with the host
application's entry point; standalone consumers use the Events CLI below.

### Kafka Events

Install `papilio-tasks[events-kafka]`. Define the topic on a `KafkaPublisher`
and an explicit, nonempty `group_id` on each `KafkaSubscriber`. Use different
consumer groups for independent business handlers; replicas of the same handler
share a group and Kafka distributes partitions between them.

```python
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream.kafka import KafkaBroker as NativeKafkaBroker
from pydantic import BaseModel

from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.kafka import KafkaPublisher
from papilio_tasks.apps.events.registry.kafka import KafkaRegistrar
from papilio_tasks.apps.events.subscribers.kafka import KafkaSubscriber
from papilio_tasks.infra.faststream.brokers.backends.kafka import KafkaBroker


class OrderData(BaseModel):
    id: int


class OrderCreated(KafkaPublisher[OrderData]):
    topic: ClassVar[str] = "orders.created"


class Accounting:
    async def record(self, order: OrderData) -> None:
        print(order.id)


class Finance(KafkaSubscriber[OrderData]):
    publisher = OrderCreated
    group_id: ClassVar[str] = "finance.orders"

    def __init__(self, accounting: Accounting) -> None:
        self.accounting = accounting

    async def run(self, event: OrderData) -> None:
        await self.accounting.record(event)


class Services(Provider):
    accounting = provide(Accounting, scope=Scope.REQUEST)
    finance = provide(Finance, scope=Scope.REQUEST)


registry = KafkaRegistrar(KafkaBroker(NativeKafkaBroker("localhost:9092")))
registry.publisher(OrderCreated)
registry.subscriber(Finance, auto_offset_reset="earliest")
app = create_app(registrar=registry, providers=[Services()])


@publish(OrderCreated, select=lambda result: OrderData(id=result["id"]))
async def create_order() -> dict:
    return {"id": 42}
```

Run this module with `papilio_tasks events run myapp.events:app`. Producers can
assemble an app with only publishers, call `await app.connect()`, publish with
`await OrderCreated.publish(OrderData(id=42), key=b"customer-7")`, and stop it in
`finally`. `publish` and the decorator use the same registered sender and hooks;
the decorator returns the wrapped function's original result.

For modular apps, put declarations in `publishers.py` and `subscribers.py` (or
packages of those names) and pass their package roots through `create_app`'s
`publishers` and `subscribers` arguments. Providers remain explicit. Manual
registration before discovery preserves its native options. Pass `publish_hooks`
and `subscribe_hooks` to `create_app` exactly as with Rabbit; hook classes and
subscribers resolve through the provided Dishka container.

`ack_policy` is an optional inherited class variable. Selection is explicit
non-None registrar argument, then class value, then the native default. `None`
means no selection at that level. Native Kafka settings, including
`enable_auto_commit`, still determine offset behavior; Rabbit requeue/dead-letter
semantics do not carry over to Kafka. Advanced options belong to native broker
configuration or registrar calls. Topic administration remains outside the app.

This app API publishes one event per call. Native batch operations remain
available through the infra adapter; they bypass app-level publication hooks.
Kafka `no_confirm=True` returns the native Future: `after_send` observes that
return, not the Future's eventual delivery result. Await the returned Future
explicitly when confirmation is required. No automatic retry or beat is added.

### Redis Streams Events

Install `papilio-tasks[events-redis]`. Use `StreamPublisher[T]` for the stream
name and `StreamSubscriber[T]` for a typed handler with native `StreamSub`
configuration. Groups and consumer identities are selected by your application.

```python
import os
import socket
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream.redis import RedisBroker as NativeRedisBroker
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.streams import StreamPublisher
from papilio_tasks.apps.events.registry.streams import StreamRegistrar
from papilio_tasks.apps.events.subscribers.streams import (
    StreamSub,
    StreamSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker


class OrderData(BaseModel):
    id: int


class OrderCreated(StreamPublisher[OrderData]):
    stream: ClassVar[str] = "orders.created"


class Finance(StreamSubscriber[OrderData]):
    publisher = OrderCreated
    stream: ClassVar[StreamSub] = StreamSub(
        OrderCreated.stream,
        group="finance",
        consumer=f"{socket.gethostname()}-{os.getpid()}",
    )

    async def run(self, event: OrderData) -> None:
        print(event.id)


class Services(Provider):
    finance = provide(Finance, scope=Scope.REQUEST)


registry = StreamRegistrar(
    RedisBroker(NativeRedisBroker("redis://localhost:6379/0"))
)
registry.publisher(OrderCreated)
registry.subscriber(Finance)
app = create_app(registrar=registry, providers=[Services()])
```

Expose this app to `papilio_tasks events run myapp.events:app`. A producer can
register only `OrderCreated`, call `await app.connect()` and then
`await OrderCreated.publish(OrderData(id=42))`. Stop the app in `finally`.
Publication returns the native stream entry ID (`bytes`), not a consumer result.
The existing `@publish(OrderCreated, select=...)` decorator also works.

Different groups consume independently. Replicas sharing a group distribute
messages; choose a distinct consumer name for each active replica. The example
uses hostname and PID; deployment configuration can supply another identity.
`StreamSub` also supports reading without a group. Group/consumer combinations,
starting position, declaration and pending-message recovery follow native
FastStream configuration. The default new group starts at the stream's current
end; registration alone does not replay its history.

`registry.subscriber(Finance, stream=StreamSub(...))` can explicitly override the
class configuration for that process. Its name must match the publisher's stream.
The registrar copies the selected configuration; it does not mutate the class.
Manual registration wins over discovery. As with other Events backends, package
roots go to `create_app(publishers=..., subscribers=...)`, services and hook
classes go in providers, and `publish_hooks`/`subscribe_hooks` select app hooks.
`Published.result` preserves the native entry ID; `call.meta` contains `stream`.

The optional inherited `ack_policy` follows explicit non-None registrar argument,
then class value, then native default. With grouped reads and normal tracking,
`ACK` removes a processed entry from the group's pending list. It does not delete
the stream entry. In FastStream 0.7.5, failed `NACK_ON_ERROR` and `REJECT_ON_ERROR`
handlers leave the entry pending; `MANUAL` also leaves it pending until explicitly
acknowledged. `ACK` acknowledges even when the handler raises. These outcomes were
tested against Redis; they do not imply Rabbit-style requeue or dead lettering.
Unacknowledged entries need the application's selected native recovery mechanism.
This adapter adds no retry engine, beat or pending-message recovery loop.

Streams, [Pub/Sub Events](#redis-pubsub-events) and
[List Events](#redis-list-events) classes are available.
Native `publish_batch` targets a Redis List, not a Stream. Calling infra directly
bypasses app hooks. Cluster/Sentinel deployments and crash recovery are not covered
by the current standalone Redis tests.

### Redis Pub/Sub Events

Use the same `papilio-tasks[events-redis]` extra and Redis infra adapter.
`ChannelPublisher[T]` declares a channel. `ChannelSubscriber[T]` references a
publisher and can select native `PubSub` configuration for a channel or pattern.

```python
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream.redis import RedisBroker as NativeRedisBroker
from pydantic import BaseModel

from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.channels import ChannelPublisher
from papilio_tasks.apps.events.registry.channels import ChannelRegistrar
from papilio_tasks.apps.events.subscribers.channels import (
    ChannelSubscriber,
    PubSub,
)
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker


class Price(BaseModel):
    value: int


class GoldChanged(ChannelPublisher[Price]):
    channel: ClassVar[str] = "prices.gold"


class Display(ChannelSubscriber[Price]):
    publisher = GoldChanged

    async def run(self, event: Price) -> None:
        print(event.value)


class AllPrices(Display):
    channel: ClassVar[PubSub | None] = PubSub("prices.*", pattern=True)


class Services(Provider):
    display = provide(Display, scope=Scope.REQUEST)
    all_prices = provide(AllPrices, scope=Scope.REQUEST)


registry = ChannelRegistrar(
    RedisBroker(NativeRedisBroker("redis://localhost:6379/0"))
)
registry.publisher(GoldChanged)
registry.subscriber(Display)
registry.subscriber(AllPrices)
app = create_app(registrar=registry, providers=[Services()])


@publish(GoldChanged, select=lambda result: Price(value=result["price"]))
async def update_price() -> dict:
    return {"price": 42}
```

`Display` uses `GoldChanged.channel` by default. An explicit
`registry.subscriber(Display, channel=PubSub(...))` takes precedence over the
inherited class setting; a non-None class setting takes precedence over the
publisher-derived channel. `None` means fallback at that level. Assigning
`channel = None` on a subclass restores its publisher-derived default.
Selected native configuration is copied; user classes are not mutated.

An explicit channel or pattern intentionally can cover other publishers. Its
matching is delegated to FastStream/Redis, and its payloads must be compatible
with the subscriber's `run(event: T)` type. No Python pattern-matching or routing
engine is added. Native `polling_interval` and other explicit registrar options
retain their native behavior.

Use `create_app` with module discovery, providers, `publish_hooks` and
`subscribe_hooks` as with other Events backends. Expose the app to
`papilio_tasks events run myapp.events:app`. Producer-only apps call `connect()`,
publish through `await GoldChanged.publish(Price(value=42))`, and `stop()` in
`finally`. Direct publication and the decorator share the registered sender and
hooks. Publish hook metadata contains the registered `channel`; per-call routing
options may select another channel. `Published.result` preserves the native
integer recipient count. Zero recipients is a successful publication with result
`0`, not a send exception; it still invokes `after_send`.

Each active matching subscription receives a broadcast, including subscriptions
in separate worker processes. Multiple workers do not form a consumer group.
Redis's publication count is not proof that handlers completed successfully.
Messages sent while subscribers are disconnected are not retained or replayed.
Pub/Sub has no durable acknowledgement, pending list or consumer group, so these
are not class settings on `ChannelSubscriber`. Native advanced options do not
add durable retries. Use Streams when those delivery mechanisms are needed.

The live checks cover separate processes, exact and wildcard channels, native
counts including zero, headers, DI/hooks, clean shutdown and reconnect without
replay. Cluster/sharded Pub/Sub and network-failure recovery are not covered.

### Redis List Events

Use `papilio-tasks[events-redis]` and the same Redis infra adapter.
`ListPublisher[T]` declares a list name. `ListSubscriber[T]` references that
publisher; optional native `ListSub` configures polling and batch consumption.

```python
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream.redis import RedisBroker as NativeRedisBroker
from pydantic import BaseModel

from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.lists import ListPublisher
from papilio_tasks.apps.events.registry.lists import ListRegistrar
from papilio_tasks.apps.events.subscribers.lists import ListSub, ListSubscriber
from papilio_tasks.infra.faststream.brokers.backends.redis import RedisBroker


class Order(BaseModel):
    id: int


class Created(ListPublisher[Order]):
    list: ClassVar[str] = "orders.finance"


class Finance(ListSubscriber[Order]):
    publisher = Created

    async def run(self, event: Order) -> None:
        print(event.id)


class Services(Provider):
    finance = provide(Finance, scope=Scope.REQUEST)


registry = ListRegistrar(
    RedisBroker(NativeRedisBroker("redis://localhost:6379/0"))
)
registry.publisher(Created)
registry.subscriber(Finance)
app = create_app(registrar=registry, providers=[Services()])


@publish(Created, select=lambda result: Order(id=result["id"]))
async def create_order() -> dict:
    return {"id": 42}
```

The subscriber defaults to `ListSub(Created.list)`. An explicit
`registry.subscriber(Finance, list=ListSub(...))` takes precedence over the
inherited class setting; `None` falls back to the class setting, then the
publisher-derived default. Assigning `list = None` on a subclass restores that
default. The selected list name must match the referenced publisher. Native
configuration is copied and user classes are not mutated.

For batch consumption, use the following subscriber instead of `Finance` and
register `FinanceBatch` in your provider. The type alias keeps the payload type
separate from the class's `list` configuration attribute:

```python
type Orders = list[Order]


class FinanceBatch(ListSubscriber[Orders]):
    publisher = Created
    list: ClassVar[ListSub | None] = ListSub(
        Created.list, batch=True, max_records=100, polling_interval=0.1
    )

    async def run(self, event: Orders) -> None:
        print([order.id for order in event])
```

A batch contains up to `max_records` available messages; it does not wait to
fill that size. Subscribe hooks run once for each handler invocation, receiving
the decoded list for a batch. Each invocation shares its Dishka scope with its
hooks and injected services.

Module discovery, `publish_hooks`, `subscribe_hooks`, the `publish` decorator
and `papilio_tasks events run myapp.events:app` work as with the other backends.
Producer-only apps use `connect()`, `await Created.publish(Order(id=42))` and
`stop()` in `finally`. Publication returns the native integer list length after
insertion, not a count of successful handlers. Hook metadata includes the
registered `list`; native per-call routing options may override the destination.
Infra `publish_batch(..., list=...)` remains available, but direct infra calls
bypass Events publish hooks. There is no class-level bulk publishing API.

Messages remain in the list while workers are offline, subject to Redis data
retention and persistence configuration. Consumers of the same list compete:
each popped entry goes to one consumer, including across worker processes. Use
separate destinations when finance and notifications must both receive a copy.
List insertion/pop order does not guarantee completion order across workers.

FastStream removes entries with `BLPOP` (single) or `LPOP` (batch) **before**
calling the handler. A failure or worker crash after removal does not restore
the message. List subscribers therefore expose no consumer-group or durable
acknowledgement settings; hooks alone do not provide crash-safe delivery.
Use Streams with an explicit recovery strategy when that behavior is needed.
The live tests cover backlog, typed single/batch delivery, failure without
requeue, competing CLI processes and resource cleanup. Redis restart persistence,
cluster operation and network-failure recovery are outside these checks.

### NATS Core Events

Use `papilio-tasks[events-nats]`. `NatsPublisher[T]` declares a subject;
`NatsSubscriber[T]` references a publisher and optionally selects a subject
pattern and queue group. Group names are chosen by the application.

```python
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream.nats import NatsBroker as NativeNatsBroker
from pydantic import BaseModel

from papilio_tasks.apps.events import publish
from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.nats import NatsPublisher
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.subscribers.nats import NatsSubscriber
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


class Order(BaseModel):
    id: int


class OrderCreated(NatsPublisher[Order]):
    subject: ClassVar[str] = "orders.created"


class Finance(NatsSubscriber[Order]):
    publisher = OrderCreated
    queue: ClassVar[str] = "finance"

    async def run(self, event: Order) -> None:
        print(event.id)


class Notifications(Finance):
    subject: ClassVar[str | None] = "orders.*"
    queue: ClassVar[str] = "notifications"


class Services(Provider):
    finance = provide(Finance, scope=Scope.REQUEST)
    notifications = provide(Notifications, scope=Scope.REQUEST)


registry = NatsRegistrar(
    NatsBroker(NativeNatsBroker("nats://localhost:4222"))
)
registry.publisher(OrderCreated)
registry.subscriber(Finance)
registry.subscriber(Notifications)
app = create_app(registrar=registry, providers=[Services()])


@publish(OrderCreated, select=lambda result: Order(id=result["id"]))
async def create_order() -> dict:
    return {"id": 42}
```

The subscriber's default `subject = None` uses its publisher's subject.
Explicit `registry.subscriber(..., subject=..., queue=...)` values override
inherited class values. `None` falls back at that level; `queue=""` explicitly
clears a class group. A subclass can restore its publisher-derived subject with
`subject = None`. Registration does not mutate user classes.

With no group (`queue = ""`, the default), every active subscription receives
a copy. Members of the same queue group share matching messages across workers.
In the example, finance and notifications each receive a copy; replicas within
each named group share that group's work. A queue group is not a durable queue.
Native `*` matches one subject token and `>` matches one or more trailing tokens.
An explicit pattern can cover other publishers, whose payloads must match the
handler's DTO. Matching remains native; no Python routing engine is added.

Use the existing `create_app` module discovery, explicit providers,
`publish_hooks`, `subscribe_hooks` and `papilio_tasks events run myapp.events:app`.
Each message gets its own Dishka scope shared by its handler and subscribe hooks.
Producer-only apps use `connect()`, `await OrderCreated.publish(Order(id=42))`,
and `stop()` in `finally`. Direct publication and the decorator reuse the same
sender and hooks. Publish metadata contains the registered `subject`; native
per-call options can select a different destination. The native result is `None`,
including when there are no subscribers, and still invokes `after_send`.
Publication returning successfully does not confirm subscriber completion.

Core retains no offline backlog and exposes no durable consumer or ACK policy
ClassVars. Subscribe hooks can observe handler failure but do not create message
persistence. Use [JetStream Events](#jetstream-events) for persistent streams
and durable consumers. Request/reply remains an infra API; Events handlers use
`run(event: T) -> None` and no auto-reply.

Live checks cover both subject wildcards, disconnect without replay, native send
results, DTO/DI/hooks, and two CLI processes combining queue-group competition
with independent broadcast subscriptions. Shutdown closes message and application
resources. Cluster failover and network recovery are not covered by these checks.

### JetStream Events

Use the same `papilio-tasks[events-nats]` extra with JetStream enabled on NATS.
`JetPublisher[T]` declares a subject and native `JStream`; `JetSubscriber[T]`
references that publisher. The stream belongs to the publisher declaration.

```python
from typing import ClassVar

from dishka import Provider, Scope, provide
from faststream import AckPolicy
from faststream.nats import JStream, PullSub
from faststream.nats import NatsBroker as NativeNatsBroker
from nats.js.api import ConsumerConfig
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.jetstream import JetPublisher
from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
from papilio_tasks.apps.events.subscribers.jetstream import JetSubscriber
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


class Order(BaseModel):
    id: int


class OrderCreated(JetPublisher[Order]):
    subject: ClassVar[str] = "orders.created"
    stream: ClassVar[JStream] = JStream("orders", subjects=["orders.*"])


class Finance(JetSubscriber[Order]):
    publisher = OrderCreated
    durable: ClassVar[str | None] = "finance"
    pull_sub: ClassVar[bool | PullSub] = PullSub(timeout=1)
    config: ClassVar[ConsumerConfig | None] = ConsumerConfig(
        ack_wait=30, max_deliver=3,
    )
    ack_policy: ClassVar[AckPolicy | None] = AckPolicy.NACK_ON_ERROR

    async def run(self, event: Order) -> None:
        print(event.id)


class Services(Provider):
    finance = provide(Finance, scope=Scope.REQUEST)


registry = JetRegistrar(
    NatsBroker(NativeNatsBroker("nats://localhost:4222"))
)
registry.publisher(OrderCreated)
registry.subscriber(Finance)
app = create_app(registrar=registry, providers=[Services()])
```

Run `papilio_tasks events run myapp.events:app`. Workers using the same stream
and pull durable share that consumer's messages. Use a different durable for an
independent consumer. These names are explicit; the framework invents none.
`queue` is for native push queue groups, not pull consumers. The default remains
native push (`pull_sub=False`); explicit pull plus durable is recommended for
shared workers. Do not supply a push `durable` when you intend a scalable push
queue group; use `queue` for that mode.

A subscriber can override `subject` with a native pattern; `None` derives its
publisher's subject. Explicit non-`None` registrar options override inherited
class settings. `pull_sub=False` and `queue=""` are explicit overrides. Reset
optional settings on a subclass with `None`. `JStream`, `PullSub` and
`ConsumerConfig` are copied before registration, so native registration cannot
mutate reusable class settings. Set the `durable` field explicitly rather than
assuming `config.durable_name` supplies the native pull binding argument.

For batches use `PullSub(batch=True, batch_size=100, timeout=1)` and
`async def run(self, event: list[Order]) -> None`. Native delivery, acknowledgement,
retention and consumer configuration apply. `faststream.AckPolicy` controls
handler outcome handling; `ConsumerConfig.ack_policy` is the NATS protocol's
separate setting. `NACK_ON_ERROR` allows native redelivery after a handler error;
`MANUAL` requires explicitly acknowledging through the injected message.
`max_deliver` bounds deliveries for that consumer. Hooks observe outcomes and
use the same request scope as the handler; they do not implement retries.
Handlers and side effects must tolerate redelivery.

`await OrderCreated.publish(Order(id=42))` returns the native `PubAck` and shared
publish hooks receive that same result. It confirms the server's stream publish
acknowledgement, not successful business processing. Failures propagate unchanged
and invoke `on_error`. Metadata contains the registered `subject` and `stream`
name; per-call native options remain separate. The shared `@publish(...,
select=lambda result: ...)` decorator also works and preserves the wrapped
function's return value.

`start()` can declare the configured stream; producer-only `connect()` does not.
Provision streams before sending from a producer-only app, or start an app that
declares them first. Use `JStream(..., declare=False)` for externally managed
streams. A stopped consumer can later receive retained messages, subject to the
stream's retention policy and consumer delivery policy. An ACK does not imply
that the stream record is deleted under every retention policy.

Live tests cover offline backlog, push/pull/batch decoding, native publish ACKs
and errors, manual and automatic ACKs, bounded redelivery, shared durable workers
and independent consumers, module discovery, DI/hooks/decorator, and CLI cleanup.
They do not establish restart/cluster durability or exactly-once side effects.

### Events CLI

Expose the result of `create_app(...)` as `app` in your entry module, then run:

```bash
papilio_tasks events run myapp.events:app
papilio_tasks events run myapp.events:app --workers 4
papilio_tasks events run myapp.events:app --reload
papilio_tasks events run --help
```

For a synchronous factory, expose a function that builds fresh broker, registrar,
providers and application objects on every call:

```python
def build():
    registrar = RabbitRegistrar(RabbitBroker(NativeRabbitBroker(settings.amqp)))
    return create_app(
        registrar=registrar,
        providers=[Services()],
        publishers=["myapp.modules"],
        subscribers=["myapp.modules"],
    )
```

```bash
papilio_tasks events run myapp.events:build --factory --workers 4
papilio_tasks events run myapp.events:build --factory --app-dir ./src
```

`events` installation extras include FastStream's CLI dependencies. Commands
forward to the native FastStream runner, including worker processes, reload,
logging and application loading. `--reload` and multiple workers cannot be used
together. Factory/import time is assembly only; open external resources during
lifecycle or DI resolution, since the native supervisor also loads the entry.

The returned Application supports FastStream execution while retaining `connect`,
`start` and `stop`: `connect` is for publishers only, while CLI execution starts
consumption. On shutdown, broker processing stops before the DI container closes
and publisher bindings are released. Startup failure and cancellation also run
cleanup. Each worker owns its own container and connections. Events needs no beat.
Existing publish and subscribe hooks use their normal DI scopes in CLI execution.


### Publish after a function succeeds

Use the standalone `publish` decorator to select the message from an async
function's return value. Direct `OrderCreated.publish(payload)` remains available.

```python
from papilio_tasks.apps.events import publish


class Orders:
    @publish(
        OrderCreated,
        select=lambda order: OrderData(order_id=order.id),
        headers={"origin": "orders"},
    )
    async def create(self, data: OrderInput) -> Order:
        return await self.repository.create(data)
```

The wrapper awaits the function, calls the synchronous selector once, awaits
`OrderCreated.publish` once, and returns the original function result unchanged.
`select` returns the Publisher's single payload, not a mapping of keyword
arguments. Extra decorator options forward to native publication. Existing
app-wide publish hooks run through the same send path, without duplicate hooks.
The function's metadata, call signature and result type are preserved; a selector
with its own parameter annotation can also check access to its input fields.

Only async functions/methods are accepted. Definition requires neither runtime
installation nor prior registration; the publisher must be registered when the
function is invoked. Function/selector exceptions or cancellation prevent sending.
Send errors and `PublishError` from post-send hooks propagate instead of returning
the function result. There is no detached background send, automatic retry or
implicit database commit: the function owns its transaction boundary.

### Events layout and imports

`publishers/` owns the Publisher base/decorator, runtime bindings, sender contract,
producer hook adapter (`send.py`) and backend publisher definitions. `subscribers/`
owns the Subscriber base and backend definitions. `registry/` coordinates native
registration of both roles; `application.py` owns discovery, DI and lifecycle.
Shared hook contracts stay in `tools/hooks`; transport code stays in `infra`.

Pure public imports remain available from the Events package. Backend imports
are explicit and optional:

```python
from papilio_tasks.apps.events import Publisher, Subscriber, publish
from papilio_tasks.apps.events.publishers.rabbit import RabbitPublisher
from papilio_tasks.apps.events.subscribers.rabbit import RabbitSubscriber
```

These replace the previous combined `events.backends.rabbit` imports. The former
root `base.py`, `bindings.py`, `contracts.py` and producer `publish.py` modules have
moved into their owning groups; they are not maintained as duplicate import paths.
Package initializers import only the pure public classes/decorator.

### Events discovery

For modular applications, let the existing Bootstrapper find definitions under
selected Python package roots:

```python
app = create_app(
    registrar=registry,
    providers=[OrdersProvider(), FinanceProvider()],
    publishers=("shop.orders",),
    subscribers=("shop.finance", "shop.notifications"),
)
```

The `publishers` roots are searched for `publishers.py` or `publishers/` packages;
`subscribers` roots are searched for `subscribers.py` or `subscribers/`. Nested
packages and definitions in those packages' `__init__.py` files are supported.
Only concrete classes defined in discovered modules are registered. Imported base
classes and imported publisher references are not extra registrations; aliases
and overlapping roots are handled once in deterministic order.

The two path lists are independent and default to empty. A producer can select
only its publisher package roots without loading consumer packages elsewhere.
As with the shared Bootstrapper, Python imports execute package initializers;
keep them free of startup side effects and use narrow roots for isolation.
User imports from those initializers can still load other modules. Subscriber-only
discovery reads referenced publisher routes but does not bind those publishers
for sending. Select publisher roots or register them manually when sending from
the same application is needed.

Providers remain explicit: discovery does not construct services or register
subscriber factories in Dishka. Manual registrations keep their options when the
same class is also discovered. Different classes sharing a route or queue remain
distinct registrations; a sender already owned by another app is an error.
The backend registrar rejects definitions from another backend. Import and
registration errors propagate during assembly, before startup, and failed
assembly releases this registrar's sender bindings. Build a fresh broker and
registrar after a failed assembly, as native definitions may already be registered.


### Events hooks

Choose collections once for the application. `publish_hooks` applies to every
registered Publisher; `subscribe_hooks` applies to every registered Subscriber,
including discovered definitions. They do not attach to other applications in the
same process. Register the collections and their dependencies in ordinary Dishka
providers. Hook construction and attachment remain separate operations.

```python
from dishka import Provider, Scope, provide
from papilio_tasks.tools.hooks import Handler, Hook
from papilio_tasks.tools.hooks.publish import Published, PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeCall, SubscribeHooks


class RecordSend(Hook[Published]):
    def __init__(self, audit: AuditService):
        self.audit = audit

    async def run(self, event: Published) -> None:
        await self.audit.record(event.call.sender)


class RecordRun(Hook[SubscribeCall]):
    def __init__(self, audit: AuditService):
        self.audit = audit

    async def run(self, event: SubscribeCall) -> None:
        await self.audit.record(event.subscriber)


class AppPublishHooks(PublishHooks):
    def __init__(self, audit: RecordSend):
        super().__init__(after_send=(Handler(audit),))


class AppSubscribeHooks(SubscribeHooks):
    def __init__(self, audit: RecordRun):
        super().__init__(after_run=(Handler(audit),))


class HookProvider(Provider):
    scope = Scope.REQUEST
    sent = provide(RecordSend)
    consumed = provide(RecordRun)
    publication = provide(AppPublishHooks)
    subscription = provide(AppSubscribeHooks)


app = create_app(
    registrar=registry,
    providers=[HookProvider(), *providers],  # Also supply AuditService.
    publish_hooks=AppPublishHooks,
    subscribe_hooks=AppSubscribeHooks,
)
```

Publication uses the same `PublishHooks` contract and outcome runner as Projection.
A fresh producer request scope resolves the collection before native publication
and closes after its hooks. No subscriber services are constructed; the caller's
HTTP/message transaction scope is not reused. Without publish hooks, sending opens
no extra scope. Direct calls to the infrastructure/native broker bypass app hooks.
Events currently offers app-wide selection only; Projection's existing shared and
local selections remain available.

`Published[R].result` is the native return, including `None`. `PublishFailed.error`
is the native send exception. `call.sender` is the qualified Publisher class,
`call.args` contains the payload, and `call.kwargs` captures publication options.
`call.meta` describes the registered Rabbit `exchange` and `routing_key`, or
Kafka `topic`, Redis `stream`, Pub/Sub `channel`, Redis `list` or NATS `subject`
(plus `stream` for JetStream). It is not a claim about the final destination if
per-call options override the route.
Options and metadata are shallow read-only mappings, not serialized snapshots.
After-send or scope-cleanup failure raises `PublishError(result)` without resending
or calling send-error hooks. Send errors and cancellation retain their original
meaning; a send error does not prove that the message was not delivered.

Subscriber hooks resolve in the existing message scope with the same scoped
services as the subscriber, then execute `before_run -> run -> after_run`.
Before/after hooks receive `SubscribeCall[T](subscriber, data)`; `data` is the
parsed payload, still referenced rather than copied. Error hooks receive
`SubscribeFailed[T](call, stage, error)`, where stage is `before_run`, `run` or
`after_run`. A propagated before-hook failure prevents `run`; an after-hook failure
occurs after business work. A failing error hook does not replace the primary
exception. Cancellation propagates directly rather than invoking error hooks.
Parsing, validation, dependency construction, scope cleanup and broker-ack failures
are outside these execution hooks. For message metadata, hook providers can request
`StreamMessage` from the existing message context.

Both collections use ordered `Handler` attachments. Default `failure="raise"`
stops the remaining hooks; `failure="continue"` logs that hook failure and
continues. It never suppresses a failure of the subscriber's `run`. Native
acknowledgement/redelivery policy still applies to propagated consumer errors;
after-run hook failure can therefore make already-completed business work subject
to redelivery under the user's policy. `after_send` means native publication
returned; `after_run` means the subscriber function returned, not that broker ack
or all other subscribers completed. No automatic retry, inbox or outbox is added.

Publication payload migration: use `call.sender` instead of `call.projection`,
`call.meta["task_name"]` instead of `call.task_name` for Taskiq, and
`event.result.task_id` / `error.result.task_id` instead of `event.task_id` /
`error.task_id`. Projection pipeline hook payloads are unchanged.

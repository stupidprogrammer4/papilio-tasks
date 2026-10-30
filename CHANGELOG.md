# Changelog

## 1.1.0 — 2026-10-30

### Added

- Optional NATS pull JetStream and native Taskiq Kafka backends for schedulers
  and projections: `scheduler-nats`, `scheduler-kafka`, `projection-nats` and
  `projection-kafka`. Shared infra adapters retain caller-configured native
  brokers; matching app classes and registrars preserve DI, hooks, typed single
  and batch jobs, scheduling, publication and execution retry.
- Kafka class/include topic selection using native routing labels, independently
  of native declarations and worker subscriptions. NATS tasks retain the native
  broker's fixed subject, stream and durable pull consumer.
- Configured Redis app factories, producer lifecycle composition, Redis schedule
  inventory and optional module-owned scheduler defaults, previously published
  on main after 1.0.0.

### Compatibility and limits

- Existing APIs and Memory/Rabbit/Redis defaults are retained. Taskiq remains
  0.12; NATS tasks use taskiq-nats 0.6, independently of FastStream NATS Events.
  Base installation still has no runtime dependencies; Taskiq extras do not
  install FastStream or unrelated transports.
- Kafka task extras constrain kafka-python to `>=2.0.2,<2.3`, retaining the
  native task broker's partition-assignment protocol compatibility with aiokafka.
- Schedule storage and result storage remain explicitly selected. Neither Kafka
  nor JetStream makes a database write and publication atomic or makes effects
  exactly once. Native Kafka auto-commit is not acknowledgement after a successful
  job; native Kafka synchronous admin construction/startup remains visible.
- JetStream uses native pull acknowledgement/redelivery. Stream retention,
  durable consumers, repeated effects and external recovery remain caller-owned.

## 1.0.0 — 2026-09-17

First stable release of Papilio's modular background-job package. Install only
the application/backend extras you need; the base package has no runtime
dependencies.

### Included

- Schedulers with application-owned async jobs and Dishka providers, module
  discovery, immediate enqueue, and explicit time/cron/interval scheduling.
  Memory, RabbitMQ and Redis Streams task brokers; memory and Redis sources.
- Typed Projection read/transform/write pipelines, an identity-transform Direct
  base, single or batch arguments, enqueue and project decorators, and backend
  queue selection.
- Typed Events publishers/subscribers and publish decorators through FastStream:
  RabbitMQ, Kafka, Redis Streams/PubSub/Lists, NATS Core and JetStream.
- Provider-resolved pipeline, publication and subscription hooks with explicit
  ordering and failure policy. Per-job Retry policies for Taskiq applications,
  using an explicitly selected writable source and beat.
- One CLI for scheduler/projection workers and beats, and Events consumers.
  Shared apps/infra/tools layout, typed package metadata and optional extras.
- CI with all four real transports, package builds, and CPython 3.13/3.14 jobs.

### Compatibility

The documented Papilio imports, contracts, factory arguments and behavior are
public API for the 1.x series. Internal bindings/helpers and third-party internals
are outside that commitment. Native handles/options follow the installed
Taskiq/FastStream/backend versions and the dependency ranges in package metadata.
See the README migration section for removed pre-1.0 import paths and APIs.

### Known limitations

- Transactions, outbox/inbox, idempotency and reconciliation remain application
  responsibilities. A successful publish does not establish completed processing
  or exactly-once delivery.
- Memory brokers and sources are process-local and not durable. Separate worker
  and beat processes require shared infrastructure. Large due batches still
  incur payload-copy and execution costs.
- Retry requires a writable source and running beat, including for zero delay.
  Hooks do not replace execution retries. A post-send PublishError can mean the
  message was already published; resending may create duplicates.
- RedisSource delegates to taskiq-redis's list source. It offers no atomic replace
  or lookup by ID. In taskiq-redis 1.2.3, recurring/overdue refresh scans and native
  source shutdown pool-cleanup limitations remain; the adapter adds no pool or
  partial-startup cleanup. Prefer a long-lived source per application/process.
- Concrete job/publisher classes have one active runtime binding per process.
  Projections inheriting the optional hooks constructor need an explicit provider
  factory when using empty hooks; constructor dependencies remain user-owned.
- Redis Pub/Sub is ephemeral; Redis Lists lack acknowledgement/redelivery. Select
  the transport whose delivery semantics fit the application.
- Linux and standard CPython are the current CI scope. Other operating systems,
  free-threaded builds, future Python versions, cluster recovery and production
  capacity are not covered by those checks.

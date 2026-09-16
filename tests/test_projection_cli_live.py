"""Separate producer, native worker and optional beat processes."""

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.mark.parametrize("kind", ["redis", "rabbit"])
@pytest.mark.parametrize("retry", [False, True])
def test_projection_processes(tmp_path, kind, retry):
    if not os.getenv("TEST_REDIS_URL") or (
        kind == "rabbit" and not os.getenv("TEST_RABBIT_URL")
    ):
        pytest.skip("Requires isolated Redis and selected broker service")
    pytest.importorskip("taskiq_redis")
    if kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
    package = tmp_path / "projection_app"
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "projections.py").write_text(
        textwrap.dedent("""
        import json
        import os
        from pathlib import Path
        from collections.abc import AsyncIterator
        from dishka import Provider, Scope, provide
        from papilio_tasks.apps.projections import Direct
        from papilio_tasks.tools.retry import Retry
        from papilio_tasks.tools.hooks import Hook, Handler
        from papilio_tasks.tools.hooks.publish import PublishHooks, Published
        from papilio_tasks.tools.hooks.projection import Hooks, Data

        def record(phase, **data):
            with Path(os.environ['PROJECTION_EVENTS']).open('a') as file:
                file.write(json.dumps(dict(
                    role=os.environ['PROJECTION_ROLE'], pid=os.getpid(),
                    phase=phase, **data,
                )) + '\\n')

        class Root: pass
        class Session: pass
        class Audit: pass

        class RecordSend(Hook[Published]):
            def __init__(self, audit: Audit): self.audit = audit
            async def run(self, event: Published) -> None:
                record('published', task_id=event.result.task_id)

        class ProducerHooks(PublishHooks):
            def __init__(self, hook: RecordSend):
                super().__init__(after_send=(Handler(hook),))

        class RecordRead(Hook[Data[object]]):
            def __init__(self, session: Session): self.session = session
            async def run(self, event: Data[object]) -> None:
                record('after_read')

        class WorkerHooks(Hooks[object, object, object]):
            def __init__(self, hook: RecordRead):
                super().__init__(after_read=(Handler(hook),))

        class Product(Direct[int, int]):
            retry = (Retry(attempts=2, delay=0, errors=(ConnectionError,))
                     if os.environ['PROJECTION_RETRY'] == '1' else None)
            attempts = 0
            def __init__(self, session: Session, root: Root):
                super().__init__()
                self.session = session
                record('job_created')
            async def read(self, id: int) -> int:
                record('read', value=id)
                return id
            async def write(self, data: int) -> int:
                type(self).attempts += 1
                if self.retry is not None and self.attempts == 1:
                    record('failed', value=data)
                    raise ConnectionError('temporary')
                record('written', value=data)
                return data

        class Services(Provider):
            scope = Scope.REQUEST
            product = provide(Product)
            read_hook = provide(RecordRead)
            worker_hooks = provide(WorkerHooks)
            send_hook = provide(RecordSend)
            producer_hooks = provide(ProducerHooks)
            @provide
            async def session(self) -> AsyncIterator[Session]:
                record('session_open')
                try: yield Session()
                finally: record('session_closed')
            @provide
            async def audit(self) -> AsyncIterator[Audit]:
                record('audit_open')
                try: yield Audit()
                finally: record('audit_closed')
            @provide(scope=Scope.APP)
            async def root(self) -> AsyncIterator[Root]:
                record('root_open')
                try: yield Root()
                finally: record('root_closed')
    """)
    )
    (package / "tasks.py").write_text(
        textwrap.dedent("""
        import os
        from taskiq import TaskiqEvents
        from papilio_tasks.apps.projections.application import (
            create_broker, create_beat,
        )
        from papilio_tasks.apps.projections.registry import Registrar
        from .projections import (
            Services, ProducerHooks, WorkerHooks, Product, record,
        )

        name = os.environ['PROJECTION_QUEUE']
        if os.environ['PROJECTION_KIND'] == 'rabbit':
            from taskiq_aio_pika import Queue, Exchange
            from papilio_tasks.infra.taskiq.brokers.backends.rabbit import (
                RabbitBroker,
            )
            transport = RabbitBroker(
                os.environ['TEST_RABBIT_URL'], queues=[Queue(name=name)],
                exchange=Exchange(name=name),
                dead_letter_queue=Queue(name=name+'-dead'),
            )
        else:
            from papilio_tasks.infra.taskiq.brokers.backends.redis import (
                RedisStreamBroker,
            )
            transport = RedisStreamBroker(
                os.environ['TEST_REDIS_URL'], queue_name=name,
                consumer_group_name=name, consumer_id='0',
            )
        source = None
        if Product.retry is not None:
            from papilio_tasks.infra.taskiq.sources.backends.redis import (
                RedisSource,
            )
            source = RedisSource(
                os.environ['TEST_REDIS_URL'], prefix=name+'-retry',
            )
        broker = create_broker(
            registrar=Registrar(transport, hooks=WorkerHooks),
            providers=[Services()], modules=['projection_app'],
            retry_source=source, publish_hooks=ProducerHooks,
        )
        @broker.on_event(TaskiqEvents.WORKER_STARTUP)
        async def ready(state): record('ready')
        @broker.on_event(
            TaskiqEvents.WORKER_SHUTDOWN, TaskiqEvents.CLIENT_SHUTDOWN,
        )
        async def closed(state): record('broker_closed')
        if source is not None:
            beat = create_beat(broker, sources=[source])
    """)
    )
    events = tmp_path / "events.jsonl"
    env = {
        **os.environ,
        "PROJECTION_EVENTS": str(events),
        "PROJECTION_QUEUE": "projection-cli-" + uuid4().hex,
        "PROJECTION_KIND": kind,
        "PROJECTION_RETRY": str(int(retry)),
        "PYTHONPATH": os.pathsep.join(
            [str(tmp_path), os.environ.get("PYTHONPATH", "")]
        ),
    }
    executable = os.getenv("TEST_PAPILIO_CLI")
    command = (
        [executable] if executable else [sys.executable, "-m", "papilio_tasks"]
    )
    processes, logs = [], []

    def entries():
        if not events.exists():
            return []
        # Ignore an incomplete append while another process is writing.
        return [
            json.loads(line)
            for line in events.read_text().splitlines(keepends=True)
            if line.endswith("\n")
        ]

    def output():
        return "\n".join(Path(log.name).read_text() for log in logs)

    def start(role, target, *options):
        log = (tmp_path / f"{role}.log").open("w")
        logs.append(log)
        proc = subprocess.Popen(
            [
                *command,
                "projection",
                role,
                target,
                "--app-dir",
                str(tmp_path),
                *options,
            ],
            env={**env, "PROJECTION_ROLE": role},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        processes.append(proc)

    def wait_for(phase):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            assert all(proc.poll() is None for proc in processes), output()
            if any(e["phase"] == phase for e in entries()):
                return
            time.sleep(0.05)
        pytest.fail(f"Missing {phase}: {entries()}\n{output()}")

    forced = []
    try:
        start(
            "worker",
            "projection_app.tasks:broker",
            "--workers",
            "1",
            "--max-fails",
            "0",
        )
        wait_for("ready")
        producer = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent("""
                import asyncio
                from projection_app.tasks import broker, Product, record
                async def main():
                    await broker.startup()
                    try:
                        handle = await Product.enqueue(id=17)
                        record('submitted', task_id=handle.task_id)
                    finally:
                        await broker.shutdown()
                asyncio.run(main())
            """),
            ],
            env={**env, "PROJECTION_ROLE": "producer"},
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert producer.returncode == 0, producer.stdout + producer.stderr
        if retry:
            wait_for("failed")
            # No beat has been launched: even a zero-delay retry stays queued.
            time.sleep(0.2)
            assert not any(e["phase"] == "written" for e in entries())
            start(
                "beat", "projection_app.tasks:beat", "--update-interval", "1"
            )
        wait_for("written")
        wait_for("session_closed")
    finally:
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    forced.append(proc.pid)
            # Remove surviving descendants even if a manager exited early.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)
        for log in logs:
            log.close()
        from redis import Redis

        with Redis.from_url(env["TEST_REDIS_URL"]) as redis:
            keys = list(redis.scan_iter(match=env["PROJECTION_QUEUE"] + "*"))
            if keys:
                redis.delete(*keys)
        if kind == "rabbit":
            cleanup = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    textwrap.dedent("""
                    import asyncio, os
                    from aio_pika import connect_robust
                    async def main():
                        url = os.environ['TEST_RABBIT_URL']
                        async with await connect_robust(url) as conn:
                            async with conn.channel() as channel:
                                name = os.environ['PROJECTION_QUEUE']
                                await channel.queue_delete(name)
                                await channel.queue_delete(name+'-dead')
                                await channel.exchange_delete(name)
                    asyncio.run(main())
                """),
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert cleanup.returncode == 0, cleanup.stderr
    data = entries()
    worker = [e for e in data if e["role"] == "worker"]
    sent = [e for e in data if e["role"] == "producer"]
    attempts = 2 if retry else 1
    phases = [e["phase"] for e in worker]
    assert phases.count("read") == phases.count("after_read") == attempts
    assert phases.count("session_closed") == attempts
    assert phases.count("written") == 1
    assert phases.count("failed") == int(retry)
    assert phases.count("root_closed") == 1, output()
    assert phases.count("broker_closed") == 1, output()
    producer_phases = [e["phase"] for e in sent]
    assert producer_phases == [
        "audit_open",
        "published",
        "audit_closed",
        "submitted",
        "broker_closed",
    ]
    assert sent[1]["task_id"] == sent[3]["task_id"]
    assert {e["pid"] for e in worker}.isdisjoint(e["pid"] for e in sent)
    if retry:
        beat = [e for e in data if e["role"] == "beat"]
        assert [e["phase"] for e in beat] == ["broker_closed"]
        assert {e["pid"] for e in beat}.isdisjoint(
            e["pid"] for e in worker + sent
        )
    else:
        assert not any(e["role"] == "beat" for e in data)
    assert not forced, (
        f"Native shutdown required forced cleanup: {forced}\n{output()}"
    )
    assert all(proc.returncode == 0 for proc in processes), output()

"""Real Papilio CLI processes, with an explicitly configured test Redis."""

import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_REDIS_URL"), reason="No test Redis"
)


@pytest.mark.parametrize("source_kind", ["memory", "redis"])
def test_worker_and_beat_paths_execute_and_close_scopes(tmp_path, source_kind):
    pytest.importorskip("taskiq_redis")
    package = tmp_path / "cli_app"
    feature = package / "modules" / "reports"
    feature.mkdir(parents=True)
    for directory in (package, package / "modules", feature):
        (directory / "__init__.py").touch()
    events = tmp_path / "events.txt"
    (feature / "schedulers.py").write_text(
        textwrap.dedent("""
        import os
        from pathlib import Path
        from papilio_tasks.apps.schedulers.backends.redis import RedisScheduler

        def record(value):
            with Path(os.environ['CLI_EVENTS']).open('a') as stream:
                stream.write(value + '\\n')

        class Root: pass
        class Service: pass
        class Report(RedisScheduler):
            def __init__(self, service: Service, root: Root):
                self.service = service
            async def run(self, value: int):
                record('executed:' + str(value))
    """)
    )
    (feature / "providers.py").write_text(
        textwrap.dedent("""
        from collections.abc import AsyncIterator
        from dishka import Provider, Scope, provide
        from .schedulers import Root, Service, Report, record
        class Jobs(Provider):
            scope = Scope.REQUEST
            report = provide(Report)
            @provide
            async def service(self) -> AsyncIterator[Service]:
                try:
                    yield Service()
                finally:
                    record('request_closed')
            @provide(scope=Scope.APP)
            async def root(self) -> AsyncIterator[Root]:
                try:
                    yield Root()
                finally:
                    record('root_closed')
    """)
    )
    (package / "tasks.py").write_text(
        textwrap.dedent("""
        import os
        from datetime import UTC, datetime, timedelta
        from taskiq import ScheduledTask, ScheduleSource, TaskiqEvents
        from papilio_tasks.apps.schedulers.application import (
            create_broker, create_beat,
        )
        from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar
        from papilio_tasks.apps.schedulers.backends.redis import RedisStreamBroker
        from papilio_tasks.infra.taskiq.sources.base import Source
        from .modules.reports.providers import Jobs
        from .modules.reports.schedulers import Report, record

        registrar = RedisRegistrar(RedisStreamBroker(
            os.environ['TEST_REDIS_URL'], queue_name=os.environ['CLI_QUEUE'],
            consumer_group_name=os.environ['CLI_QUEUE'], consumer_id='0',
        ))
        broker = create_broker(
            registrar=registrar, providers=[Jobs()],
            modules=['cli_app.modules'],
        )
        @broker.on_event(TaskiqEvents.WORKER_STARTUP)
        async def ready(state): record('worker_ready')

        class Plans(ScheduleSource):
            def __init__(self):
                self.items = [ScheduledTask(
                    task_name=Report.task().task_name,
                    labels=dict(Report.task().labels), args=[17], kwargs={},
                    time=datetime.now(UTC) + timedelta(seconds=1),
                )]
            async def startup(self): record('source_started')
            async def shutdown(self): record('source_closed')
            async def get_schedules(self): return self.items
            async def post_send(self, task): self.items = []

        if os.environ['CLI_SOURCE'] == 'redis':
            from papilio_tasks.infra.taskiq.sources.backends.redis import (
                RedisSource,
            )
            source = RedisSource(
                os.environ['TEST_REDIS_URL'], prefix=os.environ['CLI_PREFIX'],
            )
            # Add work after observing beat's first empty native read.
            read_schedules = source.native.get_schedules
            async def observed_read():
                items = await read_schedules()
                record('source_read:' + str(len(items)))
                return items
            source.native.get_schedules = observed_read
        else:
            source = Source(Plans())
        beat = create_beat(broker, sources=[source])
    """)
    )
    env = {
        **os.environ,
        "CLI_EVENTS": str(events),
        "CLI_QUEUE": "papilio-cli-" + uuid4().hex,
        "CLI_PREFIX": "papilio-source-" + uuid4().hex,
        "CLI_SOURCE": source_kind,
    }
    # Also used to run the same test against the isolated installed wheel.
    executable = os.getenv("TEST_PAPILIO_CLI")
    command = (
        [executable] if executable else [sys.executable, "-m", "papilio_tasks"]
    )
    processes = []
    logs = []

    def start(kind, target, *options):
        log = (tmp_path / f"{kind}.log").open("w+")
        logs.append(log)
        proc = subprocess.Popen(
            [
                *command,
                "scheduler",
                kind,
                target,
                "--app-dir",
                str(tmp_path),
                *options,
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append(proc)
        return proc

    def output():
        for log in logs:
            if not log.closed:
                log.flush()
        return "\n".join(Path(log.name).read_text() for log in logs)

    def wait_for(value):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert all(p.poll() is None for p in processes), output()
            if events.exists() and value in events.read_text().splitlines():
                return
            time.sleep(0.05)
        pytest.fail(f"Missing {value}: {output()}")

    try:
        start(
            "worker",
            "cli_app.tasks:broker",
            "--workers",
            "1",
            "--max-fails",
            "0",
        )
        wait_for("worker_ready")
        start("beat", "cli_app.tasks:beat", "--update-interval", "1")
        if source_kind == "redis":
            wait_for("source_read:0")
            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import asyncio
from datetime import UTC, datetime, timedelta
from cli_app.tasks import Report, source

async def publish():
    await source.native.startup()
    try:
        await Report.at(
            source, datetime.now(UTC) + timedelta(seconds=3), 17,
        )
    finally:
        await source.native.shutdown()

asyncio.run(publish())
""",
                ],
                env={
                    **env,
                    "PYTHONPATH": os.pathsep.join(
                        [str(tmp_path), env.get("PYTHONPATH", "")]
                    ),
                },
                check=True,
                capture_output=True,
                text=True,
                timeout=15,
            )
        wait_for("executed:17")
        wait_for("request_closed")
    finally:
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        for log in logs:
            log.close()
        # This test owns the unique stream and group, never existing app keys.
        from redis import Redis

        with Redis.from_url(env["TEST_REDIS_URL"]) as redis:
            redis.delete(env["CLI_QUEUE"])
            keys = list(redis.scan_iter(match=env["CLI_PREFIX"] + "*"))
            if keys:
                redis.delete(*keys)
    entries = events.read_text().splitlines()
    assert entries.count("executed:17") == 1, output()
    assert entries.count("request_closed") == 1
    assert entries.count("root_closed") == 1
    if source_kind == "memory":
        assert (
            entries.count("source_started")
            == entries.count("source_closed")
            == 1
        )
    else:
        assert entries.index("source_read:0") < entries.index("source_read:1")
    assert all(proc.returncode == 0 for proc in processes), output()

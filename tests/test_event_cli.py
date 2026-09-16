import asyncio
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("faststream.rabbit")
pytest.importorskip("dishka_faststream")
pytest.importorskip("typer")

from dishka import Provider, Scope, provide
from faststream.rabbit import RabbitBroker as NativeRabbitBroker

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers import bindings
from papilio_tasks.apps.events.publishers.rabbit import (
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.cli.main import main
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker


def test_forward_native_arguments_and_exit(monkeypatch):
    calls = []

    def cli(**kwargs):
        calls.append(kwargs)
        raise SystemExit(7)

    monkeypatch.setattr("faststream.__main__.cli", cli)
    args = ["run", "app:build", "--factory", "--workers", "2", "--reload"]
    with pytest.raises(SystemExit) as error:
        main(["events", *args])
    assert error.value.code == 7
    assert calls == [{"args": args, "prog_name": "papilio_tasks events"}]


def test_missing_dependency_hint(monkeypatch, capsys):
    monkeypatch.setattr(
        "papilio_tasks.cli.faststream.importlib.util.find_spec", lambda _: None
    )
    with pytest.raises(SystemExit) as error:
        main(["events", "run", "app:app"])
    assert error.value.code == 2
    assert "papilio-tasks[events]" in capsys.readouterr().err


def test_native_help_and_dependency_isolation():
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'taskiq', 'taskiq_redis', 'papilio'}:
            raise AssertionError('Unselected import: ' + fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.cli.main import main
main(['events', 'run', '--help'])
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    for flag in ("--workers", "--factory", "--reload", "--app-dir"):
        assert flag in result.stdout


def test_application_import_error_is_not_masked(tmp_path):
    (tmp_path / "broken.py").write_text("import missing_event_service\n")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "papilio_tasks",
            "events",
            "run",
            "broken:app",
            "--app-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "missing_event_service" in result.stderr
    assert "install 'papilio-tasks" not in result.stderr


@pytest.mark.parametrize("cancel", [False, True])
async def test_runner_finalizes_on_failed_start(monkeypatch, cancel):
    closed = []

    class Resource:
        pass

    class Resources(Provider):
        @provide(scope=Scope.APP)
        async def resource(self) -> AsyncIterator[Resource]:
            try:
                yield Resource()
            finally:
                closed.append(True)

    class Created(RabbitPublisher[int]):
        exchange = RabbitExchange("startup-test")
        routing_key = "created"

    native = NativeRabbitBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(Created)
    app = create_app(registrar=reg, providers=[Resources()])
    await app.container.get(Resource)
    stop = AsyncMock()
    monkeypatch.setattr(native, "stop", stop)
    error = ConnectionError("startup failed")
    started = asyncio.Event()

    async def start():
        started.set()
        if cancel:
            await asyncio.Event().wait()
        raise error

    monkeypatch.setattr(native, "start", start)
    running = asyncio.create_task(app.run())
    try:
        await asyncio.wait_for(started.wait(), 2)
        if cancel:
            running.cancel()
        with pytest.raises(
            asyncio.CancelledError if cancel else ConnectionError
        ):
            await asyncio.wait_for(running, 2)
        assert closed == [True]
        stop.assert_awaited_once()
        with pytest.raises(RuntimeError, match="not registered"):
            bindings.get(Created)
    finally:
        await app.stop()


@pytest.mark.parametrize("factory,workers", [(False, 1), (True, 2)])
def test_live_cli_consumes_and_closes(tmp_path, factory, workers):
    if not os.getenv("TEST_RABBIT_URL"):
        pytest.skip("Requires isolated RabbitMQ")
    entry = tmp_path / "event_entry.py"
    entry.write_text(
        textwrap.dedent("""
        import json
        import os
        from collections.abc import AsyncIterator
        from pathlib import Path
        from dishka import Provider, Scope, provide
        from faststream.rabbit import RabbitBroker as NativeRabbitBroker
        from papilio_tasks.apps.events.application import create_app
        from papilio_tasks.apps.events.publishers.rabbit import (
            RabbitPublisher, RabbitExchange,
        )
        from papilio_tasks.apps.events.subscribers.rabbit import (
            RabbitSubscriber, RabbitQueue,
        )
        from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
        from papilio_tasks.infra.faststream.brokers.backends.rabbit import (
            RabbitBroker,
        )
        from papilio_tasks.tools.hooks import Hook, Handler
        from papilio_tasks.tools.hooks.subscribe import (
            SubscribeHooks, SubscribeCall,
        )

        def record(phase, **values):
            with Path(os.environ['EVENT_LOG']).open('a') as file:
                file.write(json.dumps(dict(
                    phase=phase, pid=os.getpid(), **values,
                )) + '\\n')

        class Created(RabbitPublisher[int]):
            exchange = RabbitExchange(
                os.environ['EVENT_QUEUE'], auto_delete=True,
            )
            routing_key = 'created'

        class Resource: pass
        class Session: pass

        class Consumer(RabbitSubscriber[int]):
            publisher = Created
            queue = RabbitQueue(os.environ['EVENT_QUEUE'], auto_delete=True)
            def __init__(self, session: Session): self.session = session
            async def run(self, event: int) -> None:
                record('consumed', value=event)

        class Audit(Hook[SubscribeCall]):
            def __init__(self, session: Session): self.session = session
            async def run(self, event: SubscribeCall) -> None:
                record('before_run')

        class Hooks(SubscribeHooks):
            def __init__(self, audit: Audit):
                super().__init__(before_run=(Handler(audit),))

        class Services(Provider):
            consumer = provide(Consumer, scope=Scope.REQUEST)
            audit = provide(Audit, scope=Scope.REQUEST)
            hooks = provide(Hooks, scope=Scope.REQUEST)
            @provide(scope=Scope.REQUEST)
            async def session(self) -> AsyncIterator[Session]:
                record('session_open')
                try: yield Session()
                finally: record('session_closed')
            @provide(scope=Scope.APP)
            async def resource(self) -> AsyncIterator[Resource]:
                record('resource_open')
                try: yield Resource()
                finally: record('resource_closed')

        def build():
            reg = RabbitRegistrar(RabbitBroker(NativeRabbitBroker(
                os.environ['TEST_RABBIT_URL'], logger=None,
            )))
            reg.publisher(Created)
            reg.subscriber(Consumer)
            app = create_app(
                registrar=reg, providers=[Services()], subscribe_hooks=Hooks,
            )
            @app.after_startup
            async def ready():
                await app.container.get(Resource)
                record('ready')
            return app

        if os.environ['EVENT_FACTORY'] == '0':
            app = build()
    """)
    )
    log = tmp_path / "events.jsonl"
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            (
                str(Path(__file__).resolve().parents[1]),
                os.getenv("PYTHONPATH", ""),
            )
        ),
        "EVENT_LOG": str(log),
        "EVENT_QUEUE": "cli-" + uuid4().hex,
        "EVENT_FACTORY": str(int(factory)),
    }
    cmd = [
        sys.executable,
        "-m",
        "papilio_tasks",
        "events",
        "run",
        "event_entry:build" if factory else "event_entry:app",
        "--app-dir",
        str(tmp_path),
        "--workers",
        str(workers),
    ]
    if factory:
        cmd.append("--factory")

    def records():
        return (
            [json.loads(row) for row in log.read_text().splitlines()]
            if log.exists()
            else []
        )

    with (tmp_path / "worker.log").open("w+") as output:
        worker = subprocess.Popen(
            cmd,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        def wait_for(check):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if check(records()):
                    return
                if worker.poll() is not None:
                    break
                time.sleep(0.05)
            output.flush()
            output.seek(0)
            pytest.fail(output.read())

        try:
            wait_for(
                lambda rows: (
                    sum(r["phase"] == "ready" for r in rows) == workers
                )
            )
            producer = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    textwrap.dedent("""
                    import asyncio
                    from event_entry import build, Created
                    async def send():
                        app = build()
                        try:
                            await app.connect()
                            await Created.publish(42)
                        finally:
                            await app.stop()
                    asyncio.run(send())
                """),
                ],
                cwd=tmp_path,
                env={**env, "EVENT_FACTORY": "1"},
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert producer.returncode == 0, producer.stderr
            wait_for(
                lambda rows: any(r["phase"] == "session_closed" for r in rows)
            )
            worker.send_signal(signal.SIGTERM)
            assert worker.wait(timeout=15) == 0
            rows = records()
            assert (
                sum(r["phase"] == "resource_closed" for r in rows) == workers
            )
            assert [r["value"] for r in rows if r["phase"] == "consumed"] == [
                42
            ]
            assert sum(r["phase"] == "before_run" for r in rows) == 1
            assert sum(r["phase"] == "session_open" for r in rows) == 1
        finally:
            if worker.poll() is None:
                os.killpg(worker.pid, signal.SIGKILL)
                worker.wait(timeout=5)

import subprocess
import sys

import pytest

from papilio_tasks.cli.main import main


@pytest.mark.parametrize("app", ["scheduler", "projection"])
def test_cli_forwards_native_options_and_exit_status(monkeypatch, app):
    from taskiq.cli.scheduler.cmd import SchedulerCMD
    from taskiq.cli.worker.cmd import WorkerCMD

    calls = []

    def worker(self, args):
        calls.append(("worker", list(args)))
        return 7

    def beat(self, args):
        calls.append(("beat", list(args)))

    monkeypatch.setattr(WorkerCMD, "exec", worker)
    monkeypatch.setattr(SchedulerCMD, "exec", beat)
    assert main([app, "worker", "app.tasks:broker", "--workers", "3"]) == 7
    assert main([app, "beat", "app.tasks:beat", "--skip-first-run"]) is None
    assert calls == [
        ("worker", ["app.tasks:broker", "--workers", "3"]),
        ("beat", ["app.tasks:beat", "--skip-first-run"]),
    ]


@pytest.mark.parametrize(
    "command",
    [
        [],
        ["scheduler"],
        ["scheduler", "worker"],
        ["scheduler", "beat"],
        ["projection"],
        ["projection", "worker"],
        ["projection", "beat"],
    ],
)
def test_help_paths(command):
    result = subprocess.run(
        [sys.executable, "-m", "papilio_tasks", *command, "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
    if command[-1:] == ["worker"]:
        assert "--workers" in result.stdout
    if command[-1:] == ["beat"]:
        assert "--skip-first-run" in result.stdout


def test_core_and_help_without_app_dependencies():
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'taskiq', 'dishka', 'faststream', 'papilio'}
        if fullname.split('.')[0] in blocked:
            raise AssertionError('Unselected import: ' + fullname)
sys.meta_path.insert(0, Block())
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.cli.main import main
assert Bootstrapper().modules('schedulers') == []
assert main([]) == 0
assert main(['scheduler']) == 0
assert main(['projection']) == 0
try:
    main(['--help'])
except SystemExit as error:
    assert error.code == 0
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("app", ["scheduler", "projection"])
def test_missing_dependency_has_install_hint(monkeypatch, capsys, app):
    monkeypatch.setattr(
        "papilio_tasks.cli.taskiq.importlib.util.find_spec", lambda _: None
    )
    with pytest.raises(SystemExit) as error:
        main([app, "worker", "app:broker"])
    assert error.value.code == 2
    assert f"papilio-tasks[{app}]" in capsys.readouterr().err


@pytest.mark.parametrize("app", ["scheduler", "projection"])
def test_native_application_import_error_is_not_masked(tmp_path, app):
    (tmp_path / "broken_entry.py").write_text("import missing_app_service\n")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "papilio_tasks",
            app,
            "beat",
            "broken_entry:beat",
            "--app-dir",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "missing_app_service" in result.stderr
    assert "install" not in result.stderr


def test_projection_help_explains_optional_beat(capsys):
    assert main(["projection"]) == 0
    help = capsys.readouterr().out
    assert "Beat is optional" in help
    assert "retry_source" in help and "delay=0" in help


def test_projection_command_does_not_import_scheduler_app(monkeypatch):
    from taskiq.cli.worker.cmd import WorkerCMD

    monkeypatch.setattr(WorkerCMD, "exec", lambda self, args: 0)
    before = set(sys.modules)
    assert main(["projection", "worker", "app:broker"]) == 0
    assert not any(
        name.startswith("papilio_tasks.apps.schedulers")
        for name in set(sys.modules) - before
    )

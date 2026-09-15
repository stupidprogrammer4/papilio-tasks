import subprocess
import sys

import pytest

from papilio_tasks.cli.main import main


def test_cli_forwards_native_options_and_exit_status(monkeypatch):
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
    assert (
        main(["scheduler", "worker", "app.tasks:broker", "--workers", "3"])
        == 7
    )
    assert (
        main(["scheduler", "beat", "app.tasks:beat", "--skip-first-run"])
        is None
    )
    assert calls == [
        ("worker", ["app.tasks:broker", "--workers", "3"]),
        ("beat", ["app.tasks:beat", "--skip-first-run"]),
    ]


@pytest.mark.parametrize(
    "command",
    [[], ["scheduler"], ["scheduler", "worker"], ["scheduler", "beat"]],
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
from papilio_tasks.core.bootstrap import Bootstrapper
from papilio_tasks.cli.main import main
assert Bootstrapper().modules('schedulers') == []
assert main([]) == 0
try:
    main(['--help'])
except SystemExit as error:
    assert error.code == 0
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_missing_dependency_has_install_hint(monkeypatch, capsys):
    monkeypatch.setattr(
        "papilio_tasks.cli.main.importlib.util.find_spec", lambda _: None
    )
    with pytest.raises(SystemExit) as error:
        main(["scheduler", "worker", "app:broker"])
    assert error.value.code == 2
    assert "papilio-tasks[scheduler]" in capsys.readouterr().err


def test_native_application_import_error_is_not_masked(tmp_path):
    (tmp_path / "broken_entry.py").write_text("import missing_app_service\n")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "papilio_tasks",
            "scheduler",
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

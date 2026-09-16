"""Expose native Taskiq execution under the Papilio Tasks command."""

import argparse
import importlib.util
from collections.abc import Sequence


def main(app: str, args: Sequence[str]) -> int | None:
    parser = argparse.ArgumentParser(
        prog=f"papilio_tasks {app}",
        description=(
            "Run projections with worker. Beat is optional: "
            "run it to dispatch "
            "retries from your explicit retry_source, including delay=0. "
            "Factories do not start beat automatically."
            if app == "projection"
            else "Run workers and dispatch schedules with native Taskiq."
        ),
    )
    parser.add_argument("command", choices=("worker", "beat"), nargs="?")
    selected = parser.parse_args(args[:1])
    if selected.command is None:
        parser.print_help()
        return 0
    for dependency in ("taskiq", "dishka"):
        if importlib.util.find_spec(dependency) is None:
            parser.error(
                f"{app.title()} requires {dependency}; install "
                f"'papilio-tasks[{app}]' (or a {app} backend extra)"
            )
    if selected.command == "worker":
        from taskiq.cli.worker.cmd import WorkerCMD

        return WorkerCMD().exec(args[1:])

    from taskiq.cli.scheduler.cmd import SchedulerCMD

    return SchedulerCMD().exec(args[1:])

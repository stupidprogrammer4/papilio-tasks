"""Expose native Taskiq execution under the Papilio Tasks command."""

import argparse
from collections.abc import Sequence


def main(args: Sequence[str]) -> int | None:
    parser = argparse.ArgumentParser(prog="papilio_tasks scheduler")
    parser.add_argument("command", choices=("worker", "beat"), nargs="?")
    selected = parser.parse_args(args[:1])
    if selected.command is None:
        parser.print_help()
        return 0
    if selected.command == "worker":
        from taskiq.cli.worker.cmd import WorkerCMD

        return WorkerCMD().exec(args[1:])

    from taskiq.cli.scheduler.cmd import SchedulerCMD

    return SchedulerCMD().exec(args[1:])

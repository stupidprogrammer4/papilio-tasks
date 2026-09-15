"""Lightweight command routing; import only the selected application."""

import argparse
import importlib.util
import sys
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int | None:
    args = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="papilio_tasks")
    parser.add_argument("app", choices=("scheduler",), nargs="?")
    selected = parser.parse_args(args[:1])
    if selected.app is None:
        parser.print_help()
        return 0

    for dependency in ("taskiq", "dishka"):
        if importlib.util.find_spec(dependency) is None:
            parser.error(
                f"Scheduler requires {dependency}; install "
                "'papilio-tasks[scheduler]' (or a scheduler backend extra)"
            )
    from .scheduler import main as scheduler

    return scheduler(args[1:])

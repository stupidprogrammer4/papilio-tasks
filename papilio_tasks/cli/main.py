"""Lightweight command routing; import only the selected application."""

import argparse
import sys
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int | None:
    args = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="papilio_tasks")
    parser.add_argument("app", choices=("scheduler", "projection"), nargs="?")
    selected = parser.parse_args(args[:1])
    if selected.app is None:
        parser.print_help()
        return 0

    from .taskiq import main as run

    return run(selected.app, args[1:])

"""Expose native FastStream execution without importing unselected apps."""

import argparse
import importlib.util
from collections.abc import Sequence


def main(args: Sequence[str]) -> int | None:
    parser = argparse.ArgumentParser(
        prog="papilio_tasks events",
        description="Run Events consumers with native FastStream.",
    )
    parser.add_argument("command", choices=("run",), nargs="?")
    selected = parser.parse_args(args[:1])
    if selected.command is None:
        parser.print_help()
        return 0
    for dependency in ("faststream", "dishka_faststream", "typer"):
        if importlib.util.find_spec(dependency) is None:
            parser.error(
                f"Events CLI requires {dependency}; install "
                "'papilio-tasks[events]' (or an events backend extra)"
            )

    # Optional Events/CLI dependencies must not affect other commands or help.
    # Use the published console entry, despite its missing explicit re-export.
    from faststream.__main__ import (
        cli,  # pyright: ignore[reportPrivateImportUsage]
    )

    cli(args=list(args), prog_name="papilio_tasks events")
    return 0

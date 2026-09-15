"""Discover definitions in application-selected Python packages."""

import importlib
import inspect
import pkgutil
from collections.abc import Iterator, Sequence
from types import ModuleType


class Bootstrapper:
    def __init__(self, modules: Sequence[str] = ()) -> None:
        self.roots = tuple(modules)

    def modules(self, name: str) -> list[ModuleType]:
        """Find name.py or name/ below the roots; propagate import errors."""
        visited: set[str] = set()

        def walk(path: str, selected: bool = False) -> Iterator[ModuleType]:
            if path in visited:
                return
            visited.add(path)
            module = importlib.import_module(path)
            selected = selected or path.rsplit(".", 1)[-1] == name
            if selected:
                yield module
            if not hasattr(module, "__path__"):
                return
            for info in sorted(
                pkgutil.iter_modules(module.__path__, prefix=path + ".")
            ):
                if (
                    selected
                    or info.ispkg
                    or info.name.rsplit(".", 1)[-1] == name
                ):
                    yield from walk(info.name, selected)

        found = {}
        for root in sorted(set(self.roots)):
            for module in walk(root):
                found[module.__name__] = module
        return [found[key] for key in sorted(found)]

    def classes[T](self, name: str, base: type[T]) -> list[type[T]]:
        """Return local concrete subclasses once, including package files."""
        found: list[type[T]] = []
        seen: set[type[T]] = set()
        for module in self.modules(name):
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    obj is not base
                    and issubclass(obj, base)
                    and obj.__module__ == module.__name__
                    and not inspect.isabstract(obj)
                    and obj not in seen
                ):
                    seen.add(obj)
                    found.append(obj)
        return found

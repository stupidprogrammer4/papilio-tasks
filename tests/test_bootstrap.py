import importlib
import textwrap

import pytest
from dishka import Provider, Scope

from papilio_tasks.core.bootstrap import Bootstrapper
from papilio_tasks.schedulers import Scheduler
from papilio_tasks.schedulers.application import create_broker


def write(root, path, content=""):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(textwrap.dedent(content))


def test_discovery_modules_packages_imported_bases_and_duplicates(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    for path in (
        "discovery_app",
        "discovery_app/group",
        "discovery_app/group/one",
        "discovery_app/two",
        "discovery_app/two/schedulers",
    ):
        write(tmp_path, path + "/__init__.py")
    write(
        tmp_path,
        "discovery_app/shared.py",
        """
        from papilio_tasks.schedulers import Scheduler
        class Imported(Scheduler):
            def __init__(self):
                raise AssertionError('must never be constructed')
            async def run(self): pass
    """,
    )
    write(
        tmp_path,
        "discovery_app/group/one/schedulers.py",
        """
        from discovery_app.shared import Imported
        class Report(Imported):
            async def run(self): return 1
        Alias = Report
    """,
    )
    write(
        tmp_path,
        "discovery_app/two/schedulers/report.py",
        """
        from papilio_tasks.schedulers import Scheduler
        class Abstract(Scheduler): pass
        class Other(Scheduler):
            async def run(self): return 2
    """,
    )
    write(
        tmp_path,
        "discovery_app/two/unused.py",
        "raise AssertionError('unrelated file imported')",
    )
    roots = ["discovery_app", "discovery_app.group", "discovery_app"]
    classes = Bootstrapper(roots).classes("schedulers", Scheduler)
    assert [cls.__name__ for cls in classes] == ["Report", "Other"]
    broker = create_broker(modules=roots)
    assert sorted(broker.local_task_registry) == [
        "discovery_app.group.one.schedulers.Report",
        "discovery_app.two.schedulers.report.Other",
    ]


async def test_booted_scheduler_uses_supplied_module_provider(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    write(tmp_path, "provided_app/__init__.py")
    write(
        tmp_path,
        "provided_app/schedulers.py",
        """
        from papilio_tasks.schedulers import Scheduler
        class Service:
            def value(self): return 7
        class Report(Scheduler):
            def __init__(self, service: Service): self.service = service
            async def run(self): return self.service.value()
    """,
    )
    module = importlib.import_module("provided_app.schedulers")
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(module.Service)
    provider.provide(module.Report)
    broker = create_broker(modules=["provided_app"], providers=[provider])
    await broker.startup()
    try:
        result = await (await module.Report.enqueue()).wait_result(timeout=3)
        assert not result.is_err and result.return_value == 7
    finally:
        await broker.shutdown()


def test_discovery_preserves_nested_import_error(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write(tmp_path, "broken_app/__init__.py")
    write(
        tmp_path,
        "broken_app/schedulers.py",
        "import missing_business_dependency",
    )
    with pytest.raises(ModuleNotFoundError) as error:
        Bootstrapper(["broken_app"]).classes("schedulers", Scheduler)
    assert error.value.name == "missing_business_dependency"
    with pytest.raises(ModuleNotFoundError):
        Bootstrapper(["missing_root"]).modules("schedulers")


def test_missing_scheduler_module_is_allowed(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    write(tmp_path, "empty_app/__init__.py")
    assert Bootstrapper(["empty_app"]).classes("schedulers", Scheduler) == []

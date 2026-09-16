import pytest

from papilio_tasks.infra.taskiq import bindings


@pytest.fixture(autouse=True)
def isolated_bindings(monkeypatch):
    # Each test models a fresh process. Lifecycle tests assert release BEFORE
    # fixture restoration, so it cannot hide missing production cleanup.
    monkeypatch.setattr(bindings, "_tasks", {})

import subprocess
import sys


def test_apps_initialization_and_tools_have_no_runtime_dependencies():
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = ('taskiq', 'dishka', 'faststream', 'redis', 'aio_pika',
                   'papilio_tasks.infra', 'papilio_tasks.apps.schedulers',
                   'papilio_tasks.apps.projections',
                   'papilio_tasks.apps.events')
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
import papilio_tasks.apps
import papilio_tasks.tools
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.retry import Retry
from papilio_tasks.tools.hooks import Hook, Handler, emit
from papilio_tasks.tools.hooks.projection import Hooks, Failure
assert Bootstrapper().classes('schedulers', object) == []
assert Retry(attempts=1, delay=0, errors=(ValueError,)).attempts == 1
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)

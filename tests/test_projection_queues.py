import importlib
import subprocess
import sys
from unittest.mock import Mock
from uuid import uuid4

import pytest

from papilio_tasks.apps.projections import Direct
from papilio_tasks.apps.projections.application import create_broker
from papilio_tasks.apps.projections.registry import Registrar


def backend(kind):
    if kind == "rabbit":
        pytest.importorskip("taskiq_aio_pika")
        from papilio_tasks.apps.projections.backends.rabbit import (
            RabbitBroker,
            RabbitDirect,
            RabbitProjection,
            RabbitQueue,
        )
        from papilio_tasks.apps.projections.registry.rabbit import (
            RabbitRegistrar,
        )

        broker = RabbitBroker(
            "amqp://guest:guest@localhost/",
            queues=[RabbitQueue(name="default")],
        )
        return (
            RabbitProjection,
            RabbitDirect,
            RabbitQueue,
            RabbitRegistrar(broker),
        )
    pytest.importorskip("taskiq_redis")
    from papilio_tasks.apps.projections.backends.redis import (
        RedisDirect,
        RedisProjection,
        RedisQueue,
        RedisStreamBroker,
    )
    from papilio_tasks.apps.projections.registry.redis import RedisRegistrar

    return (
        RedisProjection,
        RedisDirect,
        RedisQueue,
        RedisRegistrar(
            RedisStreamBroker("redis://localhost", queue_name="default")
        ),
    )


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
async def test_default_inherited_override_and_shared_queue(kind):
    _, base, Queue, registry = backend(kind)
    spec = Queue(name="products")

    class Product(base[int, int]):
        async def read(self, id: int) -> int:
            return id

        async def write(self, data: int) -> int:
            return data

    class Selected(Product):
        queue = spec

    class Inherited(Selected):
        pass

    class Overridden(Selected):
        pass

    class Batch(base[list[int], int]):
        queue = spec

        async def read(self, ids: list[int]) -> list[int]:
            return ids

        async def write(self, data: list[int]) -> int:
            return sum(data)

    class Default(Selected):
        queue = None

    assert registry.include(Product).labels["queue_name"] == "default"
    assert registry.include(Inherited).labels["queue_name"] == "products"
    assert registry.include(Batch).labels["queue_name"] == "products"
    assert registry.include(Default).labels["queue_name"] == "default"
    assert (
        registry.include(Overridden, queue=Queue(name="elsewhere")).labels[
            "queue_name"
        ]
        == "elsewhere"
    )
    assert Overridden.queue is spec and Selected.queue is spec
    assert registry.broker.get_queue("products") == spec
    assert await Batch().run([1, 2]) == 3
    assert await Product().run(42) == 42
    if kind == "redis":
        assert registry.broker.native.queue_name == "default"
        assert registry.broker.native.additional_streams == {}
    else:
        assert registry.broker.native.write_channel is None


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
def test_conflicts_duplicates_and_invalid_backend(kind, monkeypatch):
    _, base, Queue, registry = backend(kind)

    class Product(base[int, int]):
        queue = Queue(name="products")

        async def read(self, id: int) -> int:
            return id

        async def write(self, data: int) -> int:
            return data

    with pytest.raises(TypeError, match="matching"):
        Registrar(registry.broker).include(Product)
    with pytest.raises(TypeError, match="Projection class"):
        registry.include(Direct)
    with pytest.raises(TypeError, match="Expected"):
        registry.include(Product, queue="products")

    class Invalid(Product):
        queue = "products"

    with pytest.raises(TypeError, match="Expected"):
        registry.include(Invalid)
    assert registry.broker.native.local_task_registry == {}
    task = registry.include(Product, name="one")

    class Other(Product):
        pass

    conflict = Queue(
        name="products",
        **(
            {"durable": not Product.queue.durable}
            if kind == "rabbit"
            else {"read_id": "0"}
        ),
    )
    with pytest.raises(ValueError, match="Conflicting queue"):
        registry.include(Other, queue=conflict)
    assert registry.broker.get_queue("products") is Product.queue
    with pytest.raises(RuntimeError, match="not included"):
        Other.task()
    add = Mock(side_effect=AssertionError("Queue must not be touched"))
    monkeypatch.setattr(registry.broker, "add_queue", add)
    with pytest.raises(ValueError, match="already registered"):
        registry.include(Product, queue=Queue(name="other"))
    with pytest.raises(ValueError, match="already registered"):
        registry.include(Other, name="one", queue=Queue(name="other"))
    add.assert_not_called()
    assert Product.task() is task


def test_rabbit_routing_key_comes_from_queue_spec():
    _, base, Queue, registry = backend("rabbit")

    class Product(base[int, int]):
        queue = Queue(name="products", routing_key="products.sync")

        async def read(self, id: int) -> int:
            return id

        async def write(self, data: int) -> int:
            return data

    with pytest.raises(ValueError, match="Routing label conflicts"):
        registry.include(Product, labels={"queue_name": "products"})
    task = registry.include(Product)
    assert task.labels["queue_name"] == "products.sync"
    assert registry.broker.get_queue("products").routing_key == "products.sync"


def test_backend_registrars_reject_other_backend_classes():
    _, rabbit, _, rabbit_registry = backend("rabbit")
    _, redis, _, redis_registry = backend("redis")
    with pytest.raises(TypeError, match="RabbitProjection"):
        rabbit_registry.include(redis)
    with pytest.raises(TypeError, match="RedisProjection"):
        redis_registry.include(rabbit)
    assert rabbit_registry.broker.native.local_task_registry == {}
    assert redis_registry.broker.native.local_task_registry == {}


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
def test_discovery_uses_backend_registrar(tmp_path, monkeypatch, kind):
    _, base, Queue, registry = backend(kind)
    name = "queue_app_" + uuid4().hex
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").touch()
    queue_name = "RabbitQueue" if kind == "rabbit" else "RedisQueue"
    (package / "projections.py").write_text(
        f"from {base.__module__} import {base.__name__}, {queue_name}\n"
        f"class Product({base.__name__}[int, int]):\n"
        f"    queue = {queue_name}(name='products')\n"
        "    async def read(self, id: int) -> int: return id\n"
        "    async def write(self, data: int) -> int: return data\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    broker = create_broker(registrar=registry, modules=[name])
    cls = importlib.import_module(name + ".projections").Product
    assert cls.task() is broker.find_task(name + ".projections.Product")
    assert cls.task().labels["queue_name"] == "products"


@pytest.mark.parametrize("kind", ["rabbit", "redis"])
def test_optional_backend_imports_are_isolated(kind):
    backend(kind)  # Skip when this optional backend is not installed.
    code = """
import importlib, importlib.abc, sys
kind=sys.argv[1]
blocked=({'taskiq_redis', 'redis'} if kind=='rabbit'
         else {'taskiq_aio_pika', 'aio_pika'})
blocked.add('papilio_tasks.apps.schedulers')
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname==p or fullname.startswith(p+'.') for p in blocked):
            raise AssertionError(fullname)
sys.meta_path.insert(0,Block())
importlib.import_module('papilio_tasks.apps.projections.backends.'+kind)
importlib.import_module('papilio_tasks.apps.projections.registry.'+kind)
"""
    subprocess.run([sys.executable, "-c", code, kind], check=True, timeout=15)

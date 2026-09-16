from collections.abc import Awaitable, Callable
from functools import wraps
from inspect import Parameter, isabstract, iscoroutinefunction, signature
from typing import Any, get_type_hints

from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.brokers.contracts.base import BrokerContract
from papilio_tasks.infra.taskiq.retry import retry_labels

from ..base import Scheduler


class Registrar[S: Scheduler]:
    """Include module-owned classes; default to an in-process Memory broker."""

    _backend = "memory"

    def __init__(
        self,
        broker: BrokerContract | None = None,
    ) -> None:
        self.broker = broker if broker is not None else MemoryBroker()

    def include(
        self,
        cls: type[S],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AsyncTaskiqDecoratedTask:
        execute, name = self._prepare(cls, name)
        labels = retry_labels(self.broker.native, cls.retry, labels)
        task = self.broker.register(execute, name=name, labels=labels)
        bindings.add(cls, task)
        return task

    def _prepare(
        self, cls: type[S], name: str | None
    ) -> tuple[Callable[..., Awaitable[Any]], str]:
        from dishka import FromDishka
        from dishka.integrations.taskiq import inject

        if not isinstance(cls, type) or not issubclass(cls, Scheduler):
            raise TypeError("Include a Scheduler class")
        if cls._backend != self._backend:
            raise TypeError(f"Expected a {self._backend} scheduler")
        if isabstract(cls) or not iscoroutinefunction(cls.run):
            raise TypeError("Scheduler must implement async run")
        bindings.check(cls)
        if name is None:
            name = f"{cls.__module__}.{cls.__qualname__}"
        if not name:
            raise ValueError("Task name cannot be empty")
        if self.broker.native.find_task(name) is not None:
            raise ValueError(f"Task name already registered: {name}")

        run = cls.run
        sig = signature(run)
        parameters = list(sig.parameters.values())
        if not parameters or parameters[0].name != "self":
            raise TypeError("run must be an instance method with self")
        if {"_job", "dishka_container"} & sig.parameters.keys():
            raise TypeError(
                "_job and dishka_container are reserved for injection"
            )

        @wraps(run)
        async def execute(*args: Any, _job: Scheduler, **kwargs: Any) -> Any:
            return await _job.run(*args, **kwargs)

        parameters = parameters[1:]
        index = len(parameters)
        if parameters and parameters[-1].kind == Parameter.VAR_KEYWORD:
            index -= 1
        parameters.insert(
            index,
            Parameter(
                "_job", Parameter.KEYWORD_ONLY, annotation=FromDishka[cls]
            ),
        )
        setattr(execute, "__signature__", sig.replace(parameters=parameters))
        execute.__annotations__ = get_type_hints(run)
        execute.__annotations__.pop("self", None)
        execute.__annotations__["_job"] = FromDishka[cls]
        injected = inject(execute, patch_module=True)
        # Keep native DI metadata in __signature__, but don't let Taskiq's
        # positional payload parser try to cast a job argument to a container.
        injected.__annotations__.pop("dishka_container", None)
        return injected, name

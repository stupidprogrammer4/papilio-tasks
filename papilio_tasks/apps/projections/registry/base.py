from functools import wraps
from inspect import Parameter, isabstract, iscoroutinefunction, signature
from typing import Any, get_type_hints

from dishka import FromDishka
from dishka.integrations.taskiq import inject
from taskiq.decor import AsyncTaskiqDecoratedTask

from papilio_tasks.infra.taskiq import bindings
from papilio_tasks.infra.taskiq.brokers.backends.memory import MemoryBroker
from papilio_tasks.infra.taskiq.brokers.contracts.base import BrokerContract
from papilio_tasks.tools.hooks.projection import Hooks

from ..base import Projection


class Registrar:
    """Register tasks; the caller owns container setup and lifetime."""

    def __init__(
        self,
        broker: BrokerContract | None = None,
        *,
        hooks: type[Hooks[object, object, object]] | None = None,
    ) -> None:
        if hooks is not None and (
            not isinstance(hooks, type) or not issubclass(hooks, Hooks)
        ):
            raise TypeError("Select a Hooks class provided by your container")
        self.broker = broker if broker is not None else MemoryBroker()
        self.hooks = hooks

    def include[T, D, R](
        self,
        cls: type[Projection[T, D, R]],
        *,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AsyncTaskiqDecoratedTask[Any, R]:
        if not isinstance(cls, type) or not issubclass(cls, Projection):
            raise TypeError("Include a Projection class")
        if isabstract(cls) or not all(
            iscoroutinefunction(getattr(cls, method))
            for method in ("read", "transform", "write")
        ):
            raise TypeError("Projection must implement async operations")
        bindings.check(cls)

        read = cls.read
        hints = get_type_hints(read)
        hints.pop("self", None)
        hints["return"] = get_type_hints(cls.write).get("return", Any)
        sig = signature(read)
        parameters = list(sig.parameters.values())
        if not parameters or parameters[0].name != "self":
            raise TypeError("read must be an instance method with self")
        if {"_job", "_hooks", "dishka_container"} & sig.parameters.keys():
            raise TypeError(
                "_job, _hooks and dishka_container are reserved for injection"
            )

        @wraps(read)
        async def execute(
            *args: Any,
            _job: Projection[T, D, R],
            _hooks: Hooks[object, object, object] | None = None,
            **kwargs: Any,
        ) -> R:
            return await _job._run(_hooks, args, kwargs)

        dependencies = {"_job": FromDishka[cls]}
        if self.hooks is not None:
            dependencies["_hooks"] = FromDishka[self.hooks]
        parameters = [
            p.replace(annotation=hints.get(p.name, p.annotation))
            for p in parameters[1:]
        ]
        index = len(parameters)
        if parameters and parameters[-1].kind == Parameter.VAR_KEYWORD:
            index -= 1
        parameters[index:index] = [
            Parameter(key, Parameter.KEYWORD_ONLY, annotation=value)
            for key, value in dependencies.items()
        ]
        setattr(
            execute,
            "__signature__",
            sig.replace(
                parameters=parameters, return_annotation=hints["return"]
            ),
        )
        execute.__annotations__ = hints | dependencies
        injected = inject(execute, patch_module=True)
        # Native DI reads __signature__; exclude its container from Taskiq's
        # annotation-based positional payload conversion, as for schedulers.
        injected.__annotations__.pop("dishka_container", None)
        task = self.broker.register(
            injected,
            name=name
            if name is not None
            else f"{cls.__module__}.{cls.__qualname__}",
            labels=labels,
        )
        bindings.add(cls, task)
        return task

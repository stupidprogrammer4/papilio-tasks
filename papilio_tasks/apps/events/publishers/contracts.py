from collections.abc import Awaitable, Callable
from typing import Any

type Sender = Callable[..., Awaitable[Any]]

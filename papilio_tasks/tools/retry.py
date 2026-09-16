"""Immutable per-job retry settings, independent of the task runtime."""

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True, kw_only=True)
class Retry:
    """Total attempts include the initial call; delay is in seconds."""

    attempts: int
    delay: float
    errors: tuple[type[Exception], ...]

    def __post_init__(self) -> None:
        if type(self.attempts) is not int or self.attempts < 1:
            raise ValueError("attempts must be a positive integer")
        if (
            isinstance(self.delay, bool)
            or not isinstance(self.delay, (int, float))
            or not isfinite(self.delay)
            or self.delay < 0
        ):
            raise ValueError("delay must be finite and nonnegative")
        if not isinstance(self.errors, tuple) or not self.errors:
            raise ValueError("errors must be a nonempty tuple of exceptions")
        if any(
            not isinstance(error, type) or not issubclass(error, Exception)
            for error in self.errors
        ):
            raise TypeError("errors must contain Exception subclasses")

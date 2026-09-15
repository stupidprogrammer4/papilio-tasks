from dataclasses import dataclass


@dataclass(frozen=True)
class RedisQueue:
    """A stream and read cursor. Group settings belong to the broker."""

    name: str
    read_id: str | int = ">"

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Queue name cannot be empty")

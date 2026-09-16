from .contracts import Sender

_senders: dict[type, tuple[object, Sender]] = {}


def check(cls: type) -> None:
    if cls in _senders:
        raise ValueError(f"Publisher already registered: {cls.__qualname__}")


def add(cls: type, owner: object, sender: Sender) -> None:
    check(cls)
    _senders[cls] = (owner, sender)


def get(cls: type) -> Sender:
    try:
        return _senders[cls][1]
    except KeyError:
        raise RuntimeError(
            f"Publisher is not registered: {cls.__qualname__}"
        ) from None


def release(owner: object) -> None:
    for cls, (registered, _) in tuple(_senders.items()):
        if registered is owner:
            del _senders[cls]

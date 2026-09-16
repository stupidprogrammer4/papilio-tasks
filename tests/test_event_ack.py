import pytest

pytest.importorskip("dishka_faststream")
pytest.importorskip("faststream.rabbit")

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker as NativeRabbitBroker

from papilio_tasks.apps.events.publishers.rabbit import (
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker


@pytest.mark.parametrize(
    "broker_policy,class_policy,options,expected",
    [
        (None, None, {}, AckPolicy.REJECT_ON_ERROR),
        (AckPolicy.ACK, None, {}, AckPolicy.ACK),
        (AckPolicy.ACK, None, {"ack_policy": None}, AckPolicy.ACK),
        (AckPolicy.ACK, AckPolicy.NACK_ON_ERROR, {}, AckPolicy.NACK_ON_ERROR),
        (
            AckPolicy.ACK,
            AckPolicy.NACK_ON_ERROR,
            {"ack_policy": None},
            AckPolicy.NACK_ON_ERROR,
        ),
        (
            AckPolicy.NACK_ON_ERROR,
            AckPolicy.ACK,
            {"ack_policy": AckPolicy.MANUAL},
            AckPolicy.MANUAL,
        ),
    ],
)
def test_native_policy_precedence_and_inheritance(
    broker_policy, class_policy, options, expected
):
    native = NativeRabbitBroker(
        logger=None,
        **({"ack_policy": broker_policy} if broker_policy is not None else {}),
    )
    reg = RabbitRegistrar(RabbitBroker(native))

    class Created(RabbitPublisher[int]):
        exchange = RabbitExchange("orders")
        routing_key = "created"

    class Parent(RabbitSubscriber[int]):
        publisher = Created
        queue = RabbitQueue("finance")
        ack_policy = class_policy

        async def run(self, event: int) -> None:
            pass

    class Child(Parent):
        pass

    before = dict(vars(Child))
    reg.subscriber(Child, **options)
    assert native.subscribers[0].ack_policy is expected
    assert dict(vars(Child)) == before
    assert Parent.ack_policy is class_policy

    class Unset(Parent):
        queue = RabbitQueue("unset")
        ack_policy = None

    reg.subscriber(Unset)
    configured = {sub.queue.name: sub.ack_policy for sub in native.subscribers}
    assert configured["unset"] is (broker_policy or AckPolicy.REJECT_ON_ERROR)


@pytest.mark.parametrize("in_class", [False, True])
def test_invalid_policy_does_not_register(in_class):
    native = NativeRabbitBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))

    class Created(RabbitPublisher[int]):
        exchange = RabbitExchange("orders")
        routing_key = "created"

    class Consumer(RabbitSubscriber[int]):
        publisher = Created
        queue = RabbitQueue("finance")
        ack_policy = "nack_on_error" if in_class else None

        async def run(self, event: int) -> None:
            pass

    options = {} if in_class else {"ack_policy": "nack_on_error"}
    with pytest.raises(TypeError, match="Expected AckPolicy"):
        reg.subscriber(Consumer, **options)
    assert not native.subscribers
    assert not reg.has_subscriber(Consumer)

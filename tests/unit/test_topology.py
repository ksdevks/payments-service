from payments.infrastructure.messaging.topology import Topology


def has_arguments(actual: dict[str, object] | None, expected: dict[str, object]) -> bool:
    # FastStream добавляет свои аргументы (например, x-queue-type), проверяем наши.
    return expected.items() <= (actual or {}).items()


def test_topology_layout() -> None:
    topology = Topology.build((1000, 2000))

    assert topology.main_queue.name == "payments.new"
    assert has_arguments(
        topology.main_queue.arguments,
        {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "payments.dlx",
            "x-dead-letter-routing-key": "payments.new.dlq",
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
            "x-delivery-limit": 10,
        },
    )
    assert has_arguments(topology.dead_letter_queue.arguments, {"x-queue-type": "quorum"})
    assert topology.dead_letter_queue.name == "payments.new.dlq"

    retry = topology.retry_queues[2000]
    assert retry.name == "payments.new.retry.2000ms"
    assert has_arguments(
        retry.arguments,
        {
            "x-queue-type": "quorum",
            "x-message-ttl": 2000,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payments.new",
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


def test_prefix_isolates_names() -> None:
    topology = Topology.build((100,), prefix="t1")
    assert topology.exchange.name == "t1"
    assert topology.main_queue.name == "t1.new"
    assert topology.main_queue.routing == "payments.new"

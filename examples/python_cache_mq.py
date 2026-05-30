from __future__ import annotations

from time import time_ns

from pgapp_sdk import PGAppClient


def main() -> None:
    client = PGAppClient("127.0.0.1:50051", timeout=5)
    suffix = time_ns()
    namespace = f"example_cache_{suffix}"
    queue = f"example_orders_{suffix}"

    client.cache.set(namespace, "hello", b"world", ttl_seconds=60)
    print(client.cache.get(namespace, "hello"))

    client.mq.create_queue(queue)
    message_id = client.mq.send_json(queue, {"order_id": 123})
    messages = client.mq.read(queue, quantity=1, visibility_timeout_seconds=30)
    if not messages:
        raise RuntimeError("message was not delivered")

    message = messages[0]
    if message.message_id != message_id:
        raise RuntimeError("unexpected message id")

    client.mq.ack(queue, message.message_id, message.ack_token)


if __name__ == "__main__":
    main()

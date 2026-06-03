# PGApp Python SDK

Typed Python client for PGApp Cache, MQ, and Config gRPC services.

## Install With uv

From an application repository:

```sh
uv venv --python /opt/homebrew/bin/python3
uv pip install /path/to/pgapp/sdk/python
```

For editable local development in this repository:

```sh
cd sdk/python
uv sync --group dev --python /opt/homebrew/bin/python3
uv run python -m unittest discover -s tests
uv run mypy --strict pgapp_sdk/client.py pgapp_sdk/__init__.py pgapp/__init__.py
```

The package ships inline type hints through `pgapp_sdk/py.typed` and includes
the generated `pgapp.v1` protobuf modules, so consumers do not need to set
`PYTHONPATH` after installation.

## Usage

```python
from pgapp_sdk import PGAppClient

client = PGAppClient(
    "127.0.0.1:50051",
    timeout=5,
    key="svc-billing",
    secret="secret",
)

client.cache.set("default", "hello", "world", ttl_seconds=60)
assert client.cache.get("default", "hello") == b"world"
assert client.cache.increment("default", "counter", 1, ttl_seconds=60) == 1
assert client.cache.set_nx("default", "lock", "1", ttl_seconds=30)
old_value = client.cache.get_set("default", "last-release", "v2", ttl_seconds=60)
new_length = client.cache.append("default", "log", "\nentry", ttl_seconds=60)

client.mq.create_queue("orders")
message_id = client.mq.send_json("orders", {"order_id": 123})
message = client.mq.read("orders", quantity=1)[0]
assert message.message_id == message_id
client.mq.ack("orders", message.message_id, message.ack_token)

stream_message_id = client.mq.send_json("orders", {"stream": True})
for streamed in client.mq.stream_read("orders", quantity=1, visibility_timeout_seconds=30):
    assert streamed.message_id == stream_message_id
    print(streamed.payload)
    break

dlq_messages = client.mq.list_dlq_messages("orders")
if dlq_messages:
    recovered = client.mq.get_dlq_message("orders", dlq_messages[0].original_message_id)
    assert recovered.original_message_id == dlq_messages[0].original_message_id
    client.mq.reprocess_dlq_message("orders", dlq_messages[0].original_message_id)

scope = client.config.scope("billing", "prod", "default", "application")
release = client.config.get_latest_release(scope)
print(release.revision)

print(old_value, new_length)
```

## Installability Check

The repository includes a uv-based smoke test that creates a temporary Python
environment, installs the SDK, imports `pgapp_sdk` and `pgapp.v1`, and verifies
the package type marker:

```sh
sh scripts/check-python-sdk-install.sh
```

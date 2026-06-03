import unittest

from pgapp_sdk import PGAppClient, PGAppError


class ClientTests(unittest.TestCase):
    def test_client_keeps_endpoint_and_timeout(self) -> None:
        client = PGAppClient("http://127.0.0.1:50051", timeout=3)
        self.assertEqual(client.endpoint, "http://127.0.0.1:50051")
        self.assertEqual(client.timeout, 3)
        self.assertIs(client.cache.client, client)
        self.assertIs(client.mq.client, client)
        self.assertIs(client.config.client, client)

    def test_client_auth_metadata_requires_key_and_secret(self) -> None:
        client = PGAppClient("http://127.0.0.1:50051", key="svc", secret="top-secret")
        self.assertEqual(
            client.metadata,
            (("x-pgapp-key", "svc"), ("x-pgapp-secret", "top-secret")),
        )
        with self.assertRaises(PGAppError):
            PGAppClient(key="svc")

    def test_cache_bytes_are_preserved(self) -> None:
        client = PGAppClient()
        self.assertEqual(client.cache.encode_value(b"abc"), b"abc")
        self.assertEqual(client.cache.encode_value("abc"), b"abc")

    def test_mq_json_is_stable(self) -> None:
        client = PGAppClient()
        self.assertEqual(client.mq.encode_json({"b": 2, "a": 1}), '{"a":1,"b":2}')

    def test_config_scope_and_json_are_stable(self) -> None:
        client = PGAppClient()
        scope = client.config.scope("billing", "prod", "default", "application")
        self.assertEqual(scope.app_id, "billing")
        self.assertEqual(client.config.encode_json({"b": 2, "a": 1}), '{"a":1,"b":2}')

    def test_python_sdk_error_preserves_status(self) -> None:
        client = PGAppClient()
        with self.assertRaises(PGAppError) as raised:
            client.cache.encode_value(object())
        self.assertEqual(raised.exception.status_code, "invalid_argument")

    def test_phase_two_methods_are_exposed(self) -> None:
        client = PGAppClient()
        for name in (
            "increment",
            "decrement",
            "set_nx",
            "get_set",
            "append",
            "prepend",
        ):
            self.assertTrue(callable(getattr(client.cache, name)))
        for name in (
            "list_dlq_messages",
            "get_dlq_message",
            "reprocess_dlq_message",
            "purge_dlq",
            "stream_read",
        ):
            self.assertTrue(callable(getattr(client.mq, name)))


if __name__ == "__main__":
    unittest.main()

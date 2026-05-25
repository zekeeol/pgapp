import os
import time
import unittest

from pgapp_sdk import PGAppClient, PGAppError


class LivePythonSDKTests(unittest.TestCase):
    def setUp(self) -> None:
        endpoint = os.environ.get("PGAPP_TEST_ENDPOINT")
        if not endpoint:
            self.skipTest("PGAPP_TEST_ENDPOINT is not set")
        self.client = PGAppClient(endpoint, timeout=5)

    def test_cache_and_mq_round_trip(self) -> None:
        suffix = time.time_ns()
        namespace = f"py_sdk_cache_{suffix}"
        queue = f"py_sdk_orders_{suffix}"

        self.assertTrue(self.client.cache.set(namespace, "hello", b"world", ttl_seconds=60))
        self.assertEqual(self.client.cache.get(namespace, "hello"), b"world")

        self.assertTrue(self.client.mq.create_queue(queue))
        message_id = self.client.mq.send_json(queue, {"ok": True})
        messages = self.client.mq.read(queue, quantity=1, visibility_timeout_seconds=30)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message_id, message_id)
        self.assertTrue(self.client.mq.delete(queue, message_id))

    def test_phase_one_sdk_surface(self) -> None:
        suffix = time.time_ns()
        namespace = f"py_sdk_surface_cache_{suffix}"
        queue = f"py_sdk_surface_orders_{suffix}"

        self.assertTrue(self.client.cache.set(namespace, "a", b"one", ttl_seconds=60))
        self.assertTrue(self.client.cache.set(namespace, "b", b"two", ttl_seconds=60))
        items = self.client.cache.mget(namespace, ["a", "missing"])
        self.assertEqual(len(items), 2)
        self.assertTrue(items[0].hit)
        self.assertTrue(self.client.cache.exists(namespace, "a"))
        self.assertTrue(self.client.cache.delete(namespace, "b"))
        self.assertTrue(self.client.cache.invalidate_namespace(namespace))
        self.assertGreaterEqual(self.client.cache.stats().writes, 2)

        self.assertTrue(self.client.mq.create_queue(queue))
        ids = self.client.mq.send_batch_json(queue, [{"n": 1}, {"n": 2}], delay_seconds=0)
        self.assertEqual(len(ids), 2)
        messages = self.client.mq.read_with_poll(
            queue,
            quantity=1,
            visibility_timeout_seconds=30,
            max_poll_seconds=1,
            poll_interval_millis=25,
        )
        self.assertEqual(len(messages), 1)
        self.assertTrue(
            self.client.mq.set_visibility_timeout(queue, messages[0].message_id, 0)
        )
        self.assertTrue(self.client.mq.archive(queue, messages[0].message_id))
        self.assertEqual(self.client.mq.metrics(queue).archived_message_count, 1)
        self.assertTrue(self.client.mq.purge_queue(queue))
        self.assertTrue(self.client.mq.drop_queue(queue))

    def test_error_status_is_preserved(self) -> None:
        with self.assertRaises(PGAppError) as raised:
            self.client.cache.set("bad namespace", "key", b"value", ttl_seconds=60)
        self.assertEqual(raised.exception.status_code, "INVALID_ARGUMENT")


if __name__ == "__main__":
    unittest.main()

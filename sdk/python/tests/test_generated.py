import unittest

from pgapp.v1 import cache_pb2, cache_pb2_grpc, config_pb2, config_pb2_grpc, mq_pb2, mq_pb2_grpc


class GeneratedProtoTests(unittest.TestCase):
    def test_generated_python_messages_and_grpc_stubs_import(self) -> None:
        request = cache_pb2.SetCacheRequest(
            namespace="default",
            key="hello",
            value=b"world",
        )
        self.assertEqual(request.value, b"world")
        self.assertTrue(hasattr(cache_pb2_grpc, "CacheServiceStub"))
        message = mq_pb2.QueueMessage(message_id=42, ack_token="receipt")
        ack = mq_pb2.AckMessageRequest(
            queue_name="orders",
            message_id=message.message_id,
            ack_token=message.ack_token,
        )
        self.assertEqual(ack.ack_token, "receipt")
        self.assertTrue(hasattr(mq_pb2_grpc, "MQServiceStub"))
        scope = config_pb2.ConfigScope(
            app_id="billing",
            environment="prod",
            cluster="default",
            namespace="application",
        )
        watch = config_pb2.WatchConfigRequest(
            scope=scope,
            known_revision=1,
            timeout_seconds=30,
        )
        self.assertEqual(watch.scope.app_id, "billing")
        self.assertTrue(hasattr(config_pb2_grpc, "ConfigServiceStub"))


if __name__ == "__main__":
    unittest.main()

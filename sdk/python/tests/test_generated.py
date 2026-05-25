import unittest

from pgapp.v1 import cache_pb2, cache_pb2_grpc


class GeneratedProtoTests(unittest.TestCase):
    def test_generated_python_messages_and_grpc_stubs_import(self) -> None:
        request = cache_pb2.SetCacheRequest(
            namespace="default",
            key="hello",
            value=b"world",
        )
        self.assertEqual(request.value, b"world")
        self.assertTrue(hasattr(cache_pb2_grpc, "CacheServiceStub"))


if __name__ == "__main__":
    unittest.main()

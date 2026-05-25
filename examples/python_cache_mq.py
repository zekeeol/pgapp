from pgapp_sdk import PGAppClient


client = PGAppClient("http://127.0.0.1:50051", timeout=3)
print(client.endpoint)

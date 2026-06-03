# @pgapp/sdk

Typed Node.js client for PGApp Cache, MQ, and Config Center gRPC services.

```bash
npm install @pgapp/sdk @grpc/grpc-js
```

```ts
import { PGAppClient } from "@pgapp/sdk";

const client = new PGAppClient("127.0.0.1:50051", {
  timeoutMs: 5000,
  credentials: { key: "svc-billing", secret: "secret" }
});

await client.cache.set("default", "hello", "world", 60);
const value = await client.cache.get("default", "hello");
await client.cache.increment("default", "counter", 1, 60);
await client.cache.setNX("default", "lock", "1", 30);

await client.mq.createQueue("orders");
const id = await client.mq.sendJson("orders", { orderId: 123 });
const messages = await client.mq.read("orders", 1, 30);
await client.mq.ack("orders", id, messages[0].ackToken);

const dlq = await client.mq.listDlqMessages("orders");
if (dlq.length > 0) {
  await client.mq.reprocessDlqMessage("orders", Number(dlq[0].originalMessageId));
}

const streamId = await client.mq.sendJson("orders", { stream: true });
for await (const message of client.mq.streamRead("orders")) {
  if (Number(message.messageId) !== streamId) continue;
  console.log(message.jsonPayload);
  break;
}

const scope = client.config.scope("billing", "prod", "default", "application");
const release = await client.config.getLatestRelease(scope);
console.log(release.revision, release.snapshot);
```

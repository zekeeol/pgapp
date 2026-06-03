import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import type { RpcOptions } from "@protobuf-ts/runtime-rpc";
import { ChannelCredentials, status as GrpcStatus } from "@grpc/grpc-js";
import type { ServiceError } from "@grpc/grpc-js";
import { CacheServiceClient } from "./gen/pgapp/v1/cache.client";
import {
  AppendRequest,
  CacheStatsRequest,
  DeleteCacheRequest,
  DecrementRequest,
  ExistsCacheRequest,
  GetCacheRequest,
  GetSetRequest,
  IncrementRequest,
  InvalidateNamespaceRequest,
  MGetCacheRequest,
  PrependRequest,
  SetCacheRequest,
  SetNXRequest
} from "./gen/pgapp/v1/cache";
import { ConfigServiceClient } from "./gen/pgapp/v1/config.client";
import {
  ConfigScope,
  GetConfigReleaseRequest,
  WatchConfigRequest
} from "./gen/pgapp/v1/config";
import { MQServiceClient } from "./gen/pgapp/v1/mq.client";
import {
  AckMessageRequest,
  ArchiveMessageRequest,
  CreateQueueRequest,
  DropQueueRequest,
  GetDlqMessageRequest,
  ListDlqMessagesRequest,
  PurgeDlqRequest,
  PurgeQueueRequest,
  QueueMessage,
  QueueMetricsRequest,
  QueueStorageMode,
  ReadMessagesRequest,
  ReadWithPollRequest,
  ReprocessDlqMessageRequest,
  SendBatchRequest,
  SendMessageRequest,
  SetVisibilityTimeoutRequest,
  StreamReadRequest
} from "./gen/pgapp/v1/mq";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type PGAppClientOptions = {
  timeoutMs?: number;
  credentials?: {
    key: string;
    secret: string;
  };
};

export class PGAppError extends Error {
  readonly code: number | undefined;

  constructor(message: string, code?: number) {
    super(message);
    this.name = "PGAppError";
    this.code = code;
  }

  static from(error: unknown): PGAppError {
    const candidate = error as Partial<ServiceError>;
    if (typeof candidate.message === "string") {
      return new PGAppError(candidate.message, candidate.code);
    }
    return new PGAppError(error instanceof Error ? error.message : String(error));
  }
}

export class PGAppClient {
  readonly endpoint: string;
  readonly timeoutMs?: number;
  readonly cache: CacheClient;
  readonly mq: MQClient;
  readonly config: ConfigClient;
  private readonly credentials?: { key: string; secret: string };

  constructor(endpoint = "127.0.0.1:50051", options: PGAppClientOptions = {}) {
    this.endpoint = endpoint;
    this.timeoutMs = options.timeoutMs;
    this.credentials = options.credentials;
    const transport = new GrpcTransport({
      host: endpoint,
      channelCredentials: ChannelCredentials.createInsecure()
    });
    this.cache = new CacheClient(new CacheServiceClient(transport), this);
    this.mq = new MQClient(new MQServiceClient(transport), this);
    this.config = new ConfigClient(new ConfigServiceClient(transport), this);
  }

  rpcOptions(): RpcOptions {
    const meta: Record<string, string> = {};
    if (this.credentials) {
      meta["x-pgapp-key"] = this.credentials.key;
      meta["x-pgapp-secret"] = this.credentials.secret;
    }
    return {
      meta,
      timeout: this.timeoutMs
    };
  }
}

export class CacheClient {
  constructor(
    private readonly inner: CacheServiceClient,
    private readonly client: PGAppClient
  ) {}

  async set(namespace: string, key: string, value: Uint8Array | string, ttlSeconds = 0): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.set(SetCacheRequest.create({
        namespace,
        key,
        value: encodeBytes(value),
        ttlSeconds: String(ttlSeconds)
      }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async get(namespace: string, key: string): Promise<Uint8Array | null> {
    return this.wrap(async () => {
      const response = await this.inner.get(GetCacheRequest.create({ namespace, key }), this.client.rpcOptions());
      return response.response.hit ? response.response.value : null;
    });
  }

  async mget(namespace: string, keys: string[]) {
    return this.wrap(async () => {
      const response = await this.inner.mGet(MGetCacheRequest.create({ namespace, keys }), this.client.rpcOptions());
      return response.response.items;
    });
  }

  async delete(namespace: string, key: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.delete(DeleteCacheRequest.create({ namespace, key }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async exists(namespace: string, key: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.exists(ExistsCacheRequest.create({ namespace, key }), this.client.rpcOptions());
      return response.response.exists;
    });
  }

  async invalidateNamespace(namespace: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.invalidateNamespace(InvalidateNamespaceRequest.create({ namespace }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async stats() {
    return this.wrap(async () => {
      const response = await this.inner.stats(CacheStatsRequest.create(), this.client.rpcOptions());
      return response.response;
    });
  }

  async increment(namespace: string, key: string, delta = 1, ttlSeconds = 0): Promise<number> {
    return this.wrap(async () => {
      const response = await this.inner.increment(IncrementRequest.create({ namespace, key, delta: String(delta), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return Number(response.response.value);
    });
  }

  async decrement(namespace: string, key: string, delta = 1, ttlSeconds = 0): Promise<number> {
    return this.wrap(async () => {
      const response = await this.inner.decrement(DecrementRequest.create({ namespace, key, delta: String(delta), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return Number(response.response.value);
    });
  }

  async setNX(namespace: string, key: string, value: Uint8Array | string, ttlSeconds = 0): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.setNX(SetNXRequest.create({ namespace, key, value: encodeBytes(value), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return response.response.created;
    });
  }

  async getSet(namespace: string, key: string, value: Uint8Array | string, ttlSeconds = 0): Promise<Uint8Array | null> {
    return this.wrap(async () => {
      const response = await this.inner.getSet(GetSetRequest.create({ namespace, key, value: encodeBytes(value), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return response.response.hit ? response.response.oldValue : null;
    });
  }

  async append(namespace: string, key: string, value: Uint8Array | string, ttlSeconds = 0): Promise<number> {
    return this.wrap(async () => {
      const response = await this.inner.append(AppendRequest.create({ namespace, key, value: encodeBytes(value), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return Number(response.response.length);
    });
  }

  async prepend(namespace: string, key: string, value: Uint8Array | string, ttlSeconds = 0): Promise<number> {
    return this.wrap(async () => {
      const response = await this.inner.prepend(PrependRequest.create({ namespace, key, value: encodeBytes(value), ttlSeconds: String(ttlSeconds) }), this.client.rpcOptions());
      return Number(response.response.length);
    });
  }

  private async wrap<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error) {
      throw PGAppError.from(error);
    }
  }
}

export class MQClient {
  constructor(
    private readonly inner: MQServiceClient,
    private readonly client: PGAppClient
  ) {}

  async createQueue(queueName: string, storageMode = QueueStorageMode.DURABLE): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.createQueue(CreateQueueRequest.create({ queueName, storageMode }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async purgeQueue(queueName: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.purgeQueue(PurgeQueueRequest.create({ queueName }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async dropQueue(queueName: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.dropQueue(DropQueueRequest.create({ queueName }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async sendJson(queueName: string, payload: JsonValue, delaySeconds = 0): Promise<number> {
    return this.wrap(async () => {
      const response = await this.inner.send(SendMessageRequest.create({ queueName, jsonPayload: JSON.stringify(payload), delaySeconds: String(delaySeconds) }), this.client.rpcOptions());
      return Number(response.response.messageId);
    });
  }

  async sendBatchJson(queueName: string, payloads: JsonValue[], delaySeconds = 0): Promise<number[]> {
    return this.wrap(async () => {
      const response = await this.inner.sendBatch(SendBatchRequest.create({ queueName, jsonPayloads: payloads.map((payload) => JSON.stringify(payload)), delaySeconds: String(delaySeconds) }), this.client.rpcOptions());
      return response.response.messageIds.map(Number);
    });
  }

  async read(queueName: string, quantity = 1, visibilityTimeoutSeconds = 30): Promise<QueueMessage[]> {
    return this.wrap(async () => {
      const response = await this.inner.read(ReadMessagesRequest.create({ queueName, quantity, visibilityTimeoutSeconds: String(visibilityTimeoutSeconds) }), this.client.rpcOptions());
      return response.response.messages;
    });
  }

  async readWithPoll(queueName: string, quantity = 1, visibilityTimeoutSeconds = 30, maxPollSeconds = 10, pollIntervalMillis = 100): Promise<QueueMessage[]> {
    return this.wrap(async () => {
      const response = await this.inner.readWithPoll(ReadWithPollRequest.create({
        queueName,
        quantity,
        visibilityTimeoutSeconds: String(visibilityTimeoutSeconds),
        maxPollSeconds: String(maxPollSeconds),
        pollIntervalMillis: String(pollIntervalMillis)
      }), this.client.rpcOptions());
      return response.response.messages;
    });
  }

  async ack(queueName: string, messageId: number, ackToken: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.ack(AckMessageRequest.create({ queueName, messageId: String(messageId), ackToken }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async archive(queueName: string, messageId: number, ackToken: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.archive(ArchiveMessageRequest.create({ queueName, messageId: String(messageId), ackToken }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async setVisibilityTimeout(queueName: string, messageId: number, ackToken: string, visibilityTimeoutSeconds: number): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.setVisibilityTimeout(SetVisibilityTimeoutRequest.create({ queueName, messageId: String(messageId), ackToken, visibilityTimeoutSeconds: String(visibilityTimeoutSeconds) }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async metrics(queueName: string) {
    return this.wrap(async () => {
      const response = await this.inner.metrics(QueueMetricsRequest.create({ queueName }), this.client.rpcOptions());
      return response.response;
    });
  }

  async listDlqMessages(queueName: string, limit = 100, offset = 0) {
    return this.wrap(async () => {
      const response = await this.inner.listDlqMessages(ListDlqMessagesRequest.create({ queueName, limit, offset: String(offset) }), this.client.rpcOptions());
      return response.response.messages;
    });
  }

  async getDlqMessage(queueName: string, originalMessageId: number) {
    return this.wrap(async () => {
      const response = await this.inner.getDlqMessage(GetDlqMessageRequest.create({ queueName, originalMessageId: String(originalMessageId) }), this.client.rpcOptions());
      return response.response;
    });
  }

  async reprocessDlqMessage(queueName: string, originalMessageId: number): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.reprocessDlqMessage(ReprocessDlqMessageRequest.create({ queueName, originalMessageId: String(originalMessageId) }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async purgeDlq(queueName: string): Promise<boolean> {
    return this.wrap(async () => {
      const response = await this.inner.purgeDlq(PurgeDlqRequest.create({ queueName }), this.client.rpcOptions());
      return response.response.success;
    });
  }

  async *streamRead(queueName: string, options: { quantity?: number; visibilityTimeoutSeconds?: number } = {}): AsyncIterable<QueueMessage> {
    try {
      const responses = this.inner.streamRead(StreamReadRequest.create({
        queueName,
        quantity: options.quantity ?? 1,
        visibilityTimeoutSeconds: String(options.visibilityTimeoutSeconds ?? 30)
      }), this.client.rpcOptions());
      for await (const response of responses.responses) {
        for (const message of response.messages) {
          yield message;
        }
      }
    } catch (error) {
      throw PGAppError.from(error);
    }
  }

  private async wrap<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error) {
      throw PGAppError.from(error);
    }
  }
}

export class ConfigClient {
  constructor(
    private readonly inner: ConfigServiceClient,
    private readonly client: PGAppClient
  ) {}

  scope(appId: string, environment: string, cluster: string, namespace: string): ConfigScope {
    return ConfigScope.create({ appId, environment, cluster, namespace });
  }

  async getLatestRelease(scope: ConfigScope): Promise<ConfigReleaseSnapshot> {
    return this.getRelease(scope, 0);
  }

  async getRelease(scope: ConfigScope, revision: number): Promise<ConfigReleaseSnapshot> {
    return this.wrap(async () => {
      const response = await this.inner.getRelease(GetConfigReleaseRequest.create({ scope, revision: String(revision) }), this.client.rpcOptions());
      return releaseSnapshot(response.response);
    });
  }

  async watch(scope: ConfigScope, knownRevision: number, timeoutSeconds: number): Promise<ConfigWatchResult> {
    return this.wrap(async () => {
      const response = await this.inner.watch(WatchConfigRequest.create({ scope, knownRevision: String(knownRevision), timeoutSeconds: String(timeoutSeconds) }), this.client.rpcOptions());
      return {
        changed: response.response.changed,
        latestRevision: Number(response.response.latestRevision),
        release: response.response.release ? releaseSnapshot(response.response.release) : null
      };
    });
  }

  private async wrap<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error) {
      throw PGAppError.from(error);
    }
  }
}

export type ConfigReleaseSnapshot = {
  scope: ConfigScope | undefined;
  revision: number;
  checksum: string;
  snapshot: Record<string, JsonValue>;
  message: string;
  publishedBy: string;
  publishedAt: string;
};

export type ConfigWatchResult = {
  changed: boolean;
  latestRevision: number;
  release: ConfigReleaseSnapshot | null;
};

function releaseSnapshot(release: { scope?: ConfigScope; revision: bigint | number | string; checksum: string; snapshotJson: string; message: string; publishedBy: string; publishedAt: string }): ConfigReleaseSnapshot {
  return {
    scope: release.scope,
    revision: Number(release.revision),
    checksum: release.checksum,
    snapshot: JSON.parse(release.snapshotJson || "{}") as Record<string, JsonValue>,
    message: release.message,
    publishedBy: release.publishedBy,
    publishedAt: release.publishedAt
  };
}

function encodeBytes(value: Uint8Array | string): Uint8Array {
  if (typeof value === "string") {
    return new TextEncoder().encode(value);
  }
  return value;
}

export { QueueStorageMode, GrpcStatus };

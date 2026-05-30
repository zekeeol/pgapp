from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, TypeVar, cast

import grpc

_GENERATED_ROOT = Path(__file__).resolve().parent / "gen"
if str(_GENERATED_ROOT) not in sys.path:
    sys.path.insert(0, str(_GENERATED_ROOT))

from pgapp.v1 import cache_pb2, cache_pb2_grpc, config_pb2, config_pb2_grpc, mq_pb2, mq_pb2_grpc  # type: ignore[import-not-found]  # noqa: E402

type JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
type QueueStorageMode = Literal["durable", "transient"]

ResponseT = TypeVar("ResponseT", covariant=True)


class _UnaryUnary(Protocol[ResponseT]):
    def __call__(self, request: object, timeout: float | None = None) -> ResponseT: ...


class _OperationResult(Protocol):
    success: bool


class _GetCacheResponse(Protocol):
    hit: bool
    value: bytes


class _ExistsCacheResponse(Protocol):
    exists: bool


class _CacheItemProto(Protocol):
    key: str
    hit: bool
    value: bytes


class _MGetCacheResponse(Protocol):
    items: Sequence[_CacheItemProto]


class _NamespaceUsageProto(Protocol):
    namespace: str
    key_count: int
    byte_size: int


class _CacheStatsResponse(Protocol):
    hits: int
    misses: int
    writes: int
    deletes: int
    evictions: int
    expired_removals: int
    logical_key_count: int
    logical_byte_size: int
    namespace_usage: Sequence[_NamespaceUsageProto]


class _SendMessageResponse(Protocol):
    message_id: int


class _SendBatchResponse(Protocol):
    message_ids: Sequence[int]


class _QueueMessageProto(Protocol):
    message_id: int
    read_count: int
    enqueued_at: str
    visibility_timeout_at: str
    json_payload: str
    ack_token: str


class _ReadMessagesResponse(Protocol):
    messages: Sequence[_QueueMessageProto]


class _QueueMetricsResponse(Protocol):
    visible_message_count: int
    in_flight_message_count: int
    oldest_visible_message_age_seconds: int
    archived_message_count: int


class _ConfigScopeProto(Protocol):
    app_id: str
    environment: str
    cluster: str
    namespace: str


class _ConfigReleaseProto(Protocol):
    scope: _ConfigScopeProto
    revision: int
    checksum: str
    snapshot_json: str
    message: str
    published_by: str
    published_at: str


class _WatchConfigResponse(Protocol):
    changed: bool
    latest_revision: int
    release: _ConfigReleaseProto


@dataclass(frozen=True)
class CacheItem:
    key: str
    hit: bool
    value: bytes


@dataclass(frozen=True)
class NamespaceUsage:
    namespace: str
    key_count: int
    byte_size: int


@dataclass(frozen=True)
class CacheStats:
    hits: int
    misses: int
    writes: int
    deletes: int
    evictions: int
    expired_removals: int
    logical_key_count: int
    logical_byte_size: int
    namespace_usage: tuple[NamespaceUsage, ...]


@dataclass(frozen=True)
class MQMessage:
    message_id: int
    read_count: int
    enqueued_at: str
    visibility_timeout_at: str
    ack_token: str
    payload: JsonValue


@dataclass(frozen=True)
class QueueMetrics:
    visible_message_count: int
    in_flight_message_count: int
    oldest_visible_message_age_seconds: int
    archived_message_count: int


@dataclass(frozen=True)
class ConfigScope:
    app_id: str
    environment: str
    cluster: str
    namespace: str


@dataclass(frozen=True)
class ConfigRelease:
    scope: ConfigScope
    revision: int
    checksum: str
    snapshot: dict[str, JsonValue]
    message: str
    published_by: str
    published_at: str


@dataclass(frozen=True)
class ConfigWatchResult:
    changed: bool
    latest_revision: int
    release: ConfigRelease | None


class PGAppError(Exception):
    status_code: str | None

    def __init__(self, message: str, status_code: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class PGAppClient:
    endpoint: str
    timeout: float | None
    channel: grpc.Channel
    cache: CacheClient
    mq: MQClient
    config: ConfigClient

    def __init__(
        self,
        endpoint: str = "127.0.0.1:50051",
        timeout: float | None = None,
        channel: grpc.Channel | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self.channel = channel if channel is not None else grpc.insecure_channel(endpoint)
        self.cache = CacheClient(self)
        self.mq = MQClient(self)
        self.config = ConfigClient(self)


class CacheClient:
    client: PGAppClient
    _stub: object

    def __init__(self, client: PGAppClient) -> None:
        self.client = client
        self._stub = cache_pb2_grpc.CacheServiceStub(client.channel)

    def encode_value(self, value: bytes | str) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        raise PGAppError("cache values must be bytes or str", status_code="invalid_argument")

    def set(
        self,
        namespace: str,
        key: str,
        value: bytes | str,
        ttl_seconds: int | None = None,
    ) -> bool:
        request: object = cache_pb2.SetCacheRequest(
            namespace=namespace,
            key=key,
            value=self.encode_value(value),
            ttl_seconds=ttl_seconds or 0,
        )
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "Set")),
            request,
        )
        return bool(response.success)

    def get(self, namespace: str, key: str) -> bytes | None:
        request: object = cache_pb2.GetCacheRequest(namespace=namespace, key=key)
        response = self._call(
            cast(_UnaryUnary[_GetCacheResponse], getattr(self._stub, "Get")),
            request,
        )
        if not response.hit:
            return None
        return bytes(response.value)

    def mget(self, namespace: str, keys: Sequence[str]) -> list[CacheItem]:
        request: object = cache_pb2.MGetCacheRequest(namespace=namespace, keys=list(keys))
        response = self._call(
            cast(_UnaryUnary[_MGetCacheResponse], getattr(self._stub, "MGet")),
            request,
        )
        return [
            CacheItem(key=item.key, hit=bool(item.hit), value=bytes(item.value))
            for item in response.items
        ]

    def delete(self, namespace: str, key: str) -> bool:
        request: object = cache_pb2.DeleteCacheRequest(namespace=namespace, key=key)
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "Delete")),
            request,
        )
        return bool(response.success)

    def exists(self, namespace: str, key: str) -> bool:
        request: object = cache_pb2.ExistsCacheRequest(namespace=namespace, key=key)
        response = self._call(
            cast(_UnaryUnary[_ExistsCacheResponse], getattr(self._stub, "Exists")),
            request,
        )
        return bool(response.exists)

    def invalidate_namespace(self, namespace: str) -> bool:
        request: object = cache_pb2.InvalidateNamespaceRequest(namespace=namespace)
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "InvalidateNamespace")),
            request,
        )
        return bool(response.success)

    def stats(self) -> CacheStats:
        request: object = cache_pb2.CacheStatsRequest()
        response = self._call(
            cast(_UnaryUnary[_CacheStatsResponse], getattr(self._stub, "Stats")),
            request,
        )
        return CacheStats(
            hits=int(response.hits),
            misses=int(response.misses),
            writes=int(response.writes),
            deletes=int(response.deletes),
            evictions=int(response.evictions),
            expired_removals=int(response.expired_removals),
            logical_key_count=int(response.logical_key_count),
            logical_byte_size=int(response.logical_byte_size),
            namespace_usage=tuple(
                NamespaceUsage(
                    namespace=usage.namespace,
                    key_count=int(usage.key_count),
                    byte_size=int(usage.byte_size),
                )
                for usage in response.namespace_usage
            ),
        )

    def _call(self, method: _UnaryUnary[ResponseT], request: object) -> ResponseT:
        try:
            return method(request, timeout=self.client.timeout)
        except grpc.RpcError as exc:
            status = exc.code()
            status_code = status.name if status is not None else None
            details = exc.details() or str(exc)
            raise PGAppError(details, status_code=status_code) from exc


class MQClient:
    client: PGAppClient
    _stub: object

    def __init__(self, client: PGAppClient) -> None:
        self.client = client
        self._stub = mq_pb2_grpc.MQServiceStub(client.channel)

    def encode_json(self, payload: JsonValue) -> str:
        try:
            return json.dumps(payload, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise PGAppError(
                "payload must be JSON serializable",
                status_code="invalid_argument",
            ) from exc

    def create_queue(
        self,
        queue_name: str,
        storage_mode: QueueStorageMode = "durable",
    ) -> bool:
        request: object = mq_pb2.CreateQueueRequest(
            queue_name=queue_name,
            storage_mode=self._storage_mode_value(storage_mode),
        )
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "CreateQueue")),
            request,
        )
        return bool(response.success)

    def purge_queue(self, queue_name: str) -> bool:
        request: object = mq_pb2.PurgeQueueRequest(queue_name=queue_name)
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "PurgeQueue")),
            request,
        )
        return bool(response.success)

    def drop_queue(self, queue_name: str) -> bool:
        request: object = mq_pb2.DropQueueRequest(queue_name=queue_name)
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "DropQueue")),
            request,
        )
        return bool(response.success)

    def send_json(self, queue_name: str, payload: JsonValue, delay_seconds: int = 0) -> int:
        request: object = mq_pb2.SendMessageRequest(
            queue_name=queue_name,
            json_payload=self.encode_json(payload),
            delay_seconds=delay_seconds,
        )
        response = self._call(
            cast(_UnaryUnary[_SendMessageResponse], getattr(self._stub, "Send")),
            request,
        )
        return int(response.message_id)

    def send_batch_json(
        self,
        queue_name: str,
        payloads: Sequence[JsonValue],
        delay_seconds: int = 0,
    ) -> list[int]:
        request: object = mq_pb2.SendBatchRequest(
            queue_name=queue_name,
            json_payloads=[self.encode_json(payload) for payload in payloads],
            delay_seconds=delay_seconds,
        )
        response = self._call(
            cast(_UnaryUnary[_SendBatchResponse], getattr(self._stub, "SendBatch")),
            request,
        )
        return [int(message_id) for message_id in response.message_ids]

    def read(
        self,
        queue_name: str,
        quantity: int = 1,
        visibility_timeout_seconds: int = 30,
    ) -> list[MQMessage]:
        request: object = mq_pb2.ReadMessagesRequest(
            queue_name=queue_name,
            quantity=quantity,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        response = self._call(
            cast(_UnaryUnary[_ReadMessagesResponse], getattr(self._stub, "Read")),
            request,
        )
        return [self._message_from_proto(message) for message in response.messages]

    def read_with_poll(
        self,
        queue_name: str,
        quantity: int = 1,
        visibility_timeout_seconds: int = 30,
        max_poll_seconds: int = 10,
        poll_interval_millis: int = 100,
    ) -> list[MQMessage]:
        request: object = mq_pb2.ReadWithPollRequest(
            queue_name=queue_name,
            quantity=quantity,
            visibility_timeout_seconds=visibility_timeout_seconds,
            max_poll_seconds=max_poll_seconds,
            poll_interval_millis=poll_interval_millis,
        )
        response = self._call(
            cast(_UnaryUnary[_ReadMessagesResponse], getattr(self._stub, "ReadWithPoll")),
            request,
        )
        return [self._message_from_proto(message) for message in response.messages]

    def ack(self, queue_name: str, message_id: int, ack_token: str) -> bool:
        request: object = mq_pb2.AckMessageRequest(
            queue_name=queue_name,
            message_id=message_id,
            ack_token=ack_token,
        )
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "Ack")),
            request,
        )
        return bool(response.success)

    def archive(self, queue_name: str, message_id: int, ack_token: str) -> bool:
        request: object = mq_pb2.ArchiveMessageRequest(
            queue_name=queue_name,
            message_id=message_id,
            ack_token=ack_token,
        )
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "Archive")),
            request,
        )
        return bool(response.success)

    def set_visibility_timeout(
        self,
        queue_name: str,
        message_id: int,
        ack_token: str,
        visibility_timeout_seconds: int,
    ) -> bool:
        request: object = mq_pb2.SetVisibilityTimeoutRequest(
            queue_name=queue_name,
            message_id=message_id,
            ack_token=ack_token,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        response = self._call(
            cast(_UnaryUnary[_OperationResult], getattr(self._stub, "SetVisibilityTimeout")),
            request,
        )
        return bool(response.success)

    def metrics(self, queue_name: str) -> QueueMetrics:
        request: object = mq_pb2.QueueMetricsRequest(queue_name=queue_name)
        response = self._call(
            cast(_UnaryUnary[_QueueMetricsResponse], getattr(self._stub, "Metrics")),
            request,
        )
        return QueueMetrics(
            visible_message_count=int(response.visible_message_count),
            in_flight_message_count=int(response.in_flight_message_count),
            oldest_visible_message_age_seconds=int(
                response.oldest_visible_message_age_seconds
            ),
            archived_message_count=int(response.archived_message_count),
        )

    def _call(self, method: _UnaryUnary[ResponseT], request: object) -> ResponseT:
        try:
            return method(request, timeout=self.client.timeout)
        except grpc.RpcError as exc:
            status = exc.code()
            status_code = status.name if status is not None else None
            details = exc.details() or str(exc)
            raise PGAppError(details, status_code=status_code) from exc

    def _storage_mode_value(self, storage_mode: QueueStorageMode) -> int:
        if storage_mode == "durable":
            return int(mq_pb2.QUEUE_STORAGE_MODE_DURABLE)
        if storage_mode == "transient":
            return int(mq_pb2.QUEUE_STORAGE_MODE_TRANSIENT)
        raise PGAppError("unknown queue storage mode", status_code="invalid_argument")

    def _message_from_proto(self, message: _QueueMessageProto) -> MQMessage:
        return MQMessage(
            message_id=int(message.message_id),
            read_count=int(message.read_count),
            enqueued_at=message.enqueued_at,
            visibility_timeout_at=message.visibility_timeout_at,
            ack_token=message.ack_token,
            payload=cast(JsonValue, json.loads(message.json_payload)),
        )


class ConfigClient:
    client: PGAppClient
    _stub: object

    def __init__(self, client: PGAppClient) -> None:
        self.client = client
        self._stub = config_pb2_grpc.ConfigServiceStub(client.channel)

    def scope(
        self,
        app_id: str,
        environment: str,
        cluster: str,
        namespace: str,
    ) -> ConfigScope:
        return ConfigScope(
            app_id=app_id,
            environment=environment,
            cluster=cluster,
            namespace=namespace,
        )

    def encode_json(self, value: JsonValue) -> str:
        try:
            return json.dumps(value, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise PGAppError(
                "config value must be JSON serializable",
                status_code="invalid_argument",
            ) from exc

    def get_latest_release(self, scope: ConfigScope) -> ConfigRelease:
        return self.get_release(scope, revision=0)

    def get_release(self, scope: ConfigScope, revision: int) -> ConfigRelease:
        request: object = config_pb2.GetConfigReleaseRequest(
            scope=self._scope_to_proto(scope),
            revision=revision,
        )
        response = self._call(
            cast(_UnaryUnary[_ConfigReleaseProto], getattr(self._stub, "GetRelease")),
            request,
        )
        return self._release_from_proto(response)

    def watch(
        self,
        scope: ConfigScope,
        known_revision: int,
        timeout_seconds: int,
    ) -> ConfigWatchResult:
        request: object = config_pb2.WatchConfigRequest(
            scope=self._scope_to_proto(scope),
            known_revision=known_revision,
            timeout_seconds=timeout_seconds,
        )
        response = self._call(
            cast(_UnaryUnary[_WatchConfigResponse], getattr(self._stub, "Watch")),
            request,
        )
        release = None
        if bool(response.changed):
            release = self._release_from_proto(response.release)
        return ConfigWatchResult(
            changed=bool(response.changed),
            latest_revision=int(response.latest_revision),
            release=release,
        )

    def _call(self, method: _UnaryUnary[ResponseT], request: object) -> ResponseT:
        try:
            return method(request, timeout=self.client.timeout)
        except grpc.RpcError as exc:
            status = exc.code()
            status_code = status.name if status is not None else None
            details = exc.details() or str(exc)
            raise PGAppError(details, status_code=status_code) from exc

    def _scope_to_proto(self, scope: ConfigScope) -> object:
        return config_pb2.ConfigScope(
            app_id=scope.app_id,
            environment=scope.environment,
            cluster=scope.cluster,
            namespace=scope.namespace,
        )

    def _scope_from_proto(self, scope: _ConfigScopeProto) -> ConfigScope:
        return ConfigScope(
            app_id=scope.app_id,
            environment=scope.environment,
            cluster=scope.cluster,
            namespace=scope.namespace,
        )

    def _release_from_proto(self, release: _ConfigReleaseProto) -> ConfigRelease:
        snapshot = cast(dict[str, JsonValue], json.loads(release.snapshot_json or "{}"))
        return ConfigRelease(
            scope=self._scope_from_proto(release.scope),
            revision=int(release.revision),
            checksum=release.checksum,
            snapshot=snapshot,
            message=release.message,
            published_by=release.published_by,
            published_at=release.published_at,
        )

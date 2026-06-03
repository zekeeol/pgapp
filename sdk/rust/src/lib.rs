use pgapp_proto::pgapp::v1::{
    AckMessageRequest, AppendRequest, ArchiveMessageRequest, CacheItem, CacheStatsRequest,
    CacheStatsResponse, ConfigScope, CreateQueueRequest, DecrementRequest, DeleteCacheRequest,
    DlqMessage, DropQueueRequest, ExistsCacheRequest, GetCacheRequest, GetConfigReleaseRequest,
    GetDlqMessageRequest, GetSetRequest, IncrementRequest, InvalidateNamespaceRequest,
    ListDlqMessagesRequest, MGetCacheRequest, PrependRequest, PurgeDlqRequest, PurgeQueueRequest,
    QueueMessage, QueueMetricsRequest, QueueMetricsResponse, QueueStorageMode, ReadMessagesRequest,
    ReadMessagesResponse, ReadWithPollRequest, ReprocessDlqMessageRequest, SendBatchRequest,
    SendMessageRequest, SetCacheRequest, SetNxRequest, SetVisibilityTimeoutRequest,
    StreamReadRequest, WatchConfigRequest, cache_service_client::CacheServiceClient,
    config_service_client::ConfigServiceClient, mq_service_client::MqServiceClient,
};
use serde_json::Value;
use std::error::Error;
use std::time::Duration;
use tonic::{Request, Status, Streaming, metadata::MetadataValue, transport::Channel};

pub type SdkResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
pub struct PgAppClient {
    endpoint: String,
    timeout: Option<Duration>,
    credentials: Option<ClientCredentials>,
    channel: Channel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientCredentials {
    key: String,
    secret: String,
}

impl PgAppClient {
    pub async fn connect(endpoint: impl Into<String>) -> SdkResult<Self> {
        Self::connect_with_timeout(endpoint, None).await
    }

    pub async fn connect_with_timeout(
        endpoint: impl Into<String>,
        timeout: Option<Duration>,
    ) -> SdkResult<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
        Ok(Self {
            endpoint,
            timeout,
            credentials: None,
            channel,
        })
    }

    pub async fn connect_with_timeout_and_credentials(
        endpoint: impl Into<String>,
        timeout: Option<Duration>,
        key: impl Into<String>,
        secret: impl Into<String>,
    ) -> SdkResult<Self> {
        let endpoint = endpoint.into();
        let channel = Channel::from_shared(endpoint.clone())?.connect().await?;
        Ok(Self {
            endpoint,
            timeout,
            credentials: Some(ClientCredentials {
                key: key.into(),
                secret: secret.into(),
            }),
            channel,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn cache(&self) -> CacheClient {
        CacheClient {
            inner: CacheServiceClient::new(self.channel.clone()),
            timeout: self.timeout,
            credentials: self.credentials.clone(),
        }
    }

    pub fn mq(&self) -> MqClient {
        MqClient {
            inner: MqServiceClient::new(self.channel.clone()),
            timeout: self.timeout,
            credentials: self.credentials.clone(),
        }
    }

    pub fn config(&self) -> ConfigClient {
        ConfigClient {
            inner: ConfigServiceClient::new(self.channel.clone()),
            timeout: self.timeout,
            credentials: self.credentials.clone(),
        }
    }
}

#[derive(Clone)]
pub struct CacheClient {
    inner: CacheServiceClient<Channel>,
    timeout: Option<Duration>,
    credentials: Option<ClientCredentials>,
}

impl CacheClient {
    pub async fn set(
        &mut self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(SetCacheRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.set(request).await?.into_inner().success)
    }

    pub async fn get(&mut self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, Status> {
        let request = self.with_timeout(GetCacheRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        });
        let response = self.inner.get(request).await?.into_inner();
        Ok(response.hit.then_some(response.value))
    }

    pub async fn mget(
        &mut self,
        namespace: &str,
        keys: &[String],
    ) -> Result<Vec<CacheItem>, Status> {
        let request = self.with_timeout(MGetCacheRequest {
            namespace: namespace.to_string(),
            keys: keys.to_vec(),
        });
        Ok(self.inner.m_get(request).await?.into_inner().items)
    }

    pub async fn delete(&mut self, namespace: &str, key: &str) -> Result<bool, Status> {
        let request = self.with_timeout(DeleteCacheRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        });
        Ok(self.inner.delete(request).await?.into_inner().success)
    }

    pub async fn exists(&mut self, namespace: &str, key: &str) -> Result<bool, Status> {
        let request = self.with_timeout(ExistsCacheRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        });
        Ok(self.inner.exists(request).await?.into_inner().exists)
    }

    pub async fn invalidate_namespace(&mut self, namespace: &str) -> Result<bool, Status> {
        let request = self.with_timeout(InvalidateNamespaceRequest {
            namespace: namespace.to_string(),
        });
        Ok(self
            .inner
            .invalidate_namespace(request)
            .await?
            .into_inner()
            .success)
    }

    pub async fn stats(&mut self) -> Result<CacheStatsResponse, Status> {
        let request = self.with_timeout(CacheStatsRequest {});
        Ok(self.inner.stats(request).await?.into_inner())
    }

    pub async fn increment(
        &mut self,
        namespace: &str,
        key: &str,
        delta: i64,
        ttl_seconds: Option<i64>,
    ) -> Result<i64, Status> {
        let request = self.with_timeout(IncrementRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            delta,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.increment(request).await?.into_inner().value)
    }

    pub async fn decrement(
        &mut self,
        namespace: &str,
        key: &str,
        delta: i64,
        ttl_seconds: Option<i64>,
    ) -> Result<i64, Status> {
        let request = self.with_timeout(DecrementRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            delta,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.decrement(request).await?.into_inner().value)
    }

    pub async fn set_nx(
        &mut self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(SetNxRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.set_nx(request).await?.into_inner().created)
    }

    pub async fn get_set(
        &mut self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<Option<Vec<u8>>, Status> {
        let request = self.with_timeout(GetSetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        let response = self.inner.get_set(request).await?.into_inner();
        Ok(response.hit.then_some(response.old_value))
    }

    pub async fn append(
        &mut self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<i64, Status> {
        let request = self.with_timeout(AppendRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.append(request).await?.into_inner().length)
    }

    pub async fn prepend(
        &mut self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_seconds: Option<i64>,
    ) -> Result<i64, Status> {
        let request = self.with_timeout(PrependRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            ttl_seconds: ttl_seconds.unwrap_or_default(),
        });
        Ok(self.inner.prepend(request).await?.into_inner().length)
    }

    fn with_timeout<T>(&self, message: T) -> Request<T> {
        request_with_options(message, self.timeout, self.credentials.as_ref())
    }
}

#[derive(Clone)]
pub struct MqClient {
    inner: MqServiceClient<Channel>,
    timeout: Option<Duration>,
    credentials: Option<ClientCredentials>,
}

impl MqClient {
    pub async fn create_queue(&mut self, queue_name: &str) -> Result<bool, Status> {
        self.create_queue_with_mode(queue_name, QueueStorageMode::Durable)
            .await
    }

    pub async fn create_queue_with_mode(
        &mut self,
        queue_name: &str,
        storage_mode: QueueStorageMode,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(CreateQueueRequest {
            queue_name: queue_name.to_string(),
            storage_mode: storage_mode as i32,
        });
        Ok(self.inner.create_queue(request).await?.into_inner().success)
    }

    pub async fn send_json(&mut self, queue_name: &str, payload: &Value) -> Result<i64, Status> {
        self.send_json_with_delay(queue_name, payload, 0).await
    }

    pub async fn send_json_with_delay(
        &mut self,
        queue_name: &str,
        payload: &Value,
        delay_seconds: i64,
    ) -> Result<i64, Status> {
        let request = self.with_timeout(SendMessageRequest {
            queue_name: queue_name.to_string(),
            json_payload: payload.to_string(),
            delay_seconds,
        });
        Ok(self.inner.send(request).await?.into_inner().message_id)
    }

    pub async fn send_json_batch(
        &mut self,
        queue_name: &str,
        payloads: &[Value],
        delay_seconds: i64,
    ) -> Result<Vec<i64>, Status> {
        let request = self.with_timeout(SendBatchRequest {
            queue_name: queue_name.to_string(),
            json_payloads: payloads.iter().map(Value::to_string).collect(),
            delay_seconds,
        });
        Ok(self
            .inner
            .send_batch(request)
            .await?
            .into_inner()
            .message_ids)
    }

    pub async fn read(
        &mut self,
        queue_name: &str,
        quantity: i32,
        visibility_timeout_seconds: i64,
    ) -> Result<Vec<QueueMessage>, Status> {
        let request = self.with_timeout(ReadMessagesRequest {
            queue_name: queue_name.to_string(),
            quantity,
            visibility_timeout_seconds,
        });
        Ok(self.inner.read(request).await?.into_inner().messages)
    }

    pub async fn read_with_poll(
        &mut self,
        queue_name: &str,
        quantity: i32,
        visibility_timeout_seconds: i64,
        max_poll_seconds: i64,
        poll_interval_millis: i64,
    ) -> Result<Vec<QueueMessage>, Status> {
        let request = self.with_timeout(ReadWithPollRequest {
            queue_name: queue_name.to_string(),
            quantity,
            visibility_timeout_seconds,
            max_poll_seconds,
            poll_interval_millis,
        });
        Ok(self
            .inner
            .read_with_poll(request)
            .await?
            .into_inner()
            .messages)
    }

    pub async fn ack(
        &mut self,
        queue_name: &str,
        message_id: i64,
        ack_token: &str,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(AckMessageRequest {
            queue_name: queue_name.to_string(),
            message_id,
            ack_token: ack_token.to_string(),
        });
        Ok(self.inner.ack(request).await?.into_inner().success)
    }

    pub async fn archive(
        &mut self,
        queue_name: &str,
        message_id: i64,
        ack_token: &str,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(ArchiveMessageRequest {
            queue_name: queue_name.to_string(),
            message_id,
            ack_token: ack_token.to_string(),
        });
        Ok(self.inner.archive(request).await?.into_inner().success)
    }

    pub async fn set_visibility_timeout(
        &mut self,
        queue_name: &str,
        message_id: i64,
        ack_token: &str,
        visibility_timeout_seconds: i64,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(SetVisibilityTimeoutRequest {
            queue_name: queue_name.to_string(),
            message_id,
            visibility_timeout_seconds,
            ack_token: ack_token.to_string(),
        });
        Ok(self
            .inner
            .set_visibility_timeout(request)
            .await?
            .into_inner()
            .success)
    }

    pub async fn metrics(&mut self, queue_name: &str) -> Result<QueueMetricsResponse, Status> {
        let request = self.with_timeout(QueueMetricsRequest {
            queue_name: queue_name.to_string(),
        });
        Ok(self.inner.metrics(request).await?.into_inner())
    }

    pub async fn purge_queue(&mut self, queue_name: &str) -> Result<bool, Status> {
        let request = self.with_timeout(PurgeQueueRequest {
            queue_name: queue_name.to_string(),
        });
        Ok(self.inner.purge_queue(request).await?.into_inner().success)
    }

    pub async fn drop_queue(&mut self, queue_name: &str) -> Result<bool, Status> {
        let request = self.with_timeout(DropQueueRequest {
            queue_name: queue_name.to_string(),
        });
        Ok(self.inner.drop_queue(request).await?.into_inner().success)
    }

    pub async fn list_dlq_messages(
        &mut self,
        queue_name: &str,
        limit: i32,
        offset: i64,
    ) -> Result<Vec<DlqMessage>, Status> {
        let request = self.with_timeout(ListDlqMessagesRequest {
            queue_name: queue_name.to_string(),
            limit,
            offset,
        });
        Ok(self
            .inner
            .list_dlq_messages(request)
            .await?
            .into_inner()
            .messages)
    }

    pub async fn get_dlq_message(
        &mut self,
        queue_name: &str,
        original_message_id: i64,
    ) -> Result<DlqMessage, Status> {
        let request = self.with_timeout(GetDlqMessageRequest {
            queue_name: queue_name.to_string(),
            original_message_id,
        });
        Ok(self.inner.get_dlq_message(request).await?.into_inner())
    }

    pub async fn reprocess_dlq_message(
        &mut self,
        queue_name: &str,
        original_message_id: i64,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(ReprocessDlqMessageRequest {
            queue_name: queue_name.to_string(),
            original_message_id,
        });
        Ok(self
            .inner
            .reprocess_dlq_message(request)
            .await?
            .into_inner()
            .success)
    }

    pub async fn purge_dlq(&mut self, queue_name: &str) -> Result<bool, Status> {
        let request = self.with_timeout(PurgeDlqRequest {
            queue_name: queue_name.to_string(),
        });
        Ok(self.inner.purge_dlq(request).await?.into_inner().success)
    }

    pub async fn stream_read(
        &mut self,
        queue_name: &str,
        quantity: i32,
        visibility_timeout_seconds: i64,
    ) -> Result<Streaming<ReadMessagesResponse>, Status> {
        let request = self.with_timeout(StreamReadRequest {
            queue_name: queue_name.to_string(),
            quantity,
            visibility_timeout_seconds,
        });
        Ok(self.inner.stream_read(request).await?.into_inner())
    }

    fn with_timeout<T>(&self, message: T) -> Request<T> {
        request_with_options(message, self.timeout, self.credentials.as_ref())
    }
}

#[derive(Clone)]
pub struct ConfigClient {
    inner: ConfigServiceClient<Channel>,
    timeout: Option<Duration>,
    credentials: Option<ClientCredentials>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigReleaseSnapshot {
    pub scope: ConfigScope,
    pub revision: i64,
    pub checksum: String,
    pub snapshot: Value,
    pub message: String,
    pub published_by: String,
    pub published_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigWatchResult {
    pub changed: bool,
    pub latest_revision: i64,
    pub release: Option<ConfigReleaseSnapshot>,
}

impl ConfigClient {
    pub fn scope(
        app_id: impl Into<String>,
        environment: impl Into<String>,
        cluster: impl Into<String>,
        namespace: impl Into<String>,
    ) -> ConfigScope {
        ConfigScope {
            app_id: app_id.into(),
            environment: environment.into(),
            cluster: cluster.into(),
            namespace: namespace.into(),
        }
    }

    pub async fn get_latest_release(
        &mut self,
        scope: ConfigScope,
    ) -> Result<ConfigReleaseSnapshot, Status> {
        self.get_release(scope, 0).await
    }

    pub async fn get_release(
        &mut self,
        scope: ConfigScope,
        revision: i64,
    ) -> Result<ConfigReleaseSnapshot, Status> {
        let request = self.with_timeout(GetConfigReleaseRequest {
            scope: Some(scope),
            revision,
        });
        let release = self.inner.get_release(request).await?.into_inner();
        proto_release_to_snapshot(release)
    }

    pub async fn watch(
        &mut self,
        scope: ConfigScope,
        known_revision: i64,
        timeout_seconds: i64,
    ) -> Result<ConfigWatchResult, Status> {
        let request = self.with_timeout(WatchConfigRequest {
            scope: Some(scope),
            known_revision,
            timeout_seconds,
        });
        let response = self.inner.watch(request).await?.into_inner();
        Ok(ConfigWatchResult {
            changed: response.changed,
            latest_revision: response.latest_revision,
            release: response
                .release
                .map(proto_release_to_snapshot)
                .transpose()?,
        })
    }

    fn with_timeout<T>(&self, message: T) -> Request<T> {
        request_with_options(message, self.timeout, self.credentials.as_ref())
    }
}

fn request_with_options<T>(
    message: T,
    timeout: Option<Duration>,
    credentials: Option<&ClientCredentials>,
) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(timeout) = timeout {
        request.set_timeout(timeout);
    }
    if let Some(credentials) = credentials {
        request.metadata_mut().insert(
            "x-pgapp-key",
            MetadataValue::try_from(credentials.key.as_str())
                .expect("pgapp credential key must be valid gRPC metadata"),
        );
        request.metadata_mut().insert(
            "x-pgapp-secret",
            MetadataValue::try_from(credentials.secret.as_str())
                .expect("pgapp credential secret must be valid gRPC metadata"),
        );
    }
    request
}

fn proto_release_to_snapshot(
    release: pgapp_proto::pgapp::v1::ConfigRelease,
) -> Result<ConfigReleaseSnapshot, Status> {
    let scope = release
        .scope
        .ok_or_else(|| Status::internal("config release missing scope"))?;
    let snapshot = serde_json::from_str(&release.snapshot_json)
        .map_err(|err| Status::internal(format!("invalid release snapshot JSON: {err}")))?;
    Ok(ConfigReleaseSnapshot {
        scope,
        revision: release.revision,
        checksum: release.checksum,
        snapshot,
        message: release.message,
        published_by: release.published_by,
        published_at: release.published_at,
    })
}

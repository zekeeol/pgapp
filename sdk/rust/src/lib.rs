use pgapp_proto::pgapp::v1::{
    ArchiveMessageRequest, CacheItem, CacheStatsRequest, CacheStatsResponse, CreateQueueRequest,
    DeleteCacheRequest, DeleteMessageRequest, DropQueueRequest, ExistsCacheRequest,
    GetCacheRequest, InvalidateNamespaceRequest, MGetCacheRequest, PurgeQueueRequest, QueueMessage,
    QueueMetricsRequest, QueueMetricsResponse, QueueStorageMode, ReadMessagesRequest,
    ReadWithPollRequest, SendBatchRequest, SendMessageRequest, SetCacheRequest,
    SetVisibilityTimeoutRequest, cache_service_client::CacheServiceClient,
    mq_service_client::MqServiceClient,
};
use serde_json::Value;
use std::error::Error;
use std::time::Duration;
use tonic::{Request, Status, transport::Channel};

pub type SdkResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
pub struct PgAppClient {
    endpoint: String,
    timeout: Option<Duration>,
    channel: Channel,
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
        }
    }

    pub fn mq(&self) -> MqClient {
        MqClient {
            inner: MqServiceClient::new(self.channel.clone()),
            timeout: self.timeout,
        }
    }
}

#[derive(Clone)]
pub struct CacheClient {
    inner: CacheServiceClient<Channel>,
    timeout: Option<Duration>,
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

    fn with_timeout<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        request
    }
}

#[derive(Clone)]
pub struct MqClient {
    inner: MqServiceClient<Channel>,
    timeout: Option<Duration>,
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

    pub async fn delete(&mut self, queue_name: &str, message_id: i64) -> Result<bool, Status> {
        let request = self.with_timeout(DeleteMessageRequest {
            queue_name: queue_name.to_string(),
            message_id,
        });
        Ok(self.inner.delete(request).await?.into_inner().success)
    }

    pub async fn archive(&mut self, queue_name: &str, message_id: i64) -> Result<bool, Status> {
        let request = self.with_timeout(ArchiveMessageRequest {
            queue_name: queue_name.to_string(),
            message_id,
        });
        Ok(self.inner.archive(request).await?.into_inner().success)
    }

    pub async fn set_visibility_timeout(
        &mut self,
        queue_name: &str,
        message_id: i64,
        visibility_timeout_seconds: i64,
    ) -> Result<bool, Status> {
        let request = self.with_timeout(SetVisibilityTimeoutRequest {
            queue_name: queue_name.to_string(),
            message_id,
            visibility_timeout_seconds,
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

    fn with_timeout<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        request
    }
}

use pgapp_core::{
    PgAppError,
    admin::AdminStore,
    cache::CacheStore,
    config::{RequestLimits, ServerConfig},
    config_center::{
        ConfigLimits, ConfigRelease as CoreConfigRelease, ConfigScope as CoreConfigScope,
        ConfigStore,
    },
    db,
    metrics::MetricsRegistry,
    mq::{MqLimits, MqStore, QueueStorageMode},
};
use pgapp_proto::pgapp::v1::{
    AckMessageRequest, CacheItem, CacheStatsRequest, CacheStatsResponse, ConfigDraftResponse,
    ConfigItem, ConfigRelease, ConfigScope, ConfigScopeSummary, CreateQueueRequest,
    DeleteCacheRequest, DeleteConfigItemRequest, DropQueueRequest, ExistsCacheRequest,
    ExistsCacheResponse, GetCacheRequest, GetCacheResponse, GetConfigDraftRequest,
    GetConfigReleaseRequest, HealthRequest, HealthResponse, InvalidateNamespaceRequest,
    ListConfigReleasesRequest, ListConfigReleasesResponse, ListConfigScopesRequest,
    ListConfigScopesResponse, MGetCacheRequest, MGetCacheResponse, MethodMetric, NamespaceUsage,
    OperationResult, PgPoolMetrics, PublishConfigRequest, PurgeQueueRequest, QueueMessage,
    QueueMetricsRequest, QueueMetricsResponse, QueueStorageMode as ProtoStorageMode,
    ReadMessagesRequest, ReadMessagesResponse, ReadinessRequest, ReadinessResponse,
    RuntimeMetricsRequest, RuntimeMetricsResponse, SendBatchRequest, SendBatchResponse,
    SendMessageRequest, SendMessageResponse, ServiceCapability, ServiceState, SetCacheRequest,
    SetVisibilityTimeoutRequest, UpsertConfigItemRequest, WatchConfigRequest, WatchConfigResponse,
    cache_service_server::{CacheService, CacheServiceServer},
    config_service_server::{ConfigService, ConfigServiceServer},
    health_service_server::{HealthService, HealthServiceServer},
    mq_service_server::{MqService, MqServiceServer},
};
use std::{
    future::Future,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tonic::{Code, Request, Response, Status, transport::Server};

mod admin_http;

#[derive(Clone)]
struct CacheGrpc {
    store: CacheStore,
    enabled: bool,
    limits: RequestLimits,
    metrics: MetricsRegistry,
    request_timeout: Duration,
}

#[derive(Clone)]
struct MqGrpc {
    store: MqStore,
    enabled: bool,
    metrics: MetricsRegistry,
    request_timeout: Duration,
}

#[derive(Clone)]
struct ConfigGrpc {
    store: ConfigStore,
    enabled: bool,
    metrics: MetricsRegistry,
    request_timeout: Duration,
}

#[derive(Clone)]
struct HealthGrpc {
    pool: sqlx::PgPool,
    cache_enabled: bool,
    mq_enabled: bool,
    config_enabled: bool,
    metrics: MetricsRegistry,
    request_timeout: Duration,
}

pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = ServerConfig::from_env()?;
    let pool = db::connect(&cfg.database_url, cfg.min_connections, cfg.max_connections).await?;
    db::apply_schema(&pool).await?;
    serve(cfg.bind_addr, pool, cfg).await
}

pub async fn serve(
    addr: SocketAddr,
    pool: sqlx::PgPool,
    cfg: ServerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metrics = MetricsRegistry::default();
    let request_limits = cfg.limits.clone();
    let request_timeout = cfg.default_request_timeout;
    let cache_store = CacheStore::new(pool.clone(), cfg.cache_limits.clone());
    let config_store = ConfigStore::new(
        pool.clone(),
        ConfigLimits {
            max_watch_seconds: cfg.limits.max_config_watch_seconds,
            max_payload_bytes: cfg.limits.max_payload_bytes,
            max_page_size: cfg.admin.max_page_size,
        },
    );
    let mq_store = MqStore::with_limits(
        pool.clone(),
        cfg.transient_queues_enabled,
        MqLimits {
            max_batch_size: cfg.limits.max_batch_size,
            max_payload_bytes: cfg.limits.max_payload_bytes,
            max_visibility_timeout_seconds: cfg.limits.max_visibility_timeout_seconds,
        },
    );
    let cache = CacheGrpc {
        store: cache_store.clone(),
        enabled: cfg.services.cache,
        limits: request_limits.clone(),
        metrics: metrics.clone(),
        request_timeout,
    };
    let mq = MqGrpc {
        store: mq_store,
        enabled: cfg.services.mq,
        metrics: metrics.clone(),
        request_timeout,
    };
    let config = ConfigGrpc {
        store: config_store.clone(),
        enabled: cfg.services.config,
        metrics: metrics.clone(),
        request_timeout,
    };
    let health = HealthGrpc {
        pool: pool.clone(),
        cache_enabled: cfg.services.cache,
        mq_enabled: cfg.services.mq,
        config_enabled: cfg.services.config,
        metrics: metrics.clone(),
        request_timeout,
    };

    let router = Server::builder()
        .add_service(HealthServiceServer::new(health))
        .add_service(CacheServiceServer::new(cache))
        .add_service(MqServiceServer::new(mq))
        .add_service(ConfigServiceServer::new(config));

    tracing::info!(%addr, "starting pgapp server");
    let grpc = async move {
        router
            .serve(addr)
            .await
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })
    };
    if cfg.admin.enabled {
        let admin_state = admin_http::AdminHttpState {
            pool: pool.clone(),
            cache_store,
            config_store,
            admin_store: AdminStore::new(pool, cfg.admin.max_page_size),
            metrics,
            cache_enabled: cfg.services.cache,
            mq_enabled: cfg.services.mq,
            config_enabled: cfg.services.config,
            token: cfg
                .admin
                .token
                .clone()
                .expect("admin token validated by config"),
        };
        let admin_addr = cfg.admin.bind_addr;
        tokio::try_join!(grpc, admin_http::serve(admin_addr, admin_state))?;
    } else {
        grpc.await?;
    }
    Ok(())
}

#[tonic::async_trait]
impl CacheService for CacheGrpc {
    async fn set(
        &self,
        request: Request<SetCacheRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "set",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                validate_payload_bytes(req.value.len(), self.limits.max_payload_bytes)?;
                let ttl = if req.ttl_seconds > 0 {
                    Some(req.ttl_seconds)
                } else if req.ttl_seconds == 0 {
                    None
                } else {
                    return Err(PgAppError::InvalidArgument(
                        "ttl_seconds must not be negative".to_string(),
                    )
                    .into());
                };
                self.store
                    .set(&req.namespace, &req.key, &req.value, ttl)
                    .await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn get(
        &self,
        request: Request<GetCacheRequest>,
    ) -> Result<Response<GetCacheResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "get",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let value = self.store.get(&req.namespace, &req.key).await?;
                Ok(Response::new(match value {
                    Some(value) => GetCacheResponse { hit: true, value },
                    None => GetCacheResponse {
                        hit: false,
                        value: Vec::new(),
                    },
                }))
            },
        )
        .await
    }

    async fn m_get(
        &self,
        request: Request<MGetCacheRequest>,
    ) -> Result<Response<MGetCacheResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "m_get",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                validate_batch_len(req.keys.len(), self.limits.max_batch_size)?;
                let items = self
                    .store
                    .mget(&req.namespace, &req.keys)
                    .await?
                    .into_iter()
                    .map(|(key, value)| CacheItem {
                        key,
                        hit: value.is_some(),
                        value: value.unwrap_or_default(),
                    })
                    .collect();
                Ok(Response::new(MGetCacheResponse { items }))
            },
        )
        .await
    }

    async fn delete(
        &self,
        request: Request<DeleteCacheRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "delete",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let success = self.store.delete(&req.namespace, &req.key).await?;
                Ok(Response::new(OperationResult { success }))
            },
        )
        .await
    }

    async fn exists(
        &self,
        request: Request<ExistsCacheRequest>,
    ) -> Result<Response<ExistsCacheResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "exists",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let exists = self.store.exists(&req.namespace, &req.key).await?;
                Ok(Response::new(ExistsCacheResponse { exists }))
            },
        )
        .await
    }

    async fn invalidate_namespace(
        &self,
        request: Request<InvalidateNamespaceRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "invalidate_namespace",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                self.store.invalidate_namespace(&req.namespace).await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn stats(
        &self,
        _request: Request<CacheStatsRequest>,
    ) -> Result<Response<CacheStatsResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "cache",
            "stats",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let stats = self.store.stats().await?;
                Ok(Response::new(CacheStatsResponse {
                    hits: stats.hits,
                    misses: stats.misses,
                    writes: stats.writes,
                    deletes: stats.deletes,
                    evictions: stats.evictions,
                    expired_removals: stats.expired_removals,
                    logical_key_count: stats.logical_key_count,
                    logical_byte_size: stats.logical_byte_size,
                    namespace_usage: stats
                        .namespace_usage
                        .into_iter()
                        .map(|(namespace, usage)| NamespaceUsage {
                            namespace,
                            key_count: usage.key_count,
                            byte_size: usage.byte_size,
                        })
                        .collect(),
                }))
            },
        )
        .await
    }
}

impl CacheGrpc {
    fn ensure_enabled(&self) -> Result<(), Status> {
        if self.enabled {
            Ok(())
        } else {
            Err(Status::unavailable("cache service is disabled"))
        }
    }
}

#[tonic::async_trait]
impl MqService for MqGrpc {
    async fn create_queue(
        &self,
        request: Request<CreateQueueRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "create_queue",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let mode = match ProtoStorageMode::try_from(req.storage_mode) {
                    Ok(ProtoStorageMode::Transient) => QueueStorageMode::Transient,
                    _ => QueueStorageMode::Durable,
                };
                self.store.create_queue(&req.queue_name, mode).await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn purge_queue(
        &self,
        request: Request<PurgeQueueRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "purge_queue",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                self.store.purge_queue(&req.queue_name).await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn drop_queue(
        &self,
        request: Request<DropQueueRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "drop_queue",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                self.store.drop_queue(&req.queue_name).await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn send(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "send",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let message_id = self
                    .store
                    .send(&req.queue_name, &req.json_payload, req.delay_seconds)
                    .await?;
                Ok(Response::new(SendMessageResponse { message_id }))
            },
        )
        .await
    }

    async fn send_batch(
        &self,
        request: Request<SendBatchRequest>,
    ) -> Result<Response<SendBatchResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "send_batch",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let message_ids = self
                    .store
                    .send_batch(&req.queue_name, &req.json_payloads, req.delay_seconds)
                    .await?;
                Ok(Response::new(SendBatchResponse { message_ids }))
            },
        )
        .await
    }

    async fn read(
        &self,
        request: Request<ReadMessagesRequest>,
    ) -> Result<Response<ReadMessagesResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "read",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let messages = self
                    .store
                    .read(
                        &req.queue_name,
                        req.quantity,
                        req.visibility_timeout_seconds,
                    )
                    .await?;
                Ok(Response::new(ReadMessagesResponse {
                    messages: messages.into_iter().map(to_proto_message).collect(),
                }))
            },
        )
        .await
    }

    async fn read_with_poll(
        &self,
        request: Request<pgapp_proto::pgapp::v1::ReadWithPollRequest>,
    ) -> Result<Response<ReadMessagesResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "read_with_poll",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let messages = self
                    .store
                    .read_with_poll(
                        &req.queue_name,
                        req.quantity,
                        req.visibility_timeout_seconds,
                        req.max_poll_seconds,
                        req.poll_interval_millis,
                    )
                    .await?;
                Ok(Response::new(ReadMessagesResponse {
                    messages: messages.into_iter().map(to_proto_message).collect(),
                }))
            },
        )
        .await
    }

    async fn ack(
        &self,
        request: Request<AckMessageRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "ack",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let success = self
                    .store
                    .ack(&req.queue_name, req.message_id, &req.ack_token)
                    .await?;
                Ok(Response::new(OperationResult { success }))
            },
        )
        .await
    }

    async fn archive(
        &self,
        request: Request<pgapp_proto::pgapp::v1::ArchiveMessageRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "archive",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let success = self
                    .store
                    .archive(&req.queue_name, req.message_id, &req.ack_token)
                    .await?;
                Ok(Response::new(OperationResult { success }))
            },
        )
        .await
    }

    async fn set_visibility_timeout(
        &self,
        request: Request<SetVisibilityTimeoutRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "set_visibility_timeout",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let success = self
                    .store
                    .set_visibility_timeout(
                        &req.queue_name,
                        req.message_id,
                        &req.ack_token,
                        req.visibility_timeout_seconds,
                    )
                    .await?;
                Ok(Response::new(OperationResult { success }))
            },
        )
        .await
    }

    async fn metrics(
        &self,
        request: Request<QueueMetricsRequest>,
    ) -> Result<Response<QueueMetricsResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "mq",
            "metrics",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let metrics = self.store.metrics(&req.queue_name).await?;
                Ok(Response::new(QueueMetricsResponse {
                    visible_message_count: metrics.visible_message_count,
                    in_flight_message_count: metrics.in_flight_message_count,
                    oldest_visible_message_age_seconds: metrics.oldest_visible_message_age_seconds,
                    archived_message_count: metrics.archived_message_count,
                }))
            },
        )
        .await
    }
}

impl MqGrpc {
    fn ensure_enabled(&self) -> Result<(), Status> {
        if self.enabled {
            Ok(())
        } else {
            Err(Status::unavailable("mq service is disabled"))
        }
    }
}

#[tonic::async_trait]
impl ConfigService for ConfigGrpc {
    async fn list_scopes(
        &self,
        request: Request<ListConfigScopesRequest>,
    ) -> Result<Response<ListConfigScopesResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "list_scopes",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let page = self
                    .store
                    .list_scopes(nonzero_limit(req.limit), req.offset)
                    .await?;
                Ok(Response::new(ListConfigScopesResponse {
                    scopes: page
                        .items
                        .into_iter()
                        .map(|summary| ConfigScopeSummary {
                            scope: Some(to_proto_scope(summary.scope)),
                            current_revision: summary.current_revision,
                            created_at: summary.created_at.to_rfc3339(),
                            updated_at: summary.updated_at.to_rfc3339(),
                        })
                        .collect(),
                    next_offset: page.next_offset.unwrap_or_default(),
                }))
            },
        )
        .await
    }

    async fn upsert_item(
        &self,
        request: Request<UpsertConfigItemRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "upsert_item",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let value = parse_config_json(&req.json_value)?;
                self.store.upsert_item(&scope, &req.key, value).await?;
                Ok(Response::new(OperationResult { success: true }))
            },
        )
        .await
    }

    async fn delete_item(
        &self,
        request: Request<DeleteConfigItemRequest>,
    ) -> Result<Response<OperationResult>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "delete_item",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let success = self.store.delete_item(&scope, &req.key).await?;
                Ok(Response::new(OperationResult { success }))
            },
        )
        .await
    }

    async fn get_draft(
        &self,
        request: Request<GetConfigDraftRequest>,
    ) -> Result<Response<ConfigDraftResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "get_draft",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let items = self.store.get_draft(&scope).await?;
                Ok(Response::new(ConfigDraftResponse {
                    scope: Some(to_proto_scope(scope)),
                    items: items
                        .into_iter()
                        .map(|item| ConfigItem {
                            key: item.key,
                            json_value: item.value.to_string(),
                            deleted: item.deleted,
                            updated_at: item.updated_at.to_rfc3339(),
                        })
                        .collect(),
                }))
            },
        )
        .await
    }

    async fn publish(
        &self,
        request: Request<PublishConfigRequest>,
    ) -> Result<Response<ConfigRelease>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "publish",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let release = self
                    .store
                    .publish(&scope, &req.message, &req.published_by)
                    .await?;
                Ok(Response::new(to_proto_release(release)))
            },
        )
        .await
    }

    async fn get_release(
        &self,
        request: Request<GetConfigReleaseRequest>,
    ) -> Result<Response<ConfigRelease>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "get_release",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let release = self.store.get_release(&scope, req.revision).await?;
                Ok(Response::new(to_proto_release(release)))
            },
        )
        .await
    }

    async fn list_releases(
        &self,
        request: Request<ListConfigReleasesRequest>,
    ) -> Result<Response<ListConfigReleasesResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "list_releases",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let page = self
                    .store
                    .list_releases(&scope, nonzero_limit(req.limit), req.offset)
                    .await?;
                Ok(Response::new(ListConfigReleasesResponse {
                    releases: page.items.into_iter().map(to_proto_release).collect(),
                    next_offset: page.next_offset.unwrap_or_default(),
                }))
            },
        )
        .await
    }

    async fn watch(
        &self,
        request: Request<WatchConfigRequest>,
    ) -> Result<Response<WatchConfigResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "config",
            "watch",
            self.request_timeout,
            async {
                self.ensure_enabled()?;
                let req = request.into_inner();
                let scope = required_scope(req.scope)?;
                let result = self
                    .store
                    .watch(&scope, req.known_revision, req.timeout_seconds, 100)
                    .await?;
                Ok(Response::new(WatchConfigResponse {
                    changed: result.changed,
                    latest_revision: result.latest_revision,
                    release: result.release.map(to_proto_release),
                }))
            },
        )
        .await
    }
}

impl ConfigGrpc {
    fn ensure_enabled(&self) -> Result<(), Status> {
        if self.enabled {
            Ok(())
        } else {
            Err(Status::unavailable("config service is disabled"))
        }
    }
}

#[tonic::async_trait]
impl HealthService for HealthGrpc {
    async fn get_health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "health",
            "get_health",
            self.request_timeout,
            async { Ok(Response::new(HealthResponse { live: true })) },
        )
        .await
    }

    async fn get_readiness(
        &self,
        _request: Request<ReadinessRequest>,
    ) -> Result<Response<ReadinessResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "health",
            "get_readiness",
            self.request_timeout,
            async {
                let mut capabilities = Vec::new();
                if self.cache_enabled {
                    let status = db::check_cache_schema(&self.pool).await;
                    capabilities.push(to_capability(status));
                } else {
                    capabilities.push(ServiceCapability {
                        name: "cache".to_string(),
                        state: ServiceState::Disabled as i32,
                        message: "disabled".to_string(),
                    });
                }
                if self.mq_enabled {
                    let status = db::check_mq_schema(&self.pool).await;
                    capabilities.push(to_capability(status));
                } else {
                    capabilities.push(ServiceCapability {
                        name: "mq".to_string(),
                        state: ServiceState::Disabled as i32,
                        message: "disabled".to_string(),
                    });
                }
                if self.config_enabled {
                    let status = db::check_config_schema(&self.pool).await;
                    capabilities.push(to_capability(status));
                } else {
                    capabilities.push(ServiceCapability {
                        name: "config".to_string(),
                        state: ServiceState::Disabled as i32,
                        message: "disabled".to_string(),
                    });
                }
                let ready = capabilities
                    .iter()
                    .filter(|capability| capability.state != ServiceState::Disabled as i32)
                    .all(|capability| capability.state == ServiceState::Available as i32);
                Ok(Response::new(ReadinessResponse {
                    ready,
                    capabilities,
                }))
            },
        )
        .await
    }

    async fn get_runtime_metrics(
        &self,
        _request: Request<RuntimeMetricsRequest>,
    ) -> Result<Response<RuntimeMetricsResponse>, Status> {
        record_rpc(
            self.metrics.clone(),
            "health",
            "get_runtime_metrics",
            self.request_timeout,
            async {
                let mut methods = self
                    .metrics
                    .snapshot()
                    .into_iter()
                    .filter_map(|(key, metric)| {
                        let mut parts = key.splitn(3, '.');
                        Some(MethodMetric {
                            service: parts.next()?.to_string(),
                            method: parts.next()?.to_string(),
                            status: parts.next()?.to_string(),
                            count: metric.count,
                            errors: metric.errors,
                            total_latency_millis: u128_to_u64(metric.total_latency_millis),
                        })
                    })
                    .collect::<Vec<_>>();
                methods.sort_by(|left, right| {
                    (&left.service, &left.method, &left.status).cmp(&(
                        &right.service,
                        &right.method,
                        &right.status,
                    ))
                });

                Ok(Response::new(RuntimeMetricsResponse {
                    methods,
                    pg_pool: Some(PgPoolMetrics {
                        size: self.pool.size(),
                        idle: self.pool.num_idle() as u32,
                    }),
                }))
            },
        )
        .await
    }
}

fn to_capability(status: pgapp_core::db::CapabilityStatus) -> ServiceCapability {
    ServiceCapability {
        name: status.name.to_string(),
        state: if status.available {
            ServiceState::Available as i32
        } else {
            ServiceState::Unavailable as i32
        },
        message: status.message,
    }
}

fn to_proto_message(message: pgapp_core::mq::QueueMessage) -> QueueMessage {
    QueueMessage {
        message_id: message.id,
        read_count: message.read_count,
        enqueued_at: message.enqueued_at.to_rfc3339(),
        visibility_timeout_at: message
            .visibility_timeout_at
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_default(),
        json_payload: message.payload.to_string(),
        ack_token: message.ack_token,
    }
}

fn required_scope(scope: Option<ConfigScope>) -> Result<CoreConfigScope, Status> {
    let scope = scope.ok_or_else(|| Status::invalid_argument("scope is required"))?;
    Ok(CoreConfigScope {
        app_id: scope.app_id,
        environment: scope.environment,
        cluster: scope.cluster,
        namespace: scope.namespace,
    })
}

fn to_proto_scope(scope: CoreConfigScope) -> ConfigScope {
    ConfigScope {
        app_id: scope.app_id,
        environment: scope.environment,
        cluster: scope.cluster,
        namespace: scope.namespace,
    }
}

fn to_proto_release(release: CoreConfigRelease) -> ConfigRelease {
    ConfigRelease {
        scope: Some(to_proto_scope(release.scope)),
        revision: release.revision,
        checksum: release.checksum,
        snapshot_json: release.snapshot.to_string(),
        message: release.message,
        published_by: release.published_by,
        published_at: release.published_at.to_rfc3339(),
    }
}

fn parse_config_json(payload: &str) -> Result<serde_json::Value, Status> {
    serde_json::from_str(payload)
        .map_err(|err| PgAppError::InvalidArgument(format!("invalid JSON payload: {err}")).into())
}

fn nonzero_limit(limit: i32) -> Option<i64> {
    (limit > 0).then_some(limit as i64)
}

async fn record_rpc<T, Fut>(
    metrics: MetricsRegistry,
    service: &'static str,
    method: &'static str,
    request_timeout: Duration,
    future: Fut,
) -> Result<Response<T>, Status>
where
    Fut: Future<Output = Result<Response<T>, Status>>,
{
    let start = Instant::now();
    let result = match tokio::time::timeout(request_timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(Status::deadline_exceeded("request timed out")),
    };
    let status = match &result {
        Ok(_) => "ok",
        Err(err) => status_label(err.code()),
    };
    metrics.record(service, method, status, start.elapsed());
    result
}

fn status_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

fn validate_payload_bytes(len: usize, max: usize) -> Result<(), Status> {
    if len > max {
        return Err(PgAppError::InvalidArgument(format!("payload exceeds {max} bytes")).into());
    }
    Ok(())
}

fn validate_batch_len(len: usize, max: i32) -> Result<(), Status> {
    if len > max as usize {
        return Err(PgAppError::InvalidArgument(format!(
            "batch size must be less than or equal to {max}"
        ))
        .into());
    }
    Ok(())
}

fn u128_to_u64(value: u128) -> u64 {
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}

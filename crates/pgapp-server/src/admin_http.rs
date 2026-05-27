use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use pgapp_core::{
    PgAppError,
    admin::{AdminLogInput, AdminStore, ListQuery, LogFilter},
    cache::CacheStore,
    db,
    metrics::MetricsRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Row;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct AdminHttpState {
    pub(crate) pool: sqlx::PgPool,
    pub(crate) cache_store: CacheStore,
    pub(crate) admin_store: AdminStore,
    pub(crate) metrics: MetricsRegistry,
    pub(crate) cache_enabled: bool,
    pub(crate) mq_enabled: bool,
    pub(crate) token: String,
}

#[derive(Debug, Deserialize)]
struct ListParams {
    limit: Option<i64>,
    offset: Option<i64>,
    namespace: Option<String>,
    level: Option<String>,
    text: Option<String>,
    target: Option<String>,
}

#[derive(Debug, Serialize)]
struct AdminErrorBody {
    code: &'static str,
    message: String,
    request_id: String,
}

#[derive(Debug, Serialize)]
struct OverviewResponse {
    server_state: &'static str,
    ready: bool,
    capabilities: Vec<CapabilityDto>,
    runtime_metrics: RuntimeMetricsDto,
    pg_pool: PgPoolDto,
    cache_summary: CacheSummaryDto,
    mq_summary: MqSummaryDto,
}

#[derive(Debug, Serialize)]
struct CapabilityDto {
    name: &'static str,
    state: &'static str,
    message: String,
}

#[derive(Debug, Serialize)]
struct RuntimeMetricsDto {
    methods: Vec<MethodMetricDto>,
}

#[derive(Debug, Serialize)]
struct MethodMetricDto {
    service: String,
    method: String,
    status: String,
    count: u64,
    errors: u64,
    total_latency_millis: u64,
}

#[derive(Debug, Serialize)]
struct PgPoolDto {
    size: u32,
    idle: u32,
}

#[derive(Debug, Serialize)]
struct CacheSummaryDto {
    hits: i64,
    misses: i64,
    writes: i64,
    deletes: i64,
    evictions: i64,
    expired_removals: i64,
    logical_key_count: i64,
    logical_byte_size: i64,
}

#[derive(Debug, Serialize)]
struct MqSummaryDto {
    queue_count: i64,
    visible_message_count: i64,
    in_flight_message_count: i64,
    archived_message_count: i64,
}

#[derive(Debug, Serialize)]
struct ClientActivityResponse {
    admin_sessions: Vec<AdminSessionDto>,
    api_activity: Vec<MethodMetricDto>,
}

#[derive(Debug, Serialize)]
struct AdminSessionDto {
    request_id: String,
    path: String,
    last_seen_at: DateTime<Utc>,
}

pub(crate) fn router(state: AdminHttpState) -> Router {
    Router::new()
        .route("/api/admin/overview", get(overview))
        .route("/api/admin/logs", get(logs))
        .route("/api/admin/cache/namespaces", get(cache_namespaces))
        .route("/api/admin/cache/entries", get(cache_entries))
        .route("/api/admin/mq/queues", get(mq_queues))
        .route("/api/admin/mq/queues/{queue}/messages", get(mq_messages))
        .route("/api/admin/clients", get(clients))
        .with_state(state)
}

pub(crate) async fn serve(
    addr: SocketAddr,
    state: AdminHttpState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(%addr, "starting pgapp admin http server");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn overview(State(state): State<AdminHttpState>, headers: HeaderMap) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let response = match overview_body(&state).await {
        Ok(body) => json_response(StatusCode::OK, &request_id, body),
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "admin overview unavailable",
            &request_id,
            Some(err.to_string()),
        ),
    };
    record_request(&state, &request_id, "/api/admin/overview").await;
    response
}

async fn logs(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = state
        .admin_store
        .list_logs(
            LogFilter {
                level: params.level,
                text: params.text,
                target: params.target,
            },
            ListQuery {
                limit: params.limit,
                offset: params.offset.unwrap_or(0),
            },
        )
        .await;
    let response = match result {
        Ok(logs) => json_response(StatusCode::OK, &request_id, logs),
        Err(err) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "admin logs unavailable",
            &request_id,
            Some(err.to_string()),
        ),
    };
    record_request(&state, &request_id, "/api/admin/logs").await;
    response
}

async fn cache_namespaces(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = state
        .admin_store
        .list_cache_namespaces(ListQuery {
            limit: params.limit,
            offset: params.offset.unwrap_or(0),
        })
        .await;
    let response = match result {
        Ok(page) => json_response(StatusCode::OK, &request_id, page),
        Err(err) => admin_store_error("cache namespaces unavailable", &request_id, err),
    };
    record_request(&state, &request_id, "/api/admin/cache/namespaces").await;
    response
}

async fn cache_entries(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = state
        .admin_store
        .list_cache_entries(
            params.namespace,
            ListQuery {
                limit: params.limit,
                offset: params.offset.unwrap_or(0),
            },
        )
        .await;
    let response = match result {
        Ok(page) => json_response(StatusCode::OK, &request_id, page),
        Err(err) => admin_store_error("cache entries unavailable", &request_id, err),
    };
    record_request(&state, &request_id, "/api/admin/cache/entries").await;
    response
}

async fn mq_queues(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = state
        .admin_store
        .list_mq_queues(ListQuery {
            limit: params.limit,
            offset: params.offset.unwrap_or(0),
        })
        .await;
    let response = match result {
        Ok(page) => json_response(StatusCode::OK, &request_id, page),
        Err(err) => admin_store_error("mq queues unavailable", &request_id, err),
    };
    record_request(&state, &request_id, "/api/admin/mq/queues").await;
    response
}

async fn mq_messages(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path(queue): Path<String>,
    Query(params): Query<ListParams>,
) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = state
        .admin_store
        .list_mq_messages(
            &queue,
            ListQuery {
                limit: params.limit,
                offset: params.offset.unwrap_or(0),
            },
        )
        .await;
    let response = match result {
        Ok(page) => json_response(StatusCode::OK, &request_id, page),
        Err(err) => admin_store_error("mq messages unavailable", &request_id, err),
    };
    record_request(&state, &request_id, "/api/admin/mq/queues/{queue}/messages").await;
    response
}

async fn clients(State(state): State<AdminHttpState>, headers: HeaderMap) -> Response {
    let request_id = request_id();
    if let Err(response) = authorize(&state, &headers, &request_id).await {
        return response;
    }
    let result = client_activity(&state).await;
    let response = match result {
        Ok(activity) => json_response(StatusCode::OK, &request_id, activity),
        Err(err) => admin_error("client activity unavailable", &request_id, err),
    };
    record_request(&state, &request_id, "/api/admin/clients").await;
    response
}

async fn overview_body(
    state: &AdminHttpState,
) -> Result<OverviewResponse, Box<dyn std::error::Error>> {
    let cache_status = if state.cache_enabled {
        db::check_cache_schema(&state.pool).await
    } else {
        db::CapabilityStatus {
            name: "cache",
            available: false,
            message: "disabled".to_string(),
        }
    };
    let mq_status = if state.mq_enabled {
        db::check_mq_schema(&state.pool).await
    } else {
        db::CapabilityStatus {
            name: "mq",
            available: false,
            message: "disabled".to_string(),
        }
    };
    let capabilities = vec![to_capability(cache_status), to_capability(mq_status)];
    let ready = capabilities
        .iter()
        .filter(|capability| capability.state != "disabled")
        .all(|capability| capability.state == "available");
    let cache_stats = state.cache_store.stats().await?;
    let mq_summary = mq_summary(&state.pool).await?;

    Ok(OverviewResponse {
        server_state: if ready { "ready" } else { "unavailable" },
        ready,
        capabilities,
        runtime_metrics: RuntimeMetricsDto {
            methods: method_metrics(&state.metrics),
        },
        pg_pool: PgPoolDto {
            size: state.pool.size(),
            idle: state.pool.num_idle() as u32,
        },
        cache_summary: CacheSummaryDto {
            hits: cache_stats.hits,
            misses: cache_stats.misses,
            writes: cache_stats.writes,
            deletes: cache_stats.deletes,
            evictions: cache_stats.evictions,
            expired_removals: cache_stats.expired_removals,
            logical_key_count: cache_stats.logical_key_count,
            logical_byte_size: cache_stats.logical_byte_size,
        },
        mq_summary,
    })
}

async fn mq_summary(pool: &sqlx::PgPool) -> Result<MqSummaryDto, sqlx::Error> {
    let row: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*)::bigint FROM mq_queues) AS queue_count,
          COALESCE(COUNT(m.id) FILTER (
            WHERE m.available_at <= now()
              AND (m.visibility_timeout_at IS NULL OR m.visibility_timeout_at <= now())
          ), 0)::bigint AS visible_message_count,
          COALESCE(COUNT(m.id) FILTER (WHERE m.visibility_timeout_at > now()), 0)::bigint
            AS in_flight_message_count,
          (SELECT COUNT(*)::bigint FROM mq_archives) AS archived_message_count
        FROM mq_messages m
        "#,
    )
    .fetch_one(pool)
    .await?;
    Ok(MqSummaryDto {
        queue_count: row.0,
        visible_message_count: row.1,
        in_flight_message_count: row.2,
        archived_message_count: row.3,
    })
}

async fn client_activity(state: &AdminHttpState) -> Result<ClientActivityResponse, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
          request_id,
          COALESCE(fields_json->>'path', '') AS path,
          MAX(occurred_at) AS last_seen_at
        FROM admin_log_events
        WHERE target = 'pgapp_server::admin_http'
          AND request_id IS NOT NULL
        GROUP BY request_id, COALESCE(fields_json->>'path', '')
        ORDER BY last_seen_at DESC
        LIMIT 50
        "#,
    )
    .fetch_all(&state.pool)
    .await?;

    Ok(ClientActivityResponse {
        admin_sessions: rows
            .into_iter()
            .map(|row| AdminSessionDto {
                request_id: row.get("request_id"),
                path: row.get("path"),
                last_seen_at: row.get("last_seen_at"),
            })
            .collect(),
        api_activity: method_metrics(&state.metrics),
    })
}

fn method_metrics(metrics: &MetricsRegistry) -> Vec<MethodMetricDto> {
    metrics
        .snapshot()
        .into_iter()
        .filter_map(|(key, metric)| {
            let mut parts = key.splitn(3, '.');
            Some(MethodMetricDto {
                service: parts.next()?.to_string(),
                method: parts.next()?.to_string(),
                status: parts.next()?.to_string(),
                count: metric.count,
                errors: metric.errors,
                total_latency_millis: u128_to_u64(metric.total_latency_millis),
            })
        })
        .collect()
}

fn to_capability(status: db::CapabilityStatus) -> CapabilityDto {
    CapabilityDto {
        name: status.name,
        state: if status.message == "disabled" {
            "disabled"
        } else if status.available {
            "available"
        } else {
            "unavailable"
        },
        message: status.message,
    }
}

async fn authorize(
    state: &AdminHttpState,
    headers: &HeaderMap,
    request_id: &str,
) -> Result<(), Response> {
    let expected = format!("Bearer {}", state.token);
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if authorized {
        Ok(())
    } else {
        let response = error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "valid admin bearer token required",
            request_id,
            None,
        );
        let _ = state
            .admin_store
            .record_log(AdminLogInput {
                level: "WARN".to_string(),
                target: "pgapp_server::admin_http".to_string(),
                message: "admin request unauthorized".to_string(),
                request_id: Some(request_id.to_string()),
                fields: json!({"path": "unknown"}),
            })
            .await;
        Err(response)
    }
}

async fn record_request(state: &AdminHttpState, request_id: &str, path: &str) {
    let _ = state
        .admin_store
        .record_log(AdminLogInput {
            level: "INFO".to_string(),
            target: "pgapp_server::admin_http".to_string(),
            message: "admin request completed".to_string(),
            request_id: Some(request_id.to_string()),
            fields: json!({ "path": path }),
        })
        .await;
}

fn json_response<T: Serialize>(status: StatusCode, _request_id: &str, body: T) -> Response {
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

fn error_response(
    status: StatusCode,
    code: &'static str,
    message: &str,
    request_id: &str,
    detail: Option<String>,
) -> Response {
    if let Some(detail) = detail {
        tracing::warn!(%request_id, %code, %detail, "admin api error");
    }
    json_response(
        status,
        request_id,
        AdminErrorBody {
            code,
            message: message.to_string(),
            request_id: request_id.to_string(),
        },
    )
}

fn admin_error(message: &str, request_id: &str, err: impl std::fmt::Display) -> Response {
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        message,
        request_id,
        Some(err.to_string()),
    )
}

fn admin_store_error(message: &str, request_id: &str, err: PgAppError) -> Response {
    let detail = err.to_string();
    let (status, code) = match &err {
        PgAppError::InvalidArgument(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };
    error_response(status, code, message, request_id, Some(detail))
}

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

fn u128_to_u64(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}

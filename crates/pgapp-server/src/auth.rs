use pgapp_core::{client_auth::ClientStore, metrics::MetricsRegistry};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tonic::{
    Status,
    body::Body,
    codegen::http::{HeaderMap, Request, Response},
};
use tower::{Layer, Service};

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

#[derive(Clone)]
pub struct AuthLayer {
    enabled: bool,
    store: ClientStore,
    metrics: MetricsRegistry,
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    enabled: bool,
    store: ClientStore,
    metrics: MetricsRegistry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuthCredentials {
    key: String,
    secret: String,
}

impl AuthLayer {
    pub fn new(enabled: bool, store: ClientStore, metrics: MetricsRegistry) -> Self {
        Self {
            enabled,
            store,
            metrics,
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            enabled: self.enabled,
            store: self.store.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let enabled = self.enabled;
        let store = self.store.clone();
        let metrics = self.metrics.clone();

        Box::pin(async move {
            if !enabled || is_health_path(request.uri().path()) {
                return inner.call(request).await;
            }

            let Some(credentials) = extract_credentials(request.headers()) else {
                metrics.record("auth", "authenticate", "unauthenticated", Duration::ZERO);
                return Ok(unauthenticated_response("missing pgapp credentials"));
            };

            match store
                .authenticate(&credentials.key, &credentials.secret)
                .await
            {
                Ok(Some(identity)) => {
                    request.extensions_mut().insert(identity);
                    inner.call(request).await
                }
                Ok(None) => {
                    metrics.record("auth", "authenticate", "unauthenticated", Duration::ZERO);
                    Ok(unauthenticated_response("invalid pgapp credentials"))
                }
                Err(err) => {
                    metrics.record("auth", "authenticate", "error", Duration::ZERO);
                    Ok(Status::internal(format!("authentication failed: {err}")).into_http())
                }
            }
        })
    }
}

fn is_health_path(path: &str) -> bool {
    matches!(
        path,
        "/pgapp.v1.HealthService/GetHealth" | "/pgapp.v1.HealthService/GetReadiness"
    )
}

fn extract_credentials(headers: &HeaderMap) -> Option<AuthCredentials> {
    let key = headers.get("x-pgapp-key")?.to_str().ok()?;
    let secret = headers.get("x-pgapp-secret")?.to_str().ok()?;
    if key.is_empty() || secret.is_empty() {
        return None;
    }
    Some(AuthCredentials {
        key: key.to_string(),
        secret: secret.to_string(),
    })
}

fn unauthenticated_response(message: &str) -> Response<Body> {
    Status::unauthenticated(message).into_http()
}

#[cfg(test)]
mod tests {
    use tonic::body::Body;
    use tonic::codegen::http::Request;

    #[test]
    fn recognizes_health_paths_and_extracts_credentials() {
        let health = Request::builder()
            .uri("/pgapp.v1.HealthService/GetHealth")
            .body(Body::empty())
            .unwrap();
        assert!(super::is_health_path(health.uri().path()));

        let readiness = Request::builder()
            .uri("/pgapp.v1.HealthService/GetReadiness")
            .body(Body::empty())
            .unwrap();
        assert!(super::is_health_path(readiness.uri().path()));

        let mut cache = Request::builder()
            .uri("/pgapp.v1.CacheService/Get")
            .body(Body::empty())
            .unwrap();
        assert!(!super::is_health_path(cache.uri().path()));
        assert!(super::extract_credentials(cache.headers()).is_none());

        cache
            .headers_mut()
            .insert("x-pgapp-key", "svc-billing".parse().unwrap());
        cache
            .headers_mut()
            .insert("x-pgapp-secret", "secret-1".parse().unwrap());
        let credentials = super::extract_credentials(cache.headers()).unwrap();
        assert_eq!(credentials.key, "svc-billing");
        assert_eq!(credentials.secret, "secret-1");
    }
}

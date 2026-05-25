use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MethodMetrics {
    pub count: u64,
    pub errors: u64,
    pub total_latency_millis: u128,
}

#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<Mutex<HashMap<String, MethodMetrics>>>,
}

impl MetricsRegistry {
    pub fn record(&self, service: &str, method: &str, status: &str, latency: Duration) {
        let key = format!("{service}.{method}.{status}");
        let mut guard = self.inner.lock().expect("metrics mutex poisoned");
        let metric = guard.entry(key).or_default();
        metric.count += 1;
        if status != "ok" {
            metric.errors += 1;
        }
        metric.total_latency_millis += latency.as_millis();
    }

    pub fn snapshot(&self) -> HashMap<String, MethodMetrics> {
        self.inner.lock().expect("metrics mutex poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_request_counts_latency_and_errors() {
        let metrics = MetricsRegistry::default();
        metrics.record("cache", "get", "ok", Duration::from_millis(8));
        metrics.record("cache", "get", "invalid_argument", Duration::from_millis(2));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot["cache.get.ok"].count, 1);
        assert_eq!(snapshot["cache.get.invalid_argument"].errors, 1);
        assert_eq!(snapshot["cache.get.ok"].total_latency_millis, 8);
    }
}

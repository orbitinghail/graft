use measured::MetricGroup;

use crate::api::metrics::{HttpMetrics, HTTP_METRICS};

#[derive(MetricGroup)]
pub struct PagestoreRegistry {
    pub http: &'static HttpMetrics,
}

impl Default for PagestoreRegistry {
    fn default() -> Self {
        Self { http: &HTTP_METRICS }
    }
}

#[derive(MetricGroup)]
pub struct MetastoreRegistry {
    pub http: &'static HttpMetrics,
}

impl Default for MetastoreRegistry {
    fn default() -> Self {
        Self { http: &HTTP_METRICS }
    }
}

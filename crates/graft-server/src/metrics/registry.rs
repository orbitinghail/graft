use std::sync::Arc;

use measured::MetricGroup;

use crate::api::metrics::HttpMetrics;

#[derive(MetricGroup, Default)]
pub struct Registry {
    #[metric(namespace = "http")]
    http: Option<Arc<HttpMetrics>>,
}

impl Registry {
    pub fn register_http(&mut self, http: Arc<HttpMetrics>) {
        self.http = Some(http);
    }
}

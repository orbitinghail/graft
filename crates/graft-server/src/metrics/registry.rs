use std::{result::Result, sync::Arc};

use measured::MetricGroup;

use crate::segment::writer::SegmentWriterMetrics;

#[derive(Default, MetricGroup)]
pub struct Registry {
    #[metric(namespace = "segment_writer")]
    pub segment_writer: Option<Arc<SegmentWriterMetrics>>,
}

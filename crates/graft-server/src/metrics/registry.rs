use std::{result::Result, sync::Arc};

use measured::MetricGroup;

use crate::segment::{uploader::SegmentUploaderMetrics, writer::SegmentWriterMetrics};

#[derive(Default, MetricGroup)]
pub struct Registry {
    #[metric(namespace = "segment_writer")]
    segment_writer: Option<Arc<SegmentWriterMetrics>>,

    #[metric(namespace = "segment_uploader")]
    segment_uploader: Option<Arc<SegmentUploaderMetrics>>,
}

impl Registry {
    pub fn segment_writer(&mut self) -> Arc<SegmentWriterMetrics> {
        self.segment_writer
            .get_or_insert_with(|| Arc::new(SegmentWriterMetrics::default()))
            .clone()
    }

    pub fn segment_uploader(&mut self) -> Arc<SegmentUploaderMetrics> {
        self.segment_uploader
            .get_or_insert_with(|| Arc::new(SegmentUploaderMetrics::default()))
            .clone()
    }
}

use std::{sync::Once, time::Instant};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt::{
        format::{FmtSpan, Writer},
        time::FormatTime,
    },
    util::SubscriberInitExt,
    EnvFilter,
};

use crate::running_in_antithesis;

pub fn tracing_init(prefix: Option<String>) {
    let color = !running_in_antithesis() && !std::env::var("NO_COLOR").is_ok_and(|s| !s.is_empty());

    static INIT: Once = Once::new();
    INIT.call_once(move || {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env()
                    .unwrap()
                    .add_directive("graft_test=trace".parse().unwrap())
                    .add_directive("graft_client=trace".parse().unwrap())
                    .add_directive("graft_core=trace".parse().unwrap())
                    .add_directive("graft_server=trace".parse().unwrap()),
            )
            .with_thread_names(true)
            .with_span_events(FmtSpan::CLOSE)
            .with_ansi(color)
            .with_timer(TimeAndPrefix::new(prefix))
            .finish()
            .try_init()
            .unwrap();
    });
}

struct TimeAndPrefix {
    prefix: Option<String>,
    start: Option<Instant>,
}

impl TimeAndPrefix {
    fn new(prefix: Option<String>) -> Self {
        Self {
            prefix,
            start: (!running_in_antithesis()).then(|| Instant::now()),
        }
    }

    fn write_time(start: &Instant, w: &mut Writer<'_>) -> std::fmt::Result {
        let e = start.elapsed();
        let nanos = e.subsec_nanos();
        // round nanos to the nearest millisecond
        let millis = (nanos as f64 / 1_000_000.0).round();
        write!(w, "{:03}.{:03}s", e.as_secs(), millis)
    }
}

impl FormatTime for TimeAndPrefix {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        match (&self.start, &self.prefix) {
            (None, None) => Ok(()),
            (None, Some(prefix)) => write!(w, "{prefix}"),
            (Some(start), None) => Self::write_time(start, w),
            (Some(start), Some(prefix)) => {
                write!(w, "{prefix} ")?;
                Self::write_time(start, w)
            }
        }
    }
}

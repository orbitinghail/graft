use std::{sync::Once, time::Instant};
use tracing_subscriber::{fmt::time::SystemTime, util::SubscriberInitExt};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt::{
        format::{FmtSpan, Writer},
        time::FormatTime,
    },
    EnvFilter,
};

pub fn running_in_antithesis() -> bool {
    std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok()
}

#[derive(PartialEq, Eq)]
pub enum TracingConsumer {
    Test,
    Server,
    Tool,
}

/// Initialize tracing. If no process_id is specified one will be randomly generated.
pub fn tracing_init(consumer: TracingConsumer, process_id: Option<String>) {
    let process_id = process_id
        .unwrap_or_else(|| bs58::encode(rand::random::<u64>().to_le_bytes()).into_string());

    let antithesis = running_in_antithesis();
    let testing = consumer == TracingConsumer::Test;
    let color = !antithesis && !std::env::var("NO_COLOR").is_ok_and(|s| !s.is_empty());

    let mut filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()
        .unwrap();

    if antithesis || testing {
        filter = filter
            .add_directive("graft_client=trace".parse().unwrap())
            .add_directive("graft_core=trace".parse().unwrap())
            .add_directive("graft_server=trace".parse().unwrap())
            .add_directive("graft_test=trace".parse().unwrap())
    }

    let prefix = if antithesis || testing {
        Some(process_id.clone())
    } else {
        None
    };

    let time = if antithesis {
        TimeFormat::None
    } else if consumer == TracingConsumer::Server {
        TimeFormat::Long(SystemTime)
    } else {
        TimeFormat::Offset { start: Instant::now() }
    };

    static INIT: Once = Once::new();
    INIT.call_once(move || {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_thread_names(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_ansi(color)
            .with_timer(TimeAndPrefix::new(prefix, time))
            .finish()
            .try_init()
            .expect("failed to setup tracing subscriber");
    });
}

enum TimeFormat {
    None,
    Long(SystemTime),
    Offset { start: Instant },
}

struct TimeAndPrefix {
    prefix: Option<String>,
    time: TimeFormat,
}

impl TimeAndPrefix {
    fn new(prefix: Option<String>, time: TimeFormat) -> Self {
        Self { prefix, time }
    }

    fn write_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        match self.time {
            TimeFormat::None => Ok(()),
            TimeFormat::Long(inner) => inner.format_time(w),
            TimeFormat::Offset { start } => {
                let e = start.elapsed();
                let nanos = e.subsec_nanos();
                // round nanos to the nearest millisecond
                let millis = (nanos as f64 / 1_000_000.0).round();
                write!(w, "{:03}.{:03}s", e.as_secs(), millis)
            }
        }
    }
}

impl FormatTime for TimeAndPrefix {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        match (&self.prefix, &self.time) {
            (None, _) => self.write_time(w),
            (Some(prefix), TimeFormat::None) => write!(w, "{}", prefix),
            (Some(prefix), _) => {
                write!(w, "{} ", prefix)?;
                self.write_time(w)
            }
        }
    }
}

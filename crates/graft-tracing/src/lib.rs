use parking_lot::Once;
use std::time::Instant;
use tracing_subscriber::{
    fmt::{MakeWriter, time::SystemTime},
    util::SubscriberInitExt,
};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        format::{FmtSpan, Writer},
        time::FormatTime,
    },
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

pub fn init_tracing(consumer: TracingConsumer, process_id: Option<String>) {
    init_tracing_with_writer(consumer, process_id, std::io::stdout);
}

/// Initialize tracing. If no `process_id` is specified one will be randomly generated.
pub fn init_tracing_with_writer<W>(consumer: TracingConsumer, process_id: Option<String>, writer: W)
where
    W: for<'writer> MakeWriter<'writer> + 'static + Send + Sync,
{
    static INIT: Once = Once::new();
    INIT.call_once(move || {
        let process_id = process_id
            .unwrap_or_else(|| bs58::encode(rand::random::<u64>().to_le_bytes()).into_string());

        let antithesis = running_in_antithesis();
        let testing = consumer == TracingConsumer::Test;
        let color = !antithesis && !std::env::var("NO_COLOR").is_ok_and(|s| !s.is_empty());

        let default_level = if consumer == TracingConsumer::Tool {
            LevelFilter::WARN
        } else {
            LevelFilter::INFO
        };

        let mut filter = EnvFilter::builder()
            .with_default_directive(default_level.into())
            .from_env()
            .unwrap();

        if antithesis || testing {
            filter = filter
                .add_directive("graft_client=debug".parse().unwrap())
                .add_directive("graft_core=debug".parse().unwrap())
                .add_directive("graft_server=debug".parse().unwrap())
                .add_directive("graft_test=debug".parse().unwrap())
                .add_directive("graft_sqlite=debug".parse().unwrap())
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

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_thread_names(true)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_ansi(color)
            .with_timer(TimeAndPrefix::new(prefix, time))
            .with_writer(writer)
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
            (Some(prefix), TimeFormat::None) => write!(w, "{prefix}"),
            (Some(prefix), _) => {
                write!(w, "{prefix} ")?;
                self.write_time(w)
            }
        }
    }
}

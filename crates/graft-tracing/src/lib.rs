//! Tracing utilities for the Graft project.
//!
//! This crate provides functionality for initializing and configuring
//! [tracing](https://docs.rs/tracing) in different environments (test, server, tool).

use std::time::Instant;
use tracing_subscriber::{
    fmt::{MakeWriter, time::SystemTime},
    layer::SubscriberExt,
};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter,
    fmt::{
        format::{FmtSpan, Writer},
        time::FormatTime,
    },
};

pub use tracing_subscriber::util::SubscriberInitExt;

/// Checks if the application is running in the Antithesis testing environment.
pub fn running_in_antithesis() -> bool {
    std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok()
}

/// Specifies the type of application consuming the tracing output.
#[derive(PartialEq, Eq)]
pub enum TracingConsumer {
    /// Test environment consumer
    Test,
    /// Server application consumer
    Server,
    /// Command-line tool consumer
    Tool,
}

/// Initializes tracing with stdout as the output.
#[must_use]
pub fn setup_tracing(consumer: TracingConsumer) -> impl SubscriberExt {
    setup_tracing_with_writer(consumer, std::io::stdout)
}

/// Initializes tracing with a custom writer for output.
///
/// # Parameters
/// * `consumer` - The type of application consuming the tracing output
/// * `writer` - Custom writer implementation for tracing output
///
/// # Type Parameters
/// * `W` - Writer type that implements the [`tracing_subscriber::fmt::MakeWriter`] trait
#[must_use]
pub fn setup_tracing_with_writer<W>(consumer: TracingConsumer, writer: W) -> impl SubscriberExt
where
    W: for<'writer> MakeWriter<'writer> + 'static + Send + Sync,
{
    let antithesis = running_in_antithesis();
    let testing = consumer == TracingConsumer::Test;

    // determine if color output should be enabled
    let color = !antithesis && !std::env::var("NO_COLOR").is_ok_and(|s| !s.is_empty());
    let no_time = std::env::var("NO_TIME").is_ok_and(|s| !s.is_empty());

    // allow a log prefix to be injected from the environment
    let prefix = std::env::var("GRAFT_LOG_PREFIX")
        .ok()
        .and_then(|s| (!s.trim().is_empty()).then_some(s.trim().to_string()));

    let default_level = match consumer {
        TracingConsumer::Test => LevelFilter::INFO,
        TracingConsumer::Server => LevelFilter::INFO,
        TracingConsumer::Tool => LevelFilter::WARN,
    };

    let mut filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .from_env()
        .unwrap();

    let mut span_events = FmtSpan::NONE;

    if antithesis || testing {
        span_events = FmtSpan::NEW | FmtSpan::CLOSE;
        filter = filter
            .add_directive("graft_kernel=debug".parse().unwrap())
            .add_directive("graft_core=trace".parse().unwrap())
            .add_directive("graft_test=trace".parse().unwrap())
            .add_directive("graft_sqlite=debug".parse().unwrap())
    }

    let time = if antithesis || no_time {
        TimeFormat::None
    } else if consumer == TracingConsumer::Server {
        TimeFormat::Long(SystemTime)
    } else {
        TimeFormat::Offset { start: Instant::now() }
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_thread_names(antithesis || consumer == TracingConsumer::Server)
        .with_span_events(span_events)
        .with_ansi(color)
        .with_timer(TimeAndPrefix::new(prefix, time))
        .with_writer(writer)
        .finish()
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

//! Tracing utilities for the Graft project.
//!
//! This crate provides functionality for initializing and configuring tracing
//! in different environments (test, server, tool) with customizable output formats.
//! It supports different timestamp formats and process identification for distributed tracing.

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

/// Checks if the application is running in the Antithesis testing environment.
///
/// Returns `true` if the ANTITHESIS_OUTPUT_DIR environment variable is set.
pub fn running_in_antithesis() -> bool {
    std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok()
}

/// Specifies the type of application consuming the tracing output.
///
/// Used to configure appropriate tracing settings for different contexts.
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
///
/// This is a convenience wrapper around `init_tracing_with_writer` that uses
/// standard output as the writer.
///
/// # Parameters
/// * `consumer` - The type of application consuming the tracing output
/// * `process_id` - Optional identifier for the process, randomly generated if None
pub fn init_tracing(consumer: TracingConsumer, process_id: Option<String>) {
    init_tracing_with_writer(consumer, process_id, std::io::stdout);
}

/// Initializes tracing with a custom writer for output.
///
/// Sets up a tracing subscriber with configuration based on the consumer type
/// and environment. This function will only initialize tracing once, even if
/// called multiple times.
///
/// # Parameters
/// * `consumer` - The type of application consuming the tracing output
/// * `process_id` - Optional identifier for the process, randomly generated if None
/// * `writer` - Custom writer implementation for tracing output
///
/// # Type Parameters
/// * `W` - Writer type that implements the MakeWriter trait
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

        let mut span_events = FmtSpan::NONE;

        if antithesis || testing {
            span_events = FmtSpan::NEW | FmtSpan::CLOSE;
            filter = filter
                .add_directive("graft_client=trace".parse().unwrap())
                .add_directive("graft_core=trace".parse().unwrap())
                .add_directive("graft_server=trace".parse().unwrap())
                .add_directive("graft_test=trace".parse().unwrap())
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
            .with_span_events(span_events)
            .with_ansi(color)
            .with_timer(TimeAndPrefix::new(prefix, time))
            .with_writer(writer)
            .finish()
            .try_init()
            .expect("failed to setup tracing subscriber");
    });
}

/// Defines the format for timestamps in trace output.
enum TimeFormat {
    /// No timestamp output
    None,
    /// Full system time format
    Long(SystemTime),
    /// Time elapsed since a specific starting point
    Offset { start: Instant },
}

/// Combines a process identifier prefix with timestamp formatting.
///
/// Used to customize the time format in trace output, optionally including
/// a process identifier prefix.
struct TimeAndPrefix {
    /// Optional process identifier to include in trace output
    prefix: Option<String>,
    /// Time format specification
    time: TimeFormat,
}

impl TimeAndPrefix {
    /// Creates a new TimeAndPrefix instance.
    ///
    /// # Parameters
    /// * `prefix` - Optional process identifier to include in trace output
    /// * `time` - Time format specification
    fn new(prefix: Option<String>, time: TimeFormat) -> Self {
        Self { prefix, time }
    }

    /// Writes the time component to the output writer.
    ///
    /// Formats the time according to the configured TimeFormat.
    ///
    /// # Parameters
    /// * `w` - Writer to output the formatted time
    ///
    /// # Returns
    /// Result indicating success or failure of the write operation
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

/// Implementation of FormatTime for TimeAndPrefix.
impl FormatTime for TimeAndPrefix {
    /// Formats the time with an optional prefix for trace output.
    ///
    /// Combines the process identifier prefix (if present) with the
    /// formatted time according to the configured TimeFormat.
    ///
    /// # Parameters
    /// * `w` - Writer to output the formatted time
    ///
    /// # Returns
    /// Result indicating success or failure of the write operation
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

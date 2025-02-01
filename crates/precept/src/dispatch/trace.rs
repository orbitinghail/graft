use core::panic;

use crate::catalog::Expectation;

use super::{Dispatch, Event};

pub struct TraceDispatch;

impl Dispatch for TraceDispatch {
    fn emit(&self, event: Event) {
        match event {
            Event::RegisterEntry(entry) => {
                tracing::info!(
                    location = ?entry.location(),
                    module = ?entry.module(),
                    function = ?entry.function(),
                    "registering expectation {:?}: {}",
                    entry.expectation(),
                    entry.property()
                )
            }
            Event::EmitEntry { entry, condition, details } => {
                let passed = match (entry.expectation(), condition) {
                    (Expectation::Always, true) => true,
                    (Expectation::AlwaysOrUnreachable, true) => true,
                    (Expectation::Sometimes, _) => true,
                    (Expectation::Reachable, _) => true,
                    _ => false,
                };
                if !passed {
                    tracing::error!(
                        details = serde_json::to_string(&details).unwrap(),
                        location = ?entry.location(),
                        module = ?entry.module(),
                        function = ?entry.function(),
                        "expectation {:?} failed: {}",
                        entry.expectation(),
                        entry.property()
                    );
                    panic!("expectation failed")
                }
            }
            Event::SetupComplete { details } => {
                tracing::info!(
                    details = serde_json::to_string(&details).unwrap(),
                    "setup complete"
                )
            }
            Event::Custom { name, value } => {
                tracing::info!(
                    name,
                    value = serde_json::to_string(&value).unwrap(),
                    "custom event"
                )
            }
        }
    }

    #[inline]
    fn random(&self) -> u64 {
        rand::random()
    }
}

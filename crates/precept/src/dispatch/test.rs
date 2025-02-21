use core::panic;

use crate::catalog::Expectation;

use super::{Dispatch, Event};

pub struct TestDispatch;

impl Dispatch for TestDispatch {
    fn emit(&self, event: Event) {
        match event {
            Event::RegisterEntry(_) => {
                // nothing to do
            }
            Event::EmitEntry { entry, condition, details } => {
                let passed = matches!(
                    (entry.expectation(), condition),
                    (Expectation::Always, true)
                        | (Expectation::AlwaysOrUnreachable, true)
                        | (Expectation::Sometimes, _)
                        | (Expectation::Reachable, _)
                );
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
            Event::Fault { .. } => {
                // faults are not enabled in the test dispatcher
            }
        }
    }

    #[inline]
    fn random(&self) -> u64 {
        rand::random()
    }
}

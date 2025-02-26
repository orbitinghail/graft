use core::panic;

use super::{Dispatch, Event};

pub struct TestDispatch;

impl Dispatch for TestDispatch {
    fn emit(&self, event: Event) {
        match event {
            Event::RegisterEntry(_) | Event::Fault { .. } => {
                // registration and faults are not enabled in the test dispatcher
            }
            Event::EmitEntry { entry, condition, details } => {
                if !entry.expectation().check(condition) {
                    tracing::error!(
                        details = format!("{}", details),
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

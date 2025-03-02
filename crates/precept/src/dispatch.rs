use std::sync::OnceLock;

use noop::NoopDispatch;

use crate::catalog::CatalogEntry;

pub mod noop;
pub mod test;

#[cfg(feature = "antithesis")]
pub mod antithesis;

pub enum Event {
    RegisterEntry(&'static CatalogEntry),
    EmitEntry {
        entry: &'static CatalogEntry,
        condition: bool,
        details: serde_json::Value,
    },
    SetupComplete {
        details: serde_json::Value,
    },
    Custom {
        name: &'static str,
        value: serde_json::Value,
    },
}

pub trait Dispatch: Sync + Send {
    fn emit(&self, event: Event);
    fn random(&self) -> u64;
}

static DISPATCHER: OnceLock<&'static dyn Dispatch> = OnceLock::new();

#[derive(Debug)]
pub struct SetDispatchError;

impl std::error::Error for SetDispatchError {}

impl std::fmt::Display for SetDispatchError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.write_str("attempted to set the dispatcher after it was already set")
    }
}

/// Set the DISPATCHER to the provided dispatcher instance. This function will
/// fail if called multiple times.
pub fn set_dispatcher(dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {
    DISPATCHER.set(dispatcher).map_err(|_| SetDispatchError)
}

/// Returns a reference to the dispatcher.
///
/// If a dispatcher has not been set, a no-op implementation is returned.
pub fn dispatcher() -> &'static dyn Dispatch {
    match DISPATCHER.get() {
        Some(dispatch) => *dispatch,
        None => {
            static NOOP: NoopDispatch = NoopDispatch;
            &NOOP
        }
    }
}

/// Generate a random u64 using the dispatcher
#[inline]
pub fn get_random() -> u64 {
    dispatcher().random()
}

/// Emit an event using the dispatcher
#[inline]
pub fn emit(event: Event) {
    dispatcher().emit(event);
}

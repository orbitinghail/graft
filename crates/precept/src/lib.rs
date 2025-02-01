#[doc(hidden)]
pub mod deps {
    pub use linkme;
    pub use serde_json;
}

#[doc(hidden)]
pub mod catalog;

#[doc(hidden)]
pub mod function_name;

pub mod dispatch;
use dispatch::{Dispatch, SetDispatchError};

pub mod random;

#[cfg(not(feature = "disabled"))]
pub mod macros;

#[cfg(feature = "disabled")]
pub mod macros_stubs;

#[cfg(not(feature = "disabled"))]
pub fn init(dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {
    dispatch::set_dispatcher(dispatcher)?;
    catalog::init_catalog();
    Ok(())
}

#[cfg(feature = "disabled")]
pub fn init(_dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {
    Ok(())
}

#[doc(hidden)]
pub mod deps {
    pub use linkme;
    pub use serde_json;
}

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

pub fn init(dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {
    if cfg!(not(feature = "disabled")) {
        dispatch::set_dispatcher(dispatcher)?;
        catalog::init_catalog();
    }
    Ok(())
}

pub fn init_boxed(dispatcher: Box<dyn Dispatch>) -> Result<(), SetDispatchError> {
    if cfg!(not(feature = "disabled")) {
        init(Box::leak(dispatcher))
    } else {
        Ok(())
    }
}

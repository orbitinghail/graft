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
pub mod random;

#[cfg(any(feature = "antithesis"))]
pub mod macros;

#[cfg(not(any(feature = "antithesis")))]
pub mod macros_stubs;

use dispatch::{Dispatch, SetDispatchError};

#[cfg(any(feature = "antithesis"))]
pub fn init(dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {
    dispatch::set_dispatcher(dispatcher)?;
    catalog::init_catalog();
    Ok(())
}

#[cfg(not(any(feature = "antithesis")))]
pub fn init(dispatcher: &'static dyn Dispatch) -> Result<(), SetDispatchError> {}

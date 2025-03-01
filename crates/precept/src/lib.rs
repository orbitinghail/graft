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

pub fn init<F>(
    dispatcher: &'static dyn Dispatch,
    should_register: F,
) -> Result<(), SetDispatchError>
where
    F: FnMut(&catalog::CatalogEntry) -> bool,
{
    if cfg!(not(feature = "disabled")) {
        dispatch::set_dispatcher(dispatcher)?;
        catalog::init_catalog(should_register);
    }
    Ok(())
}

pub fn init_boxed<F>(
    dispatcher: Box<dyn Dispatch>,
    should_register: F,
) -> Result<(), SetDispatchError>
where
    F: FnMut(&catalog::CatalogEntry) -> bool,
{
    if cfg!(not(feature = "disabled")) {
        init(Box::leak(dispatcher), should_register)
    } else {
        Ok(())
    }
}

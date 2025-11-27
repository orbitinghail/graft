pub mod file;
pub mod pragma;
pub mod vfs;

mod dbg;

#[cfg(feature = "register-static")]
pub mod register;

#[cfg(feature = "register-static")]
pub use register::register_static;

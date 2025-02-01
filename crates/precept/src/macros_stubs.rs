// This file must be kept in sync with macros.rs

#[doc(hidden)]
#[macro_export]
macro_rules! define_entry {
    ($($ignore:tt)*) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! emit_entry {
    ($($ignore:tt)*) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! define_and_emit_entry {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! emit_event {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! setup_complete {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_always {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_always_or_unreachable {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_sometimes {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_reachable {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_unreachable {
    ($($ignore:tt)*) => {};
}

#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use antithesis_sdk;

#[cfg(feature = "antithesis")]
#[doc(hidden)]
pub use serde_json;

pub mod random;

pub fn init() {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! setup_complete {
    () => {
        $crate::antithesis_sdk::lifecycle::setup_complete(&$crate::serde_json::Value::Null);
    };

    ($($details:tt)+) => {
        let details = $crate::serde_json::json!($($details)+);
        $crate::antithesis_sdk::lifecycle::setup_complete(&details);
    }
}

#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! setup_complete {
    ($($ignore:tt)*) => {};
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! expect_helper {
    ($macro:ident, $message:literal) => {
        $crate::antithesis_sdk::$macro!($message);
    };
    ($macro:ident, $message:literal, $($details:tt)+) => {
        let details = $crate::serde_json::json!($($details)+);
        $crate::antithesis_sdk::$macro!($message, &details);
    };
}

#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! expect_helper {
    ($($ignore:tt)*) => {};
}

#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! expect_maybe_helper {
    ($macro:ident, $condition:expr, $message:literal) => {
        $crate::antithesis_sdk::$macro!($condition, $message);
    };
    ($macro:ident, $condition:expr, $message:literal, $($details:tt)+) => {
        let details = $crate::serde_json::json!($($details)+);
        $crate::antithesis_sdk::$macro!($condition, $message, &details);
    };
}

#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! expect_maybe_helper {
    ($($ignore:tt)*) => {};
}

#[macro_export]
macro_rules! expect_always {
    ($condition:expr, $message:literal$(, $($details:tt)+)?) => {
        $crate::expect_maybe_helper!(assert_always, $condition, $message$(, $($details)+)?);
    };
}

#[macro_export]
macro_rules! expect_always_or_unreachable {
    ($condition:expr, $message:literal$(, $($details:tt)+)?) => {
        $crate::expect_maybe_helper!(assert_always_or_unreachable, $condition, $message$(, $($details)+)?);
    }
}

#[macro_export]
macro_rules! expect_sometimes {
    ($condition:expr, $message:literal$(, $($details:tt)+)?) => {
        $crate::expect_maybe_helper!(assert_sometimes, $condition, $message$(, $($details)+)?);
    }
}

#[macro_export]
macro_rules! expect_reachable {
    ($message:literal$(, $($details:tt)+)?) => {
        $crate::expect_helper!(assert_reachable, $message$(, $($details)+)?);
    }
}

#[macro_export]
macro_rules! expect_unreachable {
    ($message:literal$(, $($details:tt)+)?) => {
        $crate::expect_helper!(assert_unreachable, $message$(, $($details)+)?);
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_macros_expansion() {
        expect_always!(true, "this should always pass", { "key": 123 });
        expect_always_or_unreachable!(true, "this should always pass", { "key": 123 });
        expect_sometimes!(true, "this should sometimes pass", { "key": 123 });
        expect_reachable!("this should always pass", { "key": 123 });
        expect_unreachable!("this should always fail", { "key": 123 });
    }
}

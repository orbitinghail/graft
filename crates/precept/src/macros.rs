#[doc(hidden)]
#[macro_export]
macro_rules! define_entry {
    ($expectation:path, $property:literal) => {{
        use $crate::catalog::CatalogEntry;
        $crate::function_name!(FN_NAME);
        #[$crate::deps::linkme::distributed_slice($crate::catalog::PRECEPT_CATALOG)]
        #[linkme(crate = $crate::deps::linkme)]
        static ENTRY: CatalogEntry = CatalogEntry::new(
            $expectation,
            $property,
            std::panic::Location::caller(),
            module_path!(),
            &FN_NAME,
        );
        &ENTRY
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! emit_entry {
    ($entry:expr, $condition:expr) => {{
        $crate::emit_entry!($entry, $condition, null)
    }};

    ($entry:expr, $condition:expr, $($details:tt)+) => {{
        let details = $crate::deps::serde_json::json!($($details)+);
        $entry.emit($condition, details);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! define_and_emit_entry {
    ($expectation:path, $property:literal, $condition:expr) => {{
        let entry = $crate::define_entry!($expectation, $property);
        $crate::emit_entry!(entry, $condition)
    }};

    ($expectation:path, $property:literal, $condition:expr, $($details:tt)+) => {{
        let entry = $crate::define_entry!($expectation, $property);
        $crate::emit_entry!(entry, $condition, $($details)+)
    }};
}

#[macro_export]
macro_rules! emit_event {
    ($name:literal, $($details:tt)+) => {
        $crate::dispatch::emit(
            $crate::dispatch::Event::Custom{
                name: $name,
                details: $crate::deps::serde_json::json!($($details)+),
            }
        );
    };
}

#[macro_export]
macro_rules! setup_complete {
    () => {
        $crate::setup_complete!(null);
    };

    ($($details:tt)+) => {
        $crate::dispatch::emit(
            $crate::dispatch::Event::SetupComplete{
                details: $crate::deps::serde_json::json!($($details)+),
            }
        );
    };
}

#[macro_export]
macro_rules! expect_always {
    ($condition:expr, $property:literal$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Always,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_always_or_unreachable {
    ($condition:expr, $property:literal$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::AlwaysOrUnreachable,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_sometimes {
    ($condition:expr, $property:literal$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Sometimes,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_reachable {
    ($property:literal$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Reachable,
            $property,
            true $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_unreachable {
    ($property:literal$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Unreachable,
            $property,
            false $(, $($details)+)?
        );
    };
}

#[cfg(test)]
mod tests {
    use crate::catalog::Expectation;

    #[test]
    fn test_entry_gen() {
        let entry = define_entry!(Expectation::Always, "test property");
        assert_eq!(entry.property(), "test property");
        assert_eq!(entry.expectation(), Expectation::Always);
        assert_eq!(entry.location().file(), file!());
        assert_eq!(entry.module(), module_path!());
        assert_eq!(
            &entry.function(),
            &concat!(module_path!(), "::test_entry_gen")
        );
    }

    #[test]
    fn test_entry_emit() {
        define_and_emit_entry!(Expectation::Always, "test property", true);
    }

    #[test]
    fn test_expect() {
        expect_always!(true, "this should always pass");
        expect_always!(true, "this should always pass", { "key": 123 });

        expect_always_or_unreachable!(true, "this should always pass or be unreachable");
        expect_always_or_unreachable!(true, "this should always pass or be unreachable", { "key": 123 });

        expect_sometimes!(true, "this should sometimes pass");
        expect_sometimes!(true, "this should sometimes pass", { "key": 123 });

        expect_reachable!("this should always pass");
        expect_reachable!("this should always pass", { "key": 123 });

        expect_unreachable!("this should always fail");
        expect_unreachable!("this should always fail", { "key": 123 });
    }
}

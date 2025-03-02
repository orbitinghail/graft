#[doc(hidden)]
#[macro_export]
macro_rules! define_entry {
    ($expectation:path, $property:expr) => {{
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
    ($expectation:path, $property:expr, $condition:expr) => {{
        let entry = $crate::define_entry!($expectation, $property);
        $crate::emit_entry!(entry, $condition)
    }};

    ($expectation:path, $property:expr, $condition:expr, $($details:tt)+) => {{
        let entry = $crate::define_entry!($expectation, $property);
        $crate::emit_entry!(entry, $condition, $($details)+)
    }};
}

#[macro_export]
macro_rules! emit_event {
    ($name:expr, $($details:tt)+) => {
        $crate::dispatch::emit(
            $crate::dispatch::Event::Custom{
                name: $name,
                value: $crate::deps::serde_json::json!($($details)+),
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
    ($condition:expr, $property:expr$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Always,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_always_or_unreachable {
    ($condition:expr, $property:expr$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::AlwaysOrUnreachable,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_sometimes {
    ($condition:expr, $property:expr$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Sometimes,
            $property,
            $condition $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_reachable {
    ($property:expr$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Reachable,
            $property,
            true $(, $($details)+)?
        );
    };
}

#[macro_export]
macro_rules! expect_unreachable {
    ($property:expr$(, $($details:tt)+)?) => {
        $crate::define_and_emit_entry!(
            $crate::catalog::Expectation::Unreachable,
            $property,
            false $(, $($details)+)?
        );
    };
}

/// emit a fault with a probability p to the dispatcher
/// p should be in the range [0.0, 1.0]
/// fault is a block that will be executed when the fault triggers
///
/// # Example
/// ```
/// use precept::maybe_fault;
/// maybe_fault!(
///     0.5,
///     "triggers 50% of the time",
///     println!("this will run when the fault triggers"),
///     { "optional": "details" }
/// );
/// ```
#[macro_export]
macro_rules! maybe_fault {
    ($p:expr, $name:expr, $fault:expr) => {
        $crate::maybe_fault!($p, $name, $fault, null);
    };

    ($p:expr, $name:expr, $fault:expr, $($details:tt)+) => {{
        if $crate::faults_enabled() {
            static THRESHOLD: u64 = (u64::MAX as f64 * $p) as u64;
            let should_fault = $crate::dispatch::get_random() < THRESHOLD;
            if should_fault {
                $crate::expect_reachable!(
                    concat!("fault is reachable: ", $name),
                    $($details)+
                );
                $crate::emit_event!("precept_fault", { "name": $name, "details": $($details)+ });
                $fault
            }
        }
    }};
}

#[cfg(test)]
mod tests {

    use crate::{catalog::Expectation, disable_faults};

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

    #[test]
    fn test_setup_complete() {
        setup_complete!();
        setup_complete!({ "key": 123 });
    }

    #[test]
    fn test_event() {
        emit_event!("test_event", { "key": 123 });
    }

    #[test]
    fn test_fault() {
        maybe_fault!(0.0, "this should never fault", unreachable!("never faults"));

        let mut faults = false;
        maybe_fault!(1.0, "this should always fault", faults = true);
        assert!(faults);

        maybe_fault!(0.5, "this should sometimes fault", ());

        // supports details
        maybe_fault!(0.5, "this should sometimes fault", (), { "key": 123 });

        disable_faults();
        maybe_fault!(
            1.0,
            "this should not fault because disabled",
            unreachable!("never faults")
        );
    }
}

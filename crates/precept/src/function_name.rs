#[macro_export]
macro_rules! function_name {
    ($static:ident) => {
        // define a function that does nothing, but inherits the current path
        fn _f() {}
        static $static: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
            fn type_name_of<T>(_: T) -> &'static str {
                ::std::any::type_name::<T>()
            }
            let name = type_name_of(_f);
            // trim `::_f` from the end of the do-nothing function to get the parent
            &name[..name.len() - 4]
        });
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_function_name() {
        function_name!(FN_NAME);
        assert_eq!(&*FN_NAME, &concat!(module_path!(), "::test_function_name"));
    }
}

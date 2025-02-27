#![no_std]
extern crate alloc;

pub mod vars {
    include!(concat!(env!("OUT_DIR"), "/vars.rs"));
}

mod ffi {
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(clippy::type_complexity)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

mod mock;

pub mod flags;
pub mod logger;
pub mod vfs;
pub use ffi::sqlite3_api_routines;

const MIN_SQLITE_VERSION: i32 = 3044000;

pub fn assert_min_sqlite_version() {
    let version = unsafe { ffi::sqlite3_libversion_number() };
    if version < MIN_SQLITE_VERSION {
        panic!(
            "sqlite3 version mismatch: expected at least {}, got {}",
            MIN_SQLITE_VERSION, version
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sanity() {
        // verify that we are linked against rusqlite in tests
        assert_eq!(
            unsafe { super::ffi::sqlite3_libversion_number() },
            rusqlite::version_number()
        );
        // verify that the rusqlite bundled sqlite3 meets our minimum requirements
        super::assert_min_sqlite_version();
    }
}

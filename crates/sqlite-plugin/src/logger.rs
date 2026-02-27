use alloc::{boxed::Box, ffi::CString, format};
use core::ffi::{c_char, c_int};

use crate::vars;

type Sqlite3Log = unsafe extern "C" fn(arg1: c_int, arg2: *const c_char, ...);

pub(crate) struct SqliteLogger {
    log: Sqlite3Log,
}

impl SqliteLogger {
    pub(crate) fn init(log: Sqlite3Log, level: log::Level) {
        let logger = Box::new(SqliteLogger { log });
        if log::set_boxed_logger(logger)
            .map(|()| log::set_max_level(level.to_level_filter()))
            .is_err()
        {
            log::warn!("existing logger already installed")
        }
    }
}

impl log::Log for SqliteLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            let z_format = CString::new(format!("{}", record.args())).unwrap();
            unsafe { (self.log)(vars::SQLITE_NOTICE, z_format.as_ptr()) }
        }
    }

    fn flush(&self) {}
}

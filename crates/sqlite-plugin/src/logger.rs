use alloc::ffi::CString;
use core::ffi::{c_char, c_int};

use crate::vars;

type Sqlite3Log = unsafe extern "C" fn(iErrCode: c_int, arg2: *const c_char, ...);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SqliteLogLevel {
    Error = 1,
    Warn,
    Notice,
}

impl SqliteLogLevel {
    fn into_err_code(self) -> c_int {
        match self {
            Self::Notice => vars::SQLITE_NOTICE,
            Self::Warn => vars::SQLITE_WARNING,
            Self::Error => vars::SQLITE_INTERNAL,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SqliteLogger {
    log: Sqlite3Log,
}

impl SqliteLogger {
    pub(crate) fn new(log: Sqlite3Log) -> Self {
        Self { log }
    }

    /// Log bytes to the `SQLite3` log handle.
    /// This function will write each line separately to `SQLite3`.
    /// Note that `SQLite3` silently truncates log lines larger than roughly
    /// 230 bytes by default.
    pub fn log(&self, level: SqliteLogLevel, buf: &[u8]) {
        let code = level.into_err_code();
        for line in buf.split(|b| *b == b'\n') {
            // skip if line only contains whitespace
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }

            let z_format = CString::new(line).unwrap();
            unsafe { (self.log)(code, z_format.as_ptr()) }
        }
    }
}

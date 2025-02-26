// cargo build --example memvfs --features dynamic

use std::{ffi::c_void, os::raw::c_char, sync::Arc};

use parking_lot::Mutex;
use sqlite_plugin::{
    flags::{AccessFlags, LockLevel, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    sqlite3_api_routines, vars,
    vfs::{register_dynamic, Pragma, PragmaErr, RegisterOpts, Vfs, VfsHandle, VfsResult},
};

#[derive(Debug, Clone)]
struct File {
    name: Option<String>,
    data: Arc<Mutex<Vec<u8>>>,
    delete_on_close: bool,
    opts: OpenOpts,
}

impl File {
    fn is_named(&self, s: &str) -> bool {
        self.name.as_ref().is_some_and(|f| f == s)
    }
}

impl VfsHandle for File {
    fn readonly(&self) -> bool {
        self.opts.mode().is_readonly()
    }

    fn in_memory(&self) -> bool {
        true
    }
}

struct MemVfs {
    files: Arc<Mutex<Vec<File>>>,
}

impl Vfs for MemVfs {
    type Handle = File;

    fn register_logger(&self, logger: SqliteLogger) {
        struct LogCompat {
            logger: Mutex<SqliteLogger>,
        }

        impl log::Log for LogCompat {
            fn enabled(&self, _metadata: &log::Metadata) -> bool {
                true
            }

            fn log(&self, record: &log::Record) {
                let level = match record.level() {
                    log::Level::Error => SqliteLogLevel::Error,
                    log::Level::Warn => SqliteLogLevel::Warn,
                    _ => SqliteLogLevel::Notice,
                };
                let msg = format!("{}", record.args());
                self.logger.lock().log(level, msg.as_bytes());
            }

            fn flush(&self) {}
        }

        let log = LogCompat { logger: Mutex::new(logger) };
        log::set_boxed_logger(Box::new(log)).expect("failed to setup global logger");
    }

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        log::debug!("open: path={:?}, opts={:?}", path, opts);
        let mode = opts.mode();
        if mode.is_readonly() {
            // readonly makes no sense since an in-memory VFS is not backed by
            // any pre-existing data.
            return Err(vars::SQLITE_CANTOPEN);
        }

        if let Some(path) = path {
            let mut files = self.files.lock();

            for file in files.iter() {
                if file.is_named(path) {
                    if mode.must_create() {
                        return Err(vars::SQLITE_CANTOPEN);
                    }
                    return Ok(file.clone());
                }
            }

            let file = File {
                name: Some(path.to_owned()),
                data: Default::default(),
                delete_on_close: opts.delete_on_close(),
                opts,
            };
            files.push(file.clone());
            Ok(file)
        } else {
            let file = File {
                name: None,
                data: Default::default(),
                delete_on_close: opts.delete_on_close(),
                opts,
            };
            Ok(file)
        }
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        log::debug!("delete: path={}", path);
        let mut found = false;
        self.files.lock().retain(|file| {
            if file.is_named(path) {
                found = true;
                false
            } else {
                true
            }
        });
        if !found {
            return Err(vars::SQLITE_IOERR_DELETE_NOENT);
        }
        Ok(())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        log::debug!("access: path={}, flags={:?}", path, flags);
        Ok(self.files.lock().iter().any(|f| f.is_named(path)))
    }

    fn file_size(&self, handle: &mut Self::Handle) -> VfsResult<usize> {
        log::debug!("file_size: file={:?}", handle.name);
        Ok(handle.data.lock().len())
    }

    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        log::debug!("truncate: file={:?}, size={}", handle.name, size);
        let mut data = handle.data.lock();
        if size > data.len() {
            data.resize(size, 0);
        } else {
            data.truncate(size);
        }
        Ok(())
    }

    fn lock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        log::debug!("lock: file={:?}, level={:?}", handle.name, level);
        Ok(())
    }

    fn unlock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        log::debug!("unlock: file={:?}, level={:?}", handle.name, level);
        Ok(())
    }

    fn write(&self, handle: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        log::debug!(
            "write: file={:?}, offset={}, len={}",
            handle.name,
            offset,
            buf.len()
        );
        let mut data = handle.data.lock();
        if offset + buf.len() > data.len() {
            data.resize(offset + buf.len(), 0);
        }
        data[offset..offset + buf.len()].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn read(&self, handle: &mut Self::Handle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        log::debug!(
            "read: file={:?}, offset={}, len={}",
            handle.name,
            offset,
            buf.len()
        );
        let data = handle.data.lock();
        if offset > data.len() {
            return Ok(0);
        }
        let len = buf.len().min(data.len() - offset);
        buf[..len].copy_from_slice(&data[offset..offset + len]);
        Ok(len)
    }

    fn sync(&self, handle: &mut Self::Handle) -> VfsResult<()> {
        log::debug!("sync: file={:?}", handle.name);
        Ok(())
    }

    fn close(&self, handle: Self::Handle) -> VfsResult<()> {
        log::debug!("close: file={:?}", handle.name);
        if handle.delete_on_close {
            if let Some(ref name) = handle.name {
                self.delete(name)?;
            }
        }
        Ok(())
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        log::debug!("pragma: file={:?}, pragma={:?}", handle.name, pragma);
        Err(PragmaErr::NotFound)
    }
}

/// This function is called by `SQLite` when the extension is loaded. It registers
/// the memvfs VFS with `SQLite`.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_memvfs_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = MemVfs { files: Default::default() };
    if let Err(err) = register_dynamic(p_api, "mem", vfs, RegisterOpts { make_default: true }) {
        return err;
    }

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);

    vars::SQLITE_OK_LOAD_PERMANENTLY
}

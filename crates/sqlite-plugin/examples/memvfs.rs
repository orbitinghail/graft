// cargo build --example memvfs --features dynamic

use std::{cell::RefCell, ffi::c_void, os::raw::c_char, rc::Rc, sync::Mutex};

use sqlite_plugin::{
    flags::{AccessFlags, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    sqlite3_api_routines, vars,
    vfs::{register_dynamic, Pragma, RegisterOpts, Vfs, VfsHandle, VfsResult},
};

#[derive(Debug, Clone)]
struct File {
    name: Option<String>,
    data: Rc<RefCell<Vec<u8>>>,
    delete_on_close: bool,
    opts: OpenOpts,
}

impl File {
    fn is_named(&self, s: &str) -> bool {
        self.name.as_ref().map_or(false, |f| f == s)
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
    files: Vec<File>,
}

impl Vfs for MemVfs {
    type Handle = File;

    fn register_logger(&mut self, logger: SqliteLogger) {
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
                self.logger.lock().unwrap().log(level, msg.as_bytes());
            }

            fn flush(&self) {}
        }

        let log = LogCompat { logger: Mutex::new(logger) };
        log::set_boxed_logger(Box::new(log)).expect("failed to setup global logger");
    }

    fn open(&mut self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        log::debug!("open: path={:?}, opts={:?}", path, opts);
        let mode = opts.mode();
        if mode.is_readonly() {
            // readonly makes no sense since an in-memory VFS is not backed by
            // any pre-existing data.
            return Err(vars::SQLITE_CANTOPEN);
        }

        if let Some(path) = path {
            for file in &self.files {
                if file.is_named(&path) {
                    if mode.must_create() {
                        return Err(vars::SQLITE_CANTOPEN);
                    }
                    return Ok(file.clone());
                }
            }

            let file = File {
                name: Some(path.to_owned()),
                data: Rc::new(RefCell::new(Vec::new())),
                delete_on_close: opts.delete_on_close(),
                opts,
            };
            self.files.push(file.clone());
            Ok(file)
        } else {
            let file = File {
                name: None,
                data: Rc::new(RefCell::new(Vec::new())),
                delete_on_close: opts.delete_on_close(),
                opts,
            };
            Ok(file)
        }
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        log::debug!("delete: path={}", path);
        let mut found = false;
        self.files.retain(|file| {
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

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        log::debug!("access: path={}, flags={:?}", path, flags);
        Ok(self.files.iter().any(|f| f.is_named(path)))
    }

    fn file_size(&mut self, handle: &mut Self::Handle) -> VfsResult<usize> {
        log::debug!("file_size: file={:?}", handle.name);
        Ok(handle.data.borrow().len())
    }

    fn truncate(&mut self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        log::debug!("truncate: file={:?}, size={}", handle.name, size);
        let mut data = handle.data.borrow_mut();
        if size > data.len() {
            data.resize(size, 0);
        } else {
            data.truncate(size);
        }
        Ok(())
    }

    fn write(&mut self, handle: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        log::debug!(
            "write: file={:?}, offset={}, len={}",
            handle.name,
            offset,
            buf.len()
        );
        let mut data = handle.data.borrow_mut();
        if offset + buf.len() > data.len() {
            data.resize(offset + buf.len(), 0);
        }
        data[offset..offset + buf.len()].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn read(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        buf: &mut [u8],
    ) -> VfsResult<usize> {
        log::debug!(
            "read: file={:?}, offset={}, len={}",
            handle.name,
            offset,
            buf.len()
        );
        let data = handle.data.borrow();
        if offset > data.len() {
            return Ok(0);
        }
        let len = buf.len().min(data.len() - offset);
        buf[..len].copy_from_slice(&data[offset..offset + len]);
        Ok(len)
    }

    fn sync(&mut self, handle: &mut Self::Handle) -> VfsResult<()> {
        log::debug!("sync: file={:?}", handle.name);
        Ok(())
    }

    fn close(&mut self, handle: &mut Self::Handle) -> VfsResult<()> {
        log::debug!("close: file={:?}", handle.name);
        if handle.delete_on_close {
            if let Some(ref name) = handle.name {
                self.delete(name)?;
            }
        }
        Ok(())
    }

    fn pragma(
        &mut self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> VfsResult<Option<String>> {
        log::debug!("pragma: file={:?}, pragma={:?}", handle.name, pragma);
        Err(vars::SQLITE_NOTFOUND)
    }
}

/// This function is called by SQLite when the extension is loaded. It registers
/// the memvfs VFS with SQLite.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_memvfs_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = MemVfs { files: Vec::new() };
    if let Err(err) = register_dynamic(p_api, "mem", vfs, RegisterOpts { make_default: true }) {
        return err;
    }

    // set the log level to trace
    log::set_max_level(log::LevelFilter::Trace);

    vars::SQLITE_OK_LOAD_PERMANENTLY
}

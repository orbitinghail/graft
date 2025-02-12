#![cfg(test)]

// tests use std
extern crate std;

use core::fmt::{self, Display};
use std::boxed::Box;
use std::collections::HashMap;
use std::println;
use std::{string::String, vec::Vec};

use alloc::borrow::{Cow, ToOwned};
use alloc::format;
use alloc::sync::Arc;
use parking_lot::{Mutex, MutexGuard};

use crate::flags::{self, AccessFlags, OpenOpts};
use crate::logger::{SqliteLogLevel, SqliteLogger};
use crate::vars;
use crate::vfs::{
    Pragma, Vfs, VfsHandle, VfsResult, DEFAULT_DEVICE_CHARACTERISTICS, DEFAULT_SECTOR_SIZE,
};

pub struct File {
    pub name: String,
    pub data: Vec<u8>,
    pub delete_on_close: bool,
}

#[allow(unused_variables)]
pub trait Hooks {
    fn canonical_path(&mut self, path: &str) {}
    fn open(&mut self, path: &Option<&str>, opts: &OpenOpts) {}
    fn delete(&mut self, path: &str) {}
    fn access(&mut self, path: &str, flags: AccessFlags) {}
    fn file_size(&mut self, handle: MockHandle) {}
    fn truncate(&mut self, handle: MockHandle, size: usize) {}
    fn write(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {}
    fn read(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {}
    fn sync(&mut self, handle: MockHandle) {}
    fn close(&mut self, handle: MockHandle) {}
    fn pragma(&mut self, handle: MockHandle, pragma: Pragma<'_>) -> VfsResult<Option<String>> {
        Err(vars::SQLITE_NOTFOUND)
    }
    fn sector_size(&mut self) {}
    fn device_characteristics(&mut self) {
        println!("device_characteristics");
    }
}

pub struct NoopHooks;
impl Hooks for NoopHooks {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MockHandle {
    id: usize,
    readonly: bool,
}

impl Display for MockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockHandle({})", self.id)
    }
}

impl MockHandle {
    pub fn new(id: usize, readonly: bool) -> Self {
        Self { id, readonly }
    }
}

impl VfsHandle for MockHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }

    fn in_memory(&self) -> bool {
        false
    }
}

// MockVfs implements a very simple in-memory VFS for testing purposes.
// See the memvfs example for a more complete implementation.
pub struct MockVfs {
    next_id: usize,
    shared: Arc<Mutex<Shared>>,
    log: Option<SqliteLogger>,
}

struct Shared {
    files: HashMap<MockHandle, File>,
    hooks: Box<dyn Hooks + Send>,
}

impl MockVfs {
    pub fn new(hooks: Box<dyn Hooks + Send>) -> Self {
        Self {
            next_id: 0,
            shared: Arc::new(Mutex::new(Shared { files: HashMap::new(), hooks })),
            log: None,
        }
    }

    fn log<'a>(&mut self, f: fmt::Arguments<'a>) {
        if let Some(log) = self.log.as_mut() {
            let buf = format!("{}", f);
            log.log(SqliteLogLevel::Notice, buf.as_bytes());
        } else {
            panic!("MockVfs is missing registered log handler")
        }
    }

    fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }
}

impl Vfs for MockVfs {
    // a simple usize that represents a file handle.
    type Handle = MockHandle;

    fn register_logger(&mut self, logger: SqliteLogger) {
        self.log = Some(logger);
    }

    fn canonical_path<'a>(&mut self, path: Cow<'a, str>) -> VfsResult<Cow<'a, str>> {
        self.log(format_args!("canonical_path: path={:?}", path));
        self.shared().hooks.canonical_path(&path);
        Ok(path.into())
    }

    fn open(&mut self, path: Option<&str>, opts: flags::OpenOpts) -> VfsResult<Self::Handle> {
        self.log(format_args!("open: path={:?} opts={:?}", path, opts));
        self.shared().hooks.open(&path, &opts);

        let id = self.next_id;
        self.next_id += 1;
        let file_handle = MockHandle::new(id, opts.mode().is_readonly());

        if let Some(path) = path {
            let files = &mut self.shared().files;
            // if file is already open return existing handle
            for (handle, file) in files.iter() {
                if file.name == path {
                    return Ok(*handle);
                }
            }
            files.insert(
                file_handle,
                File {
                    name: path.to_owned(),
                    data: Vec::new(),
                    delete_on_close: opts.delete_on_close(),
                },
            );
        }
        Ok(file_handle)
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        self.log(format_args!("delete: path={:?}", path));
        let mut shared = self.shared();
        shared.hooks.delete(path);
        shared.files.retain(|_, file| file.name != path);
        Ok(())
    }

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        self.log(format_args!("access: path={:?} flags={:?}", path, flags));
        let mut shared = self.shared();
        shared.hooks.access(path, flags);
        Ok(shared.files.values().any(|file| file.name == path))
    }

    fn file_size(&mut self, meta: &mut Self::Handle) -> VfsResult<usize> {
        self.log(format_args!("file_size: handle={:?}", meta));
        let mut shared = self.shared();
        shared.hooks.file_size(*meta);
        Ok(shared
            .files
            .get(meta)
            .map(|file| file.data.len())
            .unwrap_or(0))
    }

    fn truncate(&mut self, meta: &mut Self::Handle, size: usize) -> VfsResult<()> {
        self.log(format_args!("truncate: handle={:?} size={:?}", meta, size));
        let mut shared = self.shared();
        shared.hooks.truncate(*meta, size);
        if let Some(file) = shared.files.get_mut(meta) {
            if size > file.data.len() {
                file.data.resize(size, 0);
            } else {
                file.data.truncate(size);
            }
        }
        Ok(())
    }

    fn write(&mut self, meta: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        self.log(format_args!(
            "write: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
        let mut shared = self.shared();
        shared.hooks.write(*meta, offset, buf);
        if let Some(file) = shared.files.get_mut(meta) {
            if offset + buf.len() > file.data.len() {
                file.data.resize(offset + buf.len(), 0);
            }
            file.data[offset..offset + buf.len()].copy_from_slice(buf);
            Ok(buf.len())
        } else {
            Err(vars::SQLITE_IOERR_WRITE)
        }
    }

    fn read(&mut self, meta: &mut Self::Handle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        self.log(format_args!(
            "read: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
        let mut shared = self.shared();
        shared.hooks.read(*meta, offset, buf);
        if let Some(file) = shared.files.get(meta) {
            if offset > file.data.len() {
                return Ok(0);
            }
            let len = buf.len().min(file.data.len() - offset);
            buf[..len].copy_from_slice(&file.data[offset..offset + len]);
            Ok(len)
        } else {
            Err(vars::SQLITE_IOERR_READ)
        }
    }

    fn sync(&mut self, meta: &mut Self::Handle) -> VfsResult<()> {
        self.log(format_args!("sync: handle={:?}", meta));
        self.shared().hooks.sync(*meta);
        Ok(())
    }

    fn close(&mut self, meta: Self::Handle) -> VfsResult<()> {
        self.log(format_args!("close: handle={:?}", meta));
        let mut shared = self.shared();
        shared.hooks.close(meta);
        if let Some(file) = shared.files.get(&meta) {
            if file.delete_on_close {
                shared.files.remove(&meta);
            }
        }
        Ok(())
    }

    fn pragma(&mut self, meta: &mut Self::Handle, pragma: Pragma<'_>) -> VfsResult<Option<String>> {
        self.log(format_args!(
            "pragma: handle={:?} pragma={:?}",
            meta, pragma
        ));
        self.shared().hooks.pragma(*meta, pragma)
    }

    fn sector_size(&mut self) -> i32 {
        self.log(format_args!("sector_size"));
        self.shared().hooks.sector_size();
        DEFAULT_SECTOR_SIZE
    }

    fn device_characteristics(&mut self) -> i32 {
        self.log(format_args!("device_characteristics"));
        self.shared().hooks.device_characteristics();
        DEFAULT_DEVICE_CHARACTERISTICS
    }
}

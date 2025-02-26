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
    DEFAULT_DEVICE_CHARACTERISTICS, DEFAULT_SECTOR_SIZE, Pragma, PragmaErr, Vfs, VfsHandle,
    VfsResult,
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
    fn pragma(
        &mut self,
        handle: MockHandle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        Err(PragmaErr::NotFound)
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
    shared: Arc<Mutex<Shared>>,
}

struct Shared {
    next_id: usize,
    files: HashMap<MockHandle, File>,
    hooks: Box<dyn Hooks + Send>,
    log: Option<SqliteLogger>,
}

impl MockVfs {
    pub fn new(hooks: Box<dyn Hooks + Send>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                next_id: 0,
                files: HashMap::new(),
                hooks,
                log: None,
            })),
        }
    }

    fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }
}

impl Shared {
    fn log(&self, f: fmt::Arguments<'_>) {
        if let Some(log) = &self.log {
            let buf = format!("{f}");
            log.log(SqliteLogLevel::Notice, buf.as_bytes());
        } else {
            panic!("MockVfs is missing registered log handler")
        }
    }

    fn next_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

impl Vfs for MockVfs {
    // a simple usize that represents a file handle.
    type Handle = MockHandle;

    fn register_logger(&self, logger: SqliteLogger) {
        let mut shared = self.shared();
        shared.log = Some(logger);
    }

    fn canonical_path<'a>(&self, path: Cow<'a, str>) -> VfsResult<Cow<'a, str>> {
        let mut shared = self.shared();
        shared.log(format_args!("canonical_path: path={path:?}"));
        shared.hooks.canonical_path(&path);
        Ok(path)
    }

    fn open(&self, path: Option<&str>, opts: flags::OpenOpts) -> VfsResult<Self::Handle> {
        let mut shared = self.shared();
        shared.log(format_args!("open: path={path:?} opts={opts:?}"));
        shared.hooks.open(&path, &opts);

        let id = shared.next_id();
        let file_handle = MockHandle::new(id, opts.mode().is_readonly());

        if let Some(path) = path {
            // if file is already open return existing handle
            for (handle, file) in shared.files.iter() {
                if file.name == path {
                    return Ok(*handle);
                }
            }
            shared.files.insert(
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

    fn delete(&self, path: &str) -> VfsResult<()> {
        let mut shared = self.shared();
        shared.log(format_args!("delete: path={path:?}"));
        shared.hooks.delete(path);
        shared.files.retain(|_, file| file.name != path);
        Ok(())
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        let mut shared = self.shared();
        shared.log(format_args!("access: path={path:?} flags={flags:?}"));
        shared.hooks.access(path, flags);
        Ok(shared.files.values().any(|file| file.name == path))
    }

    fn file_size(&self, meta: &mut Self::Handle) -> VfsResult<usize> {
        let mut shared = self.shared();
        shared.log(format_args!("file_size: handle={meta:?}"));
        shared.hooks.file_size(*meta);
        Ok(shared.files.get(meta).map_or(0, |file| file.data.len()))
    }

    fn truncate(&self, meta: &mut Self::Handle, size: usize) -> VfsResult<()> {
        let mut shared = self.shared();
        shared.log(format_args!("truncate: handle={meta:?} size={size:?}"));
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

    fn write(&self, meta: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        let mut shared = self.shared();
        shared.log(format_args!(
            "write: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
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

    fn read(&self, meta: &mut Self::Handle, offset: usize, buf: &mut [u8]) -> VfsResult<usize> {
        let mut shared = self.shared();
        shared.log(format_args!(
            "read: handle={:?} offset={:?} buf.len={}",
            meta,
            offset,
            buf.len()
        ));
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

    fn sync(&self, meta: &mut Self::Handle) -> VfsResult<()> {
        let mut shared = self.shared();
        shared.log(format_args!("sync: handle={meta:?}"));
        shared.hooks.sync(*meta);
        Ok(())
    }

    fn close(&self, meta: Self::Handle) -> VfsResult<()> {
        let mut shared = self.shared();
        shared.log(format_args!("close: handle={meta:?}"));
        shared.hooks.close(meta);
        if let Some(file) = shared.files.get(&meta) {
            if file.delete_on_close {
                shared.files.remove(&meta);
            }
        }
        Ok(())
    }

    fn pragma(
        &self,
        meta: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        let mut shared = self.shared();
        shared.log(format_args!("pragma: handle={meta:?} pragma={pragma:?}"));
        shared.hooks.pragma(*meta, pragma)
    }

    fn sector_size(&self) -> i32 {
        let mut shared = self.shared();
        shared.log(format_args!("sector_size"));
        shared.hooks.sector_size();
        DEFAULT_SECTOR_SIZE
    }

    fn device_characteristics(&self) -> i32 {
        let mut shared = self.shared();
        shared.log(format_args!("device_characteristics"));
        shared.hooks.device_characteristics();
        DEFAULT_DEVICE_CHARACTERISTICS
    }
}

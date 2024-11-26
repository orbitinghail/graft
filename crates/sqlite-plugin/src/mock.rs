#![cfg(test)]

// tests use std
extern crate std;

use core::fmt::Display;
use std::boxed::Box;
use std::collections::HashMap;
use std::{string::String, vec::Vec};

use crate::flags::{self, AccessFlags, OpenOpts};
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
    fn canonical_path(&mut self, path: &str) {
        log::info!("canonical_path: path={:?}", path);
    }
    fn open(&mut self, path: &Option<String>, opts: &OpenOpts) {
        log::info!("open: path={:?} opts={:?}", path, opts);
    }
    fn delete(&mut self, path: &str) {
        log::info!("delete: path={:?}", path);
    }
    fn access(&mut self, path: &str, flags: AccessFlags) {
        log::info!("access: path={:?}", path);
    }
    fn file_size(&mut self, handle: MockHandle) {
        log::info!("file_size: handle={:?}", handle);
    }
    fn truncate(&mut self, handle: MockHandle, size: usize) {
        log::info!("truncate: handle={:?} size={:?}", handle, size);
    }
    fn write(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {
        log::info!(
            "write: handle={:?} offset={:?} buf.len={}",
            handle,
            offset,
            buf.len()
        );
    }
    fn read(&mut self, handle: MockHandle, offset: usize, buf: &[u8]) {
        log::info!(
            "read: handle={:?} offset={:?} buf.len={}",
            handle,
            offset,
            buf.len()
        );
    }
    fn sync(&mut self, handle: MockHandle) {
        log::info!("sync: handle={:?}", handle);
    }
    fn close(&mut self, handle: MockHandle) {
        log::info!("close: handle={:?}", handle);
    }
    fn pragma(&mut self, handle: MockHandle, pragma: Pragma<'_>) -> VfsResult<Option<String>> {
        log::info!("pragma: handle={:?} pragma={:?}", handle, pragma);
        Err(vars::SQLITE_NOTFOUND)
    }
    fn sector_size(&mut self) {
        log::info!("sector_size");
    }
    fn device_characteristics(&mut self) {
        log::info!("device_characteristics");
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
    pub next_id: usize,
    pub files: HashMap<MockHandle, File>,
    pub hooks: Box<dyn Hooks>,
}

impl Vfs for MockVfs {
    // a simple usize that represents a file handle.
    type Handle = MockHandle;

    fn canonical_path(&mut self, path: &str) -> VfsResult<String> {
        self.hooks.canonical_path(path);
        Ok(path.into())
    }

    fn open(&mut self, path: Option<String>, opts: flags::OpenOpts) -> VfsResult<Self::Handle> {
        self.hooks.open(&path, &opts);
        let id = self.next_id;
        self.next_id += 1;
        let file_handle = MockHandle::new(id, opts.mode().is_readonly());

        if let Some(path) = path {
            // if file is already open return existing handle
            for (handle, file) in &self.files {
                if file.name == path {
                    return Ok(*handle);
                }
            }
            self.files.insert(
                file_handle,
                File {
                    name: path,
                    data: Vec::new(),
                    delete_on_close: opts.delete_on_close(),
                },
            );
        }
        Ok(file_handle)
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        self.hooks.delete(path);
        self.files.retain(|_, file| file.name != path);
        Ok(())
    }

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        self.hooks.access(path, flags);
        Ok(self.files.values().any(|file| file.name == path))
    }

    fn file_size(&mut self, meta: &mut Self::Handle) -> VfsResult<usize> {
        self.hooks.file_size(*meta);
        Ok(self
            .files
            .get(meta)
            .map(|file| file.data.len())
            .unwrap_or(0))
    }

    fn truncate(&mut self, meta: &mut Self::Handle, size: usize) -> VfsResult<()> {
        self.hooks.truncate(*meta, size);
        if let Some(file) = self.files.get_mut(meta) {
            if size > file.data.len() {
                file.data.resize(size, 0);
            } else {
                file.data.truncate(size);
            }
        }
        Ok(())
    }

    fn write(&mut self, meta: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        self.hooks.write(*meta, offset, buf);
        if let Some(file) = self.files.get_mut(meta) {
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
        self.hooks.read(*meta, offset, buf);
        if let Some(file) = self.files.get(meta) {
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
        self.hooks.sync(*meta);
        Ok(())
    }

    fn close(&mut self, meta: &mut Self::Handle) -> VfsResult<()> {
        self.hooks.close(*meta);
        if let Some(file) = self.files.get(meta) {
            if file.delete_on_close {
                self.files.remove(meta);
            }
        }
        Ok(())
    }

    fn pragma(&mut self, meta: &mut Self::Handle, pragma: Pragma<'_>) -> VfsResult<Option<String>> {
        self.hooks.pragma(*meta, pragma)
    }

    fn sector_size(&mut self) -> i32 {
        self.hooks.sector_size();
        DEFAULT_SECTOR_SIZE
    }

    fn device_characteristics(&mut self) -> i32 {
        self.hooks.device_characteristics();
        DEFAULT_DEVICE_CHARACTERISTICS
    }
}

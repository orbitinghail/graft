use crate::flags::{AccessFlags, LockLevel, OpenOpts};
use crate::logger::SqliteLogger;
use crate::{ffi, vars};
use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::ffi::CString;
use alloc::string::String;
use core::mem::{self, size_of, ManuallyDrop, MaybeUninit};
use core::slice;
use core::{
    ffi::{c_char, c_int, c_void, CStr},
    ptr::null_mut,
};

const DEFAULT_MAX_PATH_LEN: i32 = 512;
pub const DEFAULT_SECTOR_SIZE: i32 = 4096;

pub const DEFAULT_DEVICE_CHARACTERISTICS: i32 =
    // writes of any size are atomic
    vars::SQLITE_IOCAP_ATOMIC |
    // after reboot following a crash or power loss, the only bytes in a file that were written
    // at the application level might have changed and that adjacent bytes, even bytes within
    // the same sector are guaranteed to be unchanged
    vars::SQLITE_IOCAP_POWERSAFE_OVERWRITE |
    // when data is appended to a file, the data is appended first then the size of the file is
    // extended, never the other way around
    vars::SQLITE_IOCAP_SAFE_APPEND |
    // information is written to disk in the same order as calls to xWrite()
    vars::SQLITE_IOCAP_SEQUENTIAL;

/// A SQLite3 extended error code
pub type SqliteErr = i32;

pub type VfsResult<T> = Result<T, SqliteErr>;

// FileWrapper needs to be repr(C) and have sqlite3_file as it's first member
// because it's a "subclass" of sqlite3_file
#[repr(C)]
struct FileWrapper<Handle> {
    file: ffi::sqlite3_file,
    vfs: *mut ffi::sqlite3_vfs,
    handle: MaybeUninit<Handle>,
}

struct AppData<Vfs> {
    base_vfs: *mut ffi::sqlite3_vfs,
    vfs: Vfs,
    io_methods: ffi::sqlite3_io_methods,
    sqlite_api: SqliteApi,
}

#[derive(Debug)]
pub struct Pragma<'a> {
    pub name: &'a str,
    pub arg: Option<&'a str>,
}

fn fallible(mut cb: impl FnMut() -> Result<i32, i32>) -> i32 {
    cb().unwrap_or_else(|err| err)
}

unsafe fn lossy_cstr<'a>(p: *const c_char) -> VfsResult<Cow<'a, str>> {
    p.as_ref()
        .map(|p| CStr::from_ptr(p).to_string_lossy())
        .ok_or(vars::SQLITE_INTERNAL)
}

// uses sqlite3_mprintf to allocate memory for the string using sqlite's memory allocator
// returns a pointer to the sqlite3 allocated string
// # Safety
// the returned pointer must be freed using sqlite3_free
fn sqlite3_mprintf(api: &SqliteApi, s: &str) -> VfsResult<*mut c_char> {
    let s = CString::new(s).map_err(|_| vars::SQLITE_INTERNAL)?;
    let p = unsafe { (api.mprintf)(s.as_ptr()) };
    if p.is_null() {
        Err(vars::SQLITE_NOMEM)
    } else {
        Ok(p)
    }
}

macro_rules! unwrap_appdata {
    ($p_vfs:expr) => {
        (*$p_vfs)
            .pAppData
            .cast::<AppData<T>>()
            .as_mut()
            .ok_or(vars::SQLITE_INTERNAL)
    };
}

macro_rules! unwrap_vfs {
    ($p_vfs:expr) => {
        unwrap_appdata!($p_vfs).map(|app_data| &mut app_data.vfs)
    };
}

macro_rules! unwrap_base_vfs {
    ($p_vfs:expr) => {
        unwrap_appdata!($p_vfs)
            .and_then(|app_data| app_data.base_vfs.as_mut().ok_or(vars::SQLITE_INTERNAL))
    };
}

macro_rules! unwrap_file {
    ($p_file:expr) => {
        $p_file
            .cast::<FileWrapper<T::Handle>>()
            .as_mut()
            .ok_or(vars::SQLITE_INTERNAL)
    };
}

pub trait VfsHandle {
    fn readonly(&self) -> bool;
    fn in_memory(&self) -> bool;
}

pub trait Vfs {
    type Handle: VfsHandle;

    /// Register the provided logger with this Vfs.
    /// This function is guaranteed to only be called once per
    /// register_{static,dynamic} call.
    fn register_logger(&mut self, logger: SqliteLogger);

    /// construct a canonical version of the given path
    fn canonical_path<'a>(&mut self, path: Cow<'a, str>) -> VfsResult<Cow<'a, str>> {
        Ok(path)
    }

    // file system operations
    fn open(&mut self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle>;
    fn delete(&mut self, path: &str) -> VfsResult<()>;
    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool>;

    // file operations
    fn file_size(&mut self, handle: &mut Self::Handle) -> VfsResult<usize>;
    fn truncate(&mut self, handle: &mut Self::Handle, size: usize) -> VfsResult<()>;
    fn write(&mut self, handle: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize>;
    fn read(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        buf: &mut [u8],
    ) -> VfsResult<usize>;

    #[allow(unused_variables)]
    fn lock(&mut self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn unlock(&mut self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn sync(&mut self, handle: &mut Self::Handle) -> VfsResult<()> {
        Ok(())
    }

    fn close(&mut self, handle: &mut Self::Handle) -> VfsResult<()>;

    #[allow(unused_variables)]
    fn pragma(
        &mut self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> VfsResult<Option<String>> {
        Err(vars::SQLITE_NOTFOUND)
    }

    // system queries
    fn sector_size(&mut self) -> i32 {
        DEFAULT_SECTOR_SIZE
    }

    fn device_characteristics(&mut self) -> i32 {
        DEFAULT_DEVICE_CHARACTERISTICS
    }
}

#[derive(Clone)]
pub(crate) struct SqliteApi {
    register: unsafe extern "C" fn(arg1: *mut ffi::sqlite3_vfs, arg2: c_int) -> c_int,
    find: unsafe extern "C" fn(arg1: *const c_char) -> *mut ffi::sqlite3_vfs,
    mprintf: unsafe extern "C" fn(arg1: *const c_char, ...) -> *mut c_char,
    log: unsafe extern "C" fn(arg1: c_int, arg2: *const c_char, ...),
}

impl SqliteApi {
    #[cfg(feature = "static")]
    fn new_static() -> Self {
        Self {
            register: ffi::sqlite3_vfs_register,
            find: ffi::sqlite3_vfs_find,
            mprintf: ffi::sqlite3_mprintf,
            log: ffi::sqlite3_log,
        }
    }

    #[cfg(feature = "dynamic")]
    fn new_dynamic(api: &ffi::sqlite3_api_routines) -> VfsResult<Self> {
        Ok(Self {
            register: api.vfs_register.ok_or(vars::SQLITE_INTERNAL)?,
            find: api.vfs_find.ok_or(vars::SQLITE_INTERNAL)?,
            mprintf: api.mprintf.ok_or(vars::SQLITE_INTERNAL)?,
            log: api.log.ok_or(vars::SQLITE_INTERNAL)?,
        })
    }
}

pub struct RegisterOpts {
    pub make_default: bool,
}

#[cfg(feature = "static")]
pub fn register_static<T: Vfs>(name: &str, vfs: T, opts: RegisterOpts) -> VfsResult<()> {
    register_inner(SqliteApi::new_static(), name, vfs, opts)
}

/// Register a vfs with SQLite using the dynamic API. This API is available when
/// SQLite is initializing extensions.
/// # Safety
/// p_api must be a valid, aligned pointer to a sqlite3_api_routines struct
#[cfg(feature = "dynamic")]
pub unsafe fn register_dynamic<T: Vfs>(
    p_api: *mut ffi::sqlite3_api_routines,
    name: &str,
    vfs: T,
    opts: RegisterOpts,
) -> VfsResult<()> {
    let api = p_api.as_ref().ok_or(vars::SQLITE_INTERNAL)?;
    let sqlite_api = SqliteApi::new_dynamic(api)?;
    register_inner(sqlite_api, name, vfs, opts)
}

fn register_inner<T: Vfs>(
    sqlite_api: SqliteApi,
    name: &str,
    mut vfs: T,
    opts: RegisterOpts,
) -> VfsResult<()> {
    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(x_close::<T>),
        xRead: Some(x_read::<T>),
        xWrite: Some(x_write::<T>),
        xTruncate: Some(x_truncate::<T>),
        xSync: Some(x_sync::<T>),
        xFileSize: Some(x_file_size::<T>),
        xLock: Some(x_lock::<T>),
        xUnlock: Some(x_unlock::<T>),
        xCheckReservedLock: None,
        xFileControl: Some(x_file_control::<T>),
        xSectorSize: Some(x_sector_size::<T>),
        xDeviceCharacteristics: Some(x_device_characteristics::<T>),
        xShmMap: None,
        xShmLock: None,
        xShmBarrier: None,
        xShmUnmap: None,
        xFetch: None,
        xUnfetch: None,
    };

    vfs.register_logger(SqliteLogger::new(sqlite_api.log));

    let p_name = ManuallyDrop::new(CString::new(name).map_err(|_| vars::SQLITE_INTERNAL)?).as_ptr();
    let base_vfs = unsafe { (sqlite_api.find)(null_mut()) };
    let vfs_register = sqlite_api.register;
    let p_appdata = Box::into_raw(Box::new(AppData { base_vfs, vfs, io_methods, sqlite_api }));

    let filewrapper_size: c_int = size_of::<FileWrapper<T::Handle>>()
        .try_into()
        .map_err(|_| vars::SQLITE_INTERNAL)?;

    let p_vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        iVersion: 3,
        szOsFile: filewrapper_size,
        mxPathname: DEFAULT_MAX_PATH_LEN,
        pNext: null_mut(),
        zName: p_name,
        pAppData: p_appdata.cast(),
        xOpen: Some(x_open::<T>),
        xDelete: Some(x_delete::<T>),
        xAccess: Some(x_access::<T>),
        xFullPathname: Some(x_full_pathname::<T>),
        xDlOpen: Some(x_dlopen::<T>),
        xDlError: Some(x_dlerror::<T>),
        xDlSym: Some(x_dlsym::<T>),
        xDlClose: Some(x_dlclose::<T>),
        xRandomness: Some(x_randomness::<T>),
        xSleep: Some(x_sleep::<T>),
        xCurrentTime: Some(x_current_time::<T>),
        xGetLastError: None,
        xCurrentTimeInt64: Some(x_current_time_int64::<T>),
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    }));

    let result = unsafe { vfs_register(p_vfs, opts.make_default.into()) };
    if result != vars::SQLITE_OK {
        // cleanup memory
        unsafe {
            drop(Box::from_raw(p_vfs));
            drop(Box::from_raw(p_appdata));
            drop(CString::from_raw(p_name as *mut i8));
        };
        Err(result)
    } else {
        Ok(())
    }
}

unsafe extern "C" fn x_open<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    p_file: *mut ffi::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    fallible(|| {
        let opts = flags.into();
        let name = lossy_cstr(z_name).ok();
        let vfs = unwrap_vfs!(p_vfs)?;
        let handle = vfs.open(name.as_ref().map(|s| s.as_ref()), opts)?;

        let out_file = unwrap_file!(p_file)?;
        let appdata = unwrap_appdata!(p_vfs)?;

        if let Some(p_out_flags) = p_out_flags.as_mut() {
            let mut out_flags = flags;
            if handle.readonly() {
                out_flags |= vars::SQLITE_OPEN_READONLY;
            }
            if handle.in_memory() {
                out_flags |= vars::SQLITE_OPEN_MEMORY;
            }
            *p_out_flags = out_flags;
        }

        out_file.file.pMethods = &appdata.io_methods;
        out_file.vfs = p_vfs;
        out_file.handle.write(handle);

        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_delete<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    _sync_dir: c_int,
) -> c_int {
    fallible(|| {
        let name = lossy_cstr(z_name)?;
        let vfs = unwrap_vfs!(p_vfs)?;
        vfs.delete(&name)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_access<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    flags: c_int,
    p_res_out: *mut c_int,
) -> c_int {
    fallible(|| {
        let name = lossy_cstr(z_name)?;
        let vfs = unwrap_vfs!(p_vfs)?;
        let result = vfs.access(&name, flags.into())?;
        let out = p_res_out.as_mut().ok_or(vars::SQLITE_IOERR_ACCESS)?;
        *out = result as i32;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_full_pathname<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    fallible(|| {
        let name = lossy_cstr(z_name)?;
        let vfs = unwrap_vfs!(p_vfs)?;
        let full_name = vfs.canonical_path(name)?;
        let n_out = n_out.try_into().map_err(|_| vars::SQLITE_INTERNAL)?;
        let out = slice::from_raw_parts_mut(z_out as *mut u8, n_out);
        let from = &full_name.as_bytes()[..full_name.len().min(n_out - 1)];
        // copy the name into the output buffer
        out[..from.len()].copy_from_slice(from);
        // add the trailing null byte
        out[from.len()] = 0;
        Ok(vars::SQLITE_OK)
    })
}

// file operations

unsafe extern "C" fn x_close<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        let handle = mem::replace(&mut file.handle, MaybeUninit::uninit());
        let mut handle = handle.assume_init();
        vfs.close(&mut handle)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_read<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        let buf_len: usize = i_amt.try_into().map_err(|_| vars::SQLITE_IOERR_READ)?;
        let offset: usize = i_ofst.try_into().map_err(|_| vars::SQLITE_IOERR_READ)?;
        let buf = slice::from_raw_parts_mut(buf.cast::<u8>(), buf_len);
        vfs.read(file.handle.assume_init_mut(), offset, buf)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_write<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        let buf_len: usize = i_amt.try_into().map_err(|_| vars::SQLITE_IOERR_WRITE)?;
        let offset: usize = i_ofst.try_into().map_err(|_| vars::SQLITE_IOERR_WRITE)?;
        let buf = slice::from_raw_parts(buf.cast::<u8>(), buf_len);
        let n = vfs.write(file.handle.assume_init_mut(), offset, buf)?;
        if n != buf_len {
            return Err(vars::SQLITE_IOERR_WRITE);
        }
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_truncate<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    size: ffi::sqlite_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        let size: usize = size.try_into().map_err(|_| vars::SQLITE_IOERR_TRUNCATE)?;
        vfs.truncate(file.handle.assume_init_mut(), size)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_sync<T: Vfs>(p_file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        vfs.sync(file.handle.assume_init_mut())?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_file_size<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    p_size: *mut ffi::sqlite3_int64,
) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        let size = vfs.file_size(file.handle.assume_init_mut())?;
        let p_size = p_size.as_mut().ok_or(vars::SQLITE_INTERNAL)?;
        *p_size = size.try_into().map_err(|_| vars::SQLITE_IOERR_FSTAT)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_lock<T: Vfs>(p_file: *mut ffi::sqlite3_file, raw_lock: c_int) -> c_int {
    fallible(|| {
        let level: LockLevel = raw_lock.into();
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        vfs.lock(file.handle.assume_init_mut(), level)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_unlock<T: Vfs>(p_file: *mut ffi::sqlite3_file, raw_lock: c_int) -> c_int {
    fallible(|| {
        let level: LockLevel = raw_lock.into();
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        vfs.unlock(file.handle.assume_init_mut(), level)?;
        Ok(vars::SQLITE_OK)
    })
}

unsafe extern "C" fn x_file_control<T: Vfs>(
    p_file: *mut ffi::sqlite3_file,
    op: c_int,
    p_arg: *mut c_void,
) -> c_int {
    /*
    Other interesting ops:
    SIZE_HINT: hint of how large the database will grow during the current transaction
    COMMIT_PHASETWO: after transaction commits before file unlocks (only used in WAL mode)
    VFS_NAME: should return this vfs's name + / + base vfs's name

    Atomic write support: (requires SQLITE_IOCAP_BATCH_ATOMIC device characteristic)
    Docs: https://www3.sqlite.org/cgi/src/technote/714f6cbbf78c8a1351cbd48af2b438f7f824b336
    BEGIN_ATOMIC_WRITE: start an atomic write operation
    COMMIT_ATOMIC_WRITE: commit an atomic write operation
    ROLLBACK_ATOMIC_WRITE: rollback an atomic write operation
    */

    if op == vars::SQLITE_FCNTL_PRAGMA {
        return fallible(|| {
            let file = unwrap_file!(p_file)?;
            let vfs = unwrap_vfs!(file.vfs)?;

            // p_arg is a pointer to an array of strings
            // the second value is the pragma name
            // the third value is either null or the pragma arg
            let args = p_arg.cast::<*const c_char>();
            let name = lossy_cstr(*args.add(1))?;
            let arg = (*args.add(2))
                .as_ref()
                .map(|p| CStr::from_ptr(p).to_string_lossy());
            let pragma = Pragma { name: &name, arg: arg.as_deref() };

            if let Some(result) = vfs.pragma(file.handle.assume_init_mut(), pragma)? {
                // now we need to write the result back to the first element of the args array.
                // SQLite is responsible for eventually freeing the result
                let appdata = unwrap_appdata!(file.vfs)?;
                *args = sqlite3_mprintf(&appdata.sqlite_api, &result)?;
            }

            Ok(vars::SQLITE_OK)
        });
    }
    vars::SQLITE_NOTFOUND
}

// system queries

unsafe extern "C" fn x_sector_size<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        Ok(vfs.sector_size())
    })
}

unsafe extern "C" fn x_device_characteristics<T: Vfs>(p_file: *mut ffi::sqlite3_file) -> c_int {
    fallible(|| {
        let file = unwrap_file!(p_file)?;
        let vfs = unwrap_vfs!(file.vfs)?;
        Ok(vfs.device_characteristics())
    })
}

// the following functions are wrappers around the base vfs functions

unsafe extern "C" fn x_dlopen<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_path: *const c_char,
) -> *mut c_void {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_dlopen) = vfs.xDlOpen {
            return x_dlopen(vfs, z_path);
        }
    }
    null_mut()
}

unsafe extern "C" fn x_dlerror<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_byte: c_int,
    z_err_msg: *mut c_char,
) {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_dlerror) = vfs.xDlError {
            x_dlerror(vfs, n_byte, z_err_msg);
        }
    }
}

unsafe extern "C" fn x_dlsym<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_handle: *mut c_void,
    z_symbol: *const c_char,
) -> Option<
    unsafe extern "C" fn(arg1: *mut ffi::sqlite3_vfs, arg2: *mut c_void, zSymbol: *const c_char),
> {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_dlsym) = vfs.xDlSym {
            return x_dlsym(vfs, p_handle, z_symbol);
        }
    }
    None
}

unsafe extern "C" fn x_dlclose<T: Vfs>(p_vfs: *mut ffi::sqlite3_vfs, p_handle: *mut c_void) {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_dlclose) = vfs.xDlClose {
            x_dlclose(vfs, p_handle);
        }
    }
}

unsafe extern "C" fn x_randomness<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_byte: c_int,
    z_out: *mut c_char,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_randomness) = vfs.xRandomness {
            return x_randomness(vfs, n_byte, z_out);
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_sleep<T: Vfs>(p_vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_sleep) = vfs.xSleep {
            return x_sleep(vfs, microseconds);
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_current_time<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_time: *mut f64,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_current_time) = vfs.xCurrentTime {
            return x_current_time(vfs, p_time);
        }
    }
    vars::SQLITE_INTERNAL
}

unsafe extern "C" fn x_current_time_int64<T: Vfs>(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_time: *mut i64,
) -> c_int {
    if let Ok(vfs) = unwrap_base_vfs!(p_vfs) {
        if let Some(x_current_time_int64) = vfs.xCurrentTimeInt64 {
            return x_current_time_int64(vfs, p_time);
        }
    }
    vars::SQLITE_INTERNAL
}

#[cfg(test)]
mod tests {
    // tests use std
    extern crate std;

    use super::*;
    use crate::{
        flags::{CreateMode, OpenKind, OpenMode},
        mock::*,
    };
    use alloc::vec::Vec;
    use rusqlite::{Connection, OpenFlags};
    use std::{boxed::Box, io::Write, println};

    fn log_handler(_: i32, arg2: &str) {
        println!("{arg2}");
    }

    #[test]
    fn sanity() -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            rusqlite::trace::config_log(Some(log_handler)).unwrap();
        }

        struct H {}
        impl Hooks for H {
            fn open(&mut self, path: &Option<&str>, opts: &OpenOpts) {
                let path = path.unwrap();
                if path == "main.db" {
                    assert!(!opts.delete_on_close());
                    assert_eq!(opts.kind(), OpenKind::MainDb);
                    assert_eq!(
                        opts.mode(),
                        OpenMode::ReadWrite { create: CreateMode::Create }
                    );
                } else if path == "main.db-journal" {
                    assert!(!opts.delete_on_close());
                    assert_eq!(opts.kind(), OpenKind::MainJournal);
                    assert_eq!(
                        opts.mode(),
                        OpenMode::ReadWrite { create: CreateMode::Create }
                    );
                } else {
                    panic!("unexpected path: {}", path);
                }
            }
        }

        let vfs = MockVfs::new(Box::new(H {}));
        register_static("mock", vfs, RegisterOpts { make_default: true })
            .map_err(|_| "failed to register vfs")?;

        // create a sqlite connection using the mock vfs
        let conn = Connection::open_with_flags_and_vfs(
            "main.db",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            "mock",
        )?;

        conn.execute("create table t (val int)", [])?;
        conn.execute("insert into t (val) values (1)", [])?;
        conn.execute("insert into t (val) values (2)", [])?;

        let n: i64 = conn.query_row("select sum(val) from t", [], |row| row.get(0))?;
        assert_eq!(n, 3);

        // the blob api is interesting and stress tests reading/writing pages and journaling
        conn.execute("create table b (data blob)", [])?;
        println!("inserting zero blob");
        conn.execute("insert into b values (zeroblob(8192))", [])?;
        let rowid = conn.last_insert_rowid();
        let mut blob = conn.blob_open(rusqlite::DatabaseName::Main, "b", "data", rowid, false)?;

        // write some data to the blob
        println!("writing to blob");
        let n = blob.write(b"hello")?;
        assert_eq!(n, 5);

        // query the table for the blob and print it
        let mut stmt = conn.prepare("select data from b")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let data: Vec<u8> = row.get(0)?;
            assert_eq!(&data[0..5], b"hello");
        }

        Ok(())
    }
}

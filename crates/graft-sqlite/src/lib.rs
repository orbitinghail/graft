use std::ffi::c_void;

use sqlite_plugin::{
    sqlite3_api_routines,
    vars::SQLITE_OK_LOAD_PERMANENTLY,
    vfs::{register_dynamic, RegisterOpts},
};
use vfs::GraftVfs;

mod vfs;

/// Register the VFS with SQLite.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_graft_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_void,
    p_api: *mut sqlite3_api_routines,
) -> std::os::raw::c_int {
    let vfs = GraftVfs::new();

    if let Err(err) = register_dynamic(
        p_api,
        "graft",
        vfs,
        RegisterOpts { init_logger: true, make_default: false },
    ) {
        return err;
    }
    SQLITE_OK_LOAD_PERMANENTLY
}

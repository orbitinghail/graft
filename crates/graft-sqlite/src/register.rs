use std::ffi::CString;

use graft::setup::{GraftConfig, InitErr, setup_graft};

/// Statically register the Graft SQLite extension with `SQLite`.
///
/// `vfs_name`: The name of the VFS to register with SQLite. Use `graft` if you're not sure.
/// `make_default`: If true, will set the Graft VFS to be used by default in SQLite.
/// `config`: Graft configuration.
pub fn register_static(
    vfs_name: &str,
    make_default: bool,
    config: GraftConfig,
) -> culprit::Result<(), InitErr> {
    let vfs_name = CString::new(vfs_name).expect("VFS name must not contain nul (0) bytes");
    let runtime = setup_graft(config)?;
    let vfs = crate::vfs::GraftVfs::new(runtime);
    let opts = sqlite_plugin::vfs::RegisterOpts { make_default };

    sqlite_plugin::vfs::register_static(vfs_name, vfs, opts).map_err(|err| {
        std::io::Error::other(format!(
            "failed to register Graft VFS, received error code {err}"
        ))
    })?;

    Ok(())
}

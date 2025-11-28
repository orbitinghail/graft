use graft_kernel::setup::setup_graft;

pub use graft_kernel::remote::RemoteConfig;
pub use graft_kernel::setup::{GraftConfig, InitErr};

/// Statically register the Graft SQLite extension with `SQLite`.
pub fn register_static(make_default: bool, config: GraftConfig) -> culprit::Result<(), InitErr> {
    let runtime = setup_graft(config)?;
    let vfs = crate::vfs::GraftVfs::new(runtime);
    let opts = sqlite_plugin::vfs::RegisterOpts { make_default };

    sqlite_plugin::vfs::register_static(c"graft".to_owned(), vfs, opts).map_err(|err| {
        std::io::Error::other(format!(
            "failed to register Graft VFS, received error code {err}"
        ))
    })?;

    Ok(())
}

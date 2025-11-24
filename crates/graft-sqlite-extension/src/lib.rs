use std::{
    borrow::Cow,
    ffi::c_void,
    fmt::Display,
    fs::OpenOptions,
    future::pending,
    num::NonZero,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use config::{Config, FileFormat};
use graft_kernel::{
    local::fjall_storage::FjallStorage, remote::RemoteConfig, rt::runtime::Runtime,
};
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{TracingConsumer, init_tracing_with_writer};
use serde::Deserialize;
use sqlite_plugin::{
    vars,
    vfs::{RegisterOpts, SqliteErr},
};

fn default_data_dir() -> PathBuf {
    platform_dirs::AppDirs::new(Some("graft"), true)
        .expect("must specify explicit data_dir on this platform")
        .data_dir
}

#[derive(Debug, Deserialize)]
struct ExtensionConfig {
    remote: RemoteConfig,

    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,

    log_file: Option<PathBuf>,

    #[serde(default = "bool::default")]
    make_default: bool,

    /// if set, specifies the autosync interval in seconds
    #[serde(default = "Option::default")]
    autosync: Option<NonZero<u64>>,
}

pub fn setup_log_file(path: PathBuf) {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("failed to open log file");

    init_tracing_with_writer(TracingConsumer::Tool, Mutex::new(file));
    tracing::info!("Log file opened");
}

struct InitErr(SqliteErr, Cow<'static, str>);

impl<T: Display> From<T> for InitErr {
    fn from(err: T) -> Self {
        InitErr(vars::SQLITE_INTERNAL, err.to_string().into())
    }
}

/// Write an error message to the `SQLite` error message pointer if it is not null.
#[cfg(feature = "dynamic")]
fn write_err_msg(
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
    msg: &str,
    out: *mut *const std::ffi::c_char,
) -> Result<(), SqliteErr> {
    if !out.is_null() {
        // SAFETY: p_api must be aligned and valid
        // SAFETY: out is not null
        unsafe {
            let p_api = p_api.as_ref().ok_or(vars::SQLITE_INTERNAL)?;
            let api = sqlite_plugin::vfs::SqliteApi::new_dynamic(p_api)?;
            api.mprintf(msg, out)?;
        }
    }
    Ok(())
}

fn resolve_config() -> Result<ExtensionConfig, InitErr> {
    // a priority ordered list of config paths, the first path found will be used
    let paths = [
        std::env::var("GRAFT_CONFIG").ok().map(|s| s.into()),
        Some("graft.toml".into()),
        platform_dirs::AppDirs::new(Some("graft"), true)
            .map(|app_dirs| app_dirs.config_dir.join("graft.toml")),
    ];

    // find the first path that is Some and resolves to a file
    let path = paths
        .into_iter()
        .flatten()
        .find(|p: &PathBuf| p.is_file())
        .and_then(|p| p.to_str().map(|s| s.to_string()));

    // build the config
    let mut config = Config::builder();

    // add the config file if it exists
    if let Some(path) = path {
        config = config.add_source(config::File::new(&path, FileFormat::Toml).required(true));
    }

    config = config.add_source(
        config::Environment::with_prefix("GRAFT")
            .prefix_separator("_")
            .separator("__"),
    );

    Ok(config.build()?.try_deserialize()?)
}

fn init_vfs() -> Result<(RegisterOpts, GraftVfs), InitErr> {
    let config = resolve_config()?;
    if let Some(path) = config.log_file {
        setup_log_file(path);
    }

    // spin up a tokio current thread runtime in a new thread
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let tokio_handle = rt.handle().clone();
    std::thread::Builder::new()
        .name("graft-runtime".to_string())
        .spawn(move || {
            // run the tokio event loop in this thread
            rt.block_on(pending::<()>())
        })?;

    let remote = Arc::new(config.remote.build()?);
    let storage = Arc::new(FjallStorage::open(config.data_dir)?);
    let autosync = config.autosync.map(|s| Duration::from_secs(s.get()));
    let runtime = Runtime::new(tokio_handle, remote, storage, autosync);

    Ok((
        RegisterOpts { make_default: config.make_default },
        GraftVfs::new(runtime),
    ))
}

/// Register the VFS with `SQLite`.
/// # Safety
/// This function must be called by sqlite's extension loading mechanism.
#[cfg(feature = "dynamic")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_graft_init(
    _db: *mut c_void,
    pz_err_msg: *mut *const std::ffi::c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> std::os::raw::c_int {
    match init_vfs().and_then(|(opts, vfs)| {
        if let Err(err) =
            // Safety: `p_api` must be a valid, aligned pointer to a `sqlite3_api_routines` struct
            unsafe {
                sqlite_plugin::vfs::register_dynamic(p_api, c"graft".to_owned(), vfs, opts)
            }
        {
            Err(InitErr(err, "Failed to register Graft VFS".into()))
        } else {
            Ok(())
        }
    }) {
        Ok(()) => sqlite_plugin::vars::SQLITE_OK_LOAD_PERMANENTLY,
        Err(err) => match write_err_msg(p_api, err.1.as_ref(), pz_err_msg) {
            Ok(()) => err.0,
            Err(e) => e,
        },
    }
}

/// Register the VFS with `SQLite`.
/// # Safety
/// This function must be passed a pointer to a valid SQLite db connection.
#[cfg(feature = "static")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn graft_static_init(_db: *mut c_void) -> std::os::raw::c_int {
    match init_vfs().and_then(|(opts, vfs)| {
        if let Err(err) = sqlite_plugin::vfs::register_static(c"graft".to_owned(), vfs, opts) {
            Err(InitErr(err, "Failed to register Graft VFS".into()))
        } else {
            Ok(())
        }
    }) {
        Ok(()) => 0,
        Err(err) => {
            let _ = err.1;
            err.0
        }
    }
}

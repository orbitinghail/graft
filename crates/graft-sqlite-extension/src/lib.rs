use std::{
    borrow::Cow,
    ffi::c_void,
    fmt::Display,
    fs::OpenOptions,
    future::pending,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use config::{Config, FileFormat};
use graft_kernel::{
    local::fjall_storage::FjallStorage, remote::RemoteConfig, rt::runtime_handle::RuntimeHandle,
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

    #[serde(default = "bool::default")]
    autosync: bool,
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

fn get_or_create_tokio_rt() -> Result<tokio::runtime::Handle, InitErr> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return Ok(handle);
    }

    // spin up a tokio runtime in a new thread
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("graft-runtime-worker")
        .enable_all()
        .build()?;
    let handle = rt.handle().clone();

    // tokio needs a top level "control" thread
    std::thread::Builder::new()
        .name("graft-runtime".to_string())
        .spawn(move || {
            // run the tokio runtime forever on this thread
            rt.block_on(pending::<()>())
        })?;

    Ok(handle)
}

fn init_vfs() -> Result<(RegisterOpts, GraftVfs), InitErr> {
    let files = [
        // load from the user's application dir first
        platform_dirs::AppDirs::new(Some("graft"), true)
            .map(|app_dirs| app_dirs.config_dir.join("graft.toml"))
            .map(|path| {
                config::File::new(path.to_str().unwrap(), FileFormat::Toml).required(false)
            }),
        // then try to load from the current directory
        Some(config::File::new("graft.toml", FileFormat::Toml).required(false)),
        // then load from GRAFT_CONFIG
        std::env::var("GRAFT_CONFIG")
            .ok()
            .map(|path| config::File::new(&path, FileFormat::Toml).required(true)),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let config = Config::builder()
        .add_source(files)
        .add_source(
            config::Environment::with_prefix("GRAFT")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    let config: ExtensionConfig = config.try_deserialize()?;

    if let Some(path) = config.log_file {
        setup_log_file(path);
    }

    let tokio_handle = get_or_create_tokio_rt()?;

    let remote = Arc::new(config.remote.build()?);
    let storage = Arc::new(FjallStorage::open(config.data_dir)?);
    let runtime = RuntimeHandle::spawn(&tokio_handle, remote, storage, config.autosync);

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

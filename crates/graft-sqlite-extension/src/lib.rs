use std::{
    borrow::Cow, error::Error, ffi::c_void, fs::OpenOptions, path::PathBuf, sync::Mutex,
    time::Duration,
};

use config::{Config, FileFormat};
use graft_client::{
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
    runtime::{runtime::Runtime, storage::Storage},
};
use graft_core::ClientId;
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{TracingConsumer, init_tracing_with_writer};
use serde::Deserialize;
use sqlite_plugin::{
    vars,
    vfs::{RegisterOpts, SqliteErr},
};
use url::Url;

fn default_metastore() -> Url {
    "http://127.0.0.1:3001".parse().unwrap()
}

fn default_pagestore() -> Url {
    "http://127.0.0.1:3000".parse().unwrap()
}

fn default_data_dir() -> PathBuf {
    platform_dirs::AppDirs::new(Some("graft"), true)
        .expect("must specify explicit data_dir on this platform")
        .data_dir
}

fn default_autosync() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct ExtensionConfig {
    #[serde(default = "default_metastore")]
    metastore: Url,

    #[serde(default = "default_pagestore")]
    pagestore: Url,

    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,

    #[serde(default = "default_autosync")]
    autosync: bool,

    #[serde(default = "ClientId::random")]
    client_id: ClientId,

    log_file: Option<PathBuf>,

    token: Option<String>,

    #[serde(default = "bool::default")]
    make_default: bool,
}

pub fn setup_log_file(path: PathBuf, cid: &ClientId) {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("failed to open log file");

    init_tracing_with_writer(TracingConsumer::Tool, Some(cid.short()), Mutex::new(file));
    tracing::info!("Log file opened");
}

struct InitErr(SqliteErr, Cow<'static, str>);

impl<E: Error> From<E> for InitErr {
    fn from(err: E) -> Self {
        InitErr(vars::SQLITE_INTERNAL, err.to_string().into())
    }
}

/// Write an error message to the SQLite error message pointer if it is not null.
/// Safety:
#[cfg(feature = "dynamic")]
fn write_err_msg(
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
    msg: &str,
    out: *mut *const std::ffi::c_char,
) -> Result<(), SqliteErr> {
    if !out.is_null() {
        unsafe {
            let p_api = p_api.as_ref().ok_or(vars::SQLITE_INTERNAL)?;
            let api = sqlite_plugin::vfs::SqliteApi::new_dynamic(p_api)?;
            api.mprintf(msg, out)?;
        }
    }
    Ok(())
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
        .add_source(config::Environment::with_prefix("GRAFT"))
        .build()?;

    let config: ExtensionConfig = config.try_deserialize()?;

    if let Some(path) = config.log_file {
        setup_log_file(path, &config.client_id);
    }

    let client = NetClient::new(config.token);
    let metastore_client = MetastoreClient::new(config.metastore, client.clone());
    let pagestore_client = PagestoreClient::new(config.pagestore, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage = Storage::open(config.data_dir).unwrap();
    let runtime = Runtime::new(config.client_id, clients, storage);

    runtime
        .start_sync_task(Duration::from_secs(1), 8, config.autosync, "graft-sync")
        .map_err(|c| c.into_err())?;

    Ok((
        RegisterOpts { make_default: config.make_default },
        GraftVfs::new(runtime),
    ))
}

/// Register the VFS with `SQLite`.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[cfg(feature = "dynamic")]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_graft_init(
    _db: *mut c_void,
    pz_err_msg: *mut *const std::ffi::c_char,
    p_api: *mut sqlite_plugin::sqlite3_api_routines,
) -> std::os::raw::c_int {
    match init_vfs().and_then(|(opts, vfs)| {
        if let Err(err) =
            unsafe { sqlite_plugin::vfs::register_dynamic(p_api, c"graft".to_owned(), vfs, opts) }
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
pub extern "C" fn graft_static_init(_db: *mut c_void) -> std::os::raw::c_int {
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

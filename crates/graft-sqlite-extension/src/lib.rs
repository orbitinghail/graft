use std::{ffi::c_void, fs::OpenOptions, path::PathBuf, sync::Mutex, time::Duration};

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
    sqlite3_api_routines,
    vars::SQLITE_OK_LOAD_PERMANENTLY,
    vfs::{RegisterOpts, register_dynamic},
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

/// Register the VFS with `SQLite`.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_graft_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_void,
    p_api: *mut sqlite3_api_routines,
) -> std::os::raw::c_int {
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
        .build()
        .expect("failed to load config");

    let config: ExtensionConfig = config
        .try_deserialize()
        .expect("failed to deserialize config");

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
        .unwrap();

    let vfs = GraftVfs::new(runtime);

    if let Err(err) =
        unsafe { register_dynamic(p_api, "graft", vfs, RegisterOpts { make_default: false }) }
    {
        return err;
    }
    SQLITE_OK_LOAD_PERMANENTLY
}

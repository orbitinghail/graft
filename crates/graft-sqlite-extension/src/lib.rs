use std::{env::temp_dir, ffi::c_void, time::Duration};

use graft_client::{
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
    runtime::{runtime::Runtime, storage::Storage},
};
use graft_core::ClientId;
use graft_sqlite::vfs::GraftVfs;
use sqlite_plugin::{
    sqlite3_api_routines,
    vars::SQLITE_OK_LOAD_PERMANENTLY,
    vfs::{RegisterOpts, register_dynamic},
};

/// Register the VFS with `SQLite`.
/// # Safety
/// This function should only be called by sqlite's extension loading mechanism.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_graft_init(
    _db: *mut c_void,
    _pz_err_msg: *mut *mut c_void,
    p_api: *mut sqlite3_api_routines,
) -> std::os::raw::c_int {
    let profile = std::env::var("GRAFT_PROFILE").unwrap_or_else(|_| "default".to_string());
    let root_dir =
        std::env::var("GRAFT_DIR").map_or_else(|_| temp_dir().join("graft-sqlite"), |d| d.into());
    let metastore =
        std::env::var("GRAFT_METASTORE").unwrap_or_else(|_| "http://127.0.0.1:3001".to_string());
    let pagestore =
        std::env::var("GRAFT_PAGESTORE").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string());
    let autosync = std::env::var("GRAFT_AUTOSYNC").map_or(true, |s| s == "1");

    let cid = ClientId::derive(profile.as_bytes());
    let metastore = metastore.parse().unwrap();
    let pagestore = pagestore.parse().unwrap();

    let client = NetClient::new();
    let metastore_client = MetastoreClient::new(metastore, client.clone());
    let pagestore_client = PagestoreClient::new(pagestore, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = root_dir.join(cid.pretty());
    let storage = Storage::open(&storage_path).unwrap();
    let runtime = Runtime::new(cid, clients, storage);

    runtime
        .start_sync_task(Duration::from_secs(1), 8, autosync, "graft-sync")
        .unwrap();

    let vfs = GraftVfs::new(runtime);

    if let Err(err) =
        unsafe { register_dynamic(p_api, "graft", vfs, RegisterOpts { make_default: false }) }
    {
        return err;
    }
    SQLITE_OK_LOAD_PERMANENTLY
}

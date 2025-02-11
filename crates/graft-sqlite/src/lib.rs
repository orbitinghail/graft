use std::{env::temp_dir, ffi::c_void, time::Duration};

use graft_client::{
    runtime::{fetcher::NetFetcher, runtime::Runtime, storage::Storage},
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
};
use graft_core::ClientId;
use sqlite_plugin::{
    sqlite3_api_routines,
    vars::SQLITE_OK_LOAD_PERMANENTLY,
    vfs::{register_dynamic, RegisterOpts},
};
use vfs::GraftVfs;

mod file;
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
    let cid = ClientId::derive("default".as_bytes());
    let metastore = "http://127.0.0.1:3001".parse().unwrap();
    let pagestore = "http://127.0.0.1:3000".parse().unwrap();

    let client = NetClient::new();
    let metastore_client = MetastoreClient::new(metastore, client.clone());
    let pagestore_client = PagestoreClient::new(pagestore, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = temp_dir().join("graft-sqlite").join(cid.pretty());
    let storage = Storage::open(&storage_path).unwrap();
    let runtime = Runtime::new(cid, NetFetcher::new(clients.clone()), storage);

    runtime
        .start_sync_task(clients, Duration::from_secs(1), 8)
        .unwrap();

    let vfs = GraftVfs::new(runtime);

    if let Err(err) = register_dynamic(p_api, "graft", vfs, RegisterOpts { make_default: false }) {
        return err;
    }
    SQLITE_OK_LOAD_PERMANENTLY
}

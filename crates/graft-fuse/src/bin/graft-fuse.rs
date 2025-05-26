use std::{path::PathBuf, time::Duration};

use clap::Parser;
use fuser::MountOption;
use graft_client::{
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
    runtime::{runtime::Runtime, storage::Storage},
};
use graft_core::{ClientId, VolumeId};
use graft_fuse::{dbfs::Dbfs, fusefs::FuseFs};
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{TracingConsumer, init_tracing};
use rusqlite::{Connection, OpenFlags};
use sqlite_plugin::vfs::{RegisterOpts, register_static};
use url::Url;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cmd {
    /// Mount the Graft FUSE filesystem at this path
    mount_point: PathBuf,

    /// An optional Graft Volume Id -> will generate a random VID if not set
    #[arg(long, short)]
    vid: Option<VolumeId>,
}

fn main() {
    let cid = ClientId::random();
    init_tracing(TracingConsumer::Tool, Some(cid.short()));

    let args = Cmd::parse();

    let metastore_url = Url::parse("http://127.0.0.1:3000").unwrap();
    let pagestore_url = Url::parse("http://127.0.0.1:3001").unwrap();

    let client = NetClient::new(None);
    let metastore_client = MetastoreClient::new(metastore_url, client.clone());
    let pagestore_client = PagestoreClient::new(pagestore_url, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage = Storage::open_temporary().unwrap();
    let runtime = Runtime::new(cid, clients.clone(), storage);

    runtime
        .start_sync_task(Duration::from_secs(1), 8, false, "graft-sync")
        .unwrap();

    register_static(
        c"graft".to_owned(),
        GraftVfs::new(runtime.clone()),
        RegisterOpts { make_default: false },
    )
    .expect("failed to register sqlite vfs");

    let vid = VolumeId::random();
    let db = Connection::open_with_flags_and_vfs(
        vid.pretty(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        "graft",
    )
    .unwrap();

    fuser::mount2(
        FuseFs::new(Dbfs::new(db)),
        args.mount_point,
        &[
            MountOption::RO,
            MountOption::FSName("graft".into()),
            MountOption::AutoUnmount,
            MountOption::AllowRoot,
            MountOption::DefaultPermissions,
        ],
    )
    .unwrap();
}

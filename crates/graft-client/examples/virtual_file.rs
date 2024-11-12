use std::str::FromStr;

use clap::{Parser, Subcommand};
use fjall::Config;
use graft_client::{MetaStoreClient, PageStoreClient};
use graft_core::VolumeId;
use graft_proto::{common::v1::Snapshot, metastore::v1::SnapshotRequest};
use prost::Message;
use reqwest::Url;
use tryiter::TryIteratorExt;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Open {
        name: String,
        vid: VolumeId,
    },
    Close {
        name: String,
    },
    Snapshot {
        name: String,
    },
    Write {
        name: String,
        offset: Option<usize>,
    },
    Read {
        name: String,
        offset: Option<usize>,
        len: Option<usize>,
    },
}

fn volume_key(name: &str) -> String {
    name.to_owned()
}

fn page_key_prefix(volume_id: &VolumeId) -> String {
    volume_id.pretty()
}

fn page_key(volume_id: &VolumeId, offset: u64) -> String {
    format!("{}/{:0>8}", volume_id.pretty(), offset)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let config = Config::new("/tmp/virtual_file_cache.db");
    let keyspace = fjall::Keyspace::open(config)?;
    let volumes = keyspace.open_partition("volumes", Default::default())?;
    let pages = keyspace.open_partition("pages", Default::default())?;

    let client = reqwest::Client::new();
    let pagestore = PageStoreClient::new(
        Url::from_str("http://localhost:3000/pagestore/v1/")?,
        client.clone(),
    );
    let metastore = MetaStoreClient::new(
        Url::from_str("http://localhost:3001/metastore/v1/")?,
        client,
    );

    match args.command {
        Commands::Open { name, vid } => {
            let snapshot = metastore
                .snapshot(SnapshotRequest { vid: vid.into(), lsn: None })
                .await?;

            let key = volume_key(&name);
            volumes.insert(&key, snapshot.encode_to_vec())?;

            println!("{}: {:?}", key, snapshot);
        }
        Commands::Close { name } => {
            let key = volume_key(&name);
            if let Some(snapshot) = volumes.get(key)? {
                let snapshot = Snapshot::decode(snapshot.as_ref())?;
                let mut scan = pages.prefix(page_key_prefix(snapshot.vid()?));
                while let Some((key, _)) = scan.try_next()? {
                    pages.remove(key)?;
                }
            }
        }
        Commands::Snapshot { name } => {
            let key = volume_key(&name);
            if let Some(snapshot) = volumes.get(key)? {
                let snapshot = Snapshot::decode(snapshot.as_ref())?;
                println!("{}: {:?}", name, snapshot);
            } else {
                println!("{}: not found", name);
            }
        }
        Commands::Write { name, offset } => {
            todo!()
        }
        Commands::Read { name, offset, len } => {
            todo!()
        }
    }

    Ok(())
}

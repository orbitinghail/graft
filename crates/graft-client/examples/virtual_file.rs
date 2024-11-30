use std::io::Read;

use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use fjall::Config;
use graft_client::{ClientBuilder, MetastoreClient, PagestoreClient};
use graft_core::{
    offset::Offset,
    page::{Page, EMPTY_PAGE, PAGESIZE},
    VolumeId,
};
use graft_proto::{common::v1::Snapshot, pagestore::v1::PageAtOffset};
use prost::Message;
use reqwest::Url;
use splinter::Splinter;
use tryiter::TryIteratorExt;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// The volume id to operate on
    vid: Option<VolumeId>,

    #[command(subcommand)]
    command: Commands,

    /// Specify a client id to differentiate between multiple clients
    #[arg(short, long)]
    client_id: Option<String>,

    /// Use localhost for the metastore and pagestore URLs
    #[arg(short, long)]
    localhost: bool,

    /// The metastore root URL (without any trailing path)
    #[arg(short, long, default_value = "https://graft-metastore.fly.dev")]
    metastore: Url,

    /// The pagestore root URL (without any trailing path)
    #[arg(short, long, default_value = "https://graft-pagestore.fly.dev")]
    pagestore: Url,
}

#[derive(Subcommand, PartialEq)]
enum Commands {
    /// Generate a new volume id
    New,
    /// Show the latest snapshot for a volume
    Show,
    /// Update the cache with the latest snapshot for a volume
    Pull,
    /// Remove a volume from the cache
    Remove,
    /// Write a page to a volume
    /// This synchronously writes to Graft and updates the cache
    Write {
        page: Option<Offset>,
        data: Option<BytesMut>,
    },
    /// Read a page from a volume
    /// This will read the page from Graft at the current LSN if it's not in the cache
    Read {
        page: Option<Offset>,

        #[arg(long)]
        latest: bool,
    },
}

fn page_key(volume_id: &VolumeId, offset: Offset) -> String {
    format!("{}/{:0>8}", volume_id.pretty(), offset)
}

struct Context {
    volumes: fjall::Partition,
    pages: fjall::Partition,
    metastore: MetastoreClient,
    pagestore: PagestoreClient,
}

async fn get_snapshot(ctx: &Context, vid: &VolumeId) -> anyhow::Result<Option<Snapshot>> {
    if let Some(snapshot) = get_cached_snapshot(ctx, vid)? {
        return Ok(Some(snapshot));
    }
    pull_snapshot(ctx, vid).await
}

fn get_cached_snapshot(ctx: &Context, vid: &VolumeId) -> anyhow::Result<Option<Snapshot>> {
    if let Some(snapshot) = ctx.volumes.get(vid)? {
        let snapshot = Snapshot::decode(snapshot.as_ref())?;
        return Ok(Some(snapshot));
    }
    Ok(None)
}

async fn pull_snapshot(ctx: &Context, vid: &VolumeId) -> anyhow::Result<Option<Snapshot>> {
    // pull starting at the next LSN after the last cached snapshot
    let start_lsn = get_cached_snapshot(ctx, vid)?
        .and_then(|s| s.lsn().next())
        .unwrap_or_default();

    match ctx.metastore.pull_offsets(vid, start_lsn..).await? {
        Some((snapshot, _, offsets)) => {
            // clear any changed offsets from the cache
            for offset in offsets.iter() {
                ctx.pages.remove(page_key(vid, offset))?;
            }

            // update the cache with the new snapshot
            let snapshot_bytes = snapshot.encode_to_vec();
            ctx.volumes.insert(vid, &snapshot_bytes)?;

            Ok(Some(snapshot))
        }
        None => Ok(None),
    }
}

fn remove(ctx: &Context, vid: &VolumeId) -> anyhow::Result<()> {
    ctx.volumes.remove(vid)?;
    // remove all pages for the volume
    let prefix = format!("{}/", vid.pretty());
    let mut scan = ctx.pages.prefix(&prefix);
    while let Some((key, _)) = scan.try_next()? {
        ctx.pages.remove(&key)?;
    }
    Ok(())
}

async fn read_page(ctx: &Context, vid: &VolumeId, page: Offset) -> anyhow::Result<Page> {
    // Check if we have the page in the cache
    if let Some(page) = ctx
        .pages
        .get(page_key(vid, page))?
        .map(|p| Page::try_from(p.as_ref()))
    {
        return Ok(page?);
    }

    // Otherwise read the page from Graft
    if let Some(snapshot) = get_snapshot(ctx, vid).await? {
        // if the page is larger then the last offset, return an empty page
        if page > snapshot.last_offset() {
            return Ok(EMPTY_PAGE);
        }

        let pages = ctx
            .pagestore
            .read_pages(
                vid,
                snapshot.lsn(),
                Splinter::from_iter([page]).serialize_to_bytes(),
            )
            .await?;

        if let Some(p) = pages.into_iter().next() {
            assert_eq!(p.offset, page, "unexpected page: {:?}", p);
            ctx.pages.insert(page_key(vid, p.offset), &p.data)?;
            return Ok(Page::try_from(p.data)?);
        }
    }

    Ok(EMPTY_PAGE)
}

async fn write_page(
    ctx: &Context,
    vid: &VolumeId,
    page: Offset,
    data: Bytes,
) -> anyhow::Result<()> {
    // remove the page from the cache in case the write fails
    ctx.pages.remove(page_key(vid, page))?;

    // read the current snapshot lsn
    let snapshot = get_snapshot(ctx, vid).await?;

    // first we upload the page to the page store
    let segments = ctx
        .pagestore
        .write_pages(vid, vec![PageAtOffset { offset: page, data: data.clone() }])
        .await?;

    // then we commit the new segments to the metastore
    let snapshot = ctx
        .metastore
        .commit(
            vid,
            snapshot.as_ref().map(|s| s.lsn()),
            snapshot.map(|s| s.last_offset().max(page)).unwrap_or(page),
            segments,
        )
        .await?;

    // Update the cache with the new page and snapshot
    ctx.volumes.insert(vid, snapshot.encode_to_vec())?;
    ctx.pages.insert(page_key(vid, page), &data)?;

    Ok(())
}

/// print all printable characters in the page
fn print_page(page: Page) {
    let mut is_empty = true;
    for &byte in page.iter() {
        // if byte is a printable ascii character
        if byte.is_ascii_alphanumeric() || byte.is_ascii_punctuation() || byte.is_ascii_whitespace()
        {
            is_empty = false;
            print!("{}", byte as char);
        }
    }
    if is_empty {
        print!("(empty page)");
    }
    println!();
}

fn print_snapshot(snapshot: Option<Snapshot>) {
    match snapshot {
        Some(snapshot) => {
            println!(
                "vid: {}",
                snapshot.vid().expect("failed to parse snapshot vid")
            );
            println!("lsn: {}", snapshot.lsn());
            println!("checkpoint: {}", snapshot.checkpoint());
            println!("last offset: {}", snapshot.last_offset());
            println!(
                "unix timestamp: {:?}",
                snapshot.timestamp.map(|t| t.seconds)
            );
        }
        None => println!("no snapshot found"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut args = Cli::parse();
    let client_id = args.client_id.unwrap_or_else(|| "default".to_string());

    let config = Config::new(format!("/tmp/virtual_file_cache_{client_id}"));
    let keyspace = fjall::Keyspace::open(config)?;

    if args.localhost {
        args.metastore = Url::parse("http://127.0.0.1:3001")?;
        args.pagestore = Url::parse("http://127.0.0.1:3000")?;
    }

    let ctx = Context {
        volumes: keyspace.open_partition("volumes", Default::default())?,
        pages: keyspace.open_partition("pages", Default::default())?,
        metastore: ClientBuilder::new(args.metastore).build()?,
        pagestore: ClientBuilder::new(args.pagestore).build()?,
    };

    let Some(vid) = args.vid else {
        if args.command == Commands::New {
            println!("{}", VolumeId::random());
            std::process::exit(0);
        } else {
            eprintln!("volume id is required, run with `new` to generate one");
            std::process::exit(1);
        }
    };

    match args.command {
        Commands::New => unreachable!("handled above"),
        Commands::Show => print_snapshot(get_snapshot(&ctx, &vid).await?),
        Commands::Pull => print_snapshot(pull_snapshot(&ctx, &vid).await?),
        Commands::Remove => {
            remove(&ctx, &vid)?;
            println!("removed volume {}", vid);
        }
        Commands::Write { page, data } => {
            let mut data = if let Some(data) = data {
                data
            } else {
                // gather up to PAGE_SIZE bytes from stdin
                let mut data = BytesMut::with_capacity(PAGESIZE.as_usize());
                let mut buf = [0; PAGESIZE.as_usize()];

                // loop until we have a full page or EOF
                while data.has_remaining_mut() {
                    let n = std::io::stdin().read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    data.put_slice(&buf[..n.min(data.remaining_mut())]);
                }

                data
            };

            // fill the remainder of the page with zeros
            data.resize(PAGESIZE.as_usize(), 0);

            write_page(&ctx, &vid, page.unwrap_or(0), data.freeze()).await?;
        }
        Commands::Read { page, latest } => {
            if latest {
                pull_snapshot(&ctx, &vid).await?;
            }
            print_page(read_page(&ctx, &vid, page.unwrap_or(0)).await?)
        }
    }

    Ok(())
}

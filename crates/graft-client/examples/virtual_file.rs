use std::io::{self, Read};
use std::ops::RangeBounds;
use std::str::FromStr;

use bytes::{BufMut, Bytes, BytesMut};
use clap::{Parser, Subcommand};
use culprit::{Culprit, ResultExt};
use fjall::Config;
use graft_client::{ClientBuildErr, ClientBuilder, MetastoreClient, PagestoreClient};
use graft_core::gid::{ClientId, GidParseErr};
use graft_core::lsn::LSN;
use graft_core::{
    page::{Page, PageSizeErr, EMPTY_PAGE, PAGESIZE},
    page_offset::PageOffset,
    VolumeId,
};
use graft_proto::{common::v1::Snapshot, pagestore::v1::PageAtOffset};
use graft_tracing::{tracing_init, TracingConsumer};
use prost::Message;
use splinter::Splinter;
use thiserror::Error;
use tryiter::TryIteratorExt;
use url::Url;

type Result<T> = std::result::Result<T, Culprit<CliErr>>;

#[derive(Error, Debug)]
enum CliErr {
    #[error("client error: {0}")]
    Client(#[from] graft_client::ClientErr),

    #[error("fjall error")]
    Fjall,

    #[error("prost decode error")]
    Prost,

    #[error("invalid page size")]
    PageSize(#[from] PageSizeErr),

    #[error("failed to build graft client")]
    ClientBuild(#[from] ClientBuildErr),

    #[error("url parse error")]
    UrlParse,

    #[error("io error")]
    Io(io::ErrorKind),

    #[error("gid parse error")]
    GidParseErr(#[from] GidParseErr),
}

impl From<fjall::Error> for CliErr {
    fn from(_: fjall::Error) -> Self {
        Self::Fjall
    }
}

impl From<prost::DecodeError> for CliErr {
    fn from(_: prost::DecodeError) -> Self {
        Self::Prost
    }
}

impl From<url::ParseError> for CliErr {
    fn from(_: url::ParseError) -> Self {
        Self::UrlParse
    }
}

impl From<io::Error> for CliErr {
    fn from(err: io::Error) -> Self {
        Self::Io(err.kind())
    }
}

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
    cid: Option<ClientId>,

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
        offset: Option<PageOffset>,
        data: Option<BytesMut>,
    },
    /// Read a page from a volume
    /// This will read the page from Graft at the current LSN if it's not in the cache
    Read {
        offset: Option<PageOffset>,

        #[arg(long)]
        latest: bool,
    },
}

fn page_key(volume_id: &VolumeId, offset: PageOffset) -> String {
    format!("{}/{:0>8}", volume_id.pretty(), offset)
}

struct Context {
    cid: ClientId,
    volumes: fjall::Partition,
    pages: fjall::Partition,
    metastore: MetastoreClient,
    pagestore: PagestoreClient,
}

fn get_snapshot(ctx: &Context, vid: &VolumeId) -> Result<Option<Snapshot>> {
    if let Some(snapshot) = get_cached_snapshot(ctx, vid)? {
        return Ok(Some(snapshot));
    }
    pull_snapshot(ctx, vid)
}

fn get_cached_snapshot(ctx: &Context, vid: &VolumeId) -> Result<Option<Snapshot>> {
    if let Some(snapshot) = ctx.volumes.get(vid)? {
        let snapshot = Snapshot::decode(snapshot.as_ref())?;
        return Ok(Some(snapshot));
    }
    Ok(None)
}

fn pull_snapshot(ctx: &Context, vid: &VolumeId) -> Result<Option<Snapshot>> {
    // pull starting at the next LSN after the last cached snapshot
    let start_lsn = get_cached_snapshot(ctx, vid)?
        .and_then(|s| s.lsn().expect("invalid LSN").next())
        .unwrap_or(LSN::FIRST);

    match ctx.metastore.pull_offsets(vid, start_lsn..).or_into_ctx()? {
        Some((snapshot, _, offsets)) => {
            // clear any changed offsets from the cache
            for offset in offsets.iter() {
                ctx.pages.remove(page_key(vid, offset.into()))?;
            }

            // update the cache with the new snapshot
            let snapshot_bytes = snapshot.encode_to_vec();
            ctx.volumes.insert(vid.as_ref(), snapshot_bytes)?;

            Ok(Some(snapshot))
        }
        None => Ok(None),
    }
}

fn remove(ctx: &Context, vid: &VolumeId) -> Result<()> {
    ctx.volumes.remove(vid.as_ref())?;
    // remove all pages for the volume
    let prefix = format!("{}/", vid.pretty());
    let mut scan = ctx.pages.prefix(&prefix);
    while let Some((key, _)) = scan.try_next()? {
        ctx.pages.remove(key)?;
    }
    Ok(())
}

fn read_page(ctx: &Context, vid: &VolumeId, offset: PageOffset) -> Result<Page> {
    // Check if we have the page in the cache
    if let Some(page) = ctx
        .pages
        .get(page_key(vid, offset))?
        .map(|p| Page::try_from(p.as_ref()))
    {
        return Ok(page.or_into_ctx()?);
    }

    // Otherwise read the page from Graft
    if let Some(snapshot) = get_snapshot(ctx, vid)? {
        // if the page is not contained by the snapshot, return an empty page
        if !snapshot.offsets().contains(&offset) {
            return Ok(EMPTY_PAGE);
        }

        let pages = ctx
            .pagestore
            .read_pages(
                vid,
                snapshot.lsn().expect("invalid LSN"),
                Splinter::from_iter([offset]).serialize_to_bytes(),
            )
            .or_into_ctx()?;

        if let Some(p) = pages.into_iter().next() {
            assert_eq!(offset, p.offset(), "unexpected page: {:?}", p);
            let page = p.page().or_into_ctx()?;
            ctx.pages
                .insert(page_key(vid, p.offset()), Bytes::from(page.clone()))?;
            return Ok(page);
        }
    }

    Ok(EMPTY_PAGE)
}

fn write_page(ctx: &Context, vid: &VolumeId, offset: PageOffset, data: Bytes) -> Result<()> {
    // remove the page from the cache in case the write fails
    ctx.pages.remove(page_key(vid, offset))?;

    // read the current snapshot lsn
    let snapshot = get_snapshot(ctx, vid)?;

    // first we upload the page to the page store
    let segments = ctx
        .pagestore
        .write_pages(
            vid,
            vec![PageAtOffset {
                offset: offset.into(),
                data: data.clone(),
            }],
        )
        .or_into_ctx()?;

    // then we commit the new segments to the metastore
    let snapshot = ctx
        .metastore
        .commit(
            vid,
            &ctx.cid,
            snapshot.as_ref().map(|s| s.lsn().expect("invalid LSN")),
            snapshot
                .map(|s| s.pages().max(offset.pages()))
                .unwrap_or(offset.pages()),
            segments,
        )
        .or_into_ctx()?;

    // Update the cache with the new page and snapshot
    ctx.volumes.insert(vid.as_ref(), snapshot.encode_to_vec())?;
    ctx.pages.insert(page_key(vid, offset), data)?;

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
            println!("lsn: {}", snapshot.lsn().expect("invalid LSN"));
            println!(
                "checkpoint: {}",
                snapshot.checkpoint().expect("invalid LSN")
            );
            println!("page count: {}", snapshot.pages());
            println!(
                "unix timestamp: {:?}",
                snapshot.timestamp.map(|t| t.seconds)
            );
        }
        None => println!("no snapshot found"),
    }
}

fn main() -> Result<()> {
    tracing_init(TracingConsumer::Tool, None);

    let mut args = Cli::parse();
    let default_cid = ClientId::from_str("QiAa1boZemVHi3G8puxCvR")?;
    let cid = args.cid.unwrap_or(default_cid);

    let config = Config::new(format!("/tmp/virtual_file_cache/{cid}"));
    let keyspace = fjall::Keyspace::open(config)?;

    if args.localhost {
        args.metastore = Url::parse("http://127.0.0.1:3001")?;
        args.pagestore = Url::parse("http://127.0.0.1:3000")?;
    }

    let ctx = Context {
        cid,
        volumes: keyspace.open_partition("volumes", Default::default())?,
        pages: keyspace.open_partition("pages", Default::default())?,
        metastore: ClientBuilder::new(args.metastore).build().or_into_ctx()?,
        pagestore: ClientBuilder::new(args.pagestore).build().or_into_ctx()?,
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
        Commands::Show => print_snapshot(get_snapshot(&ctx, &vid)?),
        Commands::Pull => print_snapshot(pull_snapshot(&ctx, &vid)?),
        Commands::Remove => {
            remove(&ctx, &vid)?;
            println!("removed volume {}", vid);
        }
        Commands::Write { offset, data } => {
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

            write_page(&ctx, &vid, offset.unwrap_or_default(), data.freeze())?;
        }
        Commands::Read { offset, latest } => {
            if latest {
                pull_snapshot(&ctx, &vid)?;
            }
            print_page(read_page(&ctx, &vid, offset.unwrap_or_default())?)
        }
    }

    Ok(())
}

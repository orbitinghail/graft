use std::{
    env::temp_dir,
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    str::FromStr,
    time::Duration,
};

use bytes::BytesMut;
use clap::{Parser, Subcommand};
use culprit::ResultExt;
use graft_client::{
    runtime::{
        fetcher::{Fetcher, NetFetcher},
        runtime::Runtime,
        storage::{
            volume_state::{SyncDirection, VolumeConfig},
            Storage, StorageErr,
        },
        sync::StartupErr,
        volume_reader::VolumeRead,
        volume_writer::{VolumeWrite, VolumeWriter},
    },
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
};
use graft_core::{
    gid::GidParseErr,
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
    ClientId, VolumeId,
};
use graft_tracing::{init_tracing, TracingConsumer};
use thiserror::Error;
use url::Url;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

type Result<T> = culprit::Result<T, CliErr>;

#[derive(Error, Debug)]
enum CliErr {
    #[error("client error: {0}")]
    Client(#[from] graft_client::ClientErr),

    #[error("gid parse error")]
    GidParseErr(#[from] GidParseErr),

    #[error("url parse error")]
    UrlParseErr(#[from] url::ParseError),

    #[error("graft storage error")]
    StorageErr(#[from] StorageErr),

    #[error("io error")]
    IoErr(#[from] std::io::Error),

    #[error("startup error")]
    StartupErr(#[from] StartupErr),
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// The volume id to operate on
    /// Uses a default VolumeId if not specified
    #[arg(short, long)]
    vid: Option<VolumeId>,

    /// Specify a client id to differentiate between multiple clients
    /// Uses a default ClientId if not specified
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

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq)]
enum Command {
    /// Reset local storage
    Reset,

    /// Push all local changes to the server
    Push,
    /// Pull changes from the server
    Pull {
        /// Overwrite any local changes
        #[arg(short, long)]
        reset: bool,
    },
    /// List all of the keys and values
    List,
    /// Set a key to a value
    Set { key: String, value: String },
    /// Get the value of a key
    Get { key: String },
}

struct PageView<T> {
    offset: PageOffset,
    page: BytesMut,
    _phantom: PhantomData<T>,
}

impl<T> PageView<T> {
    fn new(offset: impl Into<PageOffset>) -> Self {
        Self {
            offset: offset.into(),
            page: BytesMut::zeroed(PAGESIZE.as_usize()),
            _phantom: PhantomData,
        }
    }

    fn load(reader: &impl VolumeRead, offset: impl Into<PageOffset>) -> Result<Self> {
        let offset = offset.into();
        let page = reader.read(offset).or_into_ctx()?;
        Ok(Self {
            offset,
            page: page.into(),
            _phantom: PhantomData,
        })
    }

    fn zero(mut self) -> Self {
        self.page.clear();
        self.page.resize(PAGESIZE.as_usize(), 0);
        self
    }
}

impl<T: Debug + FromBytes + Immutable + KnownLayout> Debug for PageView<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: FromBytes + Immutable + KnownLayout> Deref for PageView<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        T::ref_from_bytes(&self.page).unwrap()
    }
}

impl<T: IntoBytes + FromBytes + Immutable + KnownLayout> DerefMut for PageView<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        T::mut_from_bytes(&mut self.page).unwrap()
    }
}

impl<T> Into<Page> for PageView<T> {
    fn into(self) -> Page {
        self.page
            .try_into()
            .expect("failed to convert PageView to Page")
    }
}

#[derive(Clone, IntoBytes, FromBytes, Immutable, KnownLayout)]
struct ListHeader {
    head: PageOffset,
    free: PageOffset,
    _padding: [u8; PAGESIZE.as_usize() - 8],
}

static_assertions::assert_eq_size!(ListHeader, [u8; PAGESIZE.as_usize()]);
type HeaderView = PageView<ListHeader>;

impl ListHeader {
    fn head(&self, reader: &impl VolumeRead) -> Result<Option<NodeView>> {
        if self.head == 0 {
            return Ok(None);
        }
        Ok(Some(NodeView::load(reader, self.head)?))
    }

    /// allocates a node by either reusing a previously freed node or
    /// creating a new one;
    fn allocate<F: Fetcher>(&mut self, writer: &VolumeWriter<F>) -> Result<NodeView> {
        let last_offset = writer.snapshot().and_then(|s| s.pages().last_offset());
        let unused_offset = last_offset.map_or(PageOffset::new(1), |o| o.next());

        if self.free == 0 {
            // no free nodes, create a new one
            return Ok(NodeView::new(unused_offset));
        } else {
            // pop the first node from the free list
            let node = NodeView::load(writer, self.free)?;
            self.free = node.next;
            return Ok(node.zero());
        }
    }
}

impl std::fmt::Debug for ListHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListHeader")
            .field("head", &self.head)
            .field("free", &self.free)
            .finish()
    }
}

#[derive(Clone, IntoBytes, FromBytes, Immutable, KnownLayout)]
struct ListNode {
    next: PageOffset,
    key_len: u32,
    value_len: u32,
    buf: [u8; PAGESIZE.as_usize() - 12],
}
static_assertions::assert_eq_size!(ListNode, [u8; PAGESIZE.as_usize()]);

impl ListNode {
    fn update(&mut self, key: &str, value: &str) {
        self.key_len = key.len() as u32;
        self.value_len = value.len() as u32;
        assert!(
            self.key_len + self.value_len < PAGESIZE.as_u32() - 12,
            "key and value too large"
        );
        self.buf[..key.len()].copy_from_slice(key.as_bytes());
        self.buf[key.len()..key.len() + value.len()].copy_from_slice(value.as_bytes());
    }

    fn key(&self) -> &str {
        let end = self.key_len as usize;
        std::str::from_utf8(&self.buf[..end]).unwrap()
    }

    fn value(&self) -> &str {
        let start = self.key_len as usize;
        let end = start + self.value_len as usize;
        std::str::from_utf8(&self.buf[start..end]).unwrap()
    }

    fn next(&self, reader: &impl VolumeRead) -> Result<Option<NodeView>> {
        if self.next == 0 {
            return Ok(None);
        }
        Ok(Some(NodeView::load(reader, self.next)?))
    }
}

impl Debug for ListNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListNode")
            .field("next", &self.next)
            .field("key", &self.key())
            .field("value", &self.value())
            .finish()
    }
}

type NodeView = PageView<ListNode>;

struct ListIter<'a, R> {
    reader: &'a R,
    cursor: Option<NodeView>,
}

impl<'a, R: VolumeRead> ListIter<'a, R> {
    fn new(reader: &'a R) -> Result<Self> {
        let header = HeaderView::load(reader, 0)?;
        let cursor = header.head(reader)?;
        Ok(Self { reader, cursor })
    }

    fn try_next(&mut self) -> Result<Option<NodeView>> {
        if let Some(current) = self.cursor.take() {
            self.cursor = current.next(self.reader)?;
            Ok(Some(current))
        } else {
            Ok(None)
        }
    }
}

impl<'a, R: VolumeRead> Iterator for ListIter<'a, R> {
    type Item = Result<NodeView>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

fn list_get(reader: &impl VolumeRead, key: &str) -> Result<Option<NodeView>> {
    let mut iter = ListIter::new(reader)?;
    while let Some(node) = iter.try_next().or_into_ctx()? {
        if node.key() == key {
            return Ok(Some(node));
        } else if node.key() > key {
            break;
        }
    }
    Ok(None)
}

fn list_set<F: Fetcher>(writer: &mut VolumeWriter<F>, key: &str, value: &str) -> Result<()> {
    let mut header = HeaderView::load(writer, 0)?;

    // find the insert or update position, while keeping track of the last node
    let mut cursor = header.head(writer)?;
    while let Some(current) = &cursor {
        if let Some(next) = current.next(writer)? {
            if key < next.key() {
                break;
            }
            cursor = Some(next);
        } else {
            break;
        }
    }

    match cursor {
        // cursor missing, list is empty
        None => {
            let mut node = header.allocate(writer)?;
            node.update(key, value);
            header.head = node.offset;
            writer.write(node.offset, node.into());
            writer.write(0, header.into());
        }

        // node at cursor is the search key
        Some(mut node) if node.key() == key => {
            node.update(key, value);
            writer.write(node.offset, node.into());
        }

        // node at cursor is after the search key
        // -> this means cursor is the first node in the list
        Some(node) if node.key() > key => {
            assert_eq!(header.head, node.offset, "cursor must be at the list head");

            let mut next = header.allocate(writer)?;
            next.update(key, value);
            next.next = node.offset;
            header.head = next.offset;
            writer.write(node.offset, node.into());
            writer.write(next.offset, next.into());
            writer.write(0, header.into());
        }

        // insert key after cursor
        Some(mut node) => {
            let mut next = header.allocate(writer)?;
            next.update(key, value);
            next.next = node.next;
            node.next = next.offset;
            writer.write(node.offset, node.into());
            writer.write(next.offset, next.into());
            writer.write(0, header.into());
        }
    }

    Ok(())
}

// fn list_remove<F: Fetcher>(writer: &mut VolumeWriter<F>, key: &str) -> Result<bool> {
//     let mut header = HeaderView::load(writer, 0)?;
//     todo!("implement remove")
// }

fn main() -> Result<()> {
    init_tracing(TracingConsumer::Tool, None);

    let default_vid = VolumeId::from_str("GontkHa6QVLMYfkyt16wUP")?;
    let default_cid = ClientId::from_str("QiAa1boZemVHi3G8puxCvR")?;

    let mut args = Cli::parse();
    let vid = args.vid.unwrap_or(default_vid);
    let cid = args.cid.unwrap_or(default_cid);

    if args.localhost {
        args.metastore = "http://127.0.0.1:3001".parse()?;
        args.pagestore = "http://127.0.0.1:3000".parse()?;
    }

    let client = NetClient::new();
    let metastore_client = MetastoreClient::new(args.metastore, client.clone());
    let pagestore_client = PagestoreClient::new(args.pagestore, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = temp_dir().join("graft-kv").join(cid.pretty());
    let storage = Storage::open(&storage_path).or_into_ctx()?;
    let runtime = Runtime::new(cid, NetFetcher::new(clients.clone()), storage);
    runtime
        .start_sync_task(clients, Duration::from_secs(1), 8)
        .or_into_ctx()?;

    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .or_into_ctx()?;

    match args.command {
        Command::Reset => {
            drop(runtime);
            std::fs::remove_dir_all(storage_path).or_into_ctx()?;
        }

        Command::Push => handle.sync_with_remote(SyncDirection::Push).or_into_ctx()?,
        Command::Pull { reset } => {
            if reset {
                handle.reset_to_remote().or_into_ctx()?
            } else {
                handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;
            }
        }
        Command::List => {
            let reader = handle.reader().or_into_ctx()?;
            let iter = ListIter::new(&reader).or_into_ctx()?;
            for node in iter {
                let node = node.or_into_ctx()?;
                println!("{}: {}", node.key(), node.value());
            }
        }
        Command::Set { key, value } => {
            let mut writer = handle.writer().or_into_ctx()?;
            list_set(&mut writer, &key, &value).or_into_ctx()?;
            writer.commit().or_into_ctx()?;
        }
        Command::Get { key } => {
            let reader = handle.reader().or_into_ctx()?;
            let node = list_get(&reader, &key).or_into_ctx()?;
            if let Some(node) = node {
                println!("{}", node.value());
            } else {
                println!("key not found");
            }
        }
    }

    Ok(())
}

//! Implements a silly key-value store on top of graft.
//! It is silly because it stores a single key per page, and organizes the pages
//! into a sorted linked list.
//! It is useful, however, to quickly sanity test Graft's functionality and get
//! a feeling for how it behaves in different scenarios.

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
use tryiter::TryIteratorExt;
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

    /// Remove a key from the list
    Del { key: String },

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
    fn allocate(&mut self, reader: &impl VolumeRead) -> Result<NodeView> {
        let last_offset = reader.snapshot().and_then(|s| s.pages().last_offset());
        let unused_offset = last_offset.map_or(PageOffset::new(1), |o| o.next());

        if self.free == 0 {
            // no free nodes, create a new one
            return Ok(NodeView::new(unused_offset));
        } else {
            // pop the first node from the free list
            let node = NodeView::load(reader, self.free)?;
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

/// find the last node in the list matching the predicate
/// terminates as soon as the predicate returns false
fn list_find_last<V: VolumeRead, P: FnMut(&str) -> bool>(
    reader: &V,
    mut pred: P,
) -> Result<Option<NodeView>> {
    let mut iter = ListIter::new(reader)?;
    let mut last_valid = None;
    while let Some(cursor) = iter.try_next().or_into_ctx()? {
        if !pred(cursor.key()) {
            return Ok(last_valid);
        }
        last_valid = Some(cursor);
    }
    Ok(last_valid)
}

fn list_get(reader: &impl VolumeRead, key: &str) -> Result<Option<NodeView>> {
    let iter = ListIter::new(reader)?;
    iter.try_filter(|n| Ok(n.key() == key))
        .try_next()
        .or_into_ctx()
}

fn list_set<F: Fetcher>(writer: &mut VolumeWriter<F>, key: &str, value: &str) -> Result<()> {
    let mut header = HeaderView::load(writer, 0)?;

    // either find the node to update, or find the insertion point
    let candidate = list_find_last(writer, |candidate| candidate <= key)?;
    match candidate {
        // candidate missing, insert new node at head of list
        None => {
            let mut new_node = header.allocate(writer)?;
            new_node.update(key, value);
            new_node.next = header.head;
            header.head = new_node.offset;
            writer.write(new_node.offset, new_node.into());
            writer.write(0, header.into());
        }

        // candidate matches search key, update node in place
        Some(mut candidate) if candidate.key() == key => {
            candidate.update(key, value);
            writer.write(candidate.offset, candidate.into());
        }

        // candidate is the last node in the list with key < search key
        // insert node after candidate
        Some(mut candidate) => {
            let mut new_node = header.allocate(writer)?;
            new_node.update(key, value);
            new_node.next = candidate.next;
            candidate.next = new_node.offset;
            writer.write(candidate.offset, candidate.into());
            writer.write(new_node.offset, new_node.into());
            writer.write(0, header.into());
        }
    }

    Ok(())
}

fn list_remove<F: Fetcher>(writer: &mut VolumeWriter<F>, key: &str) -> Result<bool> {
    let mut header = HeaderView::load(writer, 0)?;

    // find the node immediately before the node to remove (if it exists)
    if let Some(mut prev) = list_find_last(writer, |candidate| candidate < key)? {
        // check if the next node is the one we want to remove
        if let Some(mut next) = prev.next(writer)? {
            if next.key() == key {
                prev.next = next.next;
                next.next = header.free;
                header.free = next.offset;
                writer.write(next.offset, next.into());
                writer.write(prev.offset, prev.into());
                writer.write(0, header.into());
                return Ok(true);
            }
        }
    } else {
        // check if the head node is the one we want to remove
        if let Some(mut head) = header.head(writer)? {
            if head.key() == key {
                header.head = head.next;
                head.next = header.free;
                header.free = head.offset;
                writer.write(head.offset, head.into());
                writer.write(0, header.into());
                return Ok(true);
            }
        }
    }
    return Ok(false);
}

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
        Command::Del { key } => {
            let mut writer = handle.writer().or_into_ctx()?;
            if list_remove(&mut writer, &key).or_into_ctx()? {
                writer.commit().or_into_ctx()?;
            } else {
                println!("key not found");
            }
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

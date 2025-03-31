//! Implements a silly key-value store on top of graft.
//! It is silly because it stores a single key per page, and organizes the pages
//! into a sorted linked list.
//! It is useful, however, to quickly sanity test Graft's functionality and get
//! a feeling for how it behaves in different scenarios.

use std::{
    cell::RefCell,
    env::temp_dir,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    time::Duration,
};

use bytes::BytesMut;
use clap::{Parser, Subcommand};
use culprit::ResultExt;
use graft_client::{
    ClientPair, MetastoreClient, NetClient, PagestoreClient,
    oracle::LeapOracle,
    runtime::{
        runtime::Runtime,
        storage::{
            Storage, StorageErr,
            volume_state::{SyncDirection, VolumeConfig},
        },
        sync::StartupErr,
        volume_handle::VolumeHandle,
        volume_reader::VolumeRead,
        volume_writer::{VolumeWrite, VolumeWriter},
    },
};
use graft_core::{
    ClientId, PageIdx, VolumeId,
    gid::GidParseErr,
    page::{PAGESIZE, Page},
    pageidx,
    zerocopy_ext::ZerocopyErr,
};
use graft_tracing::{TracingConsumer, init_tracing};
use rand::Rng;
use thiserror::Error;
use tryiter::TryIteratorExt;
use url::Url;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

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

    #[error(transparent)]
    ZerocopyErr(#[from] ZerocopyErr),
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// The volume id to operate on
    /// Uses a default `VolumeId` if not specified
    #[arg(short, long)]
    vid: VolumeId,

    /// Specify a client name to differentiate between multiple clients
    #[arg(short, long, default_value = "default")]
    client_name: String,

    /// Connect to graft running on fly.dev
    #[arg(long)]
    fly: bool,

    /// The metastore root URL (without any trailing path)
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    metastore: Url,

    /// The pagestore root URL (without any trailing path)
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    pagestore: Url,

    /// The API key to use when communicating with the metastore and pagestore
    #[arg(long)]
    token: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq)]
enum Command {
    /// Reset local storage
    Reset,

    /// Print out info regarding the current Graft and linked-list state
    Status,

    /// Run a simulator that executes a random stream of kv operations for a
    /// configurable number of ticks
    Sim {
        /// The number of ticks to run the simulator for
        #[arg(short, long, default_value = "10")]
        ticks: u32,
    },

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

#[derive(Debug)]
struct PageView<T> {
    idx: PageIdx,
    inner: T,
}

impl<T: Default + TryFromBytes + IntoBytes + Immutable> PageView<T> {
    fn new(idx: PageIdx) -> Self {
        Self { idx, inner: T::default() }
    }

    fn load(reader: &impl VolumeRead, idx: PageIdx) -> Result<Self> {
        // use a thread local oracle
        thread_local! {
            static ORACLE: RefCell<LeapOracle> = RefCell::new(LeapOracle::default());
        }
        let page = ORACLE.with_borrow_mut(|o| reader.read(o, idx).or_into_ctx())?;

        let inner = if page.is_empty() {
            T::default()
        } else {
            T::try_read_from_bytes(&page[..size_of::<T>()]).map_err(ZerocopyErr::from)?
        };
        Ok(Self { idx, inner })
    }

    fn save(self, writer: &mut impl VolumeWrite) {
        let mut page = BytesMut::from(self.inner.as_bytes());
        page.resize(PAGESIZE.as_usize(), 0);
        writer.write(self.idx, Page::try_from(page.freeze()).unwrap());
    }

    fn clear(mut self) -> Self {
        self.inner = T::default();
        self
    }
}

impl<T> Deref for PageView<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for PageView<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout, Default, Debug)]
#[repr(C)]
struct ListHeader {
    head: PageIdx,
    free: PageIdx,
}

type HeaderView = PageView<ListHeader>;

impl ListHeader {
    fn head(&self, reader: &impl VolumeRead) -> Result<Option<NodeView>> {
        if self.head.is_first_page() {
            return Ok(None);
        }
        Ok(Some(NodeView::load(reader, self.head)?))
    }

    /// allocates a node by either reusing a previously freed node or
    /// creating a new one;
    fn allocate(&mut self, reader: &impl VolumeRead) -> Result<NodeView> {
        let last_index = reader.snapshot().and_then(|s| s.pages().last_index());
        let unused_index = last_index.map_or(pageidx!(2), |o| o.saturating_next());

        if self.free.is_first_page() {
            // no free nodes, create a new one
            Ok(NodeView::new(unused_index))
        } else {
            // pop the first node from the free list
            let node = NodeView::load(reader, self.free)?;
            self.free = node.next;
            Ok(node.clear())
        }
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout, Debug)]
#[repr(C)]
struct ListNode {
    next: PageIdx,
    key_len: u32,
    value_len: u32,
    buf: [u8; PAGESIZE.as_usize() - 12],
}

impl Default for ListNode {
    fn default() -> Self {
        Self {
            next: PageIdx::FIRST,
            key_len: 0,
            value_len: 0,
            buf: [0; PAGESIZE.as_usize() - 12],
        }
    }
}

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
        if self.next.is_first_page() {
            return Ok(None);
        }
        Ok(Some(NodeView::load(reader, self.next)?))
    }
}

type NodeView = PageView<ListNode>;

struct ListIter<'a, R> {
    reader: &'a R,
    cursor: Option<NodeView>,
}

impl<'a, R: VolumeRead> ListIter<'a, R> {
    fn new(reader: &'a R) -> Result<Self> {
        let header = HeaderView::load(reader, PageIdx::FIRST)?;
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

impl<R: VolumeRead> Iterator for ListIter<'_, R> {
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

fn list_set(writer: &mut VolumeWriter, key: &str, value: &str) -> Result<()> {
    let mut header = HeaderView::load(writer, PageIdx::FIRST)?;

    // either find the node to update, or find the insertion point
    let candidate = list_find_last(writer, |candidate| candidate <= key)?;
    match candidate {
        // candidate missing, insert new node at head of list
        None => {
            let mut new_node = header.allocate(writer)?;
            new_node.update(key, value);
            new_node.next = header.head;
            header.head = new_node.idx;
            new_node.save(writer);
            header.save(writer);
        }

        // candidate matches search key, update node in place
        Some(mut candidate) if candidate.key() == key => {
            candidate.update(key, value);
            candidate.save(writer);
        }

        // candidate is the last node in the list with key < search key
        // insert node after candidate
        Some(mut candidate) => {
            let mut new_node = header.allocate(writer)?;
            new_node.update(key, value);
            new_node.next = candidate.next;
            candidate.next = new_node.idx;
            candidate.save(writer);
            new_node.save(writer);
            header.save(writer);
        }
    }

    Ok(())
}

fn list_remove(writer: &mut VolumeWriter, key: &str) -> Result<bool> {
    let mut header = HeaderView::load(writer, PageIdx::FIRST)?;

    // find the node immediately before the node to remove (if it exists)
    if let Some(mut prev) = list_find_last(writer, |candidate| candidate < key)? {
        // check if the next node is the one we want to remove
        if let Some(mut next) = prev.next(writer)? {
            if next.key() == key {
                prev.next = next.next;
                next.next = header.free;
                header.free = next.idx;
                next.save(writer);
                prev.save(writer);
                header.save(writer);
                return Ok(true);
            }
        }
    } else {
        // check if the head node is the one we want to remove
        if let Some(mut head) = header.head(writer)? {
            if head.key() == key {
                header.head = head.next;
                head.next = header.free;
                header.free = head.idx;
                head.save(writer);
                header.save(writer);
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct Simulator {
    handle: VolumeHandle,
    ticks: u32,
}

impl Simulator {
    fn new(handle: VolumeHandle, ticks: u32) -> Self {
        Self { handle, ticks }
    }

    fn run(&mut self) -> Result<()> {
        let mut rng = rand::rng();

        const KEYS: Range<u8> = 0..32;
        fn gen_key(rng: &mut impl rand::RngCore) -> String {
            let key = rng.random_range(KEYS);
            format!("{key:0>2}")
        }

        for _ in 0..self.ticks {
            if rng.random_bool(0.5) {
                // set a key at random
                let key = gen_key(&mut rng);
                let val = rng.random::<u8>().to_string();
                let mut writer = self.handle.writer().or_into_ctx()?;
                list_set(&mut writer, &key, &val).or_into_ctx()?;
                writer.commit().or_into_ctx()?;
                println!("set {key} = {val}");
            } else {
                // del a key at random
                let key = gen_key(&mut rng);
                let mut writer = self.handle.writer().or_into_ctx()?;
                if list_remove(&mut writer, &key).or_into_ctx()? {
                    println!("del {key}");
                    writer.commit().or_into_ctx()?;
                }
            }
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    init_tracing(TracingConsumer::Tool, None);

    let mut args = Cli::parse();
    let vid = args.vid;
    let cid = ClientId::derive(args.client_name.as_bytes());
    tracing::info!("client: {cid}, volume: {vid}");

    if args.fly {
        args.metastore = "https://graft-metastore.fly.dev".parse()?;
        args.pagestore = "https://graft-pagestore.fly.dev".parse()?;
    }

    let client = NetClient::new(args.token);
    let metastore_client = MetastoreClient::new(args.metastore, client.clone());
    let pagestore_client = PagestoreClient::new(args.pagestore, client.clone());
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage_path = temp_dir().join("silly-kv").join(cid.pretty());
    let storage = Storage::open(&storage_path).or_into_ctx()?;
    let runtime = Runtime::new(cid, clients, storage);
    runtime
        .start_sync_task(Duration::from_secs(1), 8, true, "graft-sync")
        .or_into_ctx()?;

    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Disabled))
        .or_into_ctx()?;

    match args.command {
        Command::Reset => {
            drop(runtime);
            std::fs::remove_dir_all(storage_path).or_into_ctx()?;
        }
        Command::Status => {
            let reader = handle.reader().or_into_ctx()?;
            if let Some(snapshot) = reader.snapshot() {
                println!("Current snapshot: {snapshot}")
            } else {
                println!("No snapshot")
            }
            let header = HeaderView::load(&reader, PageIdx::FIRST).or_into_ctx()?;
            println!("List header: {header:?}");
        }

        Command::Sim { ticks } => {
            let mut sim = Simulator::new(handle, ticks);
            sim.run().or_into_ctx()?;
        }

        Command::Push => {
            let pre_push = handle.snapshot().or_into_ctx()?;
            handle.sync_with_remote(SyncDirection::Push).or_into_ctx()?;
            let post_push = handle.snapshot().or_into_ctx()?;
            if pre_push != post_push {
                println!("{pre_push:?} -> {post_push:?}");
            } else {
                println!("no changes to push");
            }
        }
        Command::Pull { reset } => {
            let pre_pull = handle.snapshot().or_into_ctx()?;
            if reset {
                handle.reset_to_remote().or_into_ctx()?
            } else {
                handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;
            }
            let post_pull = handle.snapshot().or_into_ctx()?;
            if pre_pull != post_pull {
                println!("pulled {}", post_pull.unwrap());
            } else {
                println!("no changes to pull");
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

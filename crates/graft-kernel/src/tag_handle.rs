use bytestring::ByteString;
use culprit::ResultExt;

use crate::{
    KernelErr,
    graft::{Graft, GraftStatus},
    rt::runtime_handle::RuntimeHandle,
    snapshot::Snapshot,
    volume_reader::{VolumeRead, VolumeReader},
    volume_writer::VolumeWriter,
};
use graft_core::{PageCount, VolumeId};

type Result<T> = culprit::Result<T, KernelErr>;

pub struct TagHandle {
    runtime: RuntimeHandle,
    tag: ByteString,
    graft: VolumeId,
}

impl TagHandle {
    pub(crate) fn new(runtime: RuntimeHandle, tag: ByteString, graft: VolumeId) -> Self {
        Self { runtime, tag, graft }
    }

    pub fn tag(&self) -> &ByteString {
        &self.tag
    }

    pub fn graft(&self) -> &VolumeId {
        &self.graft
    }

    /// Checkout a new graft starting at the specified remote. If remote is not
    /// specified, will checkout a new Graft for the current remote.
    pub fn checkout(&mut self, remote: Option<VolumeId>) -> Result<()> {
        let remote = remote.map_or_else(|| self.remote(), Ok)?;
        let graft = self
            .runtime
            .storage()
            .checkout_graft(&self.tag, remote)
            .or_into_ctx()?;
        self.graft = graft.local;
        Ok(())
    }

    pub fn state(&self) -> Result<Graft> {
        self.runtime
            .storage()
            .read()
            .graft(&self.graft)
            .or_into_ctx()
    }

    pub fn remote(&self) -> Result<VolumeId> {
        self.state().map(|g| g.remote)
    }

    pub fn status(&self) -> Result<GraftStatus> {
        let reader = self.runtime.storage().read();
        let state = reader.graft(&self.graft).or_into_ctx()?;
        let latest_local = reader.latest_lsn(&state.local).or_into_ctx()?;
        let latest_remote = reader.latest_lsn(&state.remote).or_into_ctx()?;
        Ok(state.status(latest_local, latest_remote))
    }

    #[inline]
    pub fn page_count(&self) -> Result<PageCount> {
        self.reader()?.page_count()
    }

    #[inline]
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.runtime
            .storage()
            .read()
            .snapshot(&self.graft)
            .or_into_ctx()
    }

    #[inline]
    pub fn reader(&self) -> Result<VolumeReader> {
        Ok(VolumeReader::new(
            self.runtime.clone(),
            self.graft.clone(),
            self.snapshot()?,
        ))
    }

    pub fn writer(&self) -> Result<VolumeWriter> {
        self.reader()?.try_into()
    }

    /// Hydrates the volume by downloading all missing pages from remote storage.
    /// This operation blocks until all pages are downloaded.
    pub fn hydrate(&self) -> Result<()> {
        let state = self.state()?;
        if let Some(trunk) = state.sync().map(|s| s.remote) {
            self.runtime.rpc().hydrate_volume(state.remote, Some(trunk))
        } else {
            Ok(())
        }
    }

    /// Fetches any new changes to the remote volume. Does not immediately pull
    /// those changes into the local volume. Either enable autosync or use
    /// `pull` to do that.
    pub fn fetch(&self) -> Result<()> {
        self.runtime.rpc().fetch_volume(self.remote()?)
    }

    /// Pulls any new changes into the remote volume and then immediately
    /// attempts to sync them into to the local volume.
    pub fn pull(&self) -> Result<()> {
        self.runtime.rpc().pull_graft(self.graft.clone())
    }

    /// Pushes any local changes to the remote volume.
    pub fn push(&self) -> Result<()> {
        self.runtime.rpc().push_graft(self.graft.clone())
    }
}

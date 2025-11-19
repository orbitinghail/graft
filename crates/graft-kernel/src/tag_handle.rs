use bytestring::ByteString;
use culprit::ResultExt;

use crate::{
    KernelErr,
    graft::{Graft, GraftStatus},
    rt::runtime::Runtime,
    snapshot::Snapshot,
    volume_reader::{VolumeRead, VolumeReader},
    volume_writer::VolumeWriter,
};
use graft_core::{PageCount, VolumeId};

type Result<T> = culprit::Result<T, KernelErr>;

pub struct TagHandle {
    runtime: Runtime,
    tag: ByteString,
}

impl TagHandle {
    pub(crate) fn new(runtime: Runtime, tag: ByteString) -> Self {
        Self { runtime, tag }
    }

    pub fn tag(&self) -> &ByteString {
        &self.tag
    }

    pub fn graft(&self) -> Result<VolumeId> {
        Ok(self
            .runtime
            .storage()
            .read()
            .get_tag_graft_id(&self.tag)
            .or_into_ctx()?
            .expect("BUG: graft not found for tag"))
    }

    /// Switch to the specified Graft, creating it if it doesn't exist
    /// An optional remote volume id may be specified
    pub fn switch_graft(&mut self, graft: VolumeId, remote: Option<VolumeId>) -> Result<Graft> {
        self.runtime
            .storage()
            .switch_graft(&self.tag, graft, remote)
            .or_into_ctx()
    }

    /// Clone the specified remote into a new graft. If remote is not
    /// specified, will reuse the current remote.
    pub fn clone_remote(&mut self, remote: Option<VolumeId>) -> Result<()> {
        let remote = remote.map_or_else(|| self.remote(), Ok)?;
        self.runtime
            .storage()
            .clone_remote(&self.tag, remote)
            .or_into_ctx()?;
        Ok(())
    }

    pub fn state(&self) -> Result<Graft> {
        Ok(self
            .runtime
            .storage()
            .read()
            .get_tag(&self.tag)
            .or_into_ctx()?
            .expect("BUG: graft not found for tag"))
    }

    pub fn remote(&self) -> Result<VolumeId> {
        self.state().map(|g| g.remote)
    }

    pub fn status(&self) -> Result<GraftStatus> {
        self.runtime.graft_status(&self.graft()?)
    }

    #[inline]
    pub fn page_count(&self) -> Result<PageCount> {
        self.reader()?.page_count()
    }

    #[inline]
    pub fn is_latest_snapshot(&self, snapshot: &Snapshot) -> Result<bool> {
        self.runtime
            .storage()
            .read()
            .is_latest_snapshot(&self.graft()?, snapshot)
            .or_into_ctx()
    }

    #[inline]
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.runtime
            .storage()
            .read()
            .snapshot(&self.graft()?)
            .or_into_ctx()
    }

    #[inline]
    pub fn reader(&self) -> Result<VolumeReader> {
        Ok(VolumeReader::new(
            self.runtime.clone(),
            self.graft()?,
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
            self.runtime.hydrate_volume(state.remote, Some(trunk))
        } else {
            Ok(())
        }
    }

    /// Fetches any new changes to the remote volume. Does not immediately pull
    /// those changes into the local volume. Either enable autosync or use
    /// `pull` to do that.
    pub fn fetch(&self) -> Result<()> {
        self.runtime.fetch_volume(self.remote()?, None)
    }

    /// Pulls any new changes into the remote volume and then immediately
    /// attempts to sync them into to the local volume.
    pub fn pull(&self) -> Result<()> {
        self.runtime.pull_graft(self.graft()?)
    }

    /// Pushes any local changes to the remote volume.
    pub fn push(&self) -> Result<()> {
        self.runtime.push_graft(self.graft()?)
    }
}

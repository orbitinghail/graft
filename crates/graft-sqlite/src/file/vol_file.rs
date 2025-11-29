use std::{
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    mem,
    sync::Arc,
};

use bytes::BytesMut;
use culprit::{Culprit, Result, ResultExt};
use graft_core::{
    PageIdx, VolumeId,
    page::{PAGESIZE, Page},
    page_count::PageCount,
};
use graft_kernel::{
    rt::runtime::Runtime,
    snapshot::Snapshot,
    volume_reader::{VolumeRead, VolumeReader},
    volume_writer::{VolumeWrite, VolumeWriter},
};
use parking_lot::{Mutex, MutexGuard};
use sqlite_plugin::flags::{LockLevel, OpenOpts};

use crate::vfs::ErrCtx;

use super::VfsFile;

// The byte offset of the SQLite file change counter in the database file
const FILE_CHANGE_COUNTER_OFFSET: usize = 24;
const VERSION_VALID_FOR_NUMBER_OFFSET: usize = 92;

enum VolFileState {
    Idle,
    Shared { reader: VolumeReader },
    Reserved { writer: VolumeWriter },
    Committing,
}

impl VolFileState {
    fn name(&self) -> &'static str {
        match self {
            VolFileState::Idle => "Idle",
            VolFileState::Shared { .. } => "Shared",
            VolFileState::Reserved { .. } => "Reserved",
            VolFileState::Committing => "Committing",
        }
    }
}

impl Debug for VolFileState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolFileState::Idle => f.write_str("Idle"),
            VolFileState::Shared { reader } => {
                f.debug_tuple("Shared").field(reader.snapshot()).finish()
            }
            VolFileState::Reserved { writer } => {
                f.debug_tuple("Reserved").field(writer.snapshot()).finish()
            }
            VolFileState::Committing => f.write_str("Committing"),
        }
    }
}

pub struct VolFile {
    runtime: Runtime,
    pub tag: String,
    pub vid: VolumeId,
    opts: OpenOpts,

    reserved: Arc<Mutex<()>>,
    state: VolFileState,
}

impl Debug for VolFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VolFile")
            .field("tag", &self.tag)
            .field("vid", &self.vid)
            .field("state", &self.state)
            .finish()
    }
}

impl VolFile {
    pub fn new(
        runtime: Runtime,
        tag: String,
        vid: VolumeId,
        opts: OpenOpts,
        reserved: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime,
            tag,
            vid,
            opts,
            reserved,
            state: VolFileState::Idle,
        }
    }

    pub fn snapshot_or_latest(&self) -> Result<Snapshot, ErrCtx> {
        match &self.state {
            VolFileState::Idle => self.runtime.volume_snapshot(&self.vid).or_into_ctx(),
            VolFileState::Shared { reader } => Ok(reader.snapshot().clone()),
            VolFileState::Reserved { writer } => Ok(writer.snapshot().clone()),
            VolFileState::Committing => ErrCtx::InvalidVolumeState.into(),
        }
    }

    pub fn page_count(&self) -> Result<PageCount, ErrCtx> {
        match &self.state {
            VolFileState::Idle => {
                let snapshot = self.runtime.volume_snapshot(&self.vid).or_into_ctx()?;
                self.runtime.snapshot_pages(&snapshot).or_into_ctx()
            }
            VolFileState::Shared { reader } => reader.page_count().or_into_ctx(),
            VolFileState::Reserved { writer } => writer.page_count().or_into_ctx(),
            VolFileState::Committing => ErrCtx::InvalidVolumeState.into(),
        }
    }

    pub fn is_idle(&self) -> bool {
        matches!(self.state, VolFileState::Idle)
    }

    pub fn opts(&self) -> OpenOpts {
        self.opts
    }

    pub fn switch_volume(&mut self, vid: &VolumeId) -> Result<(), ErrCtx> {
        self.runtime
            .tag_replace(&self.tag, vid.clone())
            .or_into_ctx()?;
        self.vid = vid.clone();
        Ok(())
    }
}

impl VfsFile for VolFile {
    fn readonly(&self) -> bool {
        false
    }

    fn in_memory(&self) -> bool {
        false
    }

    fn lock(&mut self, level: LockLevel) -> Result<(), ErrCtx> {
        match level {
            LockLevel::Unlocked => {
                // SQLite should never request an Unlocked lock
                unreachable!("bug: invalid request lock(Unlocked)");
            }
            LockLevel::Shared => {
                if let VolFileState::Idle = self.state {
                    // Transition Idle -> Shared
                    let reader = self.runtime.volume_reader(self.vid.clone()).or_into_ctx()?;
                    self.state = VolFileState::Shared { reader };
                } else {
                    return Err(Culprit::new_with_note(
                        ErrCtx::InvalidLockTransition,
                        format!("invalid lock request Shared in state {}", self.state.name()),
                    ));
                }
            }
            LockLevel::Reserved => {
                if let VolFileState::Shared { ref reader } = self.state {
                    // Transition Shared -> Reserved

                    // Ensure that this VolFile is not readonly
                    if self.opts.mode().is_readonly() {
                        return Err(Culprit::new_with_note(
                            ErrCtx::InvalidLockTransition,
                            "invalid lock request: Shared -> Reserved: file is read-only",
                        ));
                    }

                    // try to acquire the reserved lock or fail if another thread has it
                    let Some(reserved) = self.reserved.try_lock() else {
                        return Err(Culprit::new(ErrCtx::Busy));
                    };

                    // check to see if the snapshot is latest. if it
                    // has changed we can immediately reject the lock upgrade
                    if !self
                        .runtime
                        .snapshot_is_latest(&self.vid, reader.snapshot())
                        .or_into_ctx()?
                    {
                        return Err(Culprit::new_with_note(
                            ErrCtx::BusySnapshot,
                            "unable to lock: Shared -> Reserved: snapshot changed",
                        ));
                    }

                    // convert the reader into a writer
                    self.state = VolFileState::Reserved {
                        writer: VolumeWriter::try_from(reader.clone()).or_into_ctx()?,
                    };

                    // Explicitly leak the reserved lock
                    // SAFETY: we depend on SQLite to release the lock when it's done
                    MutexGuard::leak(reserved);
                } else {
                    return Err(Culprit::new_with_note(
                        ErrCtx::InvalidLockTransition,
                        format!(
                            "invalid lock request Reserved in state {}",
                            self.state.name()
                        ),
                    ));
                }
            }
            LockLevel::Pending | LockLevel::Exclusive => {
                // SQLite should only request these transitions while we are in the Reserved state
                assert!(
                    matches!(self.state, VolFileState::Reserved { .. }),
                    "bug: invalid lock request {:?} in state {}",
                    level,
                    self.state.name()
                );
            }
        }

        Ok(())
    }

    fn unlock(&mut self, level: LockLevel) -> Result<(), ErrCtx> {
        match level {
            LockLevel::Unlocked => match self.state {
                VolFileState::Idle | VolFileState::Shared { .. } | VolFileState::Committing => {
                    self.state = VolFileState::Idle;
                }
                VolFileState::Reserved { .. } => {
                    return Err(Culprit::new_with_note(
                        ErrCtx::InvalidLockTransition,
                        "invalid unlock request Unlocked in state Reserved",
                    ));
                }
            },
            LockLevel::Shared => {
                if let VolFileState::Reserved { writer } =
                    mem::replace(&mut self.state, VolFileState::Committing)
                {
                    // Transition Reserved -> Shared through the Committing state
                    // If we fail the commit, SQLite will subsequently issue an
                    // Unlocked request after handling the error

                    // Commit the writer, downgrading to a reader
                    let reader = writer.commit().or_into_ctx()?;
                    self.state = VolFileState::Shared { reader };

                    // release the reserved lock
                    // between threads while holding the lock
                    // TODO: find a way to assert that this thread actually owns the lock
                    assert!(self.reserved.is_locked(), "reserved lock must be locked");
                    // SAFETY: we are in the Reserved state, thus we are holding the lock
                    // SAFETY: we depend on the connection not being passed
                    unsafe { self.reserved.force_unlock() };
                } else {
                    return Err(Culprit::new_with_note(
                        ErrCtx::InvalidLockTransition,
                        format!(
                            "invalid unlock request Shared in state {}",
                            self.state.name()
                        ),
                    ));
                }
            }
            LockLevel::Reserved | LockLevel::Pending | LockLevel::Exclusive => {
                // SQLite should only request these transitions using the lock method
                unreachable!(
                    "bug: invalid unlock request {:?} in state {}",
                    level,
                    self.state.name()
                );
            }
        }

        Ok(())
    }

    fn file_size(&mut self) -> Result<usize, ErrCtx> {
        Ok(PAGESIZE.as_usize() * self.page_count()?.to_usize())
    }

    fn read(&mut self, offset: usize, data: &mut [u8]) -> Result<usize, ErrCtx> {
        // locate the page offset of the requested page
        let pageidx: PageIdx = ((offset / PAGESIZE.as_usize()) + 1)
            .try_into()
            .expect("offset out of volume range");
        // local_offset is the offset *within* the requested page
        let local_offset = offset % PAGESIZE;

        assert!(
            local_offset + data.len() <= PAGESIZE,
            "read must not cross page boundary"
        );

        // load the page
        let page = match &mut self.state {
            VolFileState::Idle => {
                // sqlite sometimes reads the database header without holding a
                // lock, in this case we are expected to read from the latest
                // snapshot
                let reader = self.runtime.volume_reader(self.vid.clone()).or_into_ctx()?;
                reader.read_page(pageidx).or_into_ctx()?
            }
            VolFileState::Shared { reader } => reader.read_page(pageidx).or_into_ctx()?,
            VolFileState::Reserved { writer } => writer.read_page(pageidx).or_into_ctx()?,
            VolFileState::Committing => return ErrCtx::InvalidVolumeState.into(),
        };

        let range = local_offset.as_usize()..(local_offset + data.len()).as_usize();
        data.copy_from_slice(&page[range]);

        // check to see if SQLite is reading the file change counter, and if so,
        // overwrite it with a counter derived from the current snapshot
        if pageidx == PageIdx::FIRST
            && local_offset <= FILE_CHANGE_COUNTER_OFFSET
            && local_offset + data.len() >= FILE_CHANGE_COUNTER_OFFSET + 4
        {
            // find the location of the file change counter within the out buffer
            let fcc_offset = FILE_CHANGE_COUNTER_OFFSET - local_offset.as_usize();

            // compute the file change counter by hashing the snapshot.
            // IMPORTANT: we use DefaultHasher which has a fixed seed/secret of
            // 0 to ensure that the same snapshot gives the same result
            let snapshot = self.snapshot_or_latest()?;
            let mut hasher = DefaultHasher::new();
            snapshot.hash(&mut hasher);
            let hash = hasher.finish();
            let change_counter = &hash.to_be_bytes()[..4];

            // write the latest change counter to the buffer
            data[fcc_offset..fcc_offset + 4].copy_from_slice(change_counter);
        }

        Ok(data.len())
    }

    fn truncate(&mut self, size: usize) -> Result<(), ErrCtx> {
        let VolFileState::Reserved { writer, .. } = &mut self.state else {
            return Err(Culprit::new_with_note(
                ErrCtx::InvalidVolumeState,
                "must hold reserved lock to truncate",
            ));
        };

        assert_eq!(
            size % PAGESIZE.as_usize(),
            0,
            "size must be an even multiple of {PAGESIZE}"
        );

        let pages: PageCount = (size / PAGESIZE.as_usize())
            .try_into()
            .expect("size too large");

        writer.truncate(pages).or_into_ctx()?;
        Ok(())
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<usize, ErrCtx> {
        let VolFileState::Reserved { writer, .. } = &mut self.state else {
            return Err(Culprit::new_with_note(
                ErrCtx::InvalidVolumeState,
                "must hold reserved lock to write",
            ));
        };

        // locate the requested page index
        let page_idx: PageIdx = ((offset / PAGESIZE.as_usize()) + 1)
            .try_into()
            .expect("offset out of volume range");
        // local_offset is the offset *within* the requested page
        let local_offset = offset % PAGESIZE;

        assert!(
            local_offset + data.len() <= PAGESIZE,
            "write must not cross page boundary"
        );

        // if this is a write to the first page, and the write only changes the
        // file change counter and the version valid for number, we can ignore this write
        if page_idx == PageIdx::FIRST && data.len() == PAGESIZE && local_offset == 0 {
            let existing: Page = writer.read_page(page_idx).or_into_ctx()?;

            debug_assert_eq!(data.len(), existing.len(), "page size mismatch");

            let fcc = FILE_CHANGE_COUNTER_OFFSET..FILE_CHANGE_COUNTER_OFFSET + 4;
            let vvf = VERSION_VALID_FOR_NUMBER_OFFSET..VERSION_VALID_FOR_NUMBER_OFFSET + 4;

            // check the header page is unchanged while ignoring the file change
            // counter and version valid for number
            let unchanged =
                // prefix [0,24)
                data[..fcc.start]           == existing[..fcc.start] &&
                // middle (28,92)
                data[fcc.end..vvf.start]    == existing[fcc.end..vvf.start] &&
                // suffix (96, end]
                data[vvf.end..]             == existing[vvf.end..];

            if unchanged {
                tracing::trace!(
                    "ignoring write to header page, as only file change counter and version valid for number changed"
                );
                return Ok(data.len());
            }
        }

        let page = if data.len() == PAGESIZE {
            // writing a full page
            Page::try_from(data).expect("data is a full page")
        } else {
            // writing a partial page
            // we need to read and then update the page
            let mut page: BytesMut = writer.read_page(page_idx).or_into_ctx()?.into();
            // SAFETY: we already verified that the write does not cross a page boundary
            let range = local_offset.as_usize()..(local_offset + data.len()).as_usize();
            page[range].copy_from_slice(data);
            page.try_into().expect("we did not change the page size")
        };

        writer.write_page(page_idx, page).or_into_ctx()?;
        Ok(data.len())
    }
}

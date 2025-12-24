//! Thoughts on the Graft PageStore
//!
//! The simplest design would be to write out segments for each commit. This
//! allows the PageStore to maintain symmetry between local and remote segments,
//! however may have a high syscall and fsync overhead.
//!
//! Jeromes prototype uses mmap to skip the syscall overhead of writing to
//! files, and completely ignores fsync. He focuses on remote durability only.
//!
//! I think the key insight for the Graft PageStore is that it's actually a Segment store.
//! The VolumeWriter already buffers writes in-memory, so we just need to handle writing them out at commit time.
//! We can use a single active file descriptor, and journal segments into it.
//! Once the active fd gets too large we can cut a new one.
//! Thus the SegmentStore is a series of logs/packs/something, with exactly one "active".
//!
//! The SegmentStore needs to keep around an index mapping segment id to pack+offset.
//! To support multi-process, we need to store the index in shm along with a write lock.
//! The shm can also track the fsync point, to permit lazy fsync.
//!
//! The SQLite Wal SHM is good inspiration for this design. It is a series of
//! 32KB regions. Each region contains an array of u32 page idxs followed by an
//! u16 array of frame offsets. Writing a page to the wal creates a frame which
//! is appended to the most recent region. Thus the regions match the linear
//! nature of the Wal. Then the pageidx is hashed and inserted into the next
//! open position in the frame offsets table.
//! A page can be looked up by hashing the idx and checking each candidate. The
//! resulting frame offset is added to the region base offset to determine where
//! in the log the page is stored.
//!
//! So, perhaps we log local segments into a series of pack files keyed by a
//! sequence number. The shared memory region can then store two arrays:
//! - array of segment ids, in order by when they were written into a the pack log
//! - open hash table mapping segment id to candidate position in the previous array
//!
//! When pushing data to the remote, we have an opportunity to truncate some
//! prefix of the segment wal. This has to be done carefully:
//!     - a read snapshot may still be reading from the wal
//!     - a writer may be writing to the wal
//!         In theory, the writer should only ever write to the active pack/region.
//!         So the only thing that needs to be truncated is the shm region.
//!         This can be coordinated via a special region at the beginning of the shm file.
//!         Or perhaps each region has a small header that tracks it's state. effectively a region
//!         goes through states: free -> active -> sealed -> free
//!
//! Should we just put all local writes (commit + segment) in the same wal? this
//! may simplify crash recovery and durability. But now we're building a database :)
//! No, I think we can keep metadata separate.
//!
//! The SegmentStore also may need to cache segments we read from the remote.
//! However, if this proves too complex, we could store those segments in a
//! separate cache, perhaps using something like foyer to manage eviction and
//! so on.

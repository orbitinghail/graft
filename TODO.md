Time to design the storage subsystem for the PageStore.

This system has the following goals:
- maintain a disk cache of recently written and read segments
- efficiently upload segments to object storage
- make segments available for reads using memmap2

In the original design I had thought of having the segment manager own this subsystem and hence eliminate any need for concurrent access to the in-memory portions of the cache. But, it may be easier to implement this as a thread safe object that's shared between the other subsystems. I think it has fairly low concurrent requirements:

When writing a segment, the flow is basically:
1. allocate a slot in the cache for the segment
2. write to the slot, and concurrently upload to the object storage
   - this must be atomic from a fs perspective
3. release the cache slot - making it available for reads

When reading a segment:

Lookup the cache entry corresponding to the segment. This requires some kind of thread safe data. The Entry should be RAII and prevent the cached slot from being used for other segments or removed while it's alive.

**on miss**:
  download the segment into the entry using a download manager that limits the number of concurrent downloads and may implement hedging in the future
  fall through to the hit code

**on hit**:
  pin the segment using mmap if not already pinned
    the storage system should limit the number of open mmaps
  increment the ref count on the pin
  make the pinned segment available as a virtual memory buffer to consumers, wrapped in RAII to decrement the ref count allowing the memory to be unpinned

To help limit contention, the caching and pinning layer should be entirely in memory and just return allocated slots. Once a slot is allocated, the actual IO can happen outside of any critical regions.

When instantiating the storage subsystem we need to scan the cache and load any segments into the cache.
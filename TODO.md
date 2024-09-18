Next step is to investigate an alternative to on disk hash tables. In my haste to write up the design doc I failed to consider that we need to search segments for the latest version of a given offset... hence we need fast prefix scan.

Neon uses b-tree's, but a radix/prefix tree may also work well. We care about simplicity and read performance and size. Our indexes are completely immutable.

Why not just build a special purpose index? The query we want to satisfy is `readPage(vid, offset, lsn)` which should return the page corresponding to the provided vid and offset with the highest lsn <= the query lsn. Hence, we could satisfy this query efficiently using a 2 level hash table:

`Map<VolumeId, Map<Offset, [(rLSN,LocalOffset)]>>`

If we want to use ODHT for this we will need to add indirection between each hash table and the final offset list.

Alternatively we can remove one level of indirection by combining the VolumeId and Offset into a key. This costs significantly more storage though.

We should calculate how many pages we can index using the two level approach vs one level approach both for the inline index and non-inline index cases. We should gain a huge perf advantage if Segments consistently store their index inline.

We should also consider if we can make rLSN a u16 or maybe even a u8. This will affect compaction. Compaction will have to alter the packing algorithm to take the rLSN range per VolumeId into account. It seems that a u16 (65k sequential rLSNs per Offset) would be fairly good.

We can also compress rLSNs via storing deltas rather than absolute values. Or I suppose since rLSNs are already relative, it would be storing delta deltas. In this case the maximum distance between any two rLSNs is what matters. This may allow us to compress rLSNs down to a byte...

Another useful compression possibility is only storing the first local offset. Since pages can be stored in order by lsn, we only need to store the local offset once per volume offset. This allows the rLSN list to just store a list of rLSNs.

**Ask yourself this** what is the purpose of storing a lot of history for every volume? What is the use case? The system becomes much simpler if a Segment may only store a single page for every vol/off pair. We can still have short term history in this case, until a Volume is checkpointed. And we can implement longer term history by keeping around checkpoints for some time. The only thing this affects is point in time/lsn specific queries going back for awhile.

We can always add this back in later if it becomes needed. Segment metadata won't change much. We will need to come up with an efficient way to build checkpoints.
Keep track of things to test in this file.

# Concurrent writers to the same Volume

Lots of issues to test here. However, from the Pagestore's perspective we need to carefully think of the correct semantics in order to allow the rest of the system to work correctly.

Interesting cases:

1. two writers write to different portions of the same volume, their writes go into the same set of segments, both commit
   -> in theory, this is safe assuming they didn't overlap read/write sets
   -> however this will cause the metastore to record the same segments twice at different LSN's, leading to possibly subtle bugs in the future
   -> this may just require being a bit more careful about making assumptions on segments. since the graft doesn't overlap, and we keep an explicit graft per LSN, this is actually safe. we just need to ensure that GC/merging takes this into account

2. two writers write to the same pages in the same volume, they hit the same segments
   -> a segment must only contain a single write for a single pageidx
   -> thus the pagestore must either reject or overwrite
   -> perhaps a write fence is needed, to reject older writers
   -> I think it only needs to track vid/fence pairs for the in progress segment, to ensure that the correct writer wins and older writers are rejected
   -> the metastore will handle invalid writers trying to commit older segments separately

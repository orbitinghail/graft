Next tasks:
- implement volume index
- implement request limiter
- implement Splinter cut
- implement segment query

# Volume Index
Will use Sled for storage. Rationale is that it gives us durability, is fairly lightweight, and is fast.

```
keyspace:
volumes/[vid] -> { lsn, last_offset, last_commit }
segments/[vid]/[lsn]/[sid] -> OffsetSet
```

Keys and values will be encoded with zerocopy. note that bigendian encoding of numbers is lexographically sortable. So lsn's should be bigendian when stored in a key. An example of using sled in this way is here: https://github.com/spacejam/sled/blob/main/examples/structured.rs

# Find matching segments query

```
find_segments(vid, lsn, query: OffsetSet)

// this object downloads and memmaps segments in the background
let loader = SegmentsLoader

// iterate through segments in reverse order of lsn
for segment in store.segments(vid, lsn) {
  // cut segment offsets out of the query, returning the cut offsets
  let cut = query.cut(segment.offsets);

  if !cut.is_empty() {
    loader.load(segment, cut)
  }

  if query.is_empty() {
    break
  }
}

let out: Vec<PageOffset>
for (segment, cut) in loader {
  for offset in cut {
    page = segment.get(vid, offset).expect("index out of sync")
    out.append((page,offset))
  }
}

out
```

# Request limiter
In the case of downloading segments:

```
# optimistically retrieve segment from cache
if Some(segment) = cache.get(sid) {
  return segment
}

# retrieve a permit; this may block
permit = limiter.get(sid)

# check if someone else already got the segment
if Some(segment) = cache.get(sid) {
  return segment
}

# get the segment
segment = store.get(sid)
cache.put(sid, segment)

# return the segment; releasing the permit
segment

```

And getting segment metadata

```
# check to see if we have metadata up to the required lsn
if meta.has(vid, lsn) {
  return meta.query(vid, lsn, offsets)
}

# retrieve a permit; this may block
permit = limiter.get(vid)

# check if someone else retrieved the required lsn
if meta.has(vid, lsn) {
  return meta.query(vid, lsn, offsets)
}

# update metadata to latest
meta.update_latest(vid)

# release the permit to unblock other tasks
drop(permit)

# query metadata
return meta.query(vid, lsn, offsets)
```
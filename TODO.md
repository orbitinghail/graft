Next tasks:
- implement volume catalog
- implement request limiter
- implement Splinter cut
- implement segment query

# Splinter Relation

ok so we have a fundamental incompat between two goals
the ideal Relation trait uses a concrete Value with references
the ideal Ref Relation trait uses a concrete Value without references
this delta is the issue preventing a more generic codebase

ideas:
1. implement two relation types... maybe this makes things easier? doubtful
2. stick with the ideal Ref relation, and figure out how to make it work with
partition

3. implement a Join type that is generic over A,B, then implement it over each pair
    partition <> partition
    partition <> partition ref
    partition ref <> partition
    partition ref <> partition ref

this will result in a lot more codepaths that need to be tested... but maybe is
the fastest solution. maybe macros can be used to reduce the risk of errors

I think 3 is fairly promising, as it can be implemented over concrete types
the type sigs will be gnarly, but that's to be somewhat expected


# Volume Catalog
Will use an embedded kv store for storage.

```
keyspace:
volumes/[vid] -> { lsn, last_offset }
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
    page = segment.get(vid, offset).expect("catalog out of sync")
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
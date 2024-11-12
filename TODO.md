Next tasks:
- remote file utility for testing
- consider switching pagestore to websockets or http streaming bodies
- add range compression to Splinter
- end to end testing framework

# remote file utility

A small program serving as a demo and quick test harness for Graft. The utility provides a CLI which accesses a file stored in Graft.

The utility can use something like Fjall to cache data. The cache should contain a map from virtual file name to snapshot, and another map from (vid, offset) to page.

The CLI provides the following methods:

```
open(name, vid)
open a vid using the provided virtual file name

close(name, vid)
remove the provided vid from the cache

snapshot(name)
printout the virtual file's snapshot metadata

read(name, offset, [len]) -> data to stdout
read from the virtual file starting at the provided byte offset

write(name, offset) <- data from stdin
write to the virtual file starting at the provided byte offset
```
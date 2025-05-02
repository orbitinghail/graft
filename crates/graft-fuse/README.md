# Graft Fuse Driver

This is a prototype Fuse driver backed by SQLite + Graft.

Currently the driver ships with a fixed set of files as a demo. A future update will include a simple client that supports writing/updating files.

The file contents are normalized into a key/value table allowing the file format to be determined dynamically during materialization. This decision enables the following benefits:

- changing a key path doesn't require rewriting entire files
- multiple files can share keys - useful for feature flags
- formats can be driven by the client at read time

## Running the driver

You can run the driver using:

```bash
just run fuse mount
```

The filesystem is mounted in `mnt` at the root of the repo.

You can also test the driver using:

```bash
just run fuse test
```

## Future work

**Client**:

A simple client binary should be created that makes it easier to manage files and fields.

**Multi-selector**:

A file should be able to materialize multiple fields into its root.

**Format prefix?**:

Rather than encoding the format into the file name - virtualize multiple format-specific top-level directories (json, toml, yaml, etc) - allowing the client to pick at read time.

**Metadata directory**:

Expose a `.graft` (or similar) metadata directory full of useful status/metadata files like the current snapshot. Also may be interesting to expose replication control via writes to certain metadata files.

**Snapshot invalidation**:

When the snapshot changes we need to clear the kernel page cache and our file cache to ensure that new contents are visible.

Possible optimizations:

- only update file cache on open (if snapshot changed)
- similarily only trigger inode invalidation on open if snapshot changed
  - actually this might not be needed, I think the kernel always clears the page cache for an inode on open

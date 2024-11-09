```
header (4 bytes)
    magic (2 bytes)
    unused (2 bytes)

footer (4 bytes)
    partitions (2 bytes)
    unused (2 bytes)

block (cardinality)
    cardinality < 32
        data: [u8; cardinality]
    else
        data: [u8; 32]

index (cardinality, offset_size: u16|u32)
    keys: block(cardinality)
    cardinalities: [u8; cardinality] // 1 based
    offsets: [offset_size; cardinality]

partition (cardinality)
    map (cardinality, u16, block)

map (cardinality, off_type, val_type)
    values: [val_type(index->cardinalities[i]); cardinality]
    index (cardinality, off_type)

splinter
    header
    map (footer->partitions, u32, partition)
    footer

```

# TODO

## Range Compression

One way to add range compression to Splinter is to avoid storing full blocks. When querying Splinter, if the cardinality of a block is 256 then the code can short circuit rather than read the block. I suspect this doesn't add too much complexity and is probably the easiest way to add range compression.

The other option is to follow in the footsteps of Roaring and add a bit somewhere to specify if a block stores ranges. Then, we could store up to 16 non-overlapping ranges in each block (as each range is two u8s). This allows range compression to be a tiny bit more granular at the cost of storage size (extra bit per block, ends up being more due to alignment) and complexity.

Once we add range optimization to Splinter, checking to see if a Splinter is contiguous will be cheaper.

## Optimization ideas

- implement SIMD/AVX versions of block_contains and block_rank
  - implement 64-bit versions for non-AVX/SIMD
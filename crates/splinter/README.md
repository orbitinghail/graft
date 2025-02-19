```
header (4 bytes)
    magic (2 bytes)
    unused (2 bytes)

footer (4 bytes)
    partitions (2 bytes)
    unused (2 bytes)

block (cardinality)
    cardinality == 256
        data: OMITTED
    cardinality < 32
        data: [u8; cardinality]
    else
        data: [u8; 32]

index (cardinality, offset_size: u16|u32)
    keys: block(cardinality)
    cardinalities: [u8; cardinality] // 1 based
    offsets: [offset_size; cardinality]

map (cardinality, off_type, val_type)
    values: [val_type(index->cardinalities[i]); cardinality]
    index (cardinality, off_type)

splinter
    header
    map (footer->partitions, u32,
      map (cardinality, u32,
        map (cardinality, u16, block)))
    footer

```

# Future optimizations

## SIMD/AVX

- implement SIMD/AVX versions of block_contains and block_rank
- implement 64-bit versions for non-AVX/SIMD

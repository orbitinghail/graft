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

## Cut
To efficiently support cut, we will need a mutable splinter. The simplest form of this is `HashMap<Segment, HashMap<Segment, Block>>` where Block is an enum storing either a list of Segments or a bitset.

## Missing methods
- iter -> iterate through all of the elements in the splinter
- from_sorted_iter

## Optimization ideas

- implement SIMD/AVX versions of block_contains and block_rank
  - implement 64-bit versions for non-AVX/SIMD
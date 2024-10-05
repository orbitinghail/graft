```
header (4 bytes)
    magic (2 bytes)
    unused

footer (4 bytes)
    partitions (2 byte)
    unused (2 byte)

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

header (4 bytes)
map (footer->partitions, u32, partition)
footer (4 bytes)

procedure exists(key u24)
    (partition, block, segment) = split key into three 8-bit segments

    partitions = header.partitions
    splinter_index_size = min(32, partitions)+partitions+4*partitions
    splinter_index_start = splinter_size - splinter_index_size
    // index stored at end of file
    splinter_index = data[splinter_index_start..splinter_size]

    if !splinter_index.contains(partition)
        return false

    let rank = splinter_index.rank(partition)
    let cardinality = splinter_index.cardinalities[rank]
    let offset = splinter_index.offsets[rank]

    partition_index_size = min(32, cardinality)+cardinality+2*cardinality
    partition_index = data[offset..offset+partition_index_size]

    if !partition_index.contains(block)
        return false

    let rank = partition_index.rank(partition)
    let cardinality = partition_index.cardinalities[rank]
    let offset = partition_index.offsets[rank]

    block_size = min(32, cardinality)
    block = data[offset..offset+block_size]

    return block.contains(segment)
```
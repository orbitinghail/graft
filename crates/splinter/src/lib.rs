use zerocopy::{little_endian::U32, AsBytes, FromBytes, FromZeroes, Ref};

pub const SPLINTER_MAGIC: U32 = U32::from_bytes([0x57, 0x11, 0xd7, 0xe2]);
pub const SPLINTER_VERSION: u8 = 1;

#[derive(Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Header {
    magic: U32,
    version: u8,
}

/*

header (8 bytes)
    magic (4 bytes)
    version (1 byte)
    superblocks (1 byte)

block (length)
    length < 32
        data: [u8; length]
    else
        data: [u8; 32]

index (length)
    keys: block
    cardinalities: [u8; length]

header (8 bytes)
index (superblocks) = (superblocks < 32 ? superblocks*2 : 32 + superblocks)
block indexes [index(superblock cardinality); superblocks]
blocks [block (block cardinality); superblock cardinality]

procedure exists(key u24)
    superblock_index = data[8]

    (superblock, block, segment) = split key into three 8-bit segments

    if !superblock_index.contains(superblock)
        return false

    let rank = superblock_index.rank(superblock)
    let cardinality = superblock_index.cardinalities[rank]
    let offset = sum(superblock_index.cardinalities[0..rank])


TODO: we need a offset map, and it's looking like offsets will have to be 2 bytes each -> 64k max splinter size
    idea: can we replace cardinalities with offsets? can we calculate one from the other?
    idea: can we reorganize the data into fixed blocks and variable blocks -> thus amortizing offset costs?


*/

pub struct Splinter<B> {
    header: Ref<B, Header>,
    data: B,
}

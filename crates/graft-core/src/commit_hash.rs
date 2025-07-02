const HASH_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct CommitHash {
    hash: [u8; HASH_SIZE],
}

static_assertions::assert_eq_size!(CommitHash, [u8; HASH_SIZE]);

use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy, PartialEq, Eq,
)]
#[repr(u8)]
pub enum SyncDirection {
    Up = 1,
    Down = 2,
    Both = 3,
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct VolumeConfig {
    sync: SyncDirection,
}

impl AsRef<[u8]> for VolumeConfig {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

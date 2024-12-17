use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy, PartialEq, Eq,
)]
#[repr(u8)]
pub enum SyncDirection {
    Push = 1,
    Pull = 2,
    Both = 3,
}

impl SyncDirection {
    pub fn matches(&self, sync: SyncDirection) -> bool {
        match self {
            SyncDirection::Both => true,
            _ => self == &sync,
        }
    }
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct VolumeConfig {
    sync: SyncDirection,
}

impl VolumeConfig {
    pub fn new(sync: SyncDirection) -> Self {
        Self { sync }
    }

    pub fn sync(&self) -> SyncDirection {
        self.sync
    }
}

impl AsRef<[u8]> for VolumeConfig {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

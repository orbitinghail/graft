use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
    Unaligned,
    Default,
)]
#[repr(transparent)]
pub struct GidTimestamp([u8; 6]);

impl GidTimestamp {
    pub const ZERO: Self = Self([0; 6]);

    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn as_time(&self) -> SystemTime {
        let mut bytes = [0; 8];
        bytes[2..].copy_from_slice(&self.0);
        let millis = u64::from_be_bytes(bytes);
        UNIX_EPOCH + Duration::from_millis(millis)
    }
}

impl Hash for GidTimestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state);
    }
}

impl From<SystemTime> for GidTimestamp {
    fn from(time: SystemTime) -> Self {
        let millis = time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let millis = millis.to_be_bytes();
        let mut bytes = [0; 6];
        bytes.copy_from_slice(&millis[2..]);
        Self(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gid_timestamp_now() {
        let now = GidTimestamp::now();
        assert!(now != GidTimestamp::ZERO);
    }

    #[test]
    fn test_gid_timestamp_from_system_time() {
        fn st_ms(st: SystemTime) -> u128 {
            st.duration_since(UNIX_EPOCH).unwrap().as_millis()
        }
        let now = SystemTime::now();
        let gid_ts: GidTimestamp = now.into();
        assert_eq!(st_ms(gid_ts.as_time()), st_ms(now));
    }

    #[test]
    fn test_gid_timestamp_zero() {
        assert_eq!(GidTimestamp::ZERO.as_time(), UNIX_EPOCH);
    }
}

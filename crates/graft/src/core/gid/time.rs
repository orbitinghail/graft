use std::time::{Duration, SystemTime, UNIX_EPOCH};

use zerocopy::{ByteHash, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::core::gid::prefix::{ConstDefault, Prefix};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    IntoBytes,
    FromBytes,
    Immutable,
    KnownLayout,
    ByteHash,
    Unaligned,
    Default,
)]
#[repr(C)]
pub struct GidTimestamp<P: Prefix> {
    prefix: P,
    ts: [u8; 6],
}

impl<P: Prefix> ConstDefault for GidTimestamp<P> {
    const DEFAULT: Self = Self { prefix: P::DEFAULT, ts: [0; 6] };
}

impl<P: Prefix> GidTimestamp<P> {
    #[inline]
    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn as_time(&self) -> SystemTime {
        let mut bytes = [0; 8];
        bytes[2..].copy_from_slice(&self.ts);
        let millis = u64::from_be_bytes(bytes);
        UNIX_EPOCH + Duration::from_millis(millis)
    }
}

impl<P: Prefix> From<SystemTime> for GidTimestamp<P> {
    fn from(time: SystemTime) -> Self {
        let millis = time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let millis = millis.to_be_bytes();
        let mut bytes = [0; 6];
        bytes.copy_from_slice(&millis[2..]);
        Self { prefix: P::DEFAULT, ts: bytes }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::gid::prefix::Log;

    use super::*;

    #[test]
    fn test_gid_timestamp_now() {
        let now = GidTimestamp::<Log>::now();
        assert!(now != GidTimestamp::DEFAULT);
    }

    #[test]
    fn test_gid_timestamp_from_system_time() {
        fn st_ms(st: SystemTime) -> u128 {
            st.duration_since(UNIX_EPOCH).unwrap().as_millis()
        }
        let now = SystemTime::now();
        let gid_ts: GidTimestamp<Log> = now.into();
        assert_eq!(st_ms(gid_ts.as_time()), st_ms(now));
    }

    #[test]
    fn test_gid_timestamp_zero() {
        assert_eq!(GidTimestamp::<Log>::DEFAULT.as_time(), UNIX_EPOCH);
    }
}

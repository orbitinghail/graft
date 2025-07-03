use bytes::Bytes;
use splinter_rs::{Splinter, cow::CowSplinter};

use crate::derive_newtype_proxy;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Graft {
    splinter: CowSplinter<Bytes>,
}

impl Graft {
    pub const EMPTY: Self = Self {
        splinter: CowSplinter::Owned(Splinter::EMPTY),
    };

    pub fn new(splinter: CowSplinter<Bytes>) -> Self {
        Self { splinter }
    }
}

derive_newtype_proxy!(
    newtype (Graft)
    with empty value (Graft::EMPTY)
    with proxy type (Bytes) and encoding (bilrost::encoding::General)
    with sample value (Graft::new(CowSplinter::from_iter(0u32..10)))
    into_proxy(&self) {
        self.splinter.serialize_to_bytes()
    }
    from_proxy(&mut self, proxy) {
        *self = Graft {
            splinter: CowSplinter::from_bytes(proxy)
                .map_err(|_| bilrost::DecodeErrorKind::InvalidValue)?,
        };
        Ok(())
    }
);

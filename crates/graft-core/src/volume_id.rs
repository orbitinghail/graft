use std::{
    fmt::{Debug, Display},
    ops::Deref,
};

use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};

use crate::guid::{Guid, GuidParseError};

#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    BorshSerialize,
    BorshDeserialize,
    BorshSchema,
    zerocopy::AsBytes,
    zerocopy::FromZeroes,
    zerocopy::FromBytes,
)]
#[repr(transparent)]
pub struct VolumeId(Guid<'V'>);

impl VolumeId {
    pub fn random() -> VolumeId {
        VolumeId(Guid::random())
    }

    pub fn derive(name: &str) -> VolumeId {
        VolumeId(Guid::derive(name))
    }

    pub fn pretty(&self) -> String {
        self.0.pretty()
    }
}

impl Display for VolumeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Debug for VolumeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl TryFrom<&str> for VolumeId {
    type Error = GuidParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Guid::try_from(value).map(VolumeId)
    }
}

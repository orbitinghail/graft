use bytes::Bytes;
use splinter_rs::cow::CowSplinter;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Graft {
    splinter: CowSplinter<Bytes>,
}

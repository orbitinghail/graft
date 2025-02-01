use super::{Dispatch, Event};

pub struct NoopDispatch;

impl Dispatch for NoopDispatch {
    fn emit(&self, _event: Event) {}

    #[inline]
    fn random(&self) -> u64 {
        rand::random()
    }
}

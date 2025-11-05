use graft_core::lsn::LSN;

pub enum PageStatus {
    Pending(LSN),
    Empty(Option<LSN>),
    Available(LSN),
    Dirty,
}

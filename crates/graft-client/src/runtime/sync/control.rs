use crossbeam::channel::{self, Receiver, Sender};
use graft_core::VolumeId;

use crate::{ClientErr, runtime::storage::volume_state::SyncDirection};
use culprit::Result;

#[derive(Debug)]
pub enum SyncControl {
    GetAutosync {
        complete: Sender<bool>,
    },

    SetAutosync {
        autosync: bool,
        complete: Sender<()>,
    },

    Sync {
        vid: VolumeId,
        direction: SyncDirection,
        complete: Sender<Result<(), ClientErr>>,
    },

    ResetToRemote {
        vid: VolumeId,
        complete: Sender<Result<(), ClientErr>>,
    },

    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SyncRpc {
    control: Option<Sender<SyncControl>>,
}

impl SyncRpc {
    pub(crate) fn new(control: Option<Sender<SyncControl>>) -> Self {
        Self { control }
    }

    fn must_call<T>(&self, msg: SyncControl, recv: Receiver<T>) -> T {
        self.control
            .as_ref()
            .expect("SyncRpc: control channel missing")
            .send(msg)
            .expect("SyncRpc: control channel closed");
        recv.recv().expect("SyncRpc: response channel closed")
    }

    pub fn get_autosync(&self) -> bool {
        let (complete, recv) = channel::bounded(1);
        self.must_call(SyncControl::GetAutosync { complete }, recv)
    }

    pub fn set_autosync(&self, autosync: bool) {
        let (complete, recv) = channel::bounded(1);
        self.must_call(SyncControl::SetAutosync { autosync, complete }, recv)
    }

    pub fn sync(&self, vid: VolumeId, direction: SyncDirection) -> Result<(), ClientErr> {
        let (complete, recv) = channel::bounded(1);
        self.must_call(SyncControl::Sync { vid, direction, complete }, recv)
    }

    pub fn reset_to_remote(&self, vid: VolumeId) -> Result<(), ClientErr> {
        let (complete, recv) = channel::bounded(1);
        self.must_call(SyncControl::ResetToRemote { vid, complete }, recv)
    }
}

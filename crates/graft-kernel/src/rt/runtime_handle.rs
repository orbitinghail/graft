use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc::Sender, task::JoinHandle};

use crate::rt::{
    rpc::{RpcHandle, RuntimeRpc},
    runtime::{Event, Runtime, RuntimeFatalErr},
};

use tokio::sync::mpsc;
use tokio_stream::{
    StreamExt,
    wrappers::{IntervalStream, ReceiverStream},
};

use crate::local::fjall_storage::FjallStorage;

#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeHandleInner>,
}

struct RuntimeHandleInner {
    handle: JoinHandle<Result<(), RuntimeFatalErr>>,
    storage: FjallStorage,
    rpc: RpcHandle,
}

impl RuntimeHandle {
    /// Spawn the Graft Runtime into the provided Tokio Runtime.
    /// Returns a RuntimeHandle which can be used to interact with the Graft Runtime.
    pub fn spawn(handle: &tokio::runtime::Handle, storage: FjallStorage) -> RuntimeHandle {
        let (tx, rx) = mpsc::channel(8);

        let rx = ReceiverStream::new(rx).map(|rpc| Event::Rpc(rpc));
        let ticks =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).map(|_| Event::Tick);
        let events = Box::pin(rx.merge(ticks));

        let runtime = Runtime::new(storage.clone(), events);
        let handle = handle.spawn(runtime.start());

        RuntimeHandle {
            inner: Arc::new(RuntimeHandleInner { handle, storage, rpc: RpcHandle::new(tx) }),
        }
    }
}

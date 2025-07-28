use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub enum RuntimeRpc {}

#[derive(Clone, Debug)]
pub struct RpcHandle {
    tx: Sender<RuntimeRpc>,
}

impl RpcHandle {
    pub fn new(tx: Sender<RuntimeRpc>) -> Self {
        Self { tx }
    }
}

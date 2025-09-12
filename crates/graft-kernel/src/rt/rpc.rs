use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub enum Rpc {}

#[derive(Clone, Debug)]
pub struct RpcHandle {
    tx: Sender<Rpc>,
}

impl RpcHandle {
    pub fn new(tx: Sender<Rpc>) -> Self {
        Self { tx }
    }
}

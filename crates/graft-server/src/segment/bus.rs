use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct Bus<T> {
    tx: broadcast::Sender<T>,
}

impl<T: Clone> Bus<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<T> {
        self.tx.subscribe()
    }

    pub fn publish(&self, msg: T) {
        // An error here means there are no receivers, which is fine
        let _ = self.tx.send(msg);
    }
}

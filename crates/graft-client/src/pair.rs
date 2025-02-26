use crate::{MetastoreClient, PagestoreClient};

/// Convenience struct wrapping a pair of `MetastoreClient` and `PagestoreClient`
#[derive(Debug, Clone)]
pub struct ClientPair {
    metastore: MetastoreClient,
    pagestore: PagestoreClient,
}

impl ClientPair {
    pub fn new(metastore: MetastoreClient, pagestore: PagestoreClient) -> Self {
        Self { metastore, pagestore }
    }

    #[cfg(test)]
    pub fn test_empty() -> Self {
        use crate::NetClient;
        Self {
            metastore: MetastoreClient::new(
                "invalid://foo:0".parse().unwrap(),
                NetClient::default(),
            ),
            pagestore: PagestoreClient::new(
                "invalid://foo:0".parse().unwrap(),
                NetClient::default(),
            ),
        }
    }

    pub fn metastore(&self) -> &MetastoreClient {
        &self.metastore
    }

    pub fn pagestore(&self) -> &PagestoreClient {
        &self.pagestore
    }
}

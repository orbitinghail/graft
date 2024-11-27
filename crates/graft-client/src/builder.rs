use reqwest::Url;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum ClientBuildErr {
    #[error("failed to build reqwest client: {0}")]
    ReqwestErr(#[from] reqwest::Error),

    #[error("failed to parse URL: {0}")]
    UrlParseErr(#[from] ParseError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientBuilder {
    /// The root URL (without any trailing path)
    pub endpoint: Url,
}

impl ClientBuilder {
    pub fn new(endpoint: Url) -> Self {
        Self { endpoint }
    }

    pub(crate) fn http(&self) -> reqwest::Result<reqwest::Client> {
        reqwest::Client::builder().brotli(true).build()
    }

    pub fn build<T: TryFrom<ClientBuilder, Error = ClientBuildErr>>(
        self,
    ) -> Result<T, ClientBuildErr> {
        self.try_into()
    }
}

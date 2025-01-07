use culprit::Culprit;
use reqwest::Url;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum ClientBuildErr {
    #[error("failed to build reqwest client")]
    ReqwestErr,

    #[error("failed to parse URL")]
    UrlParseErr,
}

impl From<ParseError> for ClientBuildErr {
    fn from(_: ParseError) -> Self {
        Self::UrlParseErr
    }
}

impl From<reqwest::Error> for ClientBuildErr {
    fn from(_: reqwest::Error) -> Self {
        Self::ReqwestErr
    }
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

    pub fn build<T: TryFrom<ClientBuilder, Error = Culprit<ClientBuildErr>>>(
        self,
    ) -> Result<T, Culprit<ClientBuildErr>> {
        self.try_into()
    }
}

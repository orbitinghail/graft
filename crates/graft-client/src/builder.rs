use culprit::Culprit;
use reqwest::Url;

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

#[derive(Debug)]
pub struct ClientBuilder {
    /// The root URL (without any trailing path)
    endpoint: Url,
    reqwest: reqwest::ClientBuilder,
}

impl ClientBuilder {
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            reqwest: reqwest::Client::builder().brotli(true),
        }
    }

    pub fn with_compression(mut self, enable: bool) -> Self {
        self.reqwest = self.reqwest.brotli(enable);
        self
    }

    pub(crate) fn http(self) -> reqwest::Result<reqwest::Client> {
        self.reqwest.build()
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    pub fn build<T: TryFrom<ClientBuilder, Error = Culprit<ClientBuildErr>>>(
        self,
    ) -> Result<T, Culprit<ClientBuildErr>> {
        self.try_into()
    }
}

use culprit::Culprit;

use thiserror::Error;
use url::{ParseError, Url};

use crate::USER_AGENT;

#[derive(Debug, Error)]
pub enum ClientBuildErr {
    #[error("failed to parse URL")]
    UrlParseErr,
}

impl From<ParseError> for ClientBuildErr {
    fn from(_: ParseError) -> Self {
        Self::UrlParseErr
    }
}

#[derive(Debug)]
pub struct ClientBuilder {
    /// The root URL (without any trailing path)
    endpoint: Url,
    builder: ureq::AgentBuilder,
}

impl ClientBuilder {
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            builder: ureq::AgentBuilder::new()
                .max_idle_connections_per_host(5)
                .user_agent(USER_AGENT),
        }
    }

    pub(crate) fn agent(self) -> ureq::Agent {
        self.builder.build()
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

use std::{any::Any, fmt::Debug, io};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use culprit::Culprit;
use graft_core::{
    gid::GidParseErr, lsn::InvalidLSN, page::PageSizeErr, page_idx::ConvertToPageIdxErr,
};
use graft_proto::common::v1::{GraftErr, GraftErrCode};
use splinter_rs::DecodeErr;
use thiserror::Error;

use crate::{
    api::response::ProtoResponse,
    segment::closed::SegmentValidationErr,
    volume::{
        catalog::VolumeCatalogErr,
        store::{self, VolumeStoreErr},
        updater::UpdateErr,
    },
};

pub struct ApiErr(Culprit<ApiErrCtx>);

impl From<Culprit<ApiErrCtx>> for ApiErr {
    #[inline]
    fn from(value: Culprit<ApiErrCtx>) -> Self {
        Self(value)
    }
}

impl<T: Into<ApiErrCtx>> From<T> for ApiErr {
    #[inline]
    #[track_caller]
    fn from(value: T) -> Self {
        Self(Culprit::new(value.into()))
    }
}

#[derive(Error, Debug)]
pub enum ApiErrCtx {
    #[error("invalid request body")]
    InvalidRequestBody,

    #[error("failed to parse id")]
    GidParseErr(#[from] GidParseErr),

    #[error("invalid page")]
    PageSizeErr(#[from] PageSizeErr),

    #[error("catalog error")]
    CatalogErr(#[from] VolumeCatalogErr),

    #[error("volume store error")]
    VolumeStoreErr(VolumeStoreErr),

    #[error("failed to load snapshot for volume")]
    SnapshotMissing,

    #[error("io error")]
    IoErr(io::ErrorKind),

    #[error("failed to validate segment")]
    SegmentValidationErr(#[from] SegmentValidationErr),

    #[error("failed to upload segment")]
    SegmentUploadErr,

    #[error("failed to download segment")]
    SegmentDownloadErr,

    #[error("volume commit rejected")]
    RejectedCommit,

    #[error("idempotent commit does not match previous request")]
    InvalidIdempotentCommit,

    #[error("graft client request failed")]
    ClientErr(#[from] graft_client::ClientErr),

    #[error("duplicate page index")]
    DuplicatePageIdx,

    #[error("failed to parse graft")]
    GraftDecodeErr(#[from] DecodeErr),

    #[error("too many page indexes")]
    GraftTooLarge,

    #[error("page indexes must be larger than zero")]
    ZeroPageIdx,

    #[error("invalid page index")]
    ConvertToPageIdxErr(#[from] ConvertToPageIdxErr),

    #[error("invalid LSN")]
    InvalidLSN,

    #[error("unauthorized")]
    Unauthorized,
}

impl From<io::Error> for ApiErrCtx {
    fn from(error: io::Error) -> Self {
        Self::IoErr(error.kind())
    }
}

impl From<VolumeStoreErr> for ApiErrCtx {
    fn from(value: VolumeStoreErr) -> Self {
        match value {
            VolumeStoreErr::CommitAlreadyExists => Self::RejectedCommit,
            other => Self::VolumeStoreErr(other),
        }
    }
}

impl From<UpdateErr> for ApiErrCtx {
    fn from(value: UpdateErr) -> Self {
        match value {
            UpdateErr::CatalogErr(err) => Self::CatalogErr(err),
            UpdateErr::StoreErr(err) => Self::VolumeStoreErr(err),
            UpdateErr::ClientErr(err) => Self::ClientErr(err),
        }
    }
}

impl From<InvalidLSN> for ApiErrCtx {
    fn from(_: InvalidLSN) -> Self {
        Self::InvalidLSN
    }
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        use ApiErrCtx::*;

        let code = match self.0.ctx() {
            SnapshotMissing => GraftErrCode::SnapshotMissing,
            RejectedCommit => GraftErrCode::CommitRejected,
            Unauthorized => GraftErrCode::Unauthorized,

            InvalidIdempotentCommit
            | InvalidRequestBody
            | GidParseErr(_)
            | PageSizeErr(_)
            | DuplicatePageIdx
            | GraftDecodeErr(_)
            | ConvertToPageIdxErr(_)
            | ZeroPageIdx
            | GraftTooLarge
            | InvalidLSN => GraftErrCode::Client,

            SegmentDownloadErr
            | SegmentUploadErr
            | VolumeStoreErr(store::VolumeStoreErr::ObjectStoreErr)
            | ClientErr(graft_client::ClientErr::HttpErr(_)) => GraftErrCode::ServiceUnavailable,

            ClientErr(graft_client::ClientErr::GraftErr(err)) => err.code(),

            _ => GraftErrCode::Server,
        };
        let message = self.0.ctx().to_string();

        match code {
            GraftErrCode::SnapshotMissing
            | GraftErrCode::CommitRejected
            | GraftErrCode::Unauthorized => {
                tracing::trace!(culprit = ?self.0, "client error")
            }
            GraftErrCode::Client => {
                tracing::debug!(culprit = ?self.0, "client error")
            }
            _ => tracing::error!(culprit = ?self.0, "api error"),
        }

        (
            graft_err_code_to_status(code),
            ProtoResponse::new(GraftErr { code: code as i32, message }),
        )
            .into_response()
    }
}

pub(crate) fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic occurred".to_string()
    };

    tracing::error!("panic occurred while handling api request: {details}");

    precept::expect_unreachable!(
        "panic occurred while handling api request",
        { "details": details }
    );

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        ProtoResponse::new(GraftErr {
            code: GraftErrCode::Server as i32,
            message: "internal server error".into(),
        }),
    )
        .into_response()
}

#[inline]
fn graft_err_code_to_status(code: GraftErrCode) -> StatusCode {
    match code {
        GraftErrCode::Unspecified => unreachable!("graft error code unspecified"),
        GraftErrCode::Client => StatusCode::BAD_REQUEST,
        GraftErrCode::SnapshotMissing => StatusCode::NOT_FOUND,
        GraftErrCode::CommitRejected => StatusCode::CONFLICT,
        GraftErrCode::Unauthorized => StatusCode::UNAUTHORIZED,
        GraftErrCode::Server => StatusCode::INTERNAL_SERVER_ERROR,
        GraftErrCode::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
    }
}

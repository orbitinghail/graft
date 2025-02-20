use std::{fmt::Debug, io};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use culprit::Culprit;
use graft_core::{
    gid::GidParseErr, lsn::InvalidLSN, page::PageSizeErr, page_index::ConvertToPageIdxErr,
};
use graft_proto::common::v1::{GraftErr, GraftErrCode};
use splinter::DecodeErr;
use thiserror::Error;

use crate::{
    api::response::ProtoResponse,
    segment::closed::SegmentValidationErr,
    volume::{catalog::VolumeCatalogErr, store::VolumeStoreErr, updater::UpdateErr},
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

    #[error("failed to load segment")]
    SegmentValidationErr(#[from] SegmentValidationErr),

    #[error("volume commit rejected")]
    RejectedCommit,

    #[error("graft client request failed")]
    ClientErr(#[from] graft_client::ClientErr),

    #[error("duplicate page offset")]
    DuplicatePageOffset,

    #[error("failed to parse offsets")]
    OffsetsDecodeErr(#[from] DecodeErr),

    #[error("requested too many page offsets")]
    TooManyOffsets,

    #[error("requested invalid offset")]
    InvalidOffset(u32),

    #[error("invalid page index")]
    ConvertToPageIdxErr(#[from] ConvertToPageIdxErr),

    #[error("invalid LSN")]
    InvalidLSN,
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

        let (status, code) = match self.0.ctx() {
            InvalidRequestBody => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            GidParseErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            PageSizeErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            DuplicatePageOffset => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            OffsetsDecodeErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            SnapshotMissing => (StatusCode::NOT_FOUND, GraftErrCode::SnapshotMissing),
            RejectedCommit => (StatusCode::CONFLICT, GraftErrCode::CommitRejected),
            ConvertToPageIdxErr(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            InvalidOffset(_) => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            TooManyOffsets => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            InvalidLSN => (StatusCode::BAD_REQUEST, GraftErrCode::Client),
            ClientErr(graft_client::ClientErr::HttpErr(_)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                GraftErrCode::ServiceUnavailable,
            ),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, GraftErrCode::Server),
        };
        let message = self.0.ctx().to_string();

        match code {
            GraftErrCode::Client | GraftErrCode::SnapshotMissing => {
                tracing::debug!(culprit = ?self.0, "client error")
            }
            _ => tracing::error!(culprit = ?self.0, "api error"),
        }

        (
            status,
            ProtoResponse::new(GraftErr { code: code as i32, message }),
        )
            .into_response()
    }
}

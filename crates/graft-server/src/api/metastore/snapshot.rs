use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use graft_proto::metastore::v1::SnapshotRequest;
use object_store::ObjectStore;

use crate::api::{error::ApiErr, extractors::Protobuf};

use super::MetastoreApiState;

pub async fn handler<O: ObjectStore>(
    State(state): State<Arc<MetastoreApiState<O>>>,
    Protobuf(req): Protobuf<SnapshotRequest>,
) -> Result<impl IntoResponse, ApiErr> {
    Ok("Hello, World!")
}

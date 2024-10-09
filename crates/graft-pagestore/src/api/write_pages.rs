use std::{sync::Arc, vec};

use axum::{extract::State, response::IntoResponse};
use bytes::{Bytes, BytesMut};
use graft_core::{guid::VolumeId, offset::Offset, page::Page};
use graft_proto::pagestore::v1::{SegmentInfo, WritePagesRequest, WritePagesResponse};
use prost::Message;
use tokio::sync::broadcast::error::RecvError;

use crate::segment::bus::WritePageReq;

use super::{error::ApiError, extractors::Protobuf, state::ApiState};

pub async fn handler(
    State(state): State<Arc<ApiState>>,
    Protobuf(req): Protobuf<WritePagesRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let vid: VolumeId = req.vid.try_into()?;

    // subscribe to the broadcast channel
    let mut commit_rx = state.subscribe_commits();

    let expected_pages = req.pages.len();
    for page in req.pages {
        let offset: Offset = page.offset;
        let page: Page = page.data.try_into()?;

        state
            .write_page(WritePageReq::new(vid.clone(), offset, page))
            .await;
    }

    // TODO listen for commit messages, building the response as we go
    // While we listen to commits and pull out segments relevant to the vid, we
    // can't terminate until we have seen all written offsets (or some kind of timeout expires)
    // For now, we will just sum up the cardinality of every matching offset set until we see enough offsets

    let mut segments: Vec<SegmentInfo> = vec![];

    let mut count = 0;
    while count < expected_pages {
        let commit = match commit_rx.recv().await {
            Ok(commit) => commit,
            Err(RecvError::Lagged(n)) => panic!("commit channel lagged by {}", n),
            Err(RecvError::Closed) => panic!("commit channel unexpectedly closed"),
        };

        if let Some(offsets) = commit.offsets.get(&vid) {
            // TODO: calculate Splinter cardinality
            // expected -= offsets.cardinality();
            count += expected_pages;

            // store the segment
            segments.push(SegmentInfo {
                sid: Bytes::copy_from_slice(commit.sid.as_ref()),
                offsets: offsets.inner().clone(),
            });
        }
    }

    let response = WritePagesResponse { segments };
    let mut buf = BytesMut::with_capacity(response.encoded_len());
    response
        .encode(&mut buf)
        .expect("insufficient buffer capacity");

    Ok(buf)
}

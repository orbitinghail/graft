pub mod segment {
    pub mod cache;
    pub mod closed;
    pub mod index;
    pub mod loader;
    pub mod open;
    pub mod uploader;
    pub mod writer;
}

pub mod api {
    pub mod auth;
    pub mod error;
    pub mod extractors;
    pub mod health;
    pub mod metastore;
    pub mod metrics;
    pub mod pagestore;
    pub mod response;
    pub mod routes;
    pub mod task;
}

pub mod volume {
    pub mod catalog;
    pub mod commit;
    pub mod kv;
    pub mod store;
    pub mod updater;
}

pub mod metrics {
    pub mod labels;
    pub mod registry;
    pub mod split_gauge;
}

pub mod bytes_vec;
pub mod limiter;
pub mod object_store_util;
pub mod resource_pool;
pub mod supervisor;

#[cfg(test)]
pub mod testutil;

static_assertions::assert_cfg!(
    target_endian = "little",
    "Graft currently only supports little-endian systems"
);

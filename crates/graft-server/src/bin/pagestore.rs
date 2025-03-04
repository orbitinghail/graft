use std::{sync::Arc, time::Duration};

use config::{Config, FileFormat};
use futures::{FutureExt, select};
use graft_client::{MetastoreClient, NetClient};
use graft_core::byte_unit::ByteUnit;
use graft_server::{
    api::{
        pagestore::{PagestoreApiState, pagestore_routes},
        routes::build_router,
        task::ApiServerTask,
    },
    metrics::registry::Registry,
    object_store_util::ObjectStoreConfig,
    segment::{
        cache::disk::{DiskCache, DiskCacheConfig},
        loader::SegmentLoader,
        uploader::SegmentUploaderTask,
        writer::SegmentWriterTask,
    },
    supervisor::Supervisor,
    volume::{
        catalog::{VolumeCatalog, VolumeCatalogConfig},
        updater::VolumeCatalogUpdater,
    },
};
use graft_tracing::{TracingConsumer, init_tracing};
use precept::dispatch::{antithesis::AntithesisDispatch, noop::NoopDispatch};
use rlimit::Resource;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc};
use url::Url;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
struct PagestoreConfig {
    catalog: VolumeCatalogConfig,
    cache: DiskCacheConfig,
    objectstore: ObjectStoreConfig,
    port: u16,
    metastore: Url,

    catalog_update_concurrency: usize,
    download_concurrency: usize,
    write_concurrency: usize,
}

impl Default for PagestoreConfig {
    fn default() -> Self {
        Self {
            catalog: Default::default(),
            cache: DiskCacheConfig {
                path: None,
                space_limit: ByteUnit::from_gb(1),
                open_limit: rlimit::getrlimit(Resource::NOFILE)
                    .expect("failed to get nofile limit")
                    .0 as usize
                    / 2,
            },
            objectstore: Default::default(),
            port: 3000,
            metastore: "http://localhost:3001".parse().unwrap(),
            catalog_update_concurrency: 16,
            download_concurrency: 16,
            write_concurrency: 16,
        }
    }
}

#[tokio::main]
async fn main() {
    let dispatcher =
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init_boxed(dispatcher).expect("failed to setup precept");

    init_tracing(TracingConsumer::Server, None);
    tracing::info!("starting Graft pagestore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut registry = Registry::default();

    let config = Config::builder()
        .add_source(config::File::new("pagestore.toml", FileFormat::Toml).required(false))
        .add_source(config::Environment::with_prefix("PAGESTORE").separator("_"))
        .build()
        .expect("failed to load config");
    let config: PagestoreConfig = config
        .try_deserialize()
        .expect("failed to deserialize config");

    let toml_config = toml::to_string_pretty(&config).expect("failed to serialize config");
    tracing::info!("loaded configuration:\n{toml_config}");

    assert!(config.cache.open_limit > 128, "cache_open_limit is too low");

    let store = config
        .objectstore
        .build()
        .expect("failed to build object store");

    let mut supervisor = Supervisor::default();

    let cache = Arc::new(DiskCache::new(config.cache).expect("failed to create disk cache"));
    let catalog =
        VolumeCatalog::open_config(config.catalog).expect("failed to open volume catalog");
    let loader = SegmentLoader::new(store.clone(), cache.clone(), config.download_concurrency);
    let updater = VolumeCatalogUpdater::new(config.catalog_update_concurrency);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);

    let client = NetClient::new();
    let metastore = MetastoreClient::new(config.metastore, client);

    supervisor.spawn(SegmentWriterTask::new(
        registry.segment_writer(),
        page_rx,
        store_tx,
        Duration::from_secs(1),
    ));

    supervisor.spawn(SegmentUploaderTask::new(
        registry.segment_uploader(),
        store_rx,
        store,
        cache,
    ));

    let state = Arc::new(PagestoreApiState::new(
        page_tx,
        catalog,
        loader,
        metastore,
        updater,
        config.write_concurrency,
    ));
    let router = build_router(registry, state, pagestore_routes());

    let addr = format!("0.0.0.0:{}", config.port);
    tracing::info!("listening on {}", addr);

    supervisor.spawn(ApiServerTask::new(
        "pagestore-api-server",
        TcpListener::bind(addr).await.unwrap(),
        router,
    ));

    select! {
        result = supervisor.supervise().fuse() => result.unwrap(),
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
            supervisor.shutdown(Duration::from_secs(5)).await.unwrap()
        }
    };
}

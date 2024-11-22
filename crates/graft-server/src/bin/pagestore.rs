use std::{fs::exists, sync::Arc, time::Duration};

use futures::{select, FutureExt};
use graft_client::{ClientConfig, MetastoreClient};
use graft_core::byte_unit::ByteUnit;
use graft_server::{
    api::{
        pagestore::{pagestore_router, PagestoreApiState},
        task::ApiServerTask,
    },
    object_store_util::ObjectStoreConfig,
    segment::{
        bus::Bus,
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
use rlimit::Resource;
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt, EnvFilter};
use twelf::{config, Layer};

#[config]
#[derive(Debug)]
#[serde(deny_unknown_fields)]
struct Config {
    catalog: VolumeCatalogConfig,
    cache: DiskCacheConfig,
    objectstore: ObjectStoreConfig,
    port: u16,
    metastore: ClientConfig,
}

impl Default for Config {
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
            metastore: ClientConfig {
                endpoint: "http://localhost:3001".parse().unwrap(),
            },
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .finish()
        .try_init()
        .expect("failed to initialize tracing subscriber");
    tracing::info!("starting pagestore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut layers = vec![
        Layer::DefaultTrait,
        Layer::Env(Some("PAGESTORE_".to_string())),
    ];

    if exists("pagestore.toml").is_ok_and(|p| p) {
        // insert the toml layer at the second position, after the default trait
        // and before loading env vars
        layers.insert(1, Layer::Toml("pagestore.toml".into()));
    }

    let config = Config::with_layers(&layers).expect("failed to load configuration");

    tracing::info!(?config, "loaded configuration");

    assert!(config.cache.open_limit > 128, "cache_open_limit is too low");

    let store = config
        .objectstore
        .build()
        .expect("failed to build object store");

    let mut supervisor = Supervisor::default();

    let cache = Arc::new(DiskCache::new(config.cache).expect("failed to create disk cache"));
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);
    let updater = VolumeCatalogUpdater::new(8);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);
    let commit_bus = Bus::new(128);

    let metastore =
        MetastoreClient::new_config(config.metastore).expect("failed to build metastore client");

    let api_state = Arc::new(PagestoreApiState::new(
        page_tx,
        commit_bus.clone(),
        catalog.clone(),
        loader,
        metastore,
        updater,
    ));
    let router = pagestore_router().with_state(api_state);

    supervisor.spawn(SegmentWriterTask::new(
        page_rx,
        store_tx,
        Duration::from_secs(1),
    ));

    supervisor.spawn(SegmentUploaderTask::new(store_rx, commit_bus, store, cache));

    let addr = format!("0.0.0.0:{}", config.port);
    tracing::info!("listening on {}", addr);

    supervisor.spawn(ApiServerTask::new(
        TcpListener::bind(addr).await.unwrap(),
        router,
    ));

    select! {
        result = supervisor.supervise().fuse() => result.unwrap(),
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
            supervisor.shutdown(Duration::from_secs(5)).await
        }
    };
}

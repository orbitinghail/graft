use std::{sync::Arc, time::Duration};

use config::{Config, FileFormat};
use futures::{FutureExt, select};
use graft_client::{MetastoreClient, NetClient};
use graft_core::byte_unit::ByteUnit;
use graft_server::{
    api::{
        auth::AuthState,
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
struct PagestoreConfig {
    catalog: Option<VolumeCatalogConfig>,
    cache: DiskCacheConfig,
    objectstore: Option<ObjectStoreConfig>,
    auth: Option<AuthState>,

    port: u16,
    metastore: Url,
    token: Option<String>,

    catalog_update_concurrency: usize,
    download_concurrency: usize,
    write_concurrency: usize,
}

#[derive(Debug)]
struct ConfigDefaults;

impl config::Source for ConfigDefaults {
    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new(ConfigDefaults)
    }

    fn collect(&self) -> Result<config::Map<String, config::Value>, config::ConfigError> {
        let open_limit = rlimit::getrlimit(Resource::NOFILE)
            .expect("failed to get nofile limit")
            .0
            / 2;

        let mut map = config::Map::new();

        macro_rules! set_default {
            ($key:expr, $val:expr) => {
                map.insert($key.into(), $val.into());
            };
        }

        set_default!("cache.space_limit", ByteUnit::from_gb(1).to_string());
        set_default!("cache.open_limit", open_limit);
        set_default!("port", 3000);
        set_default!("metastore", "http://localhost:3001");
        set_default!("catalog_update_concurrency", 16);
        set_default!("download_concurrency", 16);
        set_default!("write_concurrency", 16);

        Ok(map)
    }
}

#[tokio::main]
async fn main() {
    let dispatcher =
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init_boxed(dispatcher).expect("failed to setup precept");

    // sanity check that we don't enable precept in production
    let is_production = std::env::var("GRAFT_PRODUCTION").is_ok();
    assert!(
        !(is_production && precept::ENABLED),
        "precept is enabled in production"
    );

    init_tracing(TracingConsumer::Server, None);
    tracing::info!("starting Graft pagestore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut registry = Registry::default();

    let config = Config::builder()
        .add_source(ConfigDefaults)
        .add_source(config::File::new("pagestore.toml", FileFormat::Toml).required(false))
        .add_source(
            config::Environment::with_prefix("PAGESTORE")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()
        .expect("failed to load config");
    let config: PagestoreConfig = config
        .try_deserialize()
        .expect("failed to deserialize config");

    assert!(
        !is_production || config.auth.is_some(),
        "auth must be configured in production"
    );
    assert!(
        !is_production || config.token.is_some(),
        "api key must be configured in production"
    );

    let toml_config = toml::to_string_pretty(&config).expect("failed to serialize config");
    tracing::info!("loaded configuration:\n{toml_config}");

    assert!(config.cache.open_limit > 128, "cache_open_limit is too low");

    let store = config
        .objectstore
        .unwrap_or_default()
        .build()
        .expect("failed to build object store");

    let mut supervisor = Supervisor::default();

    let cache = Arc::new(DiskCache::new(config.cache).expect("failed to create disk cache"));
    let catalog = VolumeCatalog::open_config(config.catalog.unwrap_or_default())
        .expect("failed to open volume catalog");
    let loader = SegmentLoader::new(store.clone(), cache.clone(), config.download_concurrency);
    let updater = VolumeCatalogUpdater::new(config.catalog_update_concurrency);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);

    let client = NetClient::new(config.token);
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
    let router = build_router(registry, config.auth, state, pagestore_routes());

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

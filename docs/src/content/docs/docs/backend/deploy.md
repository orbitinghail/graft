---
title: Deploy Graft
description: Deploying Graft with Docker
---

### Docker images

The easiest way to deploy the Graft backend is via the Docker images.

```bash
docker pull ghcr.io/orbitinghail/metastore:latest
docker pull ghcr.io/orbitinghail/pagestore:latest
```

### From source

You can build the MetaStore and PageStore from source using:

```bash
cargo build --bin metastore --release --features precept/disabled
cargo build --bin pagestore --release --features precept/disabled
```

The resulting binaries will be available at `./target/release/{metastore,pagestore}`.

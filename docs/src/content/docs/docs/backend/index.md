---
title: Overview
description: How to self-host Graft
---

The Graft backend is composed of two services: the `PageStore` and the `MetaStore`. In order to run Graft yourself, you will need to run both services somewhere, and ensure that they are connected to a compatible object store (i.e. S3, R2, Tigris).

## Deployment Architecture

The Graft PageStore and MetaStore are ephemeral services that synchronously read and write to object storage. They take advantage of local disk to cache requests but are otherwise stateless - allowing them to scale out horizontally or be distributed across availability zones and regions.

The official Graft managed service runs Graft on Fly.io and uses Tigris as it's object storage provider. This allows Graft to seamlessly be available in regions all around the world.

Graft's networking protocol is Protobuf messages over HTTP. When deployed to the internet Graft should be placed behind a hardened proxy that can terminate SSL, load balance, and pipeline requests.

FROM rust:1.82 AS base
RUN cargo install sccache --version 0.8.2
RUN cargo install cargo-chef --version 0.1.68
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache

FROM base AS planner
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef cook --bins --release --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo build --release --bins

FROM gcr.io/distroless/cc:debug AS metastore
COPY --from=builder /app/target/release/metastore /metastore
COPY ./deploy/metastore/metastore.toml /metastore.toml
ENTRYPOINT ["/metastore"]

FROM gcr.io/distroless/cc:debug AS pagestore
COPY --from=builder /app/target/release/pagestore /pagestore
COPY ./deploy/pagestore/pagestore.toml /pagestore.toml
ENTRYPOINT ["/pagestore"]
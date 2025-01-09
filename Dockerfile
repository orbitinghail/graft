FROM rust:1.83 AS base
RUN apt-get update && apt-get install -y mold && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.68
RUN cargo install sccache --version 0.9.1
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache

# Enable instrumentation when INSTRUMENTED is set:
#   --build-arg INSTRUMENTED=1
COPY ./antithesis/libvoidstar.so /usr/lib/libvoidstar.so
ARG INSTRUMENTED
ENV LD_LIBRARY_PATH=${INSTRUMENTED:+"/usr/lib/libvoidstar.so"}
ENV RUSTFLAGS=${INSTRUMENTED:+"-Ccodegen-units=1 -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3 -Cllvm-args=-sanitizer-coverage-trace-pc-guard -Clink-args=-Wl,--build-id -L/usr/lib/libvoidstar.so -lvoidstar"}
ENV RUSTFLAGS=${RUSTFLAGS:-"-Ctarget-cpu=native -Clink-arg=-fuse-ld=mold"}
ENV PROFILE=${INSTRUMENTED:+"--profile dev"}
ENV PROFILE=${PROFILE:-"--release"}
ENV TARGET_DIR=${INSTRUMENTED:+"target/debug"}
ENV TARGET_DIR=${TARGET_DIR:-"target/release"}

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
    cargo chef cook --bins ${PROFILE} --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo build --bins ${PROFILE}
RUN mv ${TARGET_DIR} /artifacts

FROM gcr.io/distroless/cc:debug AS runtime
ARG INSTRUMENTED
COPY ./antithesis/libvoidstar.so /usr/lib/libvoidstar.so
ENV LD_LIBRARY_PATH=${INSTRUMENTED:+"/usr/lib/libvoidstar.so"}

FROM runtime AS metastore
COPY --from=builder /artifacts/metastore /metastore
COPY ./deploy/metastore/metastore.toml /metastore.toml
RUN ["sh", "-c", "mkdir /symbols && ln -s /metastore /symbols/metastore"]
ENTRYPOINT ["/metastore"]

FROM runtime AS pagestore
COPY --from=builder /artifacts/pagestore /pagestore
COPY ./deploy/pagestore/pagestore.toml /pagestore.toml
RUN ["sh", "-c", "mkdir /symbols && ln -s /pagestore /symbols/pagestore"]
ENTRYPOINT ["/pagestore"]

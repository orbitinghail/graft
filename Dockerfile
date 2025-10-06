FROM --platform=$BUILDPLATFORM rust:1.90-bookworm@sha256:d13fd52243fff0ed4faa6e09199022947aec4f0f8291b718769fc8c3d2a947f1 AS base

# increment to force rebuild of all layers
RUN echo "rebuild-deps: 1"

# install deps
RUN apt-get update && apt-get install -y clang libclang-dev llvm mold libncurses-dev build-essential libfuse3-dev && rm -rf /var/lib/apt/lists/*
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall -y --version 0.1.71 cargo-chef

# Enable instrumentation when INSTRUMENTED is set:
#   --build-arg INSTRUMENTED=1
COPY ./tests/antithesis/libvoidstar.so /usr/lib/libvoidstar.so
ARG INSTRUMENTED
ENV LD_LIBRARY_PATH=${INSTRUMENTED:+"/usr/lib/libvoidstar.so"}
ENV RUSTFLAGS=${INSTRUMENTED:+"-Ccodegen-units=1 -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3 -Cllvm-args=-sanitizer-coverage-trace-pc-guard -Clink-args=-Wl,--build-id -L/usr/lib/libvoidstar.so -lvoidstar"}
ENV RUSTFLAGS=${RUSTFLAGS:-"-Ctarget-cpu=native -Clink-arg=-fuse-ld=mold"}
ENV BUILDFLAGS=${INSTRUMENTED:+"--profile dev --bins"}
ENV BUILDFLAGS=${BUILDFLAGS:-"--release --features precept/disabled --bin metastore --bin pagestore"}
ENV TARGET_DIR=${INSTRUMENTED:+"target/debug"}
ENV TARGET_DIR=${TARGET_DIR:-"target/release"}

FROM base AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook ${BUILDFLAGS} --recipe-path recipe.json
COPY . .
RUN cargo build ${BUILDFLAGS}
RUN mv ${TARGET_DIR} /artifacts

FROM --platform=$BUILDPLATFORM gcr.io/distroless/cc-debian12:debug@sha256:529ebc7bb58c730a36a03f0ff7d97bc9191ac0ae691f88ac385546ab65234cc9 AS runtime
ARG INSTRUMENTED
COPY ./tests/antithesis/libvoidstar.so /usr/lib/libvoidstar.so
COPY ./LICENSE-APACHE /LICENSE-APACHE
COPY ./LICENSE-MIT /LICENSE-MIT
COPY ./README.md /README.md
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


FROM runtime AS test_workload
COPY --from=builder /artifacts/test_workload /test_workload
COPY ./crates/graft-test/workloads /workloads
COPY ./tests/antithesis/workloads /opt/antithesis/test
RUN ["sh", "-c", "mkdir /symbols && ln -s /test_workload /symbols/test_workload"]
ENTRYPOINT ["sleep", "infinity"]

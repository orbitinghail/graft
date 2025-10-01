FROM --platform=$BUILDPLATFORM rust:1.90-bookworm@sha256:71e8f5fcab8676c731bfff7d6fb789155eac167f94ab0348d52602175d784872 AS base

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

FROM --platform=$BUILDPLATFORM gcr.io/distroless/cc-debian12:debug@sha256:01fb4c3ba57bf2443fbfcc7967a223548f53c8f82a94f211104e735c39f38aae AS runtime
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

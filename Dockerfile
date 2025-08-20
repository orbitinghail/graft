FROM --platform=$BUILDPLATFORM rust:1.88 AS base

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

FROM --platform=$BUILDPLATFORM gcr.io/distroless/cc:debug@sha256:bc26aa70f59b73970e8b59a551502d3f4d731da284e58766ce297f2f8cdb1937 AS runtime
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

FROM base AS fjall_builder
RUN rm -rf /app
RUN git clone --depth=1 https://github.com/marvin-j97/rust-storage-bench /app
WORKDIR /app
RUN git fetch --depth=1 origin aa83b7d7dce4f5827c1cb6cfa7119ee0807c9251
RUN git checkout aa83b7d7dce4f5827c1cb6cfa7119ee0807c9251
RUN cargo build --profile dev --no-default-features --features mimalloc,fjall_nightly,antithesis

FROM runtime AS test_workload
COPY --from=builder /artifacts/test_workload /test_workload
COPY --from=fjall_builder /app/target/debug/rust-storage-bench /rust-storage-bench
COPY ./crates/graft-test/workloads /workloads
COPY ./tests/antithesis/workloads /opt/antithesis/test
RUN ["sh", "-c", "mkdir /symbols && ln -s /test_workload /symbols/test_workload"]
RUN ["sh", "-c", "ln -s /rust-storage-bench /symbols/rust-storage-bench"]
ENTRYPOINT ["sleep", "infinity"]

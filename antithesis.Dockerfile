FROM rust:1.91-bookworm@sha256:8fed34f697cc63b2c9bb92233b4c078667786834d94dd51880cd0184285eefcf AS base

# increment to force rebuild of all layers
RUN echo "rebuild-deps: 1"

# install deps
RUN apt-get update && apt-get install -y clang libclang-dev llvm mold && rm -rf /var/lib/apt/lists/*
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
RUN cargo binstall -y --version 0.1.73 cargo-chef

# setup build environment
COPY ./tests/antithesis/libvoidstar.so /usr/lib/libvoidstar.so
ENV LD_LIBRARY_PATH="/usr/lib/libvoidstar.so"
ENV RUSTFLAGS="-Ccodegen-units=1 -Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=3 -Cllvm-args=-sanitizer-coverage-trace-pc-guard -Clink-args=-Wl,--build-id -L/usr/lib/libvoidstar.so -lvoidstar"
ENV BUILDFLAGS="--profile dev --bin test_client"
ENV TARGET_DIR="target/debug"

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

FROM debian:bullseye-slim AS runtime
COPY ./tests/antithesis/libvoidstar.so /usr/lib/libvoidstar.so
ENV LD_LIBRARY_PATH="/usr/lib/libvoidstar.so"

FROM runtime AS test_client
COPY --from=builder /artifacts/test_client /test_client
COPY ./tests/antithesis/workloads /opt/antithesis/test
RUN ["sh", "-c", "mkdir /symbols && ln -s /test_client /symbols/test_client"]
ENTRYPOINT ["sleep", "infinity"]

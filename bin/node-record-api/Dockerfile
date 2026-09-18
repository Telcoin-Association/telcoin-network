# Container image for the Telcoin Network node-record API (see this crate's
# README for what it does).
#
# Mirrors etc/Dockerfile (the node image): a rust:1.94 builder stage compiles a
# single release binary, which is copied into a slim debian runtime that runs as
# a non-root user. Build from the repository root so the whole workspace is in
# the build context:
#
#   docker build -f bin/node-record-api/Dockerfile -t telcoin-node-record-api:latest .
FROM rust:1.94-slim-bookworm AS builder

ENV RUSTUP_TOOLCHAIN=1.94

WORKDIR /usr/src/telcoin-network

# Build dependencies, kept in step with etc/Dockerfile so the shared workspace
# dependency graph compiles identically (some transitive crates need cmake /
# libclang at build time).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libclang-16-dev \
    pkg-config \
    libssl-dev \
    libapr1-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo* ./
COPY bin/ ./bin/
COPY crates/ ./crates/
COPY chain-configs/ ./chain-configs/
COPY tn-contracts/ ./tn-contracts/

# Build only the API binary, then move it out of the cache mount so the
# runtime stage can COPY it.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=./target,sharing=locked \
    rm -rf /usr/local/cargo/registry/src \
    && cargo build -p tn-node-record-api --bin node-record-api --release \
    && mv ./target/release/node-record-api /tmp/

# production image
FROM debian:bookworm-slim

# The optional --rpc-url may be https: reqwest is built with rustls and the
# system root store, so ca-certificates is required at runtime.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# nonroot user id (matches the node image)
ENV USER_ID=1101

RUN useradd -u $USER_ID -U -ms /bin/bash nonroot
USER nonroot

COPY --from=builder /tmp/node-record-api /usr/local/bin/node-record-api

# The API plus its own /healthz and /readyz endpoints (default 8080). The
# Prometheus scrape endpoint, when enabled with --metrics, listens on its own
# separate address/port (expose it too if you enable it).
EXPOSE 8080

# The daemon requires --chain-id plus a bootstrap and a key source: pass
# --committee-file (or --bootstrap with --rpc-url / --keys-file), or their
# NODE_RECORD_API_* environment variables, at run time.
ENTRYPOINT ["node-record-api"]

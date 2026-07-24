# Container image for the Telcoin Network worker gateway (see this crate's
# README for what it does).
#
# Mirrors etc/Dockerfile (the node image): a rust:1.94 builder stage compiles a
# single release binary, which is copied into a slim debian runtime that runs as
# a non-root user. Build from the repository root so the whole workspace is in
# the build context:
#
#   docker build -f bin/worker-gateway/Dockerfile -t telcoin-worker-gateway:latest .
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

# Build only the gateway binary, then move it out of the cache mount so the
# runtime stage can COPY it.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=./target,sharing=locked \
    rm -rf /usr/local/cargo/registry/src \
    && cargo build -p tn-worker-gateway --bin worker-gateway --release \
    && mv ./target/release/worker-gateway /tmp/

# production image
FROM debian:bookworm-slim

# The gateway is HTTP-only (reqwest is built without a TLS backend, and https
# upstreams are rejected at startup), so it links no openssl at runtime. Only
# ca-certificates is installed, and only for parity with operator tooling; drop
# this RUN entirely if you want the smallest possible image.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# nonroot user id (matches the node image)
ENV USER_ID=1101

RUN useradd -u $USER_ID -U -ms /bin/bash nonroot
USER nonroot

COPY --from=builder /tmp/worker-gateway /usr/local/bin/worker-gateway

# Client JSON-RPC plus the gateway's own /health and /ready endpoints (default
# 8545). The Prometheus scrape endpoint, when enabled with --metrics, listens on
# its own separate address/port (expose it too if you enable it).
EXPOSE 8545

# The gateway requires at least one upstream: pass --config, or the inline
# --upstream-rpc-url / --upstream-readiness-url flags (or their WORKER_GATEWAY_*
# environment variables), at run time.
ENTRYPOINT ["worker-gateway"]

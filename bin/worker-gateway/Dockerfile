# Container image for the Telcoin Network worker gateway (see this crate's
# README for what it does).
#
# Mirrors etc/Dockerfile (the node image): a rust:1.94 builder stage compiles a
# single release binary, which is copied into a slim debian runtime that runs as
# a non-root user. Build from the repository root so the whole workspace is in
# the build context:
#
#   docker build -f bin/worker-gateway/Dockerfile -t telcoin-worker-gateway:<tag> .
#
# Both base images are pinned by the digest of their multi-arch index, so a
# rebuild starts from the same bytes on every platform until a pin is moved on
# purpose. To move one, read the new index digest (the top-level "Digest", not a
# per-platform manifest) with
#
#   docker buildx imagetools inspect rust:1.94-slim-bookworm
#   docker buildx imagetools inspect debian:bookworm-slim
#
# and replace the sha256 after the tag; keep the rust tag in step with
# RUSTUP_TOOLCHAIN below.
FROM rust:1.94-slim-bookworm@sha256:cf9dd0ec73e75f827fe59123fff9dc65af1a1c8363c3c31ee8d7f8ad0b6a5fb2 AS builder

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
# runtime stage can COPY it. --locked makes the build fail instead of resolving
# versions other than the ones in Cargo.lock.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=./target,sharing=locked \
    rm -rf /usr/local/cargo/registry/src \
    && cargo build --locked -p tn-worker-gateway --bin worker-gateway --release \
    && mv ./target/release/worker-gateway /tmp/

# production image (pinned by index digest, see the top of this file)
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587

# The gateway links rustls, not openssl, so no TLS library is installed.
# ca-certificates is required for an https --redirect-queries URL: rustls reads
# its trusted roots from the system store, which is empty without this package,
# and every request to that URL would then fail. Worker upstreams stay http-only.
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

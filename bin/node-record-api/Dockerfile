# Container image for the Telcoin Network node-record API (see this crate's
# README for what it does).
#
# Follows the layout of etc/Dockerfile (the node image), but copies only the
# paths the API's build needs: a rust:1.94 builder stage compiles a single
# stripped release binary, which is copied into a slim debian runtime that runs
# as a non-root user (uid 1101). Build from the repository root so the whole
# workspace is in the build context:
#
#   docker build -f bin/node-record-api/Dockerfile -t telcoin-node-record-api:<tag> .
#
# The build context needs the tn-contracts submodule checked out (run
# `make init-submodules` first): the API's dependency graph reaches tn-config,
# which embeds the contract artifacts at compile time.
#
# Both base images are pinned by the digest of their multi-arch index, so a
# rebuild starts from the same bytes on every platform until a pin is moved on
# purpose. To move one, read the new index digest (the top-level "Digest", not a
# per-platform manifest) with
#
#   docker buildx imagetools inspect rust:1.94-slim-bookworm
#   docker buildx imagetools inspect debian:bookworm-slim
#
# and replace the sha256 after the tag; keep RUSTUP_TOOLCHAIN below equal to
# the rustc the new builder image ships.
FROM rust:1.94-slim-bookworm@sha256:cf9dd0ec73e75f827fe59123fff9dc65af1a1c8363c3c31ee8d7f8ad0b6a5fb2 AS builder

# The exact toolchain the image above ships (rustc 1.94.1), so rustup never
# downloads one at build time; a channel such as 1.94 makes it fetch that
# channel's manifest. Change it with the base image:
#   docker run --rm <image> rustc --version
ENV RUSTUP_TOOLCHAIN=1.94.1

# Strip symbols and debuginfo from the release binary, as etc/Dockerfile does.
# Scoped to this build only, so the workspace's release profile is unchanged.
ENV CARGO_PROFILE_RELEASE_STRIP=true

WORKDIR /usr/src/telcoin-network

# Build dependencies, kept in step with etc/Dockerfile so the shared workspace
# dependency graph compiles identically (some transitive crates need cmake /
# libclang at build time). No libssl-dev / libapr1-dev: Cargo.lock has no
# OpenSSL (the workspace uses rustls) or APR, and MDBX, which the API's storage
# compiles from C source, needs only the C compiler and libclang (for bindgen)
# this list keeps.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libclang-16-dev \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo* ./
COPY bin/ ./bin/
COPY crates/ ./crates/
COPY chain-configs/ ./chain-configs/
COPY tn-contracts/ ./tn-contracts/
# Workspace members and [patch.crates-io] path sources named by the root
# Cargo.toml; cargo cannot load the workspace without them, even for -p.
COPY examples/ ./examples/
COPY patches/ ./patches/
# The vendored reth: the root Cargo.toml's reth-* dependencies are paths into it.
COPY vendor/ ./vendor/

# Build only the API binary, then move it out of the cache mount so the
# runtime stage can COPY it. --locked makes the build fail instead of resolving
# versions other than the ones in Cargo.lock.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=./target,sharing=locked \
    rm -rf /usr/local/cargo/registry/src \
    && cargo build --locked -p tn-node-record-api --bin node-record-api --release \
    && mv ./target/release/node-record-api /tmp/

# production image (pinned by index digest, see the top of this file)
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587

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
# Numeric, so a pod's runAsNonRoot can verify it without also setting
# runAsUser.
USER 1101:1101

COPY --from=builder /tmp/node-record-api /usr/local/bin/node-record-api

# The API plus its own /healthz and /readyz endpoints (default 8080). The
# Prometheus scrape endpoint, when enabled with --metrics, listens on its own
# separate address/port (expose it too if you enable it).
EXPOSE 8080

# The daemon requires --chain-id plus a bootstrap and a key source: pass
# --bootstrap or --bootstrap-servers-file (or a --committee-file that lists
# bootstrap servers) and --committee-file, --rpc-url or --keys-file, or their
# NODE_RECORD_API_* environment variables, at run time.
ENTRYPOINT ["node-record-api"]

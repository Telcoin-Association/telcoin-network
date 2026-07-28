FROM rust:1.94-slim-bookworm AS builder

ENV RUSTUP_TOOLCHAIN=1.94

# Strip symbols+debuginfo from the release binary. Scoped to this build only, so the
# workspace's release profile (CI, local) is unchanged.
ENV CARGO_PROFILE_RELEASE_STRIP=true

WORKDIR /usr/src/telcoin-network

# Cargo features for the node build. Empty by default; docker-adiri passes adiri,
# docker-devnet passes faucet.
ARG CARGO_FEATURES=

# Commit hash for the version string, injected as a vergen override (see
# crates/telcoin-network-cli/build.rs). Lets the build skip git entirely, so neither the
# .git dir nor the git binary is needed. Defaults to `unknown` for the compose build.
ARG GIT_SHA=unknown
ENV VERGEN_GIT_SHA=${GIT_SHA}

# build dependencies (no libssl-dev / libapr1-dev: the node uses rustls and links no
# OpenSSL or APR — verified against Cargo.lock).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libclang-16-dev \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the whole workspace. A .dockerignore keeps target/, .git/, .env and dev junk out, so
# new workspace members are picked up automatically and can never silently break this build.
COPY . .

# move binary out of cache for subsequent build steps
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=./target,sharing=locked \
    rm -rf /usr/local/cargo/registry/src \
    && cargo build -p telcoin-network --bin telcoin-network --release ${CARGO_FEATURES:+--features ${CARGO_FEATURES}} \
    && mv ./target/release/telcoin-network /tmp/

# production image
FROM debian:bookworm-slim

# ca-certificates for outbound rustls TLS (faucet / OTLP exporters). No libssl3 / libapr1:
# unused by the binary. bash + coreutils ship in the base and are required by
# etc/setup_validator.sh and etc/genesis.sh.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# specify user id for nonroot
ENV USER_ID=1101

# Create a non-root user (uid 1101; compose chowns data to 1101 and runs the node as 1101:1101)
RUN useradd -u $USER_ID -U -ms /bin/bash nonroot
USER nonroot

# copy binary
COPY --from=builder /tmp/telcoin-network /usr/local/bin/telcoin

# default
CMD ["telcoin", "node"]

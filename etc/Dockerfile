FROM rust:1.86-slim-bookworm AS builder

WORKDIR /usr/src/telcoin-network

# install dependencies
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

# move binary out of cache for subsequent build steps
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build --bin telcoin-network --release \
    && mv ./target/release/telcoin-network /tmp/

# production image
FROM debian:bookworm-slim

# openssl
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libapr1 \
    libssl3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# specify user id for nonroot
ENV USER_ID=1101

# Create a non-root user
RUN useradd -u $USER_ID -U -ms /bin/bash nonroot
USER nonroot

# copy binary
COPY --from=builder /tmp/telcoin-network /usr/local/bin/telcoin

# default
CMD ["telcoin", "node"]

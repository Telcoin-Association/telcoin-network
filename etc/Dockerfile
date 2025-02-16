FROM rust:1.82-slim-bookworm AS builder

WORKDIR /usr/src/telcoin-network

RUN apt-get update \
    && apt-get install -y build-essential cmake libclang-16-dev pkg-config libssl-dev libapr1-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo* ./
COPY bin/ ./bin/
COPY crates/ ./crates/
COPY tn-contracts/ ./tn-contracts/

# must move the resulting binary out of the cache for subsequent build steps
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=./target \
    cargo build --release --features faucet \
    && mv ./target/release/telcoin-network /tmp/

# Production Image
FROM debian:bookworm-slim

# resolves: 'openssl: error while loading shared libraries: libssl.so.3: cannot open shared object file: No such file or directory'
RUN apt-get update \
    && apt-get install -y libapr1-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# specify user id for nonroot
ENV USER_ID=1101

# Create a non-root user
RUN useradd -u $USER_ID -U -ms /bin/bash nonroot
USER nonroot

COPY --from=builder /tmp/telcoin-network /usr/local/bin/telcoin

CMD ["telcoin", "node"]

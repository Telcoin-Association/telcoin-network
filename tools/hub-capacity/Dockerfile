FROM ubuntu@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3

# Runtime only. Rust compilation stays in the pinned workspace CI job.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates curl iproute2 iptables libgcc-s1 libgmp10 libssl3t64 libstdc++6 \
    python3 python3-yaml util-linux \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /qualification

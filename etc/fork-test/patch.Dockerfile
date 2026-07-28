# Minimal image for etc/fork-test/patch-genesis.py, so the fork-test harness does not require
# ruamel.yaml on the host. Version-pinned: ruamel's round-trip dump formatting is what every
# node's genesis hash is computed over, so a pinned version keeps runs reproducible across machines.
FROM python:3.12-slim
RUN pip install --no-cache-dir ruamel.yaml==0.18.6

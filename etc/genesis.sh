#!/bin/bash


mkdir -p /home/nonroot/data/genesis/validators
cp -r /home/nonroot/data/validator-1/node-info.yaml /home/nonroot/data/genesis/validators/validator-1.yaml
cp -r /home/nonroot/data/validator-2/node-info.yaml /home/nonroot/data/genesis/validators/validator-2.yaml
cp -r /home/nonroot/data/validator-3/node-info.yaml /home/nonroot/data/genesis/validators/validator-3.yaml
cp -r /home/nonroot/data/validator-4/node-info.yaml /home/nonroot/data/genesis/validators/validator-4.yaml

/usr/local/bin/telcoin genesis \
    --datadir /home/nonroot/data/ \
    --chain-id 0x1e7 \
    --epoch-duration-in-secs 60 \
    --dev-funded-account 0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a \
    --max-header-delay-ms 1000 \
    --min-header-delay-ms 1000 \
    --consensus-registry-owner 0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a

# mkdir -p /home/nonroot/data/validator-1/genesis/
# mkdir -p /home/nonroot/data/validator-2/genesis/
# mkdir -p /home/nonroot/data/validator-3/genesis/
# mkdir -p /home/nonroot/data/validator-4/genesis/

# cp -r /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml /home/nonroot/data/genesis/worker_cache.yaml /home/nonroot/data/validator-1/genesis/
# cp -r /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-1/
# cp -r /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml /home/nonroot/data/genesis/worker_cache.yaml /home/nonroot/data/validator-2/genesis/
# cp -r /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-2/
# cp -r /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml /home/nonroot/data/genesis/worker_cache.yaml /home/nonroot/data/validator-3/genesis/
# cp -r /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-3/
# cp -r /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml /home/nonroot/data/genesis/worker_cache.yaml /home/nonroot/data/validator-4/genesis/
# cp -r /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-4/

# Create directories and copy files for each validator
for i in {1..4}; do
    mkdir -p /home/nonroot/data/validator-$i/genesis/
    cp /home/nonroot/data/genesis/genesis.yaml /home/nonroot/data/genesis/committee.yaml /home/nonroot/data/genesis/worker_cache.yaml /home/nonroot/data/validator-$i/genesis/
    cp /home/nonroot/data/parameters.yaml /home/nonroot/data/validator-$i/
done
chown -R 1101:1101 /home/nonroot/data

echo "done"

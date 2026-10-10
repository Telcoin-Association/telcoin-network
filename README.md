# Public hub capacity qualification

The candidate passed every predeclared threshold on the recorded envelope.

Binary source: `3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b`.
Qualification source verified against independently supplied Git objects: [`3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b`](https://github.com/Telcoin-Association/telcoin-network/tree/3914e53957fcc3ff7befe8b1a3f9a3284bfaba4b/tools/hub-capacity).
Recorded measurement run: [GitHub Actions run 37990571919](https://github.com/Telcoin-Association/telcoin-network/actions/runs/37990571919).
Frozen plan SHA-256: `06b3778e5590430da0203bdcad263b077df64ffd2168e16eceb6b10966d0abea`.

The plan records hardware, link conditions, population, exact profiles and thresholds.
The report and compressed archives retain all scored observations and their original hashes.

| Scenario | Baseline success | Baseline p99 ms | Candidate attempts | Candidate success | Candidate p99 / bound ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| committee_progress | 100.00% | 281.63 | 11359 / 512 | 100.00% | 553.39 / 1500 |
| concurrent_sync | 11.72% | 10055.78 | 256 / 256 | 100.00% | 2771.38 / 30000 |
| dao_connectivity | 100.00% | 262.07 | 512 / 512 | 100.00% | 181.16 / 1000 |
| gossip_two_hops | 52.88% | 29036.24 | 4096 / 4096 | 100.00% | 435.87 / 3000 |
| public_join | 0.00% | 29039.97 | 64 / 64 | 100.00% | 2443.94 / 8000 |
| record_lookup | 50.00% | 14096.09 | 256 / 256 | 100.00% | 293.56 / 3000 |
| shared_nat_reconnect | 35.94% | 25397.76 | 64 / 64 | 100.00% | 3014.49 / 20000 |
| submit_url_lookup | 50.00% | 14098.23 | 256 / 256 | 100.00% | 303.84 / 3000 |

Candidate whole-process resources:

| Hub | Peak sampled CPU cores / bound | Peak RSS MiB / bound | Maximum progress stall seconds / bound | Canonical progress |
| --- | ---: | ---: | ---: | ---: |
| hub-0 | 0.6858 / 0.75 | 397.09 / 2048 | 2.02 / 15 | 270 to 1233 |
| hub-1 | 0.6185 / 0.75 | 386.88 / 2048 | 2.02 / 15 | 270 to 1234 |

Candidate primary and worker allocations:

| Hub / swarm | Peak connections / limit | Peak public population | Minimum DAO population | Peak queue occupancy | Peak class tasks / limits |
| --- | ---: | ---: | ---: | ---: | --- |
| hub-0 / primary | 87 / 172 | 64 | 8 | 6 | epoch_record: 2 / 5, epoch_stream: 4 / 5, primary_shed: 0 / 8 |
| hub-0 / worker-0 | 86 / 172 | 64 | 8 | 4 | batch_stream: 4 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |
| hub-0 / worker-1 | 87 / 172 | 64 | 8 | 0 | batch_stream: 4 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |
| hub-1 / primary | 86 / 172 | 64 | 8 | 6 | epoch_record: 2 / 5, epoch_stream: 0 / 5, primary_shed: 0 / 8 |
| hub-1 / worker-0 | 87 / 172 | 64 | 8 | 1 | batch_stream: 1 / 5, prefetch: 1 / 8, worker_shed: 0 / 8 |
| hub-1 / worker-1 | 88 / 172 | 64 | 8 | 0 | batch_stream: 1 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |

Checksums detect bundle changes; they do not authenticate the measurements.

Run these commands from this evidence directory:

```sh
sha256sum -c SHA256SUMS
mkdir /tmp/tn-capacity-review
tar -xzf inputs.tar.gz -C /tmp/tn-capacity-review
tar -xzf baseline.tar.gz -C /tmp/tn-capacity-review
tar -xzf candidate.tar.gz -C /tmp/tn-capacity-review
python3 -B -I /tmp/tn-capacity-review/source/qualify.py score \
  /tmp/tn-capacity-review/plan.json \
  /tmp/tn-capacity-review/baseline-evidence/evidence.json \
  /tmp/tn-capacity-review/candidate-evidence/evidence.json \
  --output /tmp/tn-capacity-review/rescored.json
cmp report.json /tmp/tn-capacity-review/rescored.json
```

Generated validator keys and executable binaries are excluded. This qualification covers the declared Hub envelope; validator Launch qualification remains separate.

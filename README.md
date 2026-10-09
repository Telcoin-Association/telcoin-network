# Public hub capacity qualification

The candidate passed every predeclared threshold on the recorded envelope.

Binary source: `ca02e454b4f2fa5f5e1a47db8e346fb1bec00666`.
Qualification source verified against independently supplied Git objects: [`ca02e454b4f2fa5f5e1a47db8e346fb1bec00666`](https://github.com/Telcoin-Association/telcoin-network/tree/ca02e454b4f2fa5f5e1a47db8e346fb1bec00666/tools/hub-capacity).
Recorded measurement run: [GitHub Actions run 37940162416](https://github.com/Telcoin-Association/telcoin-network/actions/runs/37940162416).
Frozen plan SHA-256: `63463df5fdeae6c0770b7ee96ceba2f3a6ea488a7f1d35a38942d4871a132c3a`.

The plan records hardware, link conditions, population, exact profiles and thresholds.
The report and compressed archives retain all scored observations and their original hashes.

| Scenario | Baseline success | Baseline p99 ms | Candidate attempts | Candidate success | Candidate p99 / bound ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| committee_progress | 100.00% | 301.21 | 11915 / 512 | 100.00% | 433.79 / 1500 |
| concurrent_sync | 15.62% | 10288.06 | 256 / 256 | 100.00% | 2576.04 / 30000 |
| dao_connectivity | 100.00% | 174.86 | 512 / 512 | 100.00% | 261.16 / 1000 |
| gossip_two_hops | 56.93% | 29039.82 | 4096 / 4096 | 99.93% | 311.30 / 3000 |
| public_join | 4.69% | 29042.03 | 64 / 64 | 100.00% | 1809.81 / 8000 |
| record_lookup | 48.44% | 17312.52 | 256 / 256 | 100.00% | 535.77 / 3000 |
| shared_nat_reconnect | 35.94% | 24772.47 | 64 / 64 | 100.00% | 1885.62 / 20000 |
| submit_url_lookup | 48.44% | 17310.47 | 256 / 256 | 100.00% | 434.00 / 3000 |

Candidate whole-process resources:

| Hub | Peak sampled CPU cores / bound | Peak RSS MiB / bound | Maximum progress stall seconds / bound | Canonical progress |
| --- | ---: | ---: | ---: | ---: |
| hub-0 | 0.6193 / 0.75 | 374.26 / 2048 | 2.02 / 15 | 283 to 1318 |
| hub-1 | 0.7070 / 0.75 | 375.02 / 2048 | 0.00 / 15 | 283 to 1318 |

Candidate primary and worker allocations:

| Hub / swarm | Peak connections / limit | Peak public population | Minimum DAO population | Peak queue occupancy | Peak class tasks / limits |
| --- | ---: | ---: | ---: | ---: | --- |
| hub-0 / primary | 86 / 172 | 64 | 8 | 5 | epoch_record: 1 / 5, epoch_stream: 0 / 5, primary_shed: 0 / 8 |
| hub-0 / worker-0 | 87 / 172 | 64 | 8 | 3 | batch_stream: 2 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |
| hub-0 / worker-1 | 88 / 172 | 64 | 8 | 0 | batch_stream: 1 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |
| hub-1 / primary | 87 / 172 | 64 | 8 | 5 | epoch_record: 1 / 5, epoch_stream: 4 / 5, primary_shed: 0 / 8 |
| hub-1 / worker-0 | 87 / 172 | 64 | 8 | 0 | batch_stream: 4 / 5, prefetch: 1 / 8, worker_shed: 0 / 8 |
| hub-1 / worker-1 | 83 / 172 | 64 | 8 | 0 | batch_stream: 4 / 5, prefetch: 0 / 8, worker_shed: 0 / 8 |

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

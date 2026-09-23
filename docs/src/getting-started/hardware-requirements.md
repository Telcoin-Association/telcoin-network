# Hardware requirements

GSMA MNOs have the exclusive right to operate validator nodes and earn network fees.
This section is provided to support Telcoin Association's authorized validator node operators.
Telcoin (TEL) issuance is provided to Telcoin Network validators to incentivize the growth of a secure, compliant, efficient financial platform.

## Purpose of node specifications

Minimum hardware, connectivity, and hosting requirements are specified to maintain consistent and efficient performance across Telcoin Network.
These ensure that all nodes meet a foundational standard, contributing positively to the overall network's health.

## Pre-installation review

Operators are required to submit detailed specifications of their intended node setup to Telcoin Network development team for approval before installation.
Please contact our DevOps team at grant@telcoin.org with your proposed hardware specifications.

## Summary

Each figure on this page comes from a code constant, from the resource model in [How these numbers were derived](#how-these-numbers-were-derived), or from the benchmark described in that section.
The benchmark ran on 2026-09-23 on two GCP fleets, and its results are in the [benchmark report](https://claude.ai/artifact/6qpBKmd3eKNaRBuxUtPPCR), also committed as `bench/reports/2026-09-bench-10v.html` in the tn-transaction-generator repository.
On this page, e2 means the e2-custom-4-8192 fleet (4 vCPU, which is 2 physical cores, and 8 GB; run r20260923-0708) and c3 means the c3-highcpu-8 fleet (8 vCPU, which is 4 physical cores, and 16 GB; run r20260923-0515).
Each cell below says whether its figure is measured, modelled or provisional, and its note gives the run and the `sizing.json` field or the formula behind it.
Benchmark sources and code references are listed at the end of the page.

| Role | Tier | CPU (physical cores, PassMark single-thread) | RAM | Storage (capacity, sustained IOPS, MB/s, DWPD) | Network |
| --- | --- | --- | --- | --- | --- |
| Validator | Minimum | 4 physical cores (measured) [^b-val-cpu-min] | 16 GB (measured) [^b-val-ram-min] | 2 TB TLC NVMe, 10,000 write IOPS, 300 MB/s, 2 DWPD (modelled from measured demand) [^b-val-disk-min] | 200 Mbps symmetric (measured demand, modelled margin) [^b-net] |
| Validator | Recommended | 4 physical cores, PassMark 3,500 or higher (cores measured, PassMark modelled) [^b-val-cpu-rec] | 32 GB (measured) [^b-val-ram-rec] | 4 TB TLC NVMe, 20,000 IOPS, 500 MB/s, 1 DWPD (modelled from measured demand) [^b-val-disk-rec] | 1 Gbps (measured demand, modelled margin) [^b-net] |
| Validator | Headroom | As recommended (modelled) [^b-head] | 32 GB (modelled) [^b-head] | 8 TB, otherwise as recommended (modelled) [^b-head-disk] | 1 Gbps (modelled) [^b-head] |
| Observer (follower) | Minimum | 2 physical cores (measured) [^b-obs-min] | 8 GB (measured) [^b-obs-min] | As validator minimum (measured demand) [^b-obs-disk] | 50 Mbps (measured demand, provisional figure kept) [^b-obs-net] |
| Observer (follower) | Recommended | 4 physical cores (measured) [^b-obs-rec] | 16 GB (measured) [^b-obs-rec] | As validator recommended (measured demand) [^b-obs-disk] | 50 Mbps (measured demand, provisional figure kept) [^b-obs-net] |
| Observer (follower) | Headroom | 4 physical cores (modelled) [^b-head] | 16 GB (modelled) [^b-head] | 8 TB, as validator headroom (modelled) [^b-head-disk] | 50 Mbps (modelled) [^b-obs-net] |
| Observer (public RPC) | Minimum | 4 physical cores (modelled) [^b-rpc] | 16 GB (modelled) [^b-rpc] | As validator minimum (measured demand) [^b-obs-disk] | 50 Mbps plus RPC traffic (RPC not measured) [^b-rpc] |
| Observer (public RPC) | Recommended | 8 physical cores (provisional, RPC not measured) [^b-rpc] | 32 GB (modelled) [^b-rpc] | As validator recommended (measured demand) [^b-obs-disk] | Sized for RPC traffic (not measured) [^b-rpc] |
| Observer (public RPC) | Headroom | 8 or more physical cores, scaled to RPC load (not measured) [^b-rpc] | 64 GB (modelled) [^b-rpc] | 8 TB, as validator headroom (modelled) [^b-head-disk] | Sized for RPC traffic (not measured) [^b-rpc] |

Every validator tier also needs a p95 round-trip time well below 1 s to at least 7 of the 10 committee members (see [Networking](#networking)).

The tiers mean:

- Minimum: the smallest configuration that kept up with the benchmark's mixed phase without memory or IO stalls, with enough memory for the restart replay peak described under [Memory](#memory).
- Recommended: enough margin that p95 use of CPU, memory, disk IOPS and bandwidth stays below half of capacity at the benchmark load, plus one year of storage growth at the [per-epoch batch-cache ceiling](#per-epoch-batch-cache-ceiling) (about 200 TPS with 8-hour epochs). Storage uses the ceiling because the benchmark's load is far above what the release can sustain with 8-hour epochs.
- Headroom: sized for the highest sustained load the current release can carry, which is set by the batch-cache ceiling, including restart replay at that load and three years of storage growth. That load is under a seventeenth of what the benchmark ran, so the recommended CPU, memory and network already cover it. Headroom beyond Recommended is about storage growth and RPC caching, not throughput.

Neither fleet's 100 GB network-attached volume ran free of IO stalls (see [Storage](#storage)), so every tier's disk figures come from measured demand plus margin, not from a tested volume.

Cloud vCPUs are usually hyperthreads, so 8 vCPUs are 4 physical cores.
Compare physical cores when reading this table.

### Provisional estimates

These estimates come from the model and from the networks running today.
They were made before the benchmark ran, and the table above replaces them.

| Role | CPU | RAM | Storage | Network |
| --- | --- | --- | --- | --- |
| Validator, minimum | 4 physical cores | 16 GB | 2 TB TLC NVMe, at least 15,000 sustained IOPS | 200 Mbps |
| Validator, recommended | 8 physical cores, PassMark single-thread 4000 or higher | 32 GB | 4 TB TLC NVMe rated for at least 1 DWPD | 1 Gbps |
| Observer, follower | 4 physical cores | 8 to 16 GB | Same as a validator | 50 Mbps |
| Observer, public RPC | 8 physical cores | 32 GB | Same as a validator | Sized for RPC traffic |

The benchmark moved these provisional figures:

- Recommended validator CPU fell from 8 to 4 physical cores, and its PassMark floor from 4,000 to 3,500.
- The minimum disk changed from 15,000 sustained IOPS to 10,000 sustained write IOPS plus a 300 MB/s throughput floor, and needs a 2 DWPD rating.
- The recommended disk gained a 20,000 IOPS and 500 MB/s floor.
- The follower observer minimum fell from 4 to 2 physical cores and settled at 8 GB, with 16 GB recommended.

Validator RAM (16 and 32 GB), storage capacity (2 and 4 TB) and network (200 Mbps and 1 Gbps) held.

Earlier versions of this page asked validators for 16 cores / 32 threads, 128 GB of RAM and 4 to 7.5 TB of NVMe, and observers for 8 cores / 16 threads and 16 to 32 GB.
Those figures were not derived from measurement.
Against them, validator RAM moved from 128 GB to 16 GB minimum and 32 GB recommended, CPU from 16 cores to 4 physical cores, and storage from 4 to 7.5 TB to 2 to 4 TB, sized for the batch-cache ceiling of about 200 TPS.

## Validator

### CPU

Architecture: x86-64.

Block execution is sequential.
The engine executes one consensus output at a time on a single blocking thread [^engine-single], and transactions inside a block run in order.
Signature recovery for an output's transactions runs in parallel [^ecrecover] on a thread pool sized to the core count minus two [^rayon], and incoming batches are checked on the same pool [^batch-validator].
Single-thread speed therefore sets how fast a node executes.
Extra cores help with signature recovery and with the networking, database and RPC work that runs next to execution.

Execution speed also limits consensus speed:

- A validator votes on a header only after it has executed the block that the header names as its author's latest executed block [^vote-wait].
- A node holds back each commit until it has executed the block the leader had executed when it proposed [^commit-wait].
- A header needs votes from ⌊2N/3⌋ + 1 committee members to become a certificate, which is 7 of 10 [^quorum].

Rounds therefore advance at the pace of the 7th fastest executor in a committee of 10.
A validator slower than that falls behind on its own and does not slow the others.
If 4 or more validators are slow, the committee runs at their pace.

The benchmark shows the limit.
The e2 validators (2 physical cores) used 44% of their vCPUs on average and 55% at p95, and execution could not keep up with consensus: every e2 validator's engine queue reached 7 or 8 outputs, against a limit of 8, and pending pools on seven of the ten peaked at 9,480 to 9,850 transactions, near the pool's default limit of 10,000.
The c3 validators (4 physical cores) used 13% on average and 25% at p95, their engine queues never held more than 3 outputs, and the fleet sustained 5,384 TPS against 3,569 on e2.
Total CPU percentage understates an execution bottleneck, because one saturated execution thread is only 25% of a 4-vCPU host.
Watch the engine queue instead (see [Capacity monitoring](validator-operations.md#capacity-monitoring)).

Buy single-thread speed first.
A higher-clocked 8-core part does more for a validator than a 32-core part at a lower clock.

### Memory

Steady state does not size memory.
On devnet, validators run in 2.2 to 3.6 GB with the node process at about 1.6 GB resident.
Memory is sized by two events that hold many consensus outputs at once:

- Overload: execution falls behind consensus and outputs queue up.
- Restart replay: a node that stopped mid-epoch re-executes every output it committed but had not executed. Replay has no memory gate. It streams each missed output toward the engine and relies on channel depth alone for backpressure [^replay].

Under the benchmark load, validators used:

| Fleet | Memory used, p95 | Node RSS, p95 | Anonymous RSS, peak | File-backed RSS, peak | Memory pressure (PSI some avg10), peak |
| --- | --- | --- | --- | --- | --- |
| e2, 8 GB | 81% | 7.15 GB | 5.6 to 6.4 GB | 1.7 to 1.8 GB | 0.9 to 4% |
| c3, 16 GB | 52% | 10.8 GB | 4.1 to 8.1 GB | 2.1 to 3.1 GB | 0 |

The first two columns are the worst validator in each fleet (`gcp_by_role.validator.memory_percent_used.p95_max_node`, `telcoin_rss_bytes.p95_max_node`).
The last three are the range across validators from the on-node sampler (`sampler_by_node.*.rss_anon_kb.run_max`, `rss_file_kb.run_max`, `psi_mem_some_avg10.run_max`).
No node was OOM-killed.
On e2 the node filled 7.2 of 8 GB and the kernel was reclaiming pages it needed, so 8 GB fails the minimum rule even though no node ran out.
On c3 memory pressure stayed at zero.
Its 52% at p95 is just over the half-capacity line, so the recommended tier is 32 GB.
Anonymous memory on one c3 validator reached 8.1 GB, more than an 8 GB host holds in total.
c3 RSS is higher partly because the node maps its databases into memory and those file-backed pages stay resident while RAM is free, and partly because anonymous memory rose with the higher load.

The model is:

```text
M ≈ (M_base + M_dag + M_pool + M_out + M_notif + M_rpc + M_thr) · (1 + α) + PC_hot
```

| Term | What it holds | What bounds it |
| --- | --- | --- |
| M_base | Binary, allocator arenas, static tables | Measured at idle: 0.3 to 0.5 GB on the fresh benchmark chain before load [^b-mbase]; about 1.6 GB on long-running devnet nodes |
| M_dag | Consensus DAG: certificates for up to 50 rounds per member. Headers carry batch digests, not batch bodies | Garbage-collection depth of 50 rounds [^gc] |
| M_pool | Transaction pool | `--txpool.pending-max-size`, `--txpool.basefee-max-size`, `--txpool.queued-max-size`, 20 MB each by default [^txpool] |
| M_out | Consensus outputs waiting for or under execution | See below |
| M_notif | Canonical-chain notifications held for slow subscribers (RPC subscriptions, pool maintenance, ExEx) | 256 notifications [^canon] |
| M_rpc | RPC response caches | `--rpc-cache.*` flags [^rpc-cache]; small when RPC is closed to the public |
| M_thr | Thread stacks and per-thread buffers | Core count minus two for the parallel pool [^rayon], plus async runtime threads |
| α | Allocator fragmentation | Not measured: the benchmark did not collect allocator statistics. Measured RSS includes it |
| PC_hot | Page cache that keeps the hot part of the execution database off disk | About 3 GB at the benchmark's chain size, growing with state (see below) [^b-pchot] |

M_out dominates under overload and replay:

```text
M_out ≈ 2 · N · b_h · S_b · (k_q · I + k_exec)
```

- 2 · N · b_h · S_b is the batch data in one consensus output. Leaders are elected on even rounds and commit once f+1 certificates in the next round reference them [^leader], so an output usually covers two rounds of headers from N validators. Each header carries b_h batches (at most 10, and a header is proposed once 5 are ready [^max-batches] [^threshold]) of S_b bytes (at most 1 MB or 30 million gas per batch [^batch-limits]).
- k_q is memory per byte of a queued output: not measured separately (see below).
- k_exec is the extra memory per byte for the output under execution (recovered transactions, EVM state, trie updates): not measured separately.
- I is the number of outputs held at once. It is 1 or 2 in normal operation. Under overload or replay it reaches about 73: 64 in the channel from the epoch manager to the engine [^to-engine], 8 in the engine queue [^engine-queue], and 1 executing [^engine-single].

At the protocol maximum (N = 10, b_h = 10, S_b = 1 MB) one output could hold 200 MB, and 73 of them 14.6 GB.
A second, lower limit applies.
Replay never crosses an epoch boundary [^replay], and all batch data in an epoch must fit in the 1 GiB batch cache (see [Storage](#storage)).
So replay holds at most about 1 GiB of batch data, times k_q, on top of the execution working set.

The benchmark could not separate k_q and k_exec from the rest of resident memory.
Its consensus outputs averaged 1.0 to 1.25 MB of batch data and never exceeded 5.5 MB (`prometheus.tn_primary_consensus_output_bytes`), so even a full 73-output backlog held under 0.4 GB of batch data, too little to isolate.
It measured the replay peak directly instead.
After a 5-minute outage at full load, the restarted e2 validator peaked at 7.0 GB resident with its engine queue at the limit of 8 [^b-replay].
That is the same level as the e2 validators that never stopped (7.2 GB), so at this load the replay peak is the execution working set, not queued outputs.
The restarted c3 validator did not resume execution before the run ended (see [Benchmark](#benchmark)), so there is no c3 replay figure.

PC_hot shows in disk reads.
The c3 validators held 2.1 to 3.1 GB of file-backed pages and read almost nothing from disk (5 IOPS at p95).
The e2 validators held 1.7 to 1.8 GB, and five of the ten read 1,800 to 3,000 IOPS at p95, most likely page-cache misses.
So PC_hot is about 3 GB at the benchmark's chain size, and it grows with state.

Disable swap.
A swapping validator keeps running but executes slowly, and because votes wait on execution it drags on the committee instead of failing visibly.
Use ECC memory on bare-metal hosts.

### Storage

Every node is an archive node.
The node refuses to start with any pruning configuration [^archive], so disk use only grows.
Plan capacity from the growth formula, not from the size of the chain today.

| Path under the datadir | Contents | Growth |
| --- | --- | --- |
| `db`, `static_files` | Execution database and static files | β_reth per transaction |
| `consensus-db/epochs` | Consensus packs: committed outputs with their batches, one directory per epoch | β_pack per transaction |
| `consensus-db/cache` | Batch cache | Fixed 1 GiB maximum, reused each epoch |
| `consensus-db/epoch` | Current epoch's consensus tables | Fixed 512 MiB maximum, cleared each epoch |

Paths are set in `crates/config` [^datadir], and the two fixed maximum sizes in `crates/storage` [^cache-max].

Daily growth is:

```text
G_day = 86400 · λ · (β_reth + β_pack) + G_idle
```

- λ is sustained transactions per second.
- β_reth is bytes added to the execution database and static files per transaction: 251 on e2 and 257 on c3, a lower bound [^b-beta-reth].
- β_pack is bytes added to consensus packs per transaction: 92 on e2 and 94 on c3, also a lower bound [^b-beta-pack].
- G_idle is growth per day with no user transactions: about 0.2 GB (modelled from adiri at idle).

Together the β values are about 345 bytes per transaction.
Both are measured on the benchmark mix, whose average transaction is about 190 bytes in a batch (188 on c3, 192 on e2) [^b-txbytes].
Both fleets agree to within 3%.
β_reth is a lower bound because the execution database file `db/mdbx.dat` kept the same 4.3 GB size from node start to teardown in every run: MDBX allocates that file in large steps, so growth inside it did not show, and only static-file growth was counted.
The [storage growth planning](#storage-growth-planning) table applies the formula to other loads.

Execution reads state on its single thread, so random-read latency adds directly to execution time.
The consensus database also syncs to disk on every commit in production builds [^mdbx-durable].

Disk demand at p95 on the busiest validator was [^b-disk]:

| Fleet | Read IOPS | Write IOPS | Write throughput | IO pressure (PSI some avg10), peak |
| --- | --- | --- | --- | --- |
| e2 | 2,990 | 5,120 | 137 MB/s | 15 to 37% |
| c3 | 5 | 2,440 | 190 MB/s | 47 to 60% |

c3 wrote more bytes in fewer, larger IOs, and its extra memory kept reads in page cache.
Disk was the tightest resource in both fleets.
On e2, write IOPS ran at 57% to 62% of the volume's 9,000 IOPS limit, and IO pressure was the highest pressure signal on every node.
On c3, every validator's p95 write rate sat between 181 and 190 MB/s with IO pressure around 50% at p95, so the volume held the fleet below the throughput it wanted.
Size sustained IOPS from these p95 figures and write throughput from the c3 plateau, with the same 2x margin as the recommended tier: 2 × (2,990 + 5,120) is 16,220 IOPS, so the recommended tier asks for 20,000, and twice the c3 plateau is 380 MB/s, so it asks for 500.
Quote sustained figures, not burst.
Endurance is covered under [Storage: TLC over QLC](#storage-tlc-over-qlc).

#### Per-epoch batch-cache ceiling

Every batch a validator creates or receives is written to the batch cache in `consensus-db/cache` [^cache-tables].
The cache is an MDBX environment with a fixed 1 GiB maximum [^cache-max], and batches leave it only when the epoch closes [^cache-clear].
The batch data produced by the whole committee in one epoch must therefore fit in about 1 GiB, whatever hardware the node has.

The default epoch is 8 hours [^epoch-default].
Dividing 1 GiB by the epoch length gives the ceiling:

| Epoch length | Batch bytes per second | TPS at 110 B | TPS at 188 to 192 B (benchmark mix, measured) | TPS at 250 B | TPS at 400 B |
| --- | --- | --- | --- | --- | --- |
| 8 h (default) | 37,283 | 339 | 194 to 198 | 149 | 93 |
| 6 h (adiri) | 49,710 | 452 | 259 to 264 | 199 | 124 |
| 1 h | 298,262 | 2,711 | 1,553 to 1,586 | 1,193 | 746 |
| 20 min (benchmark) | 894,785 | 8,134 | 4,660 to 4,759 | 3,579 | 2,237 |

The 110, 250 and 400 B columns count raw transaction bytes.
The benchmark measured about 190 B of batch data per transaction for its mix, which includes batch encoding [^b-txbytes].
At that size the 8-hour ceiling is about 200 TPS (194 to 198), and the 6-hour ceiling about 260 TPS.
These are still upper bounds, because MDBX page overhead inside the cache is not included.

Reaching the ceiling is fatal.
If the cache cannot store a validator's own batch, the worker returns `FatalDBFailure` [^seal-fatal] and the batch builder exits.
The batch builder is a critical task, and a critical task's exit shuts down the rest of the epoch's tasks [^bb-critical].
More hardware does not raise the ceiling because the size is a compiled-in constant.
A release with a larger cache or a shorter epoch length does.

The ceiling also caps chain growth.
At 8-hour epochs a committee can carry at most about 3.2 GB of batch data per day, 1.18 TB per year.
Disk growth is that amount times m = (β_reth + β_pack) / 190 B, which is about 1.8 on the benchmark mix, plus G_idle.
That gives about 5.9 GB a day, or with G_idle about 2.2 TB a year and 6.6 TB over three years (lower bounds, because β_reth is).
m is close to 1.8 for transfer-heavy and contract-heavy loads too, so this bound holds whatever the mix.
The recommended and headroom tiers use it.

At 20-minute epochs the ceiling is in the thousands of TPS, so a benchmark with short epochs measures hardware limits, not this ceiling.
The benchmark stayed below its own 20-minute ceiling: its epochs carried about 0.52 to 0.55 GB of batch data on average, and no node reported `FatalDBFailure`.
The cache file still reached its 1 GiB maximum on every validator, because MDBX never shrinks the file.
File size is a high-water mark, not live occupancy (see [Capacity monitoring](validator-operations.md#capacity-monitoring)).

### Networking

Open the UDP ports and apply the firewall policy in [Validator production operations](validator-operations.md#firewall-configuration).
Use a static public IP address.

Per-node ingress is modelled as:

```text
ingress ≈ λ · S_tx · (1 + N_fetch) + gossip overhead
```

- λ · S_tx is transaction data per second. Each batch author sends the batch body directly to every other committee member [^qw-fanout], so a validator receives each transaction once.
- N_fetch counts extra copies, fetched when a direct send was missed and the node pulls the batch before voting or executing.
- Gossip overhead is batch digests, headers, votes and certificates. It grows with committee size and round rate, not with λ.

Egress is similar.
A validator sends each of its own batches to N − 1 peers, so with load spread evenly its egress is about λ · S_tx · (N − 1) / N, plus gossip, plus what it serves to observers and syncing nodes.

At 1,000 TPS of 250-byte transactions, λ · S_tx is 250 KB/s, about 2 Mbps.
Steady-state bandwidth is small.
The 200 Mbps minimum is sized for catch-up and for serving consensus output and epoch packs to syncing nodes, which move whole epochs at a time.

In the benchmark, p95 ingress on the busiest validator was 24 Mbps on e2 and 21 Mbps on c3, both on the validator restarted by the kill test; the other validators received 6 to 17 Mbps [^b-net].
That matches the ingress model at the benchmark's rate.
p95 egress was 56 Mbps on e2 and 86 Mbps on c3, far above the model.
Each validator also served JSON-RPC to its in-zone load generator, which fetched every block and all of its receipts, and the benchmark did not separate RPC traffic from peer traffic.
Most of the gap is likely that RPC traffic, so a validator with closed RPC sends less.

Latency matters more than bandwidth.
Each round is a header broadcast, votes back, and a certificate broadcast, so about 1.5 round trips to the quorum.
Headers are proposed between 1 s and 2.5 s apart by default [^header-delay].
Keep p95 round-trip time to at least 7 of the 10 committee members well below 1 s.
The benchmark spans five GCP regions on three continents but did not measure round-trip time between them.
Geography still showed: the two validators in australia-southeast1 had their batches included later than the others, and senders that submitted through them saw p95 latency of 325 to 350 s, against 20 to 27 s through the European validators.

### Operating system

Supported: Linux LTS releases (Debian 11+, Ubuntu 20.04+, Red Hat Enterprise Linux 8).
Use a kernel with pressure stall information (mainline 4.20 or later) so the checks in [Capacity monitoring](validator-operations.md#capacity-monitoring) work.

- Disable swap (see [Memory](#memory)).
- Run time sync (chrony or systemd-timesyncd). A node rejects a header timestamped more than 1 s ahead of its own clock and waits out any smaller difference before voting [^drift]. Clock error of 1 s makes a validator reject honest headers or have its own rejected, and smaller errors delay its votes.

Email support@telcoin.org to confirm hardware specifications before purchasing any equipment.

## Observer

An observer is any node that is not in the current committee.
The node picks its role at startup from committee membership [^observer-role]; the old `--observer` flag is ignored.
An observer receives every consensus output, executes every block, and keeps the same archive data as a validator.
It does not vote.
Transactions sent to an observer's RPC are sealed into a local batch and forwarded to the JSON-RPC endpoints that validators advertise [^forward].

Observers suit developers building on Telcoin Network, businesses that want to verify transactions and account state themselves, auditors and researchers who need direct access to chain data, service providers that submit transactions directly, and community members who want to verify the network.

There are two profiles:

- A follower verifies the chain for its operator and serves little or no public RPC.
- A public RPC observer serves JSON-RPC to outside users. Its CPU and memory are sized by RPC traffic, not by consensus.

The benchmark ran one follower in each fleet and sent it no RPC load.
It was restarted 25 minutes into the mixed phase and caught up: its static files ended each run the same size as the validators'.

### CPU

Architecture: x86-64 or ARM64.

Observers execute the same blocks on the same single thread as validators, so single-thread speed decides whether they keep up.
An observer is not on the voting path.
If it falls behind, it catches up without slowing the committee.
RPC calls such as `eth_call` and `eth_estimateGas` run beside execution and scale with core count, which is why the public RPC profile has more cores.

The e2 observer (2 physical cores) used 22% of its vCPUs on average and 27% at p95, and its engine queue reached the limit of 8 at times.
The c3 observer (4 physical cores) used 8% on average and 13% at p95, and its queue never held more than 3 outputs [^b-obs-min] [^b-obs-rec].

### Memory

The consensus terms are smaller than on a validator.
Observers do not subscribe to batch gossip [^observer-gossip] and have no batches of their own to cache.
They queue and replay outputs the same way, so M_out applies unchanged.

In the benchmark the observer held 3.7 GB resident at p95 in both fleets, which was 38% of memory on e2 and 15% on c3.
Memory pressure peaked at 1% on e2 and stayed at zero on c3.

On a public RPC observer the main memory risk is RPC caching.
The `--rpc-cache.*` flags bound it [^rpc-cache]:

| Flag | Default | Holds |
| --- | --- | --- |
| `--rpc-cache.max-blocks` | 5000 | Full blocks |
| `--rpc-cache.max-receipts` | 2000 | Receipts, per block |
| `--rpc-cache.max-headers` | 1000 | Headers |
| `--rpc-cache.max-concurrent-db-requests` | 512 | Concurrent database reads behind the cache |
| `--rpc-cache.max-cached-tx-hashes` | 30000 | Transaction hash lookups |

The block, receipt and header limits count entries, not bytes.
Each block comes from one batch [^block-per-batch], and a batch can be up to 1 MB, so 5000 cached blocks can hold up to 5 GB of transaction data before decoding overhead.
At the benchmark's average batch of about 64 KB, the same 5000 blocks hold about 0.3 GB.
If resident memory grows with RPC load, lower `--rpc-cache.max-blocks` first.
Disable swap on observers too.

### Storage

An observer stores the same execution database and consensus packs as a validator, so use the same capacity, IOPS and endurance figures.
In the benchmark the observer wrote 64 MB/s (e2) and 174 MB/s (c3) at p95, at 4,770 and 1,790 write IOPS [^b-obs-disk].
It does not write batches into the batch cache from gossip, and its cache file reached only 64 to 192 MiB against 1 GiB on validators.
The per-epoch ceiling still limits the chain it follows.

### Networking

A follower downloads consensus output and epoch packs and forwards transactions it receives.
The steady-state bandwidth is λ · S_tx plus consensus headers.
In the benchmark the follower received 2.7 Mbps (e2) and 4.5 Mbps (c3) at p95 and sent under 1 Mbps [^b-obs-net].
A public RPC observer adds its RPC traffic, which depends on its users.
Observers need outbound access to each validator's advertised JSON-RPC endpoint to forward transactions.

### Operating system

Linux as for validators.
macOS Sequoia 15+ also runs an observer, for development.
The pressure stall checks in [Capacity monitoring](validator-operations.md#capacity-monitoring) are Linux-only.

## Storage growth planning

The table applies G_day = 86400 · λ · 345 B, the benchmark mix's measured β_reth + β_pack, and scales it by workload.
Transfer-heavy loads use 0.6 times that, because a native transfer is about 110 bytes in a batch against the mix's 190.
Contract-heavy loads use 1.3 times, which is about 250 bytes in a batch.
Both multipliers are modelled.
G_idle adds about 0.07 TB a year at any load and is left out.
Every disk figure is a lower bound, because β_reth is.

| Load | Workload | Batch bytes per transaction | Disk per day | Disk after 1 year | Disk after 3 years | Under the 8 h batch-cache ceiling? |
| --- | --- | --- | --- | --- | --- | --- |
| 100 TPS | Transfer-heavy | About 110 B | 1.8 GB | 0.65 TB | 1.96 TB | Yes |
| 100 TPS | Benchmark mix | About 190 B | 3.0 GB | 1.09 TB | 3.26 TB | Yes |
| 100 TPS | Contract-heavy | About 250 B | 3.9 GB | 1.41 TB | 4.24 TB | Yes |
| 194 to 198 TPS | Benchmark mix | About 190 B | 5.9 GB | 2.13 TB | 6.40 TB | At the ceiling |
| 500 TPS | Transfer-heavy | About 110 B | 8.9 GB | 3.26 TB | 9.79 TB | No |
| 500 TPS | Benchmark mix | About 190 B | 14.9 GB | 5.44 TB | 16.3 TB | No |
| 500 TPS | Contract-heavy | About 250 B | 19.4 GB | 7.07 TB | 21.2 TB | No |
| 1000 TPS | Transfer-heavy | About 110 B | 17.9 GB | 6.53 TB | 19.6 TB | No |
| 1000 TPS | Benchmark mix | About 190 B | 29.8 GB | 10.9 TB | 32.6 TB | No |
| 1000 TPS | Contract-heavy | About 250 B | 38.8 GB | 14.1 TB | 42.4 TB | No |

The last column compares the batch bytes per transaction with the 8-hour ceiling of 37,283 batch bytes per second.
Rows marked "No" need a release with a larger batch cache or shorter epochs before the network can carry them.
They are listed so operators can plan hardware for that release.

Worked example for 100 TPS of the benchmark mix over one year, with idle growth:

```text
G_day      = 86400 · 100 · 345 B + 0.2 GB   = 3.18 GB
disk, 1 y  = 365 · G_day                    = 1.16 TB
```

## Storage: TLC over QLC

When setting up nodes for Telcoin Network, storage selection affects node performance and drive life.
We recommend TLC (Triple-Level Cell) NVMe drives over QLC (Quad-Level Cell).

### Performance

TLC drives sustain higher write speeds and better random I/O, and they hold that performance under continuous load.
QLC drives slow down sharply once their write cache fills, which leads to uneven execution times and late votes during busy periods.

### Durability and lifespan

TLC drives typically support 1,000 to 3,000 P/E (program/erase) cycles, 3 to 5 times the endurance of QLC.
QLC drives generally support 100 to 1,000 cycles, and continuous node writes wear them out early.

### Endurance from the measured write rate

A node writes continuously even when idle.
Convert the measured write rate to drive writes per day:

```text
W_day = write rate (bytes/s) · 86400
DWPD  = W_day / capacity
TBW   = W_day · 365 · years
```

Devnet validators write 8 to 9 MB/s at the host level under light load.
At 8.5 MB/s, W_day is 734 GB.
That is 0.37 DWPD on a 2 TB drive and 0.18 DWPD on a 4 TB drive, and 1,340 TB written over five years.
Compare TBW with the drive's rated endurance, and buy a drive rated for at least twice the computed DWPD.

The benchmark's write rate at load was 137 MB/s (e2) and 190 MB/s (c3) at p95 [^b-disk].
At 190 MB/s a drive takes 16.4 TB a day, 4.1 DWPD on a 4 TB drive, but the release cannot sustain that load with 8-hour epochs.
Per landed transaction, the node process wrote 23 to 46 KB on e2 validators and 61 to 71 KB on c3 validators, about 70 to 200 times what the data directory grew [^b-writes].
At the 8-hour ceiling of about 200 TPS that is 4.6 to 14 MB/s on top of the idle 4 to 9 MB/s, about 0.7 to 2.0 TB a day.
That is 0.2 to 0.5 DWPD on a 4 TB drive and 0.4 to 1.0 DWPD on a 2 TB drive (modelled).
Twice those figures gives the ratings in the summary: 1 DWPD for 4 TB and 2 DWPD for 2 TB.

### Blockchain-specific requirements

Node storage takes continuous writes from incoming batches, block execution, state updates and chain history.
QLC drives cost less up front, but their performance limits and shorter life make TLC the cheaper and more reliable choice over the life of a node.

## How these numbers were derived

### Model

The CPU, memory, storage and network sections each state the model used for that resource.
The inputs are code constants (listed under [Sources and code references](#sources-and-code-references)), the benchmark load (λ and the average transaction size), and a small number of measured coefficients (α, k_q, k_exec, β_reth, β_pack, G_idle, PC_hot, cache bytes per transaction).
The benchmark measured β_reth, β_pack, PC_hot and cache bytes per transaction, and the replay peak in place of k_q and k_exec.
α was not measured, and G_idle comes from adiri.

### Benchmark

| Item | Setting |
| --- | --- |
| Topology | 10 validators and 1 observer; 2 validators per zone, one zone in each of us-west2, australia-southeast1, northamerica-northeast1, europe-west4 and europe-west2; the observer in us-west2 |
| Load | 10 load generators, one next to each validator in its zone, 600 senders each (6,000 in total) |
| Transaction types | 13: native transfer, batched self-send, contract deploy, WTEL wrap and unwrap, stablecoin transfer and approve, faucet drip, Uniswap v2 swap, add liquidity and remove liquidity, Uniswap v3 swap, Uniswap v4 swap |
| Phases | 90 s preamble, 2 min warmup, 40 min mixed phase, 3 min cooldown |
| Restart tests | One validator killed with SIGKILL 15 minutes into the mixed phase, held down for 5 minutes, then restarted (replay); the observer restarted at 25 minutes |
| Epoch length | 20 minutes, so the mixed phase crosses about two epoch boundaries |
| Fleets | e2-custom-4-8192 (4 vCPU, 8 GB), run r20260923-0708; c3-highcpu-8 (8 vCPU, 16 GB), run r20260923-0515 |
| Disk | 100 GB pd-ssd boot disk per node |
| Node image | `us-docker.pkg.dev/telcoin-network/tn-public/adiri:v0.1.0-devnet` |
| Load generator | tn-transaction-generator, image `blast-v2-3644adc` |
| Report | [Benchmark report](https://claude.ai/artifact/6qpBKmd3eKNaRBuxUtPPCR); committed copy `bench/reports/2026-09-bench-10v.html` in tn-transaction-generator |

Results:

| Result | e2 | c3 |
| --- | --- | --- |
| Sustained mixed TPS (best 60 s) | 3,569 | 5,384 |
| Landed of accepted | 7.35M of 7.38M | 8.08M of 8.12M |
| Reverted | 0 | 0 |
| Latency, submit to block timestamp, p50 / p95 / p99 | 7.6 s / 42 s / 361 s | 7.7 s / 31 s / 507 s |

Over 99.9% of the transactions in the window were the benchmark's own.
The generators kept transaction pools full by design, so the latency figures measure queueing under saturation, not the protocol's inclusion time.
Blocks have a 30 million gas limit and pack by each transaction's gas limit.
The mix averaged about 77,000 gas limit against 49,000 gas used per transaction, so about a third of each block's capacity was reserved but unused.

Limits of the benchmark:

- pd-ssd IOPS and throughput scale with volume size, so a 100 GB volume is throttled well below local NVMe. The volume allows 9,000 IOPS each way and 288 MiB/s (6,000 IOPS plus 30 per GB; 240 MiB/s plus 0.48 per GB), and an e2 VM with 4 vCPUs is further capped at 240 MiB/s. GCP's disk throttling metric was not available in the project, so throttling was not recorded directly. Disk figures in the summary are measured demand plus margin, not the volume's capacity.
- 100 GB holds a 40-minute run but says nothing about capacity. Capacity comes from the growth formula.
- 20-minute epochs keep the run far from the 8-hour batch-cache ceiling (see above).
- Egress includes JSON-RPC traffic to the in-zone load generators (see [Networking](#networking)).
- β_reth misses growth inside the execution database file (see [Storage](#storage)).
- In the e2 run, bench-validator-03 in australia-southeast1 fell behind, and at 08:21Z it stopped producing batches after its worker's batch-report channel closed. It failed 1,457 seals but kept voting, so consensus stayed live with 10 voters and 9 batch producers for the rest of the run. The failure is tracked separately.
- In the c3 run, bench-validator-05, restarted after the 5-minute kill, had not resumed execution when the run ended 30 minutes later. Its static files did not grow and it reported no execution or batch metrics after the restart, so the c3 run had 9 executing validators from the kill at 06:20Z and gives no replay figure. This also needs its own investigation.
- An earlier e2 run, r20260923-0215, is superseded. A load-generator bug re-sent pending transactions, so about a third of its batch content was duplicates (1.33 executed transactions per landed transaction). No figure on this page comes from it.

### Networks running today

Both networks carry little load, so these figures are a floor:

| Network | Machine | CPU | RAM used | Disk writes |
| --- | --- | --- | --- | --- |
| Devnet validators, 24 h | e2-custom-4-8192 (4 vCPU, 8 GB) | 8 to 12% | 2.2 to 3.6 GB, single-sample spikes to 5.5 to 6.4 GB; node resident memory about 1.6 GB | 8 to 9 MB/s |
| Adiri validators, idle, 6 h epochs | c3-highcpu-8 (8 vCPU, 16 GB) | 1.3% | 6.4 GB flat, one node at 9.5 GB | 3.7 MB/s |

The node's idle footprint fits in 8 GB, and it writes several MB/s to disk even when idle, which is why endurance matters.

### Confidence

| Figure | Source | Confidence |
| --- | --- | --- |
| Code constants (1 GiB batch cache, 64 + 8 output queue, 10 batches per header, 1 MB batch, 8 h default epoch) | Code | Exact for this release |
| Batch-cache TPS ceiling | Constants and the measured 190 B per transaction | Modelled from a measured input; MDBX page overhead not included |
| CPU, RAM, disk and network use at benchmark load | Benchmark, both fleets | Measured |
| Minimum and recommended CPU and RAM | Benchmark and the tier rules | Measured |
| PassMark floor | Judgement; CPU scores were not recorded | Modelled |
| Disk and network figures in every tier | Measured demand plus the tier margins | Modelled from measured demand |
| Replay memory peak | Benchmark, one e2 validator, one restart | Measured, single sample |
| β_reth, β_pack | Benchmark mixed phase | Measured lower bounds for the benchmark mix |
| PC_hot, M_base | On-node sampler | Measured proxies |
| α, k_q, k_exec | None | Not measured |
| G_idle | Adiri at idle | Modelled |
| Storage growth table and workload multipliers | Formula with measured β | Modelled |
| Endurance at the ceiling | Measured writes per transaction, scaled to the ceiling | Modelled |
| Headroom tier | Model at the batch-cache ceiling load | Modelled |
| Follower network figure | Measured demand; 50 Mbps kept from the provisional table | Measured demand, provisional figure |
| Public RPC observer CPU, memory and network | No RPC load in the benchmark | Modelled or provisional |
| Round-trip time between regions | Not collected | Not measured |
| Provisional estimates | Model and running networks | Superseded by the summary |

## Sources and code references

Notes whose names start with `b-` cite the benchmark: the run, and the field in that run's `sizing.json` unless another file is named.
The rest cite code.

[^b-val-cpu-min]: Measured, r20260923-0708 (e2) and r20260923-0515 (c3). `gcp_by_role.validator.cpu_utilization`: e2 validators ran 44% mean (`mean_avg_nodes`), 55% p95 (`p95_max_node`) and 66% max of 4 vCPU; c3 validators ran 13%, 25% and 36% of 8 vCPU. `prometheus.tn_engine_queued_outputs`: every e2 validator reached 7 or 8 queued outputs, against a limit of 8, while no c3 validator passed 3. `prometheus.tn_batch_builder_pending_pool_transactions`: seven e2 pools peaked at 9,480 to 9,850 transactions, near the default limit of 10,000. The 2-core e2 hosts were execution-bound, so they fail the minimum rule; the 4-core c3 hosts pass it on CPU.
[^b-val-ram-min]: Measured, r20260923-0708 and r20260923-0515. e2 (8 GB): `gcp_by_role.validator.memory_percent_used.p95_max_node` 81%, `telcoin_rss_bytes.p95_max_node` 7.15 GB, `sampler_by_node.*.psi_mem_some_avg10.run_max` 0.9 to 4%, replay peak 7.0 GB. c3 (16 GB): 52%, 10.8 GB, PSI memory 0. 8 GB left no room for the replay peak, so the minimum is 16 GB.
[^b-val-disk-min]: Modelled from measured demand. The minimum takes twice the e2 p95 write IOPS (5,120, `gcp_by_role.validator.disk_write_ops_per_sec.p95_max_node`) and sets throughput above the c3 plateau of 190 MB/s (`disk_write_bytes_per_sec.p95_max_node`), because both fleets' 100 GB volumes showed IO stalls. 2 TB holds about 11 months of growth at the 8-hour ceiling (about 6 GB a day with idle growth). 2 DWPD is twice the modelled 0.4 to 1.0 DWPD at the ceiling (see [Endurance](#endurance-from-the-measured-write-rate)).
[^b-net]: Measured demand, r20260923-0708 and r20260923-0515, `gcp_by_role.validator.network_sent_bytes_per_sec.p95_max_node` and `network_received_bytes_per_sec.p95_max_node`: out 56 Mbps (e2) and 86 Mbps (c3), in 24 and 21 Mbps. 200 Mbps symmetric clears the measured p95 with room for catch-up. 1 Gbps keeps p95 under a tenth of capacity and leaves room for catch-up and serving epoch packs, which the benchmark did not measure. Egress includes RPC traffic to the load generator.
[^b-val-cpu-rec]: Cores measured, r20260923-0515: 25% p95 and 36% max of 8 vCPU (`gcp_by_role.validator.cpu_utilization`) on 4 physical cores, under the half-capacity line, with the engine queue at 3 or below. The PassMark figure is modelled: the benchmark did not record CPU models or scores, and 3,500 is a floor chosen for single-thread speed, not a measurement.
[^b-val-ram-rec]: Measured, r20260923-0515: 16 GB ran at 52% p95 (`gcp_by_role.validator.memory_percent_used.p95_max_node`), just over half of capacity, so the recommended tier doubles it to 32 GB.
[^b-val-disk-rec]: Modelled from measured demand. 20,000 sustained IOPS is above twice the e2 p95 read plus write IOPS (2 × (2,990 + 5,120) = 16,220; `disk_read_ops_per_sec.p95_max_node`, `disk_write_ops_per_sec.p95_max_node`). 500 MB/s is above twice the c3 write plateau (380 MB/s). 4 TB covers one year at the 8-hour ceiling (2.2 TB) with room to spare. 1 DWPD is twice the modelled 0.2 to 0.5 DWPD at the ceiling.
[^b-head]: Modelled. The highest sustained load this release carries is the 8-hour batch-cache ceiling, about 200 TPS, under a seventeenth of the e2 fleet's 3,569 TPS. The recommended CPU, memory and network cover that load, and replay at it holds at most 1 GiB of batch data. Headroom therefore adds storage (and, for public RPC observers, memory), not throughput.
[^b-head-disk]: Modelled: G_day = 86400 · λ · 345 B + G_idle at the ceiling is about 6 GB a day, 6.6 TB over three years (a lower bound). 8 TB holds that with about 20% to spare.
[^b-obs-min]: Measured, r20260923-0708, `gcp_by_role.observer`: the e2 observer (2 physical cores, 8 GB) used 22% mean and 27% p95 CPU, 38% of memory at p95 (3.7 GB resident), and peaked at 1% memory pressure. It fell behind at times (engine queue at 8) but caught up after its restart.
[^b-obs-rec]: Measured, r20260923-0515, `gcp_by_role.observer`: the c3 observer (4 physical cores, 16 GB) used 13% CPU and 15% memory at p95 with its engine queue at 3 or below. The e2 shape already meets the half-capacity rule on CPU and memory, but its execution fell behind at times, so the recommended tier uses the c3 shape.
[^b-obs-disk]: Measured demand, `gcp_by_role.observer.disk_write_bytes_per_sec.p95_max_node` and `disk_write_ops_per_sec.p95_max_node`: 64 MB/s at 4,770 IOPS (e2) and 174 MB/s at 1,790 IOPS (c3), the same order as a validator. An observer keeps the same archive data, so it uses the validator disk figures.
[^b-obs-net]: Measured demand, `gcp_by_role.observer.network_received_bytes_per_sec.p95_max_node` and `network_sent_bytes_per_sec.p95_max_node`: 2.7 Mbps (e2) and 4.5 Mbps (c3) in, under 1 Mbps out. 50 Mbps is the provisional figure, kept because it is more than ten times the measured demand. Initial sync from genesis was not measured.
[^b-rpc]: Modelled; the benchmark sent no RPC load to the observer. Minimum: the follower's recommended CPU, and the follower's 8 GB plus up to 5 GB for the default RPC caches (5000 blocks of up to 1 MB), rounded to 16 GB. Recommended 32 GB and headroom 64 GB leave room for concurrent RPC work and for raising the cache limits. The 8-core CPU figure is the provisional estimate, kept until RPC load is measured. Network is the follower's 50 Mbps plus RPC traffic.
[^b-mbase]: Measured, r20260923-0708 and r20260923-0515, sampler CSVs (`sampler/*.csv`, `rss_anon_kb`) in the idle window after funding and before load: 0.4 to 0.5 GB per validator, 0.3 GB before funding, 0.2 GB on the observer. The chain was new, so long-running nodes sit higher.
[^b-pchot]: Measured proxy, `sampler_by_node.*.rss_file_kb.run_max` against `gcp_by_node.*.disk_read_ops_per_sec` (p95): c3 validators held 2.1 to 3.1 GB of file-backed pages and read at most 5 IOPS; e2 validators held 1.7 to 1.8 GB and five of ten read 1,800 to 3,000 IOPS.
[^b-replay]: Measured, r20260923-0708, bench-validator-05 killed at 08:30:12Z and restarted at 08:35:11Z: `prometheus.reth_process_resident_memory_bytes` peaked at 6.9 GB and the sampler's `rss_total_kb` at 7.0 GB, with `tn_engine_queued_outputs` at 8. The validators that never stopped peaked at 7.2 GB (`telcoin_rss_bytes.max_max_node`).
[^b-beta-reth]: Measured, `storage_growth.beta_reth_bytes_per_tx.median`: 251 (r20260923-0708) and 257 (r20260923-0515) bytes per landed transaction over the mixed window. `db/mdbx.dat` stayed at 4,295,995,405 bytes on every node in every run (sampler `db_bytes`), so this counts `static_files` growth only.
[^b-beta-pack]: Measured, `storage_growth.beta_pack_bytes_per_tx.median`: 92 (r20260923-0708) and 94 (r20260923-0515). Packs for the open epoch are folded in at epoch close, so the figure is a lower bound when the window ends mid-epoch.
[^b-txbytes]: Measured from Prometheus over each fleet's mixed window: batch bytes divided by batch transactions (`tn_worker_batch_size_bytes`, `tn_worker_batch_transactions`) is 192 B on e2 and 188 B on c3, about 340 transactions and 63 to 65 KiB per batch.
[^b-disk]: Measured, `gcp_by_role.validator.disk_*_per_sec.p95_max_node`: e2 2,990 read and 5,120 write IOPS (5,570 max) at 137 MB/s (145 max); c3 5 read and 2,440 write IOPS at 190 MB/s. IO pressure from `sampler_by_node.*.psi_io_some_avg10.run_max`. Volume limits from each run's `limits` section and the report.
[^b-writes]: Measured, `sampler_by_node.*.growth_mixed.io_write_bytes` over the mixed window, divided by `storage_growth.chain_tx_in_window` and by the data directory's growth, excluding the restarted validator. The process write rate is `storage_growth.telcoin_process_write_bytes_per_sec`: 56 MB/s median on e2 and 163 MB/s on c3. The scaling to 200 TPS is modelled.
[^engine-single]: `crates/engine/src/lib.rs:66-68` (one pending execution task), `crates/engine/src/lib.rs:159` (execution on a blocking thread).
[^ecrecover]: `crates/tn-reth/src/env/execution.rs:167-181`.
[^rayon]: `crates/telcoin-network-cli/src/node.rs:218-226` (global pool size is available cores minus 2, at least 1).
[^batch-validator]: `crates/batch-validator/src/validator.rs:170`.
[^vote-wait]: `crates/consensus/primary/src/network/handler.rs:787`.
[^commit-wait]: `crates/consensus/primary/src/consensus/state.rs:501-502`.
[^quorum]: `crates/types/src/committee.rs:1090-1091`.
[^replay]: `crates/node/src/manager/node/start_epoch.rs:88-104` (replay loop; lines 89-97 refuse to cross an epoch boundary).
[^gc]: `crates/types/src/primary/mod.rs:49` (`MAX_GC_DEPTH = 50`), `crates/config/src/node.rs:345-346` (default gc depth), `crates/storage/src/consensus_pack.rs:1587-1589` (batches per output bound).
[^txpool]: `crates/tn-reth/src/cli.rs:88`; default from reth v1.11.3 `crates/transaction-pool/src/config.rs:18`.
[^canon]: reth v1.11.3 `crates/chain-state/src/in_memory.rs:28`, used through `crates/tn-reth/src/env/helpers.rs:91-92`.
[^rpc-cache]: `crates/tn-reth/src/rpc_server_args.rs:234-236`; defaults from reth v1.11.3 `crates/rpc/rpc-server-types/src/constants.rs:115-127`.
[^leader]: `crates/consensus/primary/src/consensus/bullshark.rs:137-138` (even rounds), `crates/consensus/primary/src/consensus/bullshark.rs:201-212` (f+1 support from round r+1).
[^max-batches]: `crates/types/src/primary/mod.rs:71` (`MAX_HEADER_NUM_OF_BATCHES = 10`).
[^threshold]: `crates/config/src/node.rs:328-330` (proposal threshold 5).
[^batch-limits]: `crates/types/src/worker/sealed_batch.rs:197-198` (30,000,000 gas), `crates/types/src/worker/sealed_batch.rs:208-209` (1,000,000 bytes), `crates/config/src/node.rs:357-358` (batch sealed after 1 s).
[^to-engine]: `crates/node/src/manager/node.rs:81` (`TO_ENGINE_CAPACITY = 64`), `crates/node/src/manager/node.rs:838`.
[^engine-queue]: `crates/engine/src/lib.rs:52` (`MAX_QUEUED_OUTPUTS = 8`), `crates/engine/src/lib.rs:271`.
[^archive]: `crates/tn-reth/src/cli.rs:517` (`ensure_archive_mode`).
[^datadir]: `crates/config/src/traits.rs:148` (`consensus-db/epochs`), `crates/config/src/traits.rs:181` (`consensus-db`), `crates/config/src/traits.rs:185` (`db`).
[^cache-max]: `crates/storage/src/lib.rs:161` (`EPOCH_MAX = 512 MB`), `crates/storage/src/lib.rs:163` (`CACHE_MAX = 1024 MB`), `crates/storage/src/lib.rs:170-175`, `crates/storage/src/mdbx/database.rs:165-171` (fixed maximum size; the file never shrinks).
[^mdbx-durable]: `crates/storage/src/mdbx/database.rs:197-198`.
[^cache-tables]: `crates/storage/src/lib.rs:109-114` (`TableHint::Cache` tables); writers at `crates/consensus/worker/src/network/handler.rs:265`, `crates/consensus/worker/src/network/handler.rs:297`, `crates/consensus/worker/src/batch_fetcher.rs:253`, `crates/consensus/worker/src/worker.rs:388`.
[^cache-clear]: `crates/node/src/manager/node/close_epoch.rs:774`; see also `crates/state-sync/src/lib.rs:118-119`.
[^epoch-default]: `crates/telcoin-network-cli/src/genesis/mod.rs:93`.
[^seal-fatal]: `crates/consensus/worker/src/worker.rs:322-326`, `crates/consensus/worker/src/worker.rs:388-391`.
[^bb-critical]: `crates/batch-builder/src/lib.rs:276-278`, `crates/node/src/engine/inner.rs:127`, `crates/types/src/task_manager.rs:94-96`.
[^qw-fanout]: `crates/consensus/worker/src/quorum_waiter.rs:136-137`.
[^header-delay]: `crates/config/src/node.rs:336-342` (2.5 s maximum, 1 s minimum).
[^drift]: `crates/consensus/primary/src/network/handler.rs:889-915`, `crates/config/src/network.rs:360` (1 s tolerance).
[^observer-role]: `crates/node/src/manager/node.rs:1047-1064`, `crates/telcoin-network-cli/src/node.rs:157-160`.
[^forward]: `crates/consensus/worker/src/worker.rs:310-312`, `crates/consensus/worker/src/worker.rs:254-296`.
[^observer-gossip]: `crates/consensus/worker/src/network/handler.rs:138-144`.
[^block-per-batch]: `crates/engine/src/payload_builder.rs:193-195`.

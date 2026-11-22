# Validator production operations

This page defines the production controls that sit around the node software.
The node does not configure the host firewall, update itself, or provide an HSM or remote BLS signer.

## Network topology

Use separate network roles so a public service cannot directly expose a consensus validator:

| Role | Exposure | Purpose |
| --- | --- | --- |
| Core validator | Private network | Consensus primary and worker traffic |
| Sentry or gateway | Public P2P, selected RPC | Absorb public connections and provide stable entry points |
| Observer | Public P2P and optional RPC | Read-only access without a consensus vote |
| Management | Private operator network | Metrics, logs, SSH, deployment, and backup access |

Core validators should accept primary and worker traffic only from the approved validator and sentry address set. Do not expose validator RPC, metrics, health, or management ports to the public internet.

Permissionless observers need public entry points. Provide those through sentries, bootstrap nodes, or RPC gateways. Avoid opening every core validator to anonymous peers simply to support discovery.

## Firewall configuration

The node has no default consensus ports: it takes them from the primary and worker addresses recorded in `node-info.yaml` at key generation. By convention, validators use UDP 49590 for the primary network and UDP 49594 for the worker network. RPC and metrics ports are TCP and should remain private unless a dedicated gateway protects them.

Apply these controls outside the node process:

- allow core validator P2P ingress only from the current validator and sentry address set;
- allow management and metrics ingress only from operator networks;
- give public observers and gateways their own security group or firewall policy;
- restrict core validator egress to approved peers, DNS, time sources, telemetry, and release infrastructure where the platform supports it;
- log rejected traffic and alert on sustained scans, unexpected destinations, and connection exhaustion.

Treat DHT records, peer exchange messages, and advertised RPC endpoints as untrusted network data. Never use them to add firewall rules or cloud security group entries.

Distribute validator and sentry addresses through an authenticated operator channel. A production address manifest should include the network, epoch or activation time, peer identity, IP addresses, ports, expiry, and signer set. Stage additions before removals, verify connectivity from every validator, and retain the previous manifest for rollback.

## Advertising an RPC endpoint

Validators should advertise a JSON-RPC endpoint. Observers do not gossip the transactions they accept. They forward each one over JSON-RPC to the endpoint advertised by the committee validator whose slot owns the sender's account, and fall back to another validator's endpoint when that one has none or cannot be reached (see [Observer](../architecture/network.md#observer)). A validator without an endpoint receives no forwarded transactions, and if no committee validator advertises one, observers cannot forward at all.

Serve the endpoint from a gateway, as [Network topology](#network-topology) recommends: bind the node's RPC server to a private interface that only the gateway can reach, terminate TLS on the gateway, and keep the validator host off the public internet. A TLS reverse proxy on the validator host itself, in front of an RPC server on loopback (`--http.addr 127.0.0.1`, the default), also works, but it exposes the validator host to the internet. Either way, advertise the gateway's or proxy's `https://` URL, never the node's own RPC ports:

```bash
telcoin-network keytool set-rpc \
    --datadir /var/lib/telcoin \
    --http https://rpc.validator.example.com/
```

`keytool generate validator --rpc-http <URL>` records the same field at key generation. The node reads the endpoint at startup, so restart it after a change.

Choose the URL with these rules:

- use `https://`: the node also accepts `http://`, but observers then send signed transactions in cleartext;
- use a public hostname or address: observers refuse loopback, private-use (RFC 1918), link-local, unique-local, shared-address-space and unspecified addresses, and the `localhost` and `.local` names, so an endpoint on one of them receives no forwarded transactions;
- make sure the hostname resolves to a public address: the check reads the host as written and does not resolve DNS, so a hostname that resolves to a private address passes it, and observers then dial that address, fail to reach it, and forward nothing to this validator;
- rate-limit the gateway or proxy, because the endpoint is published in the validator's node record and anyone can find it.

Networks where one operator runs every validator, such as a single-host test network that advertises `127.0.0.1`, can let observers dial private addresses with [`allow_private_forward_targets`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/telcoin-network-cli/README.md#allow_private_forward_targets). Production networks leave it off.

## BLS key custody

The current CLI stores the BLS12-381 key in `node-keys/bls.kw` when a passphrase is used. The file uses AES-256-GCM-SIV with PBKDF2-HMAC-SHA256. The node decrypts the key into process memory to sign consensus messages.

For production today:

- use `credential` (systemd `LoadCredentialEncrypted=`), `env`, `stdin`, or `ask` passphrase mode and never use `no-passphrase`;
- keep the data directory on an encrypted volume with owner-only access;
- isolate the validator process and host from build jobs, developer tools, and public RPC services;
- retrieve the passphrase from an operator secret system at startup and avoid shell history, command arguments, and long-lived environment files;
- keep encrypted offline backups and test recovery on an isolated replacement host;
- monitor changes to `node-info.yaml`, `node-keys/`, startup arguments, and process ownership.

Consumer hardware wallets, phones, and Chromebooks do not currently implement the node's BLS signing interface. Do not assume they can custody or use the validator key without a dedicated integration.

For stronger isolation, add a reviewed remote signer or HSM interface before mainnet. The interface should expose only the required domain-separated BLS operations, authenticate the validator client, rate limit requests, produce audit logs, and keep private key bytes outside the node host. This requires protocol integration and failure-mode testing. It is not available in the current binary.

BLS identity rotation affects committee registration and peer identity. Treat it as a coordinated protocol and governance operation, not a local file replacement.

## Release and network update process

Releases are published as signed git tags.
Each release has a Linux x86_64 tarball on its GitHub release page and a `linux/amd64` image in its network's package: `ghcr.io/telcoin-association/tn-mainnet` for mainnet releases, `ghcr.io/telcoin-association/tn-testnet` for Adiri releases.
There is no automatic update: operators decide when to install a release.
[Installing a release](installing-a-release.md) shows how to download and verify one, and [Release notes](release-notes.md) lists what changed in each.

A release provides:

- a git tag signed with a maintainer key from the allowlist in [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), at the commit `main` records for the `release` submodule;
- a `SHA256SUMS` file covering the tarball and the `IMAGE_DIGEST` file, with detached maintainer signatures in `SHA256SUMS.asc`;
- the image digest in `IMAGE_DIGEST`, so the image can be pulled by digest instead of by tag;
- the changelog section for that tag, also shown on the release notes page.

A release does not yet provide an SBOM, third-party build provenance, reproducible builds, or machine-readable activation data.
Activation conditions and the final safe rollback point arrive through the authenticated operator channel and the release notes.

Verify the signature and the digests on the target host before installation.
Record the installed tarball checksum or image digest and the `--version` output in operator inventory.

Use this rollout order:

1. Test upgrade and rollback on an isolated network using production configuration shapes.
2. Upgrade an observer or sentry canary and check sync, RPC, metrics, and peer behavior.
3. Upgrade one validator and observe at least one normal consensus cycle.
4. Continue in stake-weighted batches without taking enough stake offline to break quorum.
5. Compare every node's running version, configuration digest, peer set, and chain progress with the release's signed checksums and release notes.
6. Stop before the rollback cutoff if acceptance checks fail, restore the prior artifact and configuration, and verify recovery.

The consensus database moved from MDBX to tndb in `v0.17.0-adiri`, one-way.
On its first start a node copies its consensus database from MDBX into `<datadir>/consensus-db/tndb`, makes the copy durable, and then deletes the MDBX files.

This release moves the execution (reth) database from MDBX to tn-lmdb, also one-way.
On its first start the node copies `db/mdbx.dat` into `db/tn-lmdb.dat`, verifies the copy against it, makes it durable, and then deletes the MDBX files.
It needs free disk for the database's live pages plus a quarter plus 1 GiB (or the `--db.growth-step`, if larger) while it copies, and refuses to start, with nothing changed, when there is less.
The migrated datadir keeps reth's storage layout v1; only a new datadir gets layout v2.
A node still on `v0.16.0-adiri` makes both moves at its first start of this release.

This release also raises the validator disk minimum from 10,000 to 20,000 sustained write IOPS and from 300 MB/s to 800 MiB/s, and turns gap fill on by default on Linux (`--db.gap-fill auto`), which needs a drive that writes each logical block atomically when the power fails.
Check each validator's volume against [Execution database: disk and memory](execution-database-hardware.md) before the upgrade; on any other drive, run with `--db.gap-fill off`.

Once a node has started this release, rolling it back to an earlier release is not supported.
The last rollback point for each node is before it first starts this release.
Fix any problem found after that forward, with a later release.
An earlier release refuses the migrated execution database before it changes anything, at the version file `db/database.version`; do not edit or remove that file to get past the refusal.
Restoring a backup taken before the upgrade is no way back either: the earlier release computes the state root of some blocks differently (next paragraph), and a restored `consensus-db/` has no record of the votes and proposals the validator made since, so it could sign conflicting messages.
Whenever a validator's `consensus-db/` is restored from a backup or moved aside, keep the validator stopped until the network has closed the epoch it was last in.

Upgrade the validators from `v0.17.0-adiri` all at once, not in the rollout order above: stop every validator, then start every validator on this release.
This release fixes the state root of a rare trie shape (TN-0140 in `vendor/reth/CHANGES.md`).
On a block where the shape occurs, the two releases compute different block hashes, and a committee that mixes them splits on that block: with five validators and a quorum of four, a split of two and three halts consensus.
Adiri's history up to block 529,583 (the close of epoch 751) holds no such block: an observer on this release synced adiri from genesis and reproduced every certified epoch record's closing block.
A block the old release builds later, during a rolling upgrade included, can still be one.
Observers can upgrade first: one on this release that meets such a block stops rather than follow it.
No automated test upgrades a whole committee at once; rehearse it, every node stopped and then started on this release, on a test network first.
Once every node runs this release, an existing datadir moves to layout v2 only by rebuilding its execution database from genesis.
With the node stopped, `telcoin-network db reset-execution --datadir <datadir>` shows what it would move, and with `--force` it moves `db/`, `static_files/` and any `restored-state-floor` into `<datadir>/execution-reset-<UTC time>/`; the next start re-executes the chain from genesis.
The rebuild replays epoch 0 from the node's own consensus packs, so the command refuses, moving nothing, a datadir bootstrapped with `db load-state`, which holds consensus only from the restored epoch on; such a node rebuilds as a new one, with `consensus-db/` moved aside as well.
A validator rebuilt that way stays stopped until the network has closed the last epoch it took part in: its votes and proposals for that epoch leave with `consensus-db/`.
It renames and never deletes: on the same release, moving those entries back (after moving the new ones out) restores the execution database from before the reset, while `consensus-db/` keeps what the node wrote meanwhile (see "Rollback" in the tn-reth README's "Upgrading a testnet node").
It is no way back to an earlier release, which is not supported once this release has started on the datadir.
An observer can rebuild in place.
On a five-validator committee with a quorum of four, one validator out for a whole rebuild leaves no margin, so a validator syncs an observer on this release in a fresh datadir instead, stops it cleanly at the tip, and copies its `db/` and `static_files/` into the validator's datadir after `db reset-execution --force --for-swap` (never its `consensus-db/` or keys); the validator is down only for the swap.
`--for-swap` lets a validator bootstrapped with `db load-state` through the epoch-0 refusal, because the copied database replaces the rebuild; the validator keeps its `consensus-db/`, so it needs no wait.

For an emergency patch, name an incident owner, a release owner, and an independent verifier.
Freeze unrelated rollout changes, state the affected versions and activation deadline, use the same signature and digest checks, and record each validator's completion.
A protocol fork needs a separately approved activation plan and explicit readiness evidence from enough validator stake.

## Starting before genesis

A network can fix its genesis time in advance: the genesis ceremony's `--genesis-timestamp` flag writes it into `genesis.yaml`, and a mainnet ceremony requires the flag (see [`chain-configs/README.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/chain-configs/README.md#genesis-timestamp)).
Validators can then start their nodes hours or days before that time.
An active validator on this release holds its first header until that time, so the first block follows the genesis time however early the nodes started.
[Epoch 0 and the genesis time](../epoch-boundaries.md#epoch-0-and-the-genesis-time) describes the hold.

Every validator must run this release at genesis.
Nothing refuses a header from a build without the hold.
Fewer than a quorum of validators on such a build cannot commit a block before genesis: a round advances only on a quorum of certified headers, and validators with the hold propose none before genesis.

### Before the genesis time

1. Install and verify the release as [Installing a release](installing-a-release.md) describes.
2. Start the node with the network's genesis and the validator's usual configuration, at any time before the genesis time.
3. Leave it running, and check that it waits as [Watching the wait](#watching-the-wait) describes.

A node that has only run builds with the hold waits again when it is restarted before genesis: the hold depends only on the clock, and the node stores no epoch-0 header before the genesis time.
Keep the host clock synchronized: each proposer goes by its own clock.

### What the node does while it waits

The node enters epoch 0 at once, as at any start.
It starts its networks, connects to its peers, and serves RPC, health and metrics; the genesis time holds back only the creation of headers.
An active validator holds its first header until its clock reaches the genesis time.
It reads the clock again at least once a second, so a clock correction during a wait of days takes effect within a second.
Until the genesis time it logs its `no consensus commit happened` line, under the `primary::gc` target, at `debug` instead of `warn`.
No block is built before genesis, so the chain stays at block 0.
Observers and nodes outside the committee enter epoch 0 the same way, and have nothing to follow until genesis.

### Watching the wait

| Signal | While the node waits |
| --- | --- |
| Log, target `epoch-manager`, at `info` | `waiting for genesis: epoch 0 opens in <h>h <mm>m <ss>s`, with `genesis_timestamp` (unix seconds) and `remaining_secs`: once on entry, then at every whole 10 minutes before genesis, then every minute during the last 10 minutes. Every node that enters epoch 0 before genesis logs it, observers included |
| Log, target `primary::proposer`, at `info` | `waiting for genesis before proposing a header`, with `genesis_timestamp`, `remaining_secs` and `round`, once, when an active validator starts holding its header |
| `tn_epoch_start_timestamp_seconds` | The current epoch's start. In epoch 0 it is the genesis time, so it stays ahead of `time()` until genesis, and `tn_epoch_start_timestamp_seconds - time()` is the wait left. After epoch 0 it is the previous epoch's closing block. `tn_epoch_boundary_timestamp_seconds` beside it is the epoch's boundary: in epoch 0, the genesis time plus the epoch duration |
| `net_peerCount` | Above `0x0` once the worker network has peers (see [net\_peerCount](../rpc-methods/net/net_peercount.md)) |
| `/health/network` | `200` once every swarm has peers, as at any other time; it does not report consensus (see [Private observability listeners](observability-listeners.md)) |
| `tn_admissionSnapshot` | The committee keys each swarm has resolved and is connected to (`resolved_current`, `connected_current`), on the validator's loopback RPC (see [tn\_admissionSnapshot](../rpc-methods/tn/tn_admissionsnapshot.md)) |
| `tn_primary_quorum_observation_active` | `1` on an active validator, with `tn_primary_last_commit_timestamp_seconds` at `0` until the first commit after genesis. [Consensus liveness](consensus-liveness.md#dashboard-and-alert-examples) shows how to keep its startup alert quiet until then |

A wait line, here the one logged on entry, reads:

```text
waiting for genesis: epoch 0 opens in 0h 20m 30s genesis_timestamp=1796214660 remaining_secs=1230
```

After the entry line, the lines fall on whole intervals before genesis, so every node logs the same remaining times.

### At the genesis time

The node logs `genesis time reached: epoch 0 is open`, with `genesis_timestamp`, under the `epoch-manager` target, and its countdown ends.
An active validator's proposer logs `genesis time reached, proposing the held header`, with `genesis_timestamp` and `round`, and sends the header with a `created_at` no earlier than the genesis time.
A node that enters epoch 0 after the genesis time holds nothing and logs none of these lines.

Once validators that form a quorum have sent their headers, the headers are certified and rounds advance.
The first commit comes about a second after the genesis time, and from then on the consensus header number that [tn\_latestConsensusHeader](../rpc-methods/tn/tn_latestconsensusheader.md) reports advances with every commit.
A commit that carries no transaction builds no block unless it closes the epoch, so [eth\_blockNumber](../rpc-methods/eth/eth_blocknumber.md) stays `0` until a commit includes the first transaction or epoch 0 closes.
Transactions submitted before genesis wait in the transaction pool, and the first blocks after genesis include them, starting with block 1.
Every block's timestamp is at or after the genesis time.

### Starting late

Epoch 0's boundary is the genesis time plus the epoch duration, whenever the validators start.
A committee that forms after genesis has only the rest of epoch 0.
One that forms after the boundary has passed closes epoch 0 at its first commit, under the usual close rule (see [Epoch boundary detection](../epoch-boundaries.md#epoch-boundary-detection)); epoch 1 then starts at that commit's block and runs a full epoch duration.
An active validator that enters epoch 0 after the genesis time knowing of no commit, when its boundary has passed or is less than one maximum header delay away, logs one warning under the `epoch-manager` target:

```text
epoch 0 started after genesis, this node knows of no commit, and the epoch's boundary has passed or is less than one header delay away: if the network has not committed yet, the first commit at or past the boundary closes epoch 0 genesis_timestamp=1796214660 late_secs=30000 epoch_boundary=1796236260
```

`late_secs` is how long after the genesis time the node entered epoch 0.
A node knows of a commit once it has saved one or its peers have published one by gossip, whether or not that commit built a block.
A validator restarted inside a running epoch 0 holds the epoch's commits and does not log the warning, however close the boundary is, even when no block has been built yet.
A validator on a fresh data directory can enter epoch 0 before it hears of the network's commits, so on a running network it can log the warning too; that is why the warning only says what happens if the network has not committed.

## Capacity monitoring

Resource use rises with network load, and some limits are fixed in the release.
Watch these signals on every validator and observer.
[Hardware requirements](hardware-requirements.md) explains the model behind each one.
The alert levels are starting points; tune them after a week of baseline data.

| Signal | Where to read it | Starting alert | What a rising trend means | What to do |
| --- | --- | --- | --- | --- |
| CPU pressure | `/proc/pressure/cpu`, `some avg60` | Above 20 for 10 minutes | Runnable threads are waiting for a core | Check the engine backlog first. If execution lags, move to a CPU with faster single-thread performance. If RPC load causes it, serve public RPC from an observer instead (see [Public RPC nodes](public-rpc-nodes.md)). |
| Memory pressure | `/proc/pressure/memory`, `full avg60` | Above 1 | The kernel is reclaiming pages the node needs, so all its threads stall | Add RAM. On RPC nodes, lower `--rpc-cache.max-blocks`. Do not add swap. |
| IO pressure | `/proc/pressure/io`, `full avg60` | Above 10 | Execution is waiting on disk | Move to faster storage or raise the volume's provisioned IOPS, then check for throttling. |
| Disk throttling | The provider's volume metrics (GCP reports throttled read and write operations and bytes per disk); `iostat -x` queue size and await | Any sustained throttling | The volume has reached its provisioned IOPS or throughput | Raise the volume limits or move to local NVMe. |
| Free disk space | Free space on the filesystem that holds the datadir (`df`, or node_exporter's `node_filesystem_avail_bytes`) | Below 20% of the volume, or less than a week of growth at the current rate | Every node is an archive node, so disk use only grows. The batch cache also has no fixed maximum: batches consensus has not committed take disk until they are committed or the epoch closes | Grow the volume or move to a larger one before it fills: a full disk stops the node (see [Full disk](#full-disk)). If free space falls faster than the chain grows, check the batch cache size row. |
| Consensus database stopped | `tn_storage_tndb_failed`, one series per database (`env` is `epoch`, `kad` or `cache`); the node exiting with an error that names the database | 1 on any `env`, or the node exiting non-zero | A sync or write of that database failed (a full disk, an I/O error), or a read found damage on disk. The database refuses every write, and the node stops itself rather than run on storage it cannot write. Any other storage failure of the node's own (a consensus pack, the epoch records, the certificate archive) stops it the same way | Page. Read the exit error and the node log (target `tndb`), check free space (see [Full disk](#full-disk)) and the disk, fix the cause, and restart the node. If the error names damage, or the restart refuses to open the database, run `telcoin-network db tndb-stat` and tell the Telcoin Association network team before you delete anything. |
| Batch cache size | `tn_storage_tndb_log_bytes{env="cache"}`, the length of the cache's log under `<datadir>/consensus-db/tndb/cache`. It grows with each batch written and removed, and drops at each compaction (`tn_storage_tndb_compactions_total{env="cache",outcome="switched"}`) | Above 768 MiB for 10 minutes | Batches are not leaving the cache (consensus is not committing them), or its compactions are failing (see the next row) | Check the next row. If compactions succeed and the log still grows, tell the Telcoin Association network team. |
| Consensus database compactions failing | `tn_storage_tndb_compaction_abandons_total`, by `env` and `cause`, beside `tn_storage_tndb_compactions_total{outcome="switched"}` | 2 or more abandons in 3 hours for one `env`, with no switched compaction in those 3 hours. Retries slow to one an hour, so a plain count such as 3 in an hour goes quiet about 75 minutes into a failure that persists; this one keeps firing until a compaction succeeds | The log is not being compacted, so its disk use keeps growing. `cause` says why: `disk_full` (not enough free space for the copy: the database's current size plus 384 MiB to start one, 256 MiB while it runs), `writer_outran` (writes faster than a compaction can catch up, an extreme burst), `corruption`, `panic` or `error` | `disk_full`: free disk space. `corruption`: stop the node, run `telcoin-network db tndb-stat`, and tell the Telcoin Association network team. Otherwise tell the network team. |
| Consensus database syncs | p99 of `tn_storage_tndb_sync_seconds` per `env`, over 5 minutes | Above 100 ms | Votes and proposals wait for these syncs before they leave the node | Check IO pressure and disk throttling. |
| Engine backlog | `tn_engine_queued_outputs`, 0 to 8 | 4 or more for 5 minutes | Execution is falling behind consensus | At 8 the engine queue is full and outputs back up into the 64-slot channel in front of it. Check CPU and IO pressure and `tn_engine_execution_duration_seconds`. |
| Resident memory | `reth_process_resident_memory_bytes` | Above 70% of RAM | Queued outputs, RPC caches or the transaction pool are growing | Compare with the engine backlog and the RPC request rate. A climb right after a restart is replay and should fall once the node catches up. |

Pressure stall information needs a kernel built with PSI support, which mainline Linux added in 4.20.
If `/proc/pressure` is missing, the running kernel lacks it or has it disabled.

The consensus database is tndb: one log per database under `<datadir>/consensus-db/tndb/` (`epoch`, `kad` and `cache`), each with an index per table.
Removed and overwritten rows stay in a log until a background compaction rewrites its live rows into a new generation (`gen-N`) and deletes the old one.
The cache log compacts once it is at least 256 MiB and twice its size after the last compaction, so its size rises and falls instead of only growing; it can briefly hold two generations while a compaction runs.
The epoch log resets when each epoch closes.
Committed batches are evicted when their consensus output, already saved to the epoch pack, is forwarded to execution, and the rest are removed at each epoch close, so the cache's live data is the batches not yet committed (see [Per-epoch batch-cache ceiling](hardware-requirements.md#per-epoch-batch-cache-ceiling)).
With the node stopped, `telcoin-network db tndb-stat` reports each database's live and dead bytes, its generation and the state of its indexes.
Summed over all validators since the epoch started, `tn_worker_batch_size_bytes` gives a rough estimate of the batch data a node has written to its cache, and on a node that stayed in sync the estimate runs low.
The histogram records each batch once, when its worker reports it to the primary, but a node writes each of its own batches to the cache twice and also writes every peer batch it validates, including ones that failed quorum.
It is not the cache's live data, because committed batches are evicted.

Resident memory includes pages of the memory-mapped databases that the process has touched, so it rises slowly as the database working set grows.
Alert on how fast it climbs during load and after restarts, not only on the level.

## Execution alerts

The execution layer publishes these series with the node's other metrics (`--metrics`).
Each should stay at 0: alert on any increase, for example `increase(tn_reth_exec_cache_stale_hits_total[15m]) > 0`.
The release ships no alert rules or dashboard panels for them.

| Series | A nonzero value means | What the node does | What to do |
| --- | --- | --- | --- |
| `reth_db_latched` (a gauge, 1 or 0) | The execution database met damage on disk or an I/O error, and refuses every write | Stops at its next persist of executed blocks | See [Execution database damage](#execution-database-damage). |
| `tn_reth_exec_cache_stale_hits_total` | The execution cache served a value its sampled check (one hit of each kind in 256) found stale: a bug in the cache | Discards that block, drops the cache, builds the block again without it, and runs on. Earlier blocks are not checked again | Set `TN_EXEC_CACHE=0` on this node and restart: it then builds every block without the cache. That is safe on one node, since a node without the cache builds the blocks a correct cache must. Compare the node's recent block hashes with a peer's before you trust it: unchecked values it served before the check may have put it on another state, and those blocks are persisted. Such a block forks this node alone; the node goes inactive at its next leader check or stops at an epoch record other than its own, but nothing repairs the blocks short of a sync from scratch. Tell the Telcoin Association network team. A 0 on other nodes clears nothing, since each node checks its own hits. |
| `tn_reth_parallel_state_root_mismatches_total` | A sampled block's parallel state root (block numbers that are multiples of 256, with parallel work) differed from the serial one: a bug in the parallel code. Every node checks the same blocks. The other 255 blocks in 256 commit the parallel result unchecked, so blocks the bug hit before carry its wrong root: on every node for a bug every path shares, on some nodes for a bug in one path that scheduling picks | Uses the serial result and runs on | A network-wide response, which the Telcoin Association network team coordinates: every node sets `TN_PARALLEL_STATE_ROOT=0` and restarts. A node that switches alone parts from its peers on the next block the bug hits. Switching off repairs no trie node an unchecked block already persisted; the check cannot see such nodes. |
| `tn_reth_account_walk_fallbacks_total{reason="mismatch"}`, `tn_reth_storage_walk_fallbacks_total{reason="mismatch"}` | The mismatch above was on a block where the account walk's split (or the storage split) ran. It does not say that split was wrong: a block where both ran counts under both | As above | As above. Switching off one split only (`TN_PARALLEL_ACCOUNT_WALK=0` or `TN_PARALLEL_STORAGE_WALK=0`) is the network team's choice once it has found the fault. |
| `tn_reth_account_walk_fallbacks_total{reason="stored_root_node"}`, `tn_reth_storage_walk_fallbacks_total{reason="stored_root_node"}` | The database holds a trie node at the empty path, which reth never writes: the stored trie is not one reth wrote | Computes that root on one thread and runs on | Tell the Telcoin Association network team. |
| `tn_reth_parallel_state_root_task_failures_total{reason="tip_moved"}` | A parallel state-root task saw another finalized block than its walk: something wrote the state or trie tables while an output's blocks were built, and the node has no such writer | Leaves that part to the serial computation and runs on | Tell the Telcoin Association network team. The other reasons (`provider`, `database`, `panicked`) cost block time only; read this series first when one node parts from its peers. |
| `tn_reth_unrecoverable_txs_dropped_total` | A certified batch carried transaction bytes that do not decode: batch validation was bypassed somewhere | Drops the transaction and runs on | Tell the Telcoin Association network team. |
| `tn_reth_tx_body_inline_rows_total` | An execution database that stores transactions by their position in consensus batches (a new one does: tn-reth README, "Transactions stored by position") stored some whole instead: their block's batch did not hash to the digest the block names. Expected only while the node re-executes adiri's duplicate-batch epochs (up to epoch 160) | Stores those transactions whole, which reads correctly and costs the space the format saves; the first such block in a process logs a warning naming it and why. Runs on | Outside those epochs, tell the Telcoin Association network team, with the warning's block. Nothing needs repair. |
| `tn_reth_tx_body_resolve_failures_total` (by `reason`) | A read of transactions stored by position could not take them from the consensus packs: a pack missing or damaged (`pack_missing`, `batch_missing`, `read`), a pack left unsealed or mid-install (`handoff_timeout`), or a stored row or batch that does not match (`no_batch_digest`, `position_out_of_range`, `decode`, `batch_too_large`), or no packs attached to the process (`packs_not_attached`); or nothing wrong with the packs, only every pack read busy (`permit_timeout`: the node runs 4 pack reads at once and lets at most 124 more wait, each for up to a second) | Fails that read: the RPC call answers an error, never another transaction. Other reads answer, and the node runs on | Follow the tn-reth README ("Transactions stored by position", "Recovery"): with the node stopped, check the epoch's pack with `telcoin-network db validate --datadir <datadir> --epoch <N>`, run `telcoin-network db repair --datadir <datadir> --epoch <N>` to see what it would do, and then with `--force` to do it; otherwise put it back from a backup or another node's sealed copy with `telcoin-network db import-epoch` (see [Consensus database backups](#consensus-database-backups)). Rebuilding the execution database does not help: it re-executes from the same packs. Without a backup the node starts over as a new one (the execution database and `consensus-db/` moved aside), or a validator swaps in the execution database of an observer bootstrapped with `db load-state` from the pack's epoch or a later one. A `handoff_timeout` during an epoch change that does not repeat needs nothing. A `permit_timeout` needs no repair either: the RPC endpoint was sent more history reads (transactions, receipts, blocks or logs of blocks the node no longer holds in memory) than the node runs at once, and the same read answers when it is retried; if its rate stays above 0, limit the request rate in front of the RPC endpoint. Otherwise tell the Telcoin Association network team. A node whose consensus database lacks a pack its execution database points into does not start, and names the missing epochs and these ways to recover. |
| `tn_reth_canon_state_lagged_total` | The transaction pool's maintenance fell behind and lost canonical-state notifications | Reloads the pool's sender state and runs on | A rate that does not fall back to 0 means the worker cannot keep up: check CPU pressure and the engine backlog. |
| `tn_epoch_final_state_mismatches_total` | An epoch the node re-executed closed on another block than the epoch record it holds | Stops, and stops again at every restart | Keep the moved-aside execution database, follow the rebuild text of the tn-reth README ("Upgrading a testnet node": a record of the node's own from a diverged execution needs a fresh `consensus-db/`), and tell the Telcoin Association network team. |

### Execution database damage

The execution database (tn-lmdb, `<datadir>/db/tn-lmdb.dat`) stops taking writes at its first failure: damage on disk that any read finds, or an I/O error while it writes or syncs.
The read can be block execution, an RPC call, or the database gauges the metrics endpoint samples at each scrape.
Reads go on, but the node's next persist of executed blocks fails and the node stops.
The damage stays on disk, so after a restart the next read that reaches it stops the node again: a restart does not clear it.
The node log names the first failure (target `tn_lmdb`), and `reth_db_latched` reads 1 until the process exits.
Reads do not recompute most pages' checksums, so some damage is read as data until a write copies the page or `db check` runs.

What to do:

1. Stop the node.
2. After an I/O error, check the disk, then reboot the host: `db check` reads through the page cache, which can still hold pages the device never took.
3. Run `telcoin-network db check --datadir <datadir>`. It checks every page's checksum and the structure of `db/tn-lmdb.dat`, and names what failed. It checks nothing else: not the static files and not the consensus database (see [What each check covers](#what-each-check-covers)). If it passes, the execution database holds no damage, and the failure was something else: a full disk, say ([Full disk](#full-disk)), or a damaged static file, which the error names ([Static file damage](#static-file-damage)). Fix the cause and start the node.
4. A damaged database is not repaired in place. Move it aside with `telcoin-network db reset-execution --datadir <datadir> --force` and rebuild it: in place, or for a validator by the side-observer swap (the tn-reth README's "Upgrading a testnet node"). The rebuild re-executes every epoch from 0 from the node's own consensus packs, so `db reset-execution` refuses, moving nothing, a datadir that lacks one of them: one bootstrapped with `db load-state` (it holds packs only from the restored epoch on), or one that lost a pack. Such a node rebuilds by the swap, with an observer whose database fits its packs (the tn-reth README's "Transactions stored by position" says which), or starts over as a new one with `consensus-db/` moved aside as well (a validator then stays stopped until the network has closed the last epoch it took part in). One damaged page usually means more: check the disk and the memory before you trust the host again.

### Static file damage

The execution layer keeps part of the chain's history (headers and transactions among it) in static files under `<datadir>/static_files/`, beside `db/`.
In the storage layout new datadirs get (v2), every value in them carries a checksum that each read verifies, so damage shows as an error naming the file, such as `static file <path> is damaged: the value at row <r>, column <c> fails its checksum`.
A datadir in the older layout (v1) has no static-file checksums.
No command checks the static files at rest: `db check` does not read them.
Reads of the damaged rows fail (an RPC call answers an error) and other reads answer, but damage under the committee and epoch reads stops the node, and damage in a segment's latest file stops its next start.

What to do:

1. Stop the node.
2. The file is damaged on this node only (a disk or memory fault, or a bad copy), and nothing repairs it in place. Rebuild the execution database as in step 4 of [Execution database damage](#execution-database-damage): `db reset-execution` moves `static_files/` aside with `db/`.
3. Check the disk: one damaged file usually means more.

Copy or restore whole datadirs, never single static files: a file from another copy can read as this one's without an error.

## Storage tools and recovery

### Exit status

The node's process exits with one of these statuses:

| Status | Meaning |
| --- | --- |
| 0 | A requested stop (SIGINT or SIGTERM) |
| 1 | An error. The process prints it to stderr (`Error: ...`), and the node log names it (`Error running node: ...`) |
| 2 | A usage error: an argument or flag the node does not accept |
| 10 | This node has forked: an epoch record it holds differs from the one the network certified, found at the epoch's vote or by the epoch record sync |

A restart meets the same fork and stops again, so do not restart a node automatically on status 10: keep it stopped, keep its datadir as it is, and tell the Telcoin Association network team.
Under systemd, add `RestartPreventExitStatus=10` to the unit's `[Service]` section, next to `Restart=on-failure`.
A supervisor that cannot filter by exit status restarts a forked node into the same stop every time: cap its restarts (Docker's `restart: on-failure:<max-retries>`) and alert on status 10.
Other stops that repeat at every restart exit 1, such as a final-state mismatch at an epoch close (`tn_epoch_final_state_mismatches_total` under [Execution alerts](#execution-alerts)).

### Database commands

The `telcoin-network db` commands work on the datadir that `--datadir <datadir>` names, and on the OS data directory without it.
Each prints the datadir it acts on (`datadir: <path>`) to stderr before anything else; check it. `db validate` given a pack's path ignores `--datadir`, and prints that path (`pack: <path>`).
"Node stopped" means the command refuses to run while a node holds the datadir, and holds the datadir while it runs, so a node started meanwhile is refused.
A command exits with status 1 when it fails or finds damage that needs action. The dry runs of `db repair` and `db migrate` exit 0 when they find only what `--force` would fix, and `db validate` exits 0 for a torn tail of the newest epoch, which the node's next start truncates (with `--all`, 1 when any epoch fails).

| Command | What it does | Node stopped | Writes |
| --- | --- | --- | --- |
| `db stats` | Reports the execution database: its static files, its storage format, then each table's rows, pages and size | Yes | No |
| `db check` | Checks every page's checksum and the structure of the execution database, `db/tn-lmdb.dat` | Yes | No |
| `db tndb-stat` | Reports the consensus database (tndb): for each of `epoch`, `kad` and `cache`, its generation, live and dead bytes, and each table's rows and index state | Yes | No |
| `db validate --epoch <N>`, `db validate --all`, or `db validate <path>` | Checks one epoch's consensus pack (`<datadir>/consensus-db/epochs/epoch-<N>`; for an interrupted install, the `epoch-<N>.replaced` copy the next start puts back), every epoch's in turn from the lowest present to the newest (a missing one fails, except one after the epoch the node resumes from, which its next start fetches), or the pack at a path (an `epoch-<N>` directory or its `data` file, anywhere): a torn tail, or damage that loses data, with what to do. With `--epoch` or `--all`, a past epoch's pack is also checked against this node's epoch records, and fails when it does not end with its epoch's final output | Only for the newest epoch, or a pack not sealed yet; a sealed past epoch can be checked while the node runs | No |
| `db repair [--epoch <N>] [--force]` | Repairs consensus packs: truncates a torn tail, rebuilds indexes, puts back an install that stopped half-way, and heals the epoch records. Without `--epoch` it skips the newest epoch | Yes | Only with `--force`; without it, reports what it would do |
| `db migrate [--epoch <N>] [--force]` | Rewrites consensus packs from before v2 in the v2 format | Yes | Only with `--force`; without it, reports what it would do |
| `db migrate-execution` | Moves the execution database from MDBX to tn-lmdb now, instead of at the node's next start; pass the node's `--db.*` flags | Yes | Yes |
| `db import-epoch --epoch <N> --from <copy> [--force] [--db.growth-step <size>]` | Puts back one past epoch's consensus pack from a copy (this node's backup of `epoch-<N>/`, or another node's sealed one), verified against this node's certified epoch records before anything is replaced; replaces a missing or damaged pack, or one that lost its last outputs. Keep the copy outside `consensus-db/epochs/`. A legacy v0 copy (a backup from before the header-first pack format) is converted first with `db migrate --force` in a scratch datadir (see [Consensus database backups](#consensus-database-backups)). It needs about the pack's size free beyond the floor a peer's import keeps (Full disk, step 5); pass the node's `--db.growth-step` if it runs with one | Yes | Only with `--force`; without it, verifies the copy and changes nothing |
| `db load-state <bundle> [--verify-only]` | Restores a state-export bundle into a fresh datadir (the tn-reth README's "Snapshot export / restore"); `--verify-only` runs the checks a restore runs before it writes and those it makes as it writes (the state data read whole, its header window, the import of the bundle's consensus pack), and stops; it reads the whole bundle, so it takes time on a large one, and only the restore recomputes the state root. The import is staged under `$TMPDIR` (else the system temp directory; an empty `TMPDIR` or one inside the datadir is refused) and removed on every normal exit (its path goes to stderr; a killed run leaves `$TMPDIR/tn-load-state-verify-*` to remove by hand); it needs the pack's size plus the node's import floor (one default execution-database growth step plus a margin) free there and refuses to start without it, and on many Linux hosts `/tmp` is a RAM-backed tmpfs or a small partition, so set `TMPDIR` to a disk with room | Yes, except `--verify-only`, which warns when a node holds the datadir | Yes, except `--verify-only` |
| `db reset-execution [--force] [--for-swap]` | Moves `db/`, `static_files/` and `restored-state-floor` into `<datadir>/execution-reset-<UTC time>/`, so the next start rebuilds the execution database | Yes | Only with `--force` (renames, never deletes); without it, reports what it would move |

`telcoin-network db <command> --help` says what each refuses and why.

### What each check covers

| Data | Where | Checked at rest by |
| --- | --- | --- |
| Execution database | `db/tn-lmdb.dat` | `db check` |
| Static files | `static_files/` | Nothing: a v2 datadir's reads verify each value's checksum, and each start checks the end of every segment's latest file |
| Consensus packs | `consensus-db/epochs/epoch-<N>/` | `db validate` (one pack) and the dry run of `db repair` (every epoch but the newest, unless named) |
| Epoch records | `consensus-db/epochs/epochs.pack`, `epoch_certs.pack` | The dry run of `db repair` |
| Consensus database (tndb) | `consensus-db/tndb/` | `db tndb-stat`, which reads each log as the node's open does and reports one it cannot read |

### Full disk

A full disk stops the node: a write of its own storage failed, and it stops rather than run on storage it cannot write.
A full disk does not damage the node's files: once there is room, a start resumes.

1. Keep the node stopped (a service manager that restarts it at once only fails again).
2. Find what grew: `du -sh <datadir>/* <datadir>/consensus-db/*`, and `telcoin-network db tndb-stat --datadir <datadir>` for the consensus database's live and dead bytes. If the batch cache grew, see the batch cache size row under [Capacity monitoring](#capacity-monitoring).
3. Grow the volume, or move the datadir to a larger one. That is the fix; deleting files only buys time.
4. If you must free space on the volume first, these are safe to delete:
   - an `execution-reset-<UTC time>/` directory, once the node rebuilt (or swapped) after that reset runs well and you will not undo the reset; keep it while the Telcoin Association network team looks into a divergence. Never an `execution-reset-*.partial` directory: that reset has not finished;
   - `consensus-db.forked/`, left by the recovery from a forked epoch record, once its cause is known;
   - state-export bundles you no longer need under `consensus-db/state_exports/` (written with `--enable-state-export`, which `--state-export-keep <N>` bounds; the node never reads them back);
   - files outside the datadir that share its volume.

   Delete nothing else in the datadir. In particular never delete a `consensus-db/epochs/epoch-<N>/` or `epoch-<N>.replaced/` directory, the epoch records (`epochs.pack`, `epoch_certs.pack`), anything under `consensus-db/tndb/` (its `gen-<N>` and `idx-<N>` directories included), or the markers `load-state-in-progress`, `restored-state-floor`, `mdbx-migrated` and `db/database.version`. The node itself deletes tndb's old generations and unfinished compactions at its next start (`db tndb-stat` lists them).
5. Start the node with free space for its next writes:
   - the execution database grows its file by up to one growth step at a time, each step allocated when it is taken: `--db.growth-step`, 1 GiB by default;
   - each consensus file the node writes (the open epoch's pack, the epoch records, the tndb logs) allocates up to 128 MiB ahead of its data as it grows;
   - a consensus database compaction starts only with the size of the database it compacts plus 384 MiB free, and gives way below 256 MiB;
   - a node catching up whole epochs from peers stops a pack import when free space is under one growth step plus 512 MiB, plus 320 MiB for each import running (about 1.8 GiB for one with the default step);
   - a node still to move its execution database from MDBX to tn-lmdb refuses to start without the database's live pages plus a quarter plus 1 GiB (or the growth step, if larger) free.

   Free at least the growth step plus 2 GiB (3 GiB with the default step), and grow the volume as soon as you can.

### Consensus database backups

A datadir that stores transactions by their position in the consensus packs (the format new datadirs get: the tn-reth README's "Transactions stored by position") needs every pack its execution database points into, and nothing fetches a past epoch's pack again: a node that loses one refuses to start, naming the missing epochs.
The start log line `reth datadir: storage format` names a datadir's format (`tx_bodies=pointers` or `inline`), and `telcoin-network db stats` prints it.
Its recovery is a backup, or another node's sealed copy of the lost epoch (`db import-epoch` verifies one), or starting over as a new node (the tn-reth README's "Recovery").
`db validate` and `db repair` name the same recovery for a pack whose data is damaged.
Keep a backup.

To take one:

1. Stop the node. A copy of a running node is not a backup: it writes the open epoch's pack, the epoch records and the tndb logs as it runs.
2. Copy `<datadir>/consensus-db/` whole into a new directory on another disk, keeping it as it is: `cp -a <datadir>/consensus-db <backup>/consensus-db`, or `rsync -a` into an empty directory. Keep the previous backup until the copy has finished.
3. To keep a full restore point as well, copy `<datadir>/db/`, `<datadir>/static_files/` and, if there is one, `<datadir>/restored-state-floor` at the same stop.
4. Start the node.

A backup holds the packs of the epochs that had closed when it was taken, so take one regularly, at planned stops such as upgrades.
Back up the keys separately, as [BLS key custody](#bls-key-custody) says.

To restore, with the node stopped:

- **A whole restore point** (`consensus-db/`, `db/`, `static_files/` and `restored-state-floor` from one stop): move the datadir's entries of those names aside (never delete them), copy the backup's in, and start the node. It resumes from the backup and catches up from peers, as a node stopped since then does.
- **One lost or damaged past epoch**, as the reports of `db validate` and `db repair` name it: put back only its `epoch-<N>/`, from a backup taken after that epoch closed or from another node's sealed `consensus-db/epochs/epoch-<N>/` (a sealed epoch's pack is the same on every node). Keep the copy outside the datadir's `consensus-db/epochs/` (the import and the node remove directories there; the command refuses a copy inside it). Run `telcoin-network db import-epoch --datadir <datadir> --epoch <N> --from <copy>/epoch-<N>`: it verifies the copy against this node's certified record of epoch `<N>` and changes nothing. It reads a v1 or v2 pack as it is, and refuses a legacy v0 (batches-first) one, such as a backup from before the header-first format: convert that first in a scratch datadir (a new directory outside this one), by copying the copy's `data` file to `<scratch>/consensus-db/epochs/epoch-<N>/data` and running `telcoin-network db migrate --datadir <scratch> --epoch <N> --force`, and pass `--from <scratch>/consensus-db/epochs/epoch-<N>`. Then run it again with `--force`, which installs the verified copy in place of the missing or damaged pack (renamed into place as the node installs an import; a pack that still validates and ends with the epoch's certified final output is refused, since there is nothing to replace). It refuses the epoch the node resumes, whose pack the node writes; restore that one from a whole restore point. Epoch 0 is put back the same way, with `--epoch 0`: its copy is checked against this node's certified record of epoch 0 and the genesis committee in the datadir's `genesis/committee.yaml`, which the node opens epoch 0 with. The next start checks again that every pack the execution database points into is there, and names any still missing.
- **`consensus-db/` alone**, from a backup older than the execution database: the packs of the epochs since the backup are missing from it, so a datadir that stores transactions by position refuses to start, naming them. Restore the execution database from the same stop as well, or rebuild it as in step 4 of [Execution database damage](#execution-database-damage).

Whenever a validator's whole `consensus-db/` is restored from a backup, keep the validator stopped until the network has closed the epoch it was last in: the backup holds none of the votes and proposals the validator made since it was taken.

## Required evidence before mainnet

- firewall policy tested from allowed and denied source networks;
- signed address manifest distribution and rollback rehearsal;
- BLS backup and isolated recovery rehearsal;
- release signature and digest verification on every target host;
- canary, phased rollout, and rollback rehearsal;
- quorum dashboard based on validator stake and running version;
- incident contacts and an out-of-band coordination channel.

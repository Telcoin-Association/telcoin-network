# Validator production operations

This page defines the production controls that sit around the node software. The node does not configure the host firewall, distribute releases, or provide an HSM or remote BLS signer.

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

- use `env`, `stdin`, or `ask` passphrase mode and never use `no-passphrase`;
- keep the data directory on an encrypted volume with owner-only access;
- isolate the validator process and host from build jobs, developer tools, and public RPC services;
- retrieve the passphrase from an operator secret system at startup and avoid shell history, command arguments, and long-lived environment files;
- keep encrypted offline backups and test recovery on an isolated replacement host;
- monitor changes to `node-info.yaml`, `node-keys/`, startup arguments, and process ownership.

Consumer hardware wallets, phones, and Chromebooks do not currently implement the node's BLS signing interface. Do not assume they can custody or use the validator key without a dedicated integration.

For stronger isolation, add a reviewed remote signer or HSM interface before mainnet. The interface should expose only the required domain-separated BLS operations, authenticate the validator client, rate limit requests, produce audit logs, and keep private key bytes outside the node host. This requires protocol integration and failure-mode testing. It is not available in the current binary.

BLS identity rotation affects committee registration and peer identity. Treat it as a coordinated protocol and governance operation, not a local file replacement.

## Release and network update process

There is no automatic binary release or node update channel in this repository. Operators currently build from source. Before a mainnet rollout, define one signed release manifest containing:

- the release version, source commit, and every first-party submodule commit;
- a SHA-256 digest for each immutable artifact and container image;
- build provenance and an SBOM;
- supported network and configuration schema versions;
- any fork epoch or activation condition;
- rollback compatibility and the final safe rollback point;
- signatures from the required release owners.

Verify the manifest and artifact digest on the target host before installation. Record the installed digest and node version in operator inventory.

Use this rollout order:

1. Test upgrade and rollback on an isolated network using production configuration shapes.
2. Upgrade an observer or sentry canary and check sync, RPC, metrics, and peer behavior.
3. Upgrade one validator and observe at least one normal consensus cycle.
4. Continue in stake-weighted batches without taking enough stake offline to break quorum.
5. Compare every node's running version, configuration digest, peer set, and chain progress with the release manifest.
6. Stop before the rollback cutoff if acceptance checks fail, restore the prior artifact and configuration, and verify recovery.

For an emergency patch, name an incident owner, a release owner, and an independent verifier. Freeze unrelated rollout changes, state the affected versions and activation deadline, use the same signed manifest and digest checks, and record each validator's completion. A protocol fork needs a separately approved activation plan and explicit readiness evidence from enough validator stake.

## Capacity monitoring

Resource use rises with network load, and some limits are fixed in the release.
Watch these signals on every validator and observer.
[Hardware requirements](hardware-requirements.md) explains the model behind each one.
The alert levels are starting points; tune them after a week of baseline data.

| Signal | Where to read it | Starting alert | What a rising trend means | What to do |
| --- | --- | --- | --- | --- |
| CPU pressure | `/proc/pressure/cpu`, `some avg60` | Above 20 for 10 minutes | Runnable threads are waiting for a core | Check the engine backlog first. If execution lags, move to a CPU with faster single-thread performance. If RPC load causes it, serve public RPC from an observer instead. |
| Memory pressure | `/proc/pressure/memory`, `full avg60` | Above 1 | The kernel is reclaiming pages the node needs, so all its threads stall | Add RAM. On RPC nodes, lower `--rpc-cache.max-blocks`. Do not add swap. |
| IO pressure | `/proc/pressure/io`, `full avg60` | Above 10 | Execution is waiting on disk | Move to faster storage or raise the volume's provisioned IOPS, then check for throttling. |
| Disk throttling | The provider's volume metrics (GCP reports throttled read and write operations and bytes per disk); `iostat -x` queue size and await | Any sustained throttling | The volume has reached its provisioned IOPS or throughput | Raise the volume limits or move to local NVMe. |
| Consensus database stopped | `tn_storage_tndb_failed`, one series per database (`env` is `epoch`, `kad` or `cache`) | 1 on any `env` | A sync or write of that database failed (a full disk, an I/O error), and it refuses every write until the node restarts | Page. Check free space and the node log (target `tndb`), fix the cause, and restart the node. |
| Batch cache size | `tn_storage_tndb_log_bytes{env="cache"}`, the length of the cache's log under `<datadir>/consensus-db/tndb/cache`. It grows with each batch written and removed, and drops at each compaction (`tn_storage_tndb_compactions_total{env="cache",outcome="switched"}`) | Above 768 MiB for 10 minutes | Batches are not leaving the cache (consensus is not committing them), or its compactions are failing (see the next row) | Check the next row. If compactions succeed and the log still grows, tell the Telcoin Association network team. |
| Consensus database compactions failing | `tn_storage_tndb_compaction_abandons_total`, by `env` and `cause` | 3 or more in an hour for one `env` | The log is not being compacted, so its disk use keeps growing. `cause` says why: `disk_full` (less than 256 MiB free), `writer_outran` (writes faster than a compaction can catch up, an extreme burst), `corruption`, `panic` or `error` | `disk_full`: free disk space. `corruption`: stop the node, run `telcoin-network db tndb-stat`, and tell the Telcoin Association network team. Otherwise tell the network team. |
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

## Required evidence before mainnet

- firewall policy tested from allowed and denied source networks;
- signed address manifest distribution and rollback rehearsal;
- BLS backup and isolated recovery rehearsal;
- signed artifact verification on every target platform;
- canary, phased rollout, and rollback rehearsal;
- quorum dashboard based on validator stake and running version;
- incident contacts and an out-of-band coordination channel.

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

The default consensus ports are UDP 49590 for the primary network and UDP 49595 for the worker network. RPC and metrics ports are TCP and should remain private unless a dedicated gateway protects them.

Apply these controls outside the node process:

- allow core validator P2P ingress only from the current validator and sentry address set;
- allow management and metrics ingress only from operator networks;
- give public observers and gateways their own security group or firewall policy;
- restrict core validator egress to approved peers, DNS, time sources, telemetry, and release infrastructure where the platform supports it;
- log rejected traffic and alert on sustained scans, unexpected destinations, and connection exhaustion.

Treat DHT records, peer exchange messages, and advertised RPC endpoints as untrusted network data. Never use them to add firewall rules or cloud security group entries.

Distribute validator and sentry addresses through an authenticated operator channel. A production address manifest should include the network, epoch or activation time, peer identity, IP addresses, ports, expiry, and signer set. Stage additions before removals, verify connectivity from every validator, and retain the previous manifest for rollback.

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
| Batch cache occupancy | Live data in `<datadir>/consensus-db/cache` from an MDBX statistic, for example `mdbx_stat -ef` from the libmdbx tools (pages used minus free pages, times the page size), against the 1 GiB maximum. Not the file size (see below) | 768 MiB of live data | The committee's batch volume in an epoch is approaching the per-epoch ceiling | Tell the Telcoin Association network team. Hardware does not raise this limit. |
| Engine backlog | `tn_engine_queued_outputs`, 0 to 8 | 4 or more for 5 minutes | Execution is falling behind consensus | At 8 the engine queue is full and outputs back up into the 64-slot channel in front of it. Check CPU and IO pressure and `tn_engine_execution_duration_seconds`. |
| Resident memory | `reth_process_resident_memory_bytes` | Above 70% of RAM | Queued outputs, RPC caches or the transaction pool are growing | Compare with the engine backlog and the RPC request rate. A climb right after a restart is replay and should fall once the node catches up. |

Pressure stall information needs a kernel built with PSI support, which mainline Linux added in 4.20.
If `/proc/pressure` is missing, the running kernel lacks it or has it disabled.

The batch cache is an MDBX file that never shrinks.
Batches are removed at each epoch close, but the file keeps its size, so `du` shows the highest level the file has reached on that datadir, a high-water mark, not how full the cache is now.
In the 2026-09 benchmark the file reached its 1 GiB maximum on every validator while each 20-minute epoch carried only about 0.5 GB of batch data, and no node failed.
Read live occupancy from MDBX instead.
The release exports no metric for it.
The committee's batch output since the epoch started, summed over all validators from `tn_worker_batch_size_bytes`, gives a rough upper estimate, because that histogram also counts failed seal attempts.

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

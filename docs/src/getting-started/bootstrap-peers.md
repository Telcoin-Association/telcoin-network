# Bootstrap peers

Bootstrap peers are dial hints used to discover the network. A new node dials
them to make its first contact; once it has peers, kademlia and the peer records
it learns take over. They do not grant committee membership or permission to
publish gossip; those decisions come from chain state.

## The default set

A node started with `--chain` (`adiri`, `test-net` or `mainnet`) dials the
bootstrap servers built into its binary, from the network's
`chain-configs/*/bootstrap_servers.yaml`. A release can change that list; the
node takes the new list when it restarts on the new binary. The list is
independent of the genesis committee, and the datadir's
`genesis/committee.yaml` does not change it.

A node started from datadir genesis files without `--chain` (a local testnet,
or a network made with `telcoin-network genesis`) dials the `bootstrap_servers`
of its `genesis/committee.yaml`, which the genesis ceremony fills with the
genesis validators.

The node logs the source and the count at startup:
`bootstrap peers selected servers=5 source=built-in chain list`. The other
sources are `network-config`, `command line` and `genesis committee file`.

## Overriding the default

Configure your own set in `network-config` under the node's Telcoin data
directory, without editing the genesis committee file.

Add a `bootstrap_peers` map keyed by the peer's BLS public key. Each entry contains
the primary endpoint and a nonempty `workers` list in worker ID order (worker 0
first). Use the same base58 key encodings and multiaddresses as the peer's
advertised node information. Replace the placeholders in this example:

```yaml
bootstrap_peers:
  <BLS_PUBLIC_KEY>:
    primary:
      network_address: /ip4/192.0.2.10/udp/9000/quic-v1
      network_key: <PRIMARY_NETWORK_PUBLIC_KEY>
    workers:
      - network_address: /ip4/192.0.2.10/udp/9001/quic-v1
        network_key: <WORKER_0_NETWORK_PUBLIC_KEY>
      - network_address: /ip4/192.0.2.10/udp/9002/quic-v1
        network_key: <WORKER_1_NETWORK_PUBLIC_KEY>
```

A nonempty map replaces the entire default set. Include every seed you want the
node to dial. Entries are never merged with the default, so omitted default
peers can be deliberately excluded. An absent `bootstrap_peers` key or an empty
map (`bootstrap_peers: {}`) keeps the default set. Existing config files
continue to load unchanged. The legacy single `worker: {...}` entry is also
accepted and becomes a one-element worker list when serialized again.

For a process-only override, pass the map as YAML or JSON to `--bootstrap-peers`.
For example, save the map itself (without the enclosing `bootstrap_peers:` key)
in `bootstrap-peers.yaml` and run:

```sh
telcoin-network node --bootstrap-peers "$(cat bootstrap-peers.yaml)"
```

Or let the node read the file itself, which keeps the map off the command line:

```sh
telcoin-network node --bootstrap-peers-file bootstrap-peers.yaml
```

The file is read and checked while arguments are parsed, so a missing or invalid
file stops the node before it starts. The two flags cannot be combined. A relative
path resolves from the node's working directory, which is `/` for a systemd
service unless the unit sets `WorkingDirectory=`, so give units an absolute path.

The CLI map takes precedence over `network-config` and is never written back to
disk. An explicitly empty CLI map restores the default set even when
`network-config` contains custom peers:

```sh
telcoin-network node --bootstrap-peers '{}'
```

Invalid map entries fail during CLI parsing or config loading. Valid but
unreachable endpoints can prevent the node from joining the network and cause
the bootstrap readiness wait to time out. Changes take effect after a restart.

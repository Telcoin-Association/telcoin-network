# Bootstrap peers

Bootstrap peers are dial hints used to discover the network. Configure them in
`network-config` under the node's Telcoin data directory, without editing the
genesis committee file. They do not grant committee membership or permission to
publish gossip; those decisions come from chain state.

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

A nonempty map replaces the entire genesis bootstrap set. Include every seed you
want the node to dial. Entries are never merged with genesis, so omitted genesis
peers can be deliberately excluded. An absent `bootstrap_peers` key or an empty
map (`bootstrap_peers: {}`) keeps the genesis fallback. Existing config files
continue to load unchanged. The legacy single `worker: {...}` entry is also
accepted and becomes a one-element worker list when serialized again.

For a process-only override, pass the map as YAML or JSON to `--bootstrap-peers`.
For example, save the map itself (without the enclosing `bootstrap_peers:` key)
in `bootstrap-peers.yaml` and run:

```sh
telcoin-network node --bootstrap-peers "$(cat bootstrap-peers.yaml)"
```

The CLI map takes precedence over `network-config` and is never written back to
disk. An explicitly empty CLI map restores the genesis fallback even when
`network-config` contains custom peers:

```sh
telcoin-network node --bootstrap-peers '{}'
```

Invalid map entries fail during CLI parsing or config loading. Valid but
unreachable endpoints can prevent the node from joining the network and cause
the bootstrap readiness wait to time out. Changes take effect after a restart.

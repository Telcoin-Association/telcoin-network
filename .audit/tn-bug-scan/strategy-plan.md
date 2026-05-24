# Strategy Plan — tn-bug-scan (peers + state-sync + epoch)

## Cache metadata

- Git hash: cb8aa049
- Target scope: peers/, network-libp2p/{consensus,kad}.rs, state-sync/, node/manager/node/epoch.rs, consensus/primary/consensus_bus.rs
- User hints: 10-node committees; kad-driven committee resolution; key rotation; orphan accumulation; collect_excess_peers; score decay; IP-ban THRESHOLD=1; observer pruning; state-sync; INV-043…INV-047 + Appendix A1–A10
- Generated: 2026-05-21

## Topic groups

### Group A (parallel — peer state model)

- **RT-1 (peers/all_peers.rs):** unresolved-committee window, INV-046 vs eviction during heartbeat (manager.rs:244 → all_peers.rs:282); promote_committee_member rotation arm vs orphan in INV-047.
- **RT-2 (peers/all_peers.rs collect_excess_peers):** off-by-one math when protected peers are skipped. Walk `excess = total - max`, prove eviction count.
- **RT-3 (peers/score.rs + peer.rs):** score persistence across new_epoch (INV-032 says make_trusted overwrites Score::new_max — verify); decay across epoch boundary for non-CVV peers; restart erasure (Appendix A3).

### Group B (parallel — surrounding subsystems)

- **RT-4 (network-libp2p/consensus.rs gossipsub → peer):** process_penalty call sites; GossipsubNotSupported = Fatal; SlowPeer = Mild; pre-bootstrap timing.
- **RT-5 (kad.rs):** num_records unchecked decrement (line 389); SystemTime↔Instant translation drift; restart-after-evict-expired ordering.
- **RT-6 (state-sync):** observer fetch paths under peer eviction race; spawn_track_recent_consensus / spawn_fetch_consensus retry loops; pack-file deadlock surfaces.
- **RT-7 (node/manager/node/epoch.rs):** open_epoch_pack pre-dial path; bootstrap-before-new_epoch ordering at init_network_for_epoch.
- **RT-8 (peers/banned.rs):** IP-ban THRESHOLD=1 semantics with `>` comparison (line 100,115); remove_banned_peer unconditional saturating_sub.

### Group C (parallel — concurrency / liveness)

- **RT-9 (peers/cache.rs):** "Key must exist" expects in remove and insert (cache.rs:55,72); concurrent insert+remove race semantics within heartbeat.
- **RT-10 (peers/manager.rs heartbeat):** ordering of prune → unban → discovery (INV-036); pending_dials oneshot overwrite (A1) impact on caller-await.
EOF

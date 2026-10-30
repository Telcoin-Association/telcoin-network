# Upstream fixes worth backporting (survey 2026-10-30)

Base at survey time: upstream `v1.11.4`. Nothing below has been applied. Each one changes
behavior and needs a maintainer's decision, a build, and tests. Apply one with
`python3 -I vendor/reth/tn/vendor.py pick <sha>`, record it in CHANGES.md as a backport, and run
the tests named in the README.

Ranges searched:

- The v1.11.x line after v1.11.4: none.
- `git log --no-merges 564ffa586..origin/main` (the 1.11 branch point, 2026-02-12, to
  `84caec584`, 2026-10-30) over the vendored paths. That gives 1126 commits, about 100 of which
  were trial-picked.

"pick" is the textual result of a trial `vendor.py pick` on the 64-crate tree. Nothing was
compiled. "test-only" means the only conflicts are in test code.

## High

| sha | subject | crate | why it matters to TN | pick |
|---|---|---|---|---|
| ff2657600 | enforce websocket origin allowlists | rpc-builder | TN exposes `--ws.origins`, but the origin of WS upgrades is never checked (cross-site WebSocket hijacking). **Security.** | 3 conflicts (cors.rs ×2 because 997af404a's trim is missing; lib.rs ×1 because of the compression layer). Small manual fix; check that jsonrpsee has `ws::is_upgrade_request`. |
| 792ee9245 | prevent sender-id map growth on read-only lookups | transaction-pool | RPC lookups of random senders allocate sender ids forever, so memory grows without bound. | clean |
| d3d7fb31d | ceiling division for replacement price bump | transaction-pool | With zero or tiny tips the 10% bump rounds down to 0, so same-price replacements churn for free. | clean |
| 6e4009eed | prevent panic in log subscription on broadcast lag | rpc | A slow `eth_subscribe("logs")` WS client hits `expect()` on `Lagged` and the task panics. | clean |
| 47544d9a7 | add to pending subpool in nonce order | transaction-pool | Out-of-order delivery can put nonce gaps into batches, because the batch builder iterates a live `best_transactions()`. | clean |
| 7bf9241fe | disable read-tx timeout during check_consistency | provider | TN runs `check_consistency` at startup. A long heal on an archive DB can hit the MDBX reader timeout and crash-loop. | 1 import-only conflict; likely clean after 294e21507 |
| 95679ace2 | propagate static-file read errors | db, provider | Corrupt static-file reads currently return `Ok(None)` ("missing block"). | 1 trivial conflict (cursor.rs), rest test-only |
| 22e3708b2 | nippy-jar: end last value at its offset | nippy-jar, provider | Reading the tip row while persistence appends uncommitted rows returns over-long bytes, or panics on a stale mmap. | test-only; likely clean after 0d1324f14 |

## Medium

| sha | subject | crate | why | pick |
|---|---|---|---|---|
| 0d1324f14 | handle uncommitted rows in prune_rows | nippy-jar | The static-file heal can leave a config claiming rows whose offsets are not on disk. | clean |
| b629142d0 | preserve refreshed static file cache entries | provider | A concurrent reader can overwrite a freshly refreshed jar with a stale one. | clean |
| 294e21507 | heal finalized/safe ahead of highest header | provider | After an unwind, "finalized" can point past the tip. | clean |
| 5fc3a81d3 | bound safe-no-sync rollback with a sync period | db | Without a period, `--db.sync-mode safe-no-sync` can lose days of data on power loss. | test-only |
| bf1db83ae | release receiver lock before validating | transaction-pool | `additional_validation_tasks` currently validates one transaction at a time. | clean |
| 080a9cfc1, c4cd5c9b7 | apply `apply_pre_execution_changes` in replay and `debug_traceCallMany` | rpc-eth-api, rpc | TN's EIP-4788 and EIP-2935 system calls are skipped in replays and traces. | clean; check `executor_for_block` in the RPC context |
| b7a7a8a72, f2d2bd233 | blocking-IO semaphore for eth_callMany / eth_simulateV1 | rpc-eth-api | Heavy calls can pin unlimited blocking threads. | clean |
| 1f94488d1, e88a669b1, de401d9b4 | permits for getLogs scans and getProof; stop cancelled blocking tasks | rpc, rpc-eth-api | Same DoS class as above. | 1–2 small conflicts each |
| 8861e2724 | notify subscribers when set_block_info promotes | transaction-pool | TN calls `set_block_info`; transactions promoted by a basefee drop never reach pending listeners. | clean |
| 5b0120dbb | lower default block/receipt cache limits | rpc-eth-types | High default memory use unless TN sets its own limits. | clean |
| 5868ee478 | rewind backward overlay seeks | trie | Overlay cursors skip entries on a backward seek. Not hit by current StateRoot callers, but this code sits under TN's output_overlay. | conflicts; hand port is about 8 lines using `reset()` |

## Low

0e14f1a8a and a68f9e19f (IPC loop spin; max subscriptions per connection), 10d130182 (pool gas
limit at startup), 62808bd06 (empty code hash senders), e7ebeb869 (partial receipts),
1a6ba945a (codecs decode), 078c22d5f (iterative BlockState drop). Each picks cleanly or with
test-only conflicts.

## Not simple backports

- **JS-tracer DoS (57825d41d).** TN enables `js-tracer` on reth-rpc. In 1.11, a JS tracer ignores
  its `timeout` and keeps running after the client drops the request, up to OOM. The fix needs
  revm-inspectors 0.44.2 with Boa 0.22 (TN is on 0.34.2). Until then, keep `debug` off public
  endpoints or drop `js-tracer`.
- **IPC hardening** (95bdbe747, ed9bb9d43, c3d46f3d0) sits on a later codec refactor. Take it at
  the next sync, or default validators to `--ipcdisable`.
- **estimateGas cap** (2bd45d119) needs `rpc-eth-types/settings.rs`, which TN doesn't carry.
- **Trie storage-deletion fixes** (cb7fc64c7, 8617e42f9, 839b84027) are tied to upstream's removal
  of storage-wipe markers (c9b466c13).
- Many trie and storage fixes target code TN doesn't run: proof v2, ArenaParallelSparseTrie,
  RocksDB, storage v2, BAL, Amsterdam.
- **For the next sync:** upstream `9d1638f32` (2026-10-24) replaces revm with "evm2". Since v2.0.0,
  primitives-traits, codecs and zstd-compressors live outside the reth repository, so their
  fixes need separate tracking.

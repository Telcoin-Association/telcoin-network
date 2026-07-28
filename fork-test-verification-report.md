# Fork-test e2e verification — `stake-validator.sh` calldata fix

**Date:** 2026-07-22 · **Branch:** `local-consensus-registry-fork-test` · **Run:** fresh `local-validators`, 6 nodes, epochs 0→56, fork fired at epoch 5, torn down after verification.

## Verdict

The calldata-parsing fix (`etc/fork-test/stake-validator.sh:144`) is **correct, complete, and verified in real transactions**. It did its job so well that it unmasked **two deeper, pre-existing issues** that the parsing bug had been hiding. Sanity-check remains **15 passed / 1 failed** — but `[6]`'s failure is now fully root-caused to pre-fork ABI drift, not to anything in the harness scripting.

## 1. The fix works (verified at every level)

| Level | Evidence |
|---|---|
| Unit | The exact contaminated two-line output → `grep -E '^0x[0-9a-fA-F]+$' \| tail -n1` extracts the 650-char calldata; the old raw capture fails on the leading timestamp `2`. |
| Boot run | Staking got **past** `export-staking-args` for the first time ever: `-> stake 1000000000000000000000000 wei` printed (previously aborted with `did not return calldata`). |
| Byte-accuracy | Filtered calldata: 650 chars, selector `0x2fb0d025`, valid even-length hex — decodes exactly as `stake(bytes,(bytes))` per the current ABI. |
| Real tx | The same pipeline's calldata **executed successfully on-chain** for observer2 post-fork: stake landed in block 57 (status 1, registry event emitted), activate in block 58. |

## 2. Newly unmasked issue A — pre-fork `stake` ABI drift (why `[6]` still fails)

The pre-fork staking of observer1 aborts at the `stake` transaction: `execution reverted` at gas estimation, deterministically reproducible against historical pre-fork state (`cast call --block 2` / `--block 6`), while the identical calldata **succeeds** against post-fork state.

**Root cause — the `stake` function is the one selector that drifted across the fork:**

| function | selector | pre-fork dispatcher | post-fork dispatcher |
|---|---|---|---|
| `stake(bytes,(bytes,bytes))` — `BlsG1.ProofOfPossession { bytes uncompressedPubkey; bytes uncompressedSignature; }` | `0xb5184324` | ✅ | — |
| `stake(bytes,(bytes))` — `ProofOfPossession { bytes signature }` | `0x2fb0d025` | — | ✅ |
| `activate()` | `0x0f15f4c0` | ✅ | ✅ |
| `setNextCommitteeSize(uint16)` | `0x62fcde4b` | ✅ | ✅ |
| `mint(address)` | `0x6a627842` | ✅ | ✅ |
| `getCurrentStakeConfig()` | `0x7d06fdf8` | ✅ | ✅ |

`keytool export-staking-args` (`crates/telcoin-network-cli/src/keytool/export_staking_args.rs:43`) builds the **current** ABI: compressed pubkey (96 B) + compressed PoP signature (48 B) in a one-field tuple. The **pre-fork** contract (tn-contracts `ac22e26^`, `src/consensus/BlsG1.sol:31`) needs the old two-field tuple with **uncompressed** points (the BlsG1 library explicitly performs no on-chain (de)compression — "performed externally in Rust by the protocol"). Calling the pre-fork contract with post-fork calldata hits the fallback → empty revert.

This mirrors the README's own documented ABI-drift gotcha (`tn_getValidators` reverting pre-fork) — the harness compensated for drift on the *read* path but the *stake write* path was never exercised: every previous run aborted at the parsing bug before reaching it.

Ruled out: shell-fix corruption (calldata byte-perfect, works post-fork), timing/state-lag (mint receipt confirmed on the same RPC before estimation; mint landed in block 2, revert reproduces at any pre-fork block), missing BlsG1 library (present at block 2, 8,580 bytes), missing NFT (`balanceOf` = 1).

## 3. Newly unmasked issue B — non-committee RPC tx forwarding goes stale

Running `stake-validator.sh 6` (post-fork, per README step 4) failed earlier still: the **fund** transfer submitted via observer2's own RPC (:8540, per the script's `RPC="http://localhost:$((8546-INSTANCE))"`) was never mined — yet the identical transfer via committee RPC :8545 landed immediately (block 51). At epoch 0 the same non-committee submission path *worked* (observer1's fund/mint landed via :8541 in blocks 1–2). So transactions submitted to a non-committee node's RPC stop propagating to the committee at some later epoch — a node-software behavior worth its own investigation, independent of this harness.

## 4. Post-fork onboarding fully demonstrated (manual drive via committee RPC)

Driving the script's exact sequence through :8545 with the fixed capture pipeline:

fund (blk 54) → mint (blk 56) → **stake `0x2fb0d025` (blk 57, success)** → activate (blk 58) → Active at next boundary → `setNextCommitteeSize(5)` (blk 61) → **observer2 seated in the live committee (epoch 55)**.

Final sanity-check deltas vs the original run: `[3]` counts **5** validators (post-fork-staked validator flows correctly through migration accounting and eligible count), `[7]` reports observer2 **Active + in current committee**.

## 5. Fork health (this run, re-confirmed)

- `[1]` swap fired: code hash `0xab86d348…` (off the pinned `0x5318ebc5…`); fork applied at exec block 7, epoch-4 close, `eligible=Some(4)`.
- `[2]` all six nodes byte-identical at sampled heights (blocks 28 and 58 checked across the fleet).
- `[3]–[5]` migration, post-fork ABI reads, epoch info, and scalar getters all pass.

## 6. Recommended follow-ups

1. **Pre-fork stake encoding (unblocks `[6]` → 16/0):** teach the harness to emit old-ABI calldata when staking pre-fork. Cleanest: a `--pre-fork` flag on `keytool export-staking-args` that encodes `stake(bytes,(bytes,bytes))` with `blst` **uncompressed** serialization (`serialize()` instead of `to_bytes()`; tiny local `sol!` binding). Alternative without touching Rust: decompress the BLS points in the existing Docker patch container (needs `py_ecc`-class dependency) and `cast calldata`-encode in the script.
2. **Route harness sends through a committee RPC** (or make `RPC` overridable) so `stake-validator.sh 6` works at arbitrary epochs regardless of issue B.
3. **Investigate issue B** (non-committee tx forwarding staleness) as a standalone node bug — it affects any client submitting through a non-validator node.

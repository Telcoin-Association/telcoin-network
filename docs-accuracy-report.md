# Docs Accuracy Review

This documentation set was migrated from GitBook (tn-docs@8ec0d85) into `docs/src` and verified line-by-line against the telcoin-network source in this repo on 2026-08-21. Six review agents each took a disjoint scope: A1 Networks/Reference, A2 Fees/Gas/Epochs, A3 Architecture/FAQ, A4 Staking, A5 RPC Methods, A6 Getting Started; a final pass swept for terminology drift between scopes. "Fixed" means the doc contradicted the code and was edited to match it, with the evidence cited. "Judgment call" means the claim is questionable but was deliberately not edited — because correcting it needs new content, an owner decision, or information not verifiable from this repo — and it is listed below for maintainer review. All 77 markdown files under `docs/src` were covered.

## Summary

| Scope | Files reviewed | Claims verified clean | Fixed | Judgment calls |
|---|---|---|---|---|
| A1 Networks/Reference (README, networks, evm-compatibility, reference/) | 6 | 19 | 2 | 7 |
| A2 Fees/Gas/Epochs (basefees, gas penalties, epoch-boundaries, canonical-updates) | 5 | 9 | 13 | 3 |
| A3 Architecture/FAQ (architecture/, faqs/economic-incentives) | 6 | 5 | 9 | 6 |
| A4 Staking (staking/) | 5 | 11 | 10 | 2 |
| A5 RPC Methods (rpc-methods/, SUMMARY.md) | 44 | 28* | 17 | 9 |
| A6 Getting Started (getting-started/) | 11 | 7 | 7 | 7 |
| **Total** | **77** | **79** | **58** | **34** |

\* A5's verified count = 27 pages needing no edits at all, plus a namespace sweep confirming every documented method is served by default on TN's enabled transports. Two of A1's judgment calls were subsequently resolved by the Wave-4 cross-cutting fixes (see below) and are marked as such.

## Fixes applied

### A1 — Networks / Reference

- `docs/src/README.md`: removed phantom "Writing to Blockchain" section bullet (no such page exists anywhere in docs/src).
- `docs/src/reference/adiri.md`: typo "methane an ethane" → "methane and ethane".
- Verified clean (highlights): Prague/Pectra active from genesis (chain-configs/{testnet,mainnet}/genesis.yaml:15-17); supported tx types Legacy/2930/1559 with 4844+7702 rejected at pool admission (crates/tn-reth/src/txn_pool.rs:129-141); chain IDs 487/2017; all repurposed-header rows except `extra_data` (evm/block.rs:1487-1528, engine/src/payload_builder.rs:156-205).

### A2 — Fees / Gas / Epochs

- `docs/src/gas-limit-penalty.md`: penalty destination "governance address" → base-fee address, 4 spots (crates/tn-reth/src/evm/handler.rs:94,141-148); `unused_gas` noted as pre-refund (evm/utils.rs:75, handler.rs:108,126); 62% → 64% penalized at 2% usage (exact formula value).
- `docs/src/basefees.md`: base fee "burned (and minted to governance)" → credited, not burned, to the base-fee address (handler.rs:7-10,179-185); "block gas" → "block gas target".
- `docs/src/epoch-boundaries.md` (10 edits): system-call gas limit 30M → 100M (evm/mod.rs:90 `SYSTEM_CALL_GAS_LIMIT = 100_000_000`); shuffle randomness rewritten from keccak256(leader BLS aggregate) to the epoch seed chain, 4 spots (types/src/primary/output.rs:258-273, seed_chain.rs, forks.rs:220-260); committee selection order corrected to shuffle → truncate → sort (evm/block.rs:1557-1614); eligible pool "Active" → Active or PendingActivation (evm/block.rs:985-1027, ConsensusRegistry.sol:917-919) with PendingActivation added to the status table; rewards rewritten from "withdrawn from governance safe per block count" to epoch issuance weighted by stake x consensus header count, paid via Issuance (ConsensusRegistry.sol:74-114, StakeManager.sol:190-224); base-fee adjustment made per-worker (run_epoch.rs:745-850); dangling "basefee.md" reference → link to basefees.md.
- `docs/src/canonical-updates.md`: randomness description → epoch seed chain (same evidence as above).
- Verified clean (highlights): both penalty tables recomputed row-by-row against evm/utils.rs:14-94 and its tests; gas-penalty.md fully accurate; EpochRecord/EpochVote/EpochCertificate structure, super-quorum (2n/3)+1, committee-continuity rule (types/src/primary/epoch.rs:22-199).

### A3 — Architecture / FAQ

- `docs/src/architecture/transaction-lifecycle.md`: replaced Sui-derived per-transaction certification / EffectsCertificate flow with the real batch → quorum ack → header → votes → certificate → Bullshark commit → execution pipeline (worker/src/quorum_waiter.rs, primary/src/proposer.rs, certifier.rs:26-30, consensus/bullshark.rs:126-180, executor/src/subscriber.rs); replaced "sub-half-second validator signature round trip" rationale with the no-forks/no-reorgs rationale (txn_pool.rs:8-10).
- `docs/src/architecture/consensus-layer.md`: Bullshark no longer described as asynchronous-as-implemented; names the partially synchronous variant (leaders on even rounds, f+1 commit — bullshark.rs:132-138). Paper-accurate "first DAG-based asynchronous BAB protocol" citation retained.
- `docs/src/architecture/security.md`: "asynchronous consensus mechanism" → "Byzantine fault tolerant consensus mechanism".
- `docs/src/architecture/native-token.md`: "per block reward" → issued at end of each epoch, weighted by consensus blocks led (ConsensusRegistry.sol:74-115).
- `docs/src/faqs/economic-incentives-and-rewards.md`: rewards credited by applyIncentives from a governance-funded issuance pool, not "withdrawn from a governance safe" (system_calls.rs:9, ConsensusRegistry.sol:595-599); "per-leader per-epoch amount" → total epochIssuance split proportional to consensus blocks led (ConsensusRegistry.sol:83-110); "next block" → "next consensus block".

### A4 — Staking

- `docs/src/staking/how-staking-works.md`: "stake version is permanent / no in-place upgrade" corrected — `upgradeValidatorStakeVersion` exists, one-way, in-place (IStakeManager.sol:167-178, ConsensusRegistry.sol:433-515), 3 spots; noted reward-pool dust rollover (`undistributedIssuance`, ConsensusRegistry.sol:97-114).
- `docs/src/staking/how-to-stake.md`: `--address` must be the NFT-whitelisted validator address (PoP commits to it — keytool/generate.rs:48, ConsensusRegistry.sol:352-383); nonexistent `tn_syncing` RPC replaced with eth_blockNumber comparison (tn-rpc/src/rpc_ext.rs:40-168 has no such method).
- `docs/src/staking/future-direction.md`: system-call gas budget 30M → 100M throughout, utilization 27% → 8%, validator ceiling ~2,500-3,000 → ~8,000 with historical 30M kept as context (evm/mod.rs:90); "not a consensus rule" corrected — the value participates in consensus, lockstep fleet upgrade required (evm/mod.rs:75-84); ValidatorInfo struct has no blsPubkey field — pubkeys live in a separate `blsPubkeys` mapping, SSTORE2 section re-aimed (IConsensusRegistry.sol:16-31, ConsensusRegistry.sol:34,939-974).
- `docs/src/staking/why-membership-model.md`: "must fully exit and rejoin to change version" → in-place one-way upgrade exists (IStakeManager.sol:167-178).
- Verified clean (highlights): full stake()/delegateStake()/activate() signatures, PoP byte sizes, registry vanity address, lifecycle table, error table, keytool flags.

### A5 — RPC Methods

- Renamed 3 stub pages and completed them: `eth_sendrawtransaction.md` (TN `--rpc.txfeecap` guard documented — rpc_fee_cap.rs:38-85,187; observer forwarding — forward.rs:1-49), `eth_sendtransaction.md` and `eth_sign.md` (both honestly documented as always failing with `-32602` / "unknown account" — TN registers no signers; the old "unlock your account" claim was false). `SUMMARY.md` links updated; no other page linked the old names.
- `eth_protocolversion.md`: result `0x5` → `0x1` (worker.rs:143). `eth_syncing.md`: always returns `false`, even while catching up (worker.rs:160-166). `eth_chainid.md`: request/result id mismatch fixed.
- Four uncle pages: TN never produces uncles — documented always-`null` / `0x0` results (evm/block.rs:1532). `net_peercount.md`: wrong source anchor fixed; live libp2p count verified (worker.rs:88-99).
- `filter-methods/README.md`: no API keys on TN — filters are node-local and expire after five minutes, not fifteen (reth DEFAULT_STALE_FILTER_TTL = 5 min); Infura link → local page. `eth_getlogs.md`: `blockHash` is supported now (EIP-234), not "future".
- Malformed JSON repaired in `eth_getcode.md`, `eth_getblockreceipts.md` (Returns section also rewritten from block-object to receipt-array); `eth_gettransactionbyhash.md` heading normalized. All edited examples machine-validated as JSON.

### A6 — Getting Started

- `reading-blockchain-data/libraries.md` + `programmatic-requests/eth_call.md`: stale Telcoin AUD address `0x4392...ee70F` replaced with deployed eAUD `0xc9593289e95938FC4170B3Fc51dbcCa0A46b4486` in all 7 occurrences (tn-contracts/deployments/deployments.json:19); old address confirmed empty and new address confirmed returning the documented bytes via live rpc.adiri.tel calls.
- `eth_call.md`: "5 bytes" selector claim → four bytes; truncated 31-byte length-word quote padded to a true 32-byte word.
- `curl-requests.md`: self-referencing doc link retargeted to `rpc-methods/eth_blocknumber.md`; `libraries.md`: "[Sigher]" → "Signer"; `development-tools.md`: "hetting" → "getting".
- Verified clean (highlights): chain ID 2017 on all five listed RPC URLs (checked live), telscan.io, faucet URL reachable, ethers v6 / axios usage, selector hash values.

### Cross-cutting fixes (Wave 4)

Terminology drift between scopes, aligned to the code-verified facts the scope agents established:

- `docs/src/evm-compatibility.md:71-72`: fee-table rows "Burned then minted to governance address" (base fee + gas limit penalty) → "Credited to the base-fee address (for governance processing)" — nothing is burned or minted; both are direct per-tx credits (handler.rs:141-148,179-185). Resolves A1's open judgment call on this table.
- `docs/src/evm-compatibility.md:74`: "collected by the governance address" → "credited to the chain's base-fee address (the governance safe by default)" (handler.rs:7-15).
- `docs/src/evm-compatibility.md:148`: summary row "Base fee destination: Governance address" → "Base-fee address (governance)".
- `docs/src/evm-compatibility.md:100`: `extra_data` header-table cell "`keccak256(BLS aggregate signature)` at epoch boundaries" → "Committee-shuffle seed (the epoch seed chain value) at epoch boundaries" — matches the current fork-active behavior and the wording A2 established in epoch-boundaries.md:116 (evm/block.rs:79-94,161-172,1497). Resolves A1's open judgment call on this row; a residual history nuance remains (see below).
- Checked, no action needed: Bullshark "asynchronous" leftovers (remaining mentions are the retained paper citation and correct safety-under-asynchrony claims); 30M mentions (all are the correct batch/transaction gas limit or explicit historical context); links to renamed `-to-do`/`-todo` files (zero).

## Judgment calls — needs maintainer review

### Misleading integrator guidance (highest priority)

- **`docs/src/evm-compatibility.md` — "TEL Precompile: Native ERC-20 Interface" section (~lines 17-62) describes a removed interface.** The page promises a full ERC-20 + EIP-2612 surface at `0x7e1` (`balanceOf` = native balance, `transfer`/`approve`/`permit`, "bridge TEL as ERC-20 without wrapping"). The code says that surface was removed: crates/tn-reth/src/evm/tel_precompile/README.md:78-80 ("That surface has been removed"); the dispatcher routes only mint/claim/burn/totalSupply plus faucet-only role methods (tel_precompile/mod.rs:206-241); deployments.json names it `TEL_MINT_PRECOMPILE` and ships a `WTEL` wrapper (deployments.json:15,45). Bridge/integrator guidance built on this section is actively wrong. **Recommend:** rewrite the section as the issuance-only precompile (governance mint with 7-day timelock, claim, burn, totalSupply; faucet feature on testnet), point ERC-20 use cases at WTEL, and change the summary row "Native asset ERC-20 | Built-in at `0x7e1`" (line 147) in the same pass.
- **`docs/src/reference/stablecoin-contracts.md` lists no token addresses at all**, while tn-contracts/deployments/deployments.json:18-44 has 22 deployed eXYZ stablecoins (eAUD…eZAR) plus StablecoinImpl (…json:9) and StablecoinManager. Nothing on the page is wrong — it is empty where it matters. **Recommend:** add an address table from deployments.json.
- **`docs/src/reference/stablecoin-contracts.md` ABI drift:** the embedded ABI carries 3 OpenZeppelin library errors absent from tn-contracts/artifacts/Stablecoin.json (AddressEmptyCode, AddressInsufficientBalance, FailedInnerCall); functionally harmless, and the doc cites the external telcoin/telcoin-contracts repo, unverifiable from here. **Recommend:** regenerate from the canonical artifact or leave.
- **`docs/src/rpc-methods/eth_getproof.md` documents a captured failure as the example Result** (`-32603 "internal blocking task error"`). The method is wired and served (env/rpc.rs:59-62). **Recommend:** re-capture against a live node and replace with a successful proof response; nothing was fabricated in its place.

### Staleness vs current deployments

- **`docs/src/networks-and-rpc-endpoints.md`: "Adiri Testnet is the only network currently deployed."** A mainnet genesis exists in-repo (chain-configs/mainnet/genesis.yaml:3, chainId 487, timestamp 2026-08-17 — four days before this review) with committee and parameters files. No mainnet RPC/explorer URL is verifiable from the repo, so no endpoints were invented. **Recommend:** add a Mainnet (chain 487) section with real endpoints, or soften the sentence.
- **`docs/src/canonical-updates.md`: "one epoch record is generated per day."** Epoch duration is a governance/genesis parameter; the CLI genesis default is 8 hours (telcoin-network-cli/src/genesis/mod.rs:88-96), which would mean three records per day; the live-network value is baked into genesis contract storage and not verifiable here. **Recommend:** confirm the production epochDuration and fix the cadence claim (also "approximately one per day" later in the page).
- **`docs/src/architecture/native-token.md` bridging paragraph is pre-launch planning voice** ("TN needs this TEL bridged at genesis if possible… consider manual issuance and upgrading the protocol"). WTEL exists (tn-contracts/src/WTEL.sol); the rest is stale roadmap prose. **Recommend:** refresh to post-launch reality.

### Security-page claims vs contract reality

- **`docs/src/architecture/security.md`: "The user's stake will always be safe, and cannot be confiscated by any network user, including the validators."** True today only because the protocol feeds `applySlashes` an empty array (crates/tn-reth/src/system_calls.rs:21-22); confiscation paths exist in the contract (applySlashes, governance `_consensusBurn` → Issuance; ConsensusRegistry.sol:118-131,499,803). Falsified the day slashing activates. **Recommend:** qualify the claim.
- **`docs/src/staking/why-membership-model.md`: "well known validators act as the sole defense against malicious actors"** — overstates: governance stake-confiscation exists and protocol slashing is scaffolded (same evidence). **Recommend:** soften "sole".
- **`docs/src/architecture/security.md` open-staking narrative**: "users can stake… increasing that validator's staking power" and the DPoS label — StakeManager supports exactly one delegator per validator (StakeManager.sol:25-52) and committee voting power is equal per member (types/src/committee.rs:25). Roadmap voice, not current mechanics. Related: "secure so long as over 2/3 of the network's total stake is honest" — the BFT guarantee is over equal per-member voting power (committee.rs:516-528), which coincides with stake only while stakes are uniform; and "stakers … receive reduced rewards" rides the same delegation roadmap (the validator-side mechanism, reputation-driven leader slots, is real). **Recommend:** mark as roadmap or restate in committee terms.
- **`docs/src/architecture/security.md`**: "smart contracts are immutable" (educational generalization; TN's registry is non-upgradeable but EVM proxies exist) and validators "run by GSMA members" (organizational, not code-checkable). Same for `docs/src/README.md`'s "GSMA Operator Member MNOs… proof of stake consensus" framing. **Recommend:** owner sign-off as positioning language.

### Fee/epoch model simplifications

- **`docs/src/basefees.md` + `docs/src/epoch-boundaries.md` present one chain-wide EIP-1559 base fee.** Fees are per-worker with per-worker strategies — `Eip1559 { target_gas }` or governance-set `Static { fee }` (types/src/gas_accumulator.rs:76-82). Accurate for current single-worker deployments; wrong for multi-worker or Static configs. **Recommend:** decide whether to surface the per-worker/Static model.
- **`docs/src/epoch-boundaries.md` system-calls table names only the ConsensusRegistry target.** The closing block also writes worker fee configs to the WorkerConfigs contract (gas_accumulator.rs:107-122) and every block issues EIP-4788/2935-style pre-block calls. Not wrong in context; incomplete. **Recommend:** optionally add the other targets.

### Block-header semantics not surfaced on RPC pages

- **`docs/src/rpc-methods/eth_getblockbyhash.md`, `eth_getblockbynumber.md`, `eth_getblockreceipts.md` keep generic PoW field descriptions** (`nonce` "proof-of-work hash", `sha3Uncles`, `difficulty`, `extraData`) while TN repurposes all of them (nonce = epoch<<32|round, difficulty = batch/worker, ommers_hash = batch digest, extraData = shuffle seed at epoch close, withdrawals = rewards — evm/block.rs:1487-1528). The pages' example block is a pre-repurpose adiri snapshot (sha3Uncles = EMPTY_OMMER_ROOT). **Recommend:** add a "TN header semantics" note box linking to evm-compatibility.md's table, and re-capture examples.
- **`docs/src/evm-compatibility.md` `extra_data` history nuance** (residual after the Wave-4 fix): current behavior is the epoch seed chain, but historical adiri blocks from pre-fork epochs (< 383) carry `keccak256(aggregated BLS signature)` in `extra_data` (types/src/forks.rs:220-260; evm/block.rs:79-94). Explorers reading old testnet history need the legacy rule. **Recommend:** optional one-line history note.

### Empty stubs and unverifiable content

- **`docs/src/getting-started/README.md` and `docs/src/getting-started/dapp-development/README.md` are heading-only stubs** (as is `docs/src/reference/README.md`). Left empty — content invention was out of scope. **Recommend:** write intros or collapse the SUMMARY entries.
- **`docs/src/getting-started/hardware-requirements.md` numbers are unverifiable from the repo** (validator 16c/32t min / 32c rec, 128 GB RAM, 4-7.5 TB TLC NVMe, 1 Gb/s; observer 8c/16t, 16-32 GB, 500 GB-2 TB, 24 Mbps). Plausible and internally consistent, but no source in-repo. Also: the OS line runs distro names together with no separators ("Debian 11+Ubuntu 20.04+RHEL 8…", both sections — GitBook artifact), and the contact emails (grant@telcoin.org, support@telcoin.org) are organizational. **Recommend:** ops confirm the numbers; fix the OS list formatting.
- **`docs/src/getting-started/faucet.md`**: faucet URL verified reachable and the faucet is real (tn-faucet crate, Makefile:118), but the UI flow and support email are unverifiable from the repo. No action needed unless the flow changed.

### Minor RPC/behavior notes (recorded, not errors)

- `eth_accounts.md`: always `[]` on TN (no signers) — example already shows `[]`; an explicit sentence would help. `net_listening.md`: reth hardcodes `true` (net.rs:44-46); generic description implies it could vary.
- Gas-oracle pages (`eth_gasprice.md`, `eth_maxpriorityfeepergas.md`, `eth_feehistory.md`): examples consistent with TN (7-wei genesis base fee), but a note that TN's base fee is flat within an epoch would help fee-estimation users.
- `eth_estimategas.md` (and `eth_createaccesslist.md:13`): `maxPriorityFeePerGas` parameter description is an upstream copy of the gasPrice text and mentions geth; cosmetic. `eth_getlogs.md` Returns section carries filter-changes return shapes; harmless. A few result blocks remain in JS object notation (Infura capture artifacts).
- Nonstandard `eth_sendRawTransactionSync` is covered by the same fee-cap guard (rpc_fee_cap.rs:98-104) but has no doc page; recorded for a future page.
- `getting-started/.../eth_call.md` omits the `pending` block tag and "(optional)" labels on some fields; possibly intentional TN scoping.

### Prose defects left in place

- `docs/src/staking/why-membership-model.md`: garbled sentence "They are incentivized because the contributed more to the network, not because they contributed more." — likely intended "…not because they *staked* more"; intent not certain, so left. **Recommend:** author fix.

## Removal note

This file lives at the repo root for review convenience only; drop it from the branch before merging if it is not wanted.

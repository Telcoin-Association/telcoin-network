# tn

The `tn` namespace serves Telcoin Network data that has no `eth` equivalent: the node's identity and consensus participation mode, consensus headers and signed epoch records, the millisecond commit time of execution blocks, and validator, committee and staking state from the `ConsensusRegistry` system contract at `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`. Registry methods read the contract at the node's canonical tip and take no block parameter.

`tn` is on by default: a node serves it unless `--http.api` or `--ws.api` names a list without it. The cost to serve it is low to moderate. Most methods answer from memory or the node's database; registry methods run a read-only contract call against the latest state. The blocking work behind these methods is capped at 64 concurrent requests per RPC server, and requests beyond that queue rather than fail. Public RPC nodes (observers) should serve it; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Keep `tn` off validators**
>
> Validators should not expose the `tn` namespace publicly. A [tn\_getBlockTimestampMillis](tn_getblocktimestampmillis.md) request for a block from a sealed epoch can open that epoch's consensus pack, and the node opens packs into a small cache it shares with state sync and peer epoch serving. The lookup runs under a tight concurrency bound, but public callers can still churn that cache.
>
> Omit `tn` from the validator's `--http.api` and `--ws.api` and serve the namespace from non-validating nodes instead; see [Enabling Namespaces](../enabling-namespaces.md).

Errors use these codes: a registry call that reverts is reported as `eth_call` reports one, with code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`; `-32001` (`Not Found.`) means the node does not have the requested record; `-32602` is an invalid parameter; `-32603` (`internal error`) is a failure inside the node, whose detail is only logged on the node.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| **Node** | |
| [tn\_info](tn_info.md) | The node's publicly available identity and connection information: name, ids, public keys and network addresses |
| [tn\_nodeMode](tn_nodemode.md) | The node's current consensus participation mode, read live at call time |
| **Consensus** | |
| [tn\_latestConsensusHeader](tn_latestconsensusheader.md) | The latest consensus header |
| [tn\_latestHeader](tn_latestheader.md) | Deprecated alias for `tn_latestConsensusHeader` |
| [tn\_epochRecord](tn_epochrecord.md) | The epoch record and its certificate for an epoch number, from the node's database |
| [tn\_epochRecordByHash](tn_epochrecordbyhash.md) | The epoch record and its certificate for an epoch record hash, from the node's database |
| [tn\_getBlockTimestampMillis](tn_getblocktimestampmillis.md) | The consensus commit time, in milliseconds, of an execution block |
| [tn\_genesis](tn_genesis.md) | The chain genesis |
| **Epoch and registry** | |
| [tn\_getCurrentEpoch](tn_getcurrentepoch.md) | The current epoch number from the `ConsensusRegistry` |
| [tn\_getCurrentEpochInfo](tn_getcurrentepochinfo.md) | The current epoch's on-chain info from the `ConsensusRegistry`: committee, issuance, start block height and duration |
| [tn\_getEpochInfo](tn_getepochinfo.md) | On-chain info for an epoch the `ConsensusRegistry` still holds in its window of recent epochs |
| [tn\_getNextCommitteeSize](tn_getnextcommitteesize.md) | The committee size for the next epoch |
| **Validators** | |
| [tn\_getValidators](tn_getvalidators.md) | All validators with the requested status; `"Any"` returns validators of every status |
| [tn\_getCommitteeValidators](tn_getcommitteevalidators.md) | The committee validators for an epoch |
| [tn\_getValidator](tn_getvalidator.md) | The `ValidatorInfo` record for a validator address |
| [tn\_getBlsPubkey](tn_getblspubkey.md) | The BLS public key for a validator address |
| [tn\_getCommitteeBlsPubkeys](tn_getcommitteeblspubkeys.md) | The BLS public keys for the committee of an epoch |
| [tn\_isValidator](tn_isvalidator.md) | Whether a BLS public key belongs to a known validator |
| [tn\_isDelegated](tn_isdelegated.md) | Whether a validator's stake originates from a delegator |
| [tn\_isRetired](tn_isretired.md) | Whether a validator is permanently retired |
| **Staking** | |
| [tn\_getRewards](tn_getrewards.md) | The claimable rewards accrued for a validator address |
| [tn\_getBalanceBreakdown](tn_getbalancebreakdown.md) | A validator's outstanding balance, initial stake and rewards |
| [tn\_getCurrentStakeVersion](tn_getcurrentstakeversion.md) | The current stake config version |
| [tn\_undistributedIssuance](tn_undistributedissuance.md) | The issuance not yet distributed to validators |
| [tn\_delegationDigest](tn_delegationdigest.md) | The EIP-712 digest a validator signs to accept a delegation |
| [tn\_proofOfPossessionMessage](tn_proofofpossessionmessage.md) | The BLS12-381 proof-of-possession message a validator signs |

# Bridging
Bridging is critical for TN to succees. TEL was originally created as an ERC-20 on Ethereum. Since then, most of Telcoin's infrastructure has migrated to Polygon.

## Supported Chains
Bridging from one chain to another seems complicated enough.

Is it sufficient to only sync state from Ethereum or Polygon?
    - The two have a bridge, so it would be possible (albeit less efficient) for users to bridge to TN if TN only supported one bridge to one of these chains.
    - I'm not aware of any other chain Telcoin needs to support at this time

## Bridging Strategy
Ideas on how to bridge TEL assets from one chain to another.

## The Polygon Way
Polygon syncs the entire Ethereum state during Heimdall checkpoints.

TN does not strive to be an Ethereum side-chain, but will remain EVM-compatible.

### State Sync
Polygon validators must maintain ethereum balances to interact with the StateSender contract deployed on "rootchain" (aka Ethereum).

This is an interesting approach and worth considering.

#### TEL Bridging Contract
For now, assume bridging with Polygon

Requires known authorities on both chains.
    - staking contract on both chains
    - validators need to sign txs to interact with contracts on both chains

7 day wait period to leave TN seems pretty standard
    - base, optimism, etc.
        - https://docs.optimism.io/builders/app-developers/bridging/messaging

###### Polygon -> TN
- `BridgeTEL` contract deployed on Polygon
- User transfers TEL to the contract, locking it
    - stablecoins burn
- BridgeTEL emits an event
- TN validators monitor events
- Sync state for the BridgeTEL contract
    - tokens associated with a network?
    - owner transfers TEL from bridge contract back to their wallet?
    - TEL native contract syncs at epoch to update state?
- Native token contract mints TEL on TN to the address with locked TEL on Polygon

###### TN -> Polygon
- `BridgeTEL` contract also deployed on TN
    - this is what stays in sync between the two networks
- Also need a contract on Polygon that tracks which validators are activefor the epoch
    - bridging can only happen at epoch/sprint

## The Circle Way (CCTP)
- Generalized message passing: https://developers.circle.com/stablecoins/docs/generic-message-passing
    - Validators act as attestation service
    - still need a way to update validator set for epoch/sprint to exclude slashed/rejected validators
- message format used to attest
    - attestation role assigned to validators for epoch
    - enabled/disabled per epoch?
- requires validators to pay gas on all supported networks
    - unless call can attest without changing state
    - but state change needed for epoch/sprint change

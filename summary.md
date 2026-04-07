Summary

  Part A: External Crates (telcoin-network repo)

  tn-precompiles (crates/tn-precompiles/) - 7 source files
  - Standalone TEL ERC-20 precompile extracted from tn-reth
  - Constants, ERC-20 handlers, EIP-2612 permit, timelocked mint/claim/burn, faucet
  (feature-gated)
  - cargo check passes, both default and faucet features

  tn-evm (crates/tn-evm/) - 8 source files
  - TelcoinHardfork enum (Genesis → PRAGUE)
  - TNEvmHandler (gas penalty + basefee redirect) - made pub for foundry composition
  - TNEvmFactory, TNEvm, calculate_gas_penalty
  - cargo check passes

  Part B: Foundry Fork Changes (all additive)

  ┌───────────┬────────────────────────────────────────────────────────────────────────┐
  │   Crate   │                                Changes                                 │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Workspace │ Added tn-precompiles, tn-evm as path deps to ../telcoin-network/       │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Hardforks │ FoundryHardfork::Telcoin(TelcoinHardfork) variant, parsing (tn:,       │
  │           │ telcoin:), FromEvmVersion                                              │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Networks  │ telcoin: bool field, --telcoin CLI arg (mutual exclusion), chain ID    │
  │           │ 2017 auto-detection, precompile labels                                 │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │           │ TelcoinEvmNetwork, TelcoinEvmFactory, TelcoinFoundryEvm,               │
  │ EVM Core  │ TelcoinFoundryHandler (gas penalty + basefee + CREATE2 redirect), full │
  │           │  TEL precompile dispatcher (1031 lines, 15 handlers)                   │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ EVM       │ telcoin.rs (precompile init + fork warming), TelcoinLabels inspector   │
  │ Executor  │                                                                        │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Config    │ Telcoin field auto-inherited via NetworkConfigs flatten                │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Anvil     │ telcoin.rs backend, with_telcoin()/is_telcoin() config, genesis init,  │
  │           │ hardfork resolution                                                    │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ Forge     │ Networks::Telcoin variant in init command                              │
  ├───────────┼────────────────────────────────────────────────────────────────────────┤
  │ CLI       │ NetworkVariant::Telcoin                                                │
  └───────────┴────────────────────────────────────────────────────────────────────────┘

  Verification

  - cargo check --workspace passes (all crates compile)
  - cargo test -p foundry-evm-hardforks - 6/6 tests pass (including new telcoin tests)
  - All existing Tempo tests unaffected (nothing removed)

  Key Design Decision

  Due to alloy-evm version mismatch (telcoin-network uses 0.27, foundry uses 0.29), the TEL
   precompile dispatcher and gas penalty handler are inlined in foundry rather than
  delegating to tn-precompiles/tn-evm directly. The calculate_gas_penalty function (pure
  math) and basefee_address() (returns Address) ARE imported from tn-evm since they have no
   type conflicts.
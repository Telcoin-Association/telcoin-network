//! Genesis helper methods.
//!
//! The yaml, chainspec, and Genesis struct are used for all
//! testing purposes.
//!
//! adiri is the current name for multi-node testnet.

use crate::{now, Genesis, MIN_PROTOCOL_BASE_FEE};
use alloy::{
    genesis::GenesisAccount,
    primitives::{address, U256},
};
use reth_chainspec::ChainSpec;
use std::sync::Arc;

/// test genesis
///
/// Provide a genesis for running tests.
/// With funded [TransactionFactory] default account.
/// This is usable for many unit tests but it lacks the genesis contracts and storage.
/// Go throuigh ['GenesisArgs'] to generate a complete genesis.
pub fn test_genesis() -> Genesis {
    let mut genesis = Genesis { timestamp: now(), ..Default::default() };
    set_genesis_defaults(&mut genesis);
    genesis.config.chain_id = 2017;
    let default_factory_account = vec![
        (
            // Default transaction factory
            address!("0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        // Various accounts used by faucet.
        (
            address!("0xe626ce81714cb7777b1bf8ad2323963fb3398ad5"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xa3478861957661b2d8974d9309646a71271d98b9"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
        (
            address!("0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb"),
            GenesisAccount::default().with_balance(U256::MAX),
        ),
    ];
    genesis.extend_accounts(default_factory_account)
}

/// Set the genesis default config.
pub fn set_genesis_defaults(genesis: &mut Genesis) {
    // Configure hardforks or Reth will be cross with us...
    genesis.config.homestead_block = Some(0);
    genesis.config.eip150_block = Some(0);
    genesis.config.eip155_block = Some(0);
    genesis.config.eip158_block = Some(0);
    genesis.config.byzantium_block = Some(0);
    genesis.config.constantinople_block = Some(0);
    genesis.config.petersburg_block = Some(0);
    genesis.config.istanbul_block = Some(0);
    genesis.config.berlin_block = Some(0);
    genesis.config.london_block = Some(0);
    genesis.config.cancun_time = None; //Some(0);
    genesis.config.shanghai_time = Some(0);
    genesis.config.prague_time = None;
    genesis.config.osaka_time = None;
    // Configure some misc genesis stuff.
    // chain_id and maybe timestamp should probably be a command line option...
    genesis.timestamp = now();
    genesis.config.terminal_total_difficulty_passed = true;
    genesis.config.terminal_total_difficulty = Some(U256::from(0));
    genesis.gas_limit = 30_000_000;
    genesis.base_fee_per_gas = Some(MIN_PROTOCOL_BASE_FEE as u128);
}

/// test chain spec wrapped in [Arc].
pub fn test_chain_spec_arc() -> Arc<ChainSpec> {
    let chain: ChainSpec = test_genesis().into();
    Arc::new(chain)
}

/// adiri (testnet) genesis
pub fn adiri_genesis() -> Genesis {
    serde_yaml::from_str(TESTNET_GENESIS).expect("serde parse valid adiri yaml")
}

/// adiri (testnet) chain spec parsed from genesis.
fn _adiri_chain_spec() -> ChainSpec {
    adiri_genesis().into()
}

/// adiri (testnet) chain spec parsed from genesis and wrapped in [Arc].
fn _adiri_chain_spec_arc() -> Arc<ChainSpec> {
    Arc::new(_adiri_chain_spec())
}

// The raw strings for the testnet genesis and config.
/// Static strig for adiri (testnet) genesis.
///
/// Used by CLI and other methods above.
///
/// Note the significance of ChainId "2017":
/// - Telcoin was founded in Singapore in 2017
/// - 2017 in hex is "0x7e1" (ie- "tel")
/// - 2017 => 1 in numerology
///
/// Faucet addresses:
/// - 0xe626ce81714cb7777b1bf8ad2323963fb3398ad5
/// - 0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907
/// - 0xa3478861957661b2d8974d9309646a71271d98b9
/// - 0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb
pub const TESTNET_GENESIS: &str = include_str!("../../../chain-configs/testnet/genesis.yaml");
pub const TESTNET_COMMITTEE: &str = include_str!("../../../chain-configs/testnet/committee.yaml");
pub const TESTNET_WORKER_CACHE: &str =
    include_str!("../../../chain-configs/testnet/worker_cache.yaml");
pub const TESTNET_PARAMETERS: &str = include_str!("../../../chain-configs/testnet/parameters.yaml");

// The raw strings for the mainnet genesis and config.
pub const MAINNET_GENESIS: &str = include_str!("../../../chain-configs/mainnet/genesis.yaml");
pub const MAINNET_COMMITTEE: &str = include_str!("../../../chain-configs/mainnet/committee.yaml");
pub const MAINNET_WORKER_CACHE: &str =
    include_str!("../../../chain-configs/mainnet/worker_cache.yaml");
pub const MAINNET_PARAMETERS: &str = include_str!("../../../chain-configs/mainnet/parameters.yaml");

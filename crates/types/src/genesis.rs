//! Genesis helper methods.
//!
//! The yaml, chainspec, and Genesis struct are used for all
//! testing purposes.
//!
//! adiri is the current name for multi-node testnet.

use crate::{Genesis, MIN_PROTOCOL_BASE_FEE};
use reth_chainspec::ChainSpec;
use std::sync::Arc;

/// adiri genesis
///
/// NOTE: reth does not support deserializing certain fields, including base_fee_per_gas.
///
/// After deserializing from string, update genesis with TN-specific values.
pub fn adiri_genesis() -> Genesis {
    let yaml = adiri_genesis_string();
    let genesis: Genesis = serde_json::from_str(&yaml).expect("serde parse valid adiri yaml");
    // set min base fee for genesis
    //
    // TODO: set blob gas here
    genesis.with_base_fee(Some(MIN_PROTOCOL_BASE_FEE as u128))
}

/// adiri chain spec parsed from genesis.
pub fn adiri_chain_spec() -> ChainSpec {
    adiri_genesis().into()
}

/// adiri chain spec parsed from genesis and wrapped in [Arc].
pub fn adiri_chain_spec_arc() -> Arc<ChainSpec> {
    Arc::new(adiri_chain_spec())
}

/// adiri genesis string in yaml format.
///
/// Seed "Bob" and [0; 32] seed addresses.
fn adiri_genesis_string() -> String {
    adiri_genesis_raw().to_string()
}

/// Static strig for adiri genesis.
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
fn adiri_genesis_raw() -> &'static str {
    r#"
{
    "nonce": "0x0",
    "timestamp": "0x6553A8CC",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0xe626ce81714cb7777b1bf8ad2323963fb3398ad5": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0xa3478861957661b2d8974d9309646a71271d98b9": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x781e3f2014d83dB831df4cAA3BA78aEc57396B50": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x4f264cc3709f35f39b1fc0c2c1110141b8c44370": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0xc1612C97537c2CC62a11FC4516367AB6F62d4B23": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x649a2C65C69130a2Bfe891965A267DD39233cb3a": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x9F35A76bE2a3A84FF0c0A6365CD3C5CeB3a7FD97": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x8133Be861AD5C9Dea396E5dE5BA1B0154E87e925": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0xDEC366b889A53B93CFa561076c03C18b0b4D6C93": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x8903d35F5F941bc0C6977DBf40d0cB067473e8f2": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        },
        "0x0e26AdE1F5A99Bd6B5D40f870a87bFE143Db68B6": {
            "balance": "0xfffffffffffffffffffffffffffffffffffffffffffffffff21f494c589bffff"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "chainId": 2017,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}"#
}

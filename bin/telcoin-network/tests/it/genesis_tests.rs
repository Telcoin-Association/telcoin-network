//! Tests with RPC calls for ConsensusRegistry in genesis (through CLI).
//!
//! NOTE: this test contains code for executing a proxy/impl pre-genesis
//! however, the RPC calls don't work. The beginning of the test is left
//! because the proxy version may be re-prioritized later.
use crate::util::{spawn_local_testnet, IT_TEST_MUTEX};
use alloy::{network::EthereumWallet, primitives::utils::parse_ether, providers::ProviderBuilder};
use core::panic;
use eyre::OptionExt;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use serde_json::Value;
use std::time::Duration;
use telcoin_network::args::clap_u256_parser_to_18_decimals;
use tn_config::{NetworkGenesis, CONSENSUS_REGISTRY_JSON, DEPLOYMENTS_JSON};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethEnv,
};
use tn_types::{Address, Bytes, FromHex, GenesisAccount, U256};
use tracing::debug;

#[tokio::test]
async fn test_genesis_with_its() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));
    // spawn testnet for RPC calls
    let temp_path = tempfile::TempDir::with_suffix("genesis_with_its").expect("tempdir is okay");
    spawn_local_testnet(
        temp_path.path(),
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
        None,
    )
    .expect("failed to spawn testnet");
    // allow time for nodes to start
    tokio::time::sleep(Duration::from_secs(10)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    let itel_address =
        RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, Some("its.InterchainTEL"))?
            .as_str()
            .map(|hex_str| Address::from_hex(hex_str).unwrap())
            .unwrap();
    let tel_supply = U256::try_from(parse_ether("100_000_000_000").unwrap()).unwrap();
    let initial_stake = U256::try_from(parse_ether("4_000_000").unwrap()).unwrap();
    let governance_balance = U256::try_from(parse_ether("10").unwrap()).unwrap();
    // account for governance safe allocation and 4 million tel staked at genesis for 4 validators
    let itel_bal = tel_supply - initial_stake - governance_balance;
    let precompiles = NetworkGenesis::fetch_precompile_genesis_accounts(itel_address, itel_bal)
        .expect("its precompiles not found");
    for (address, genesis_account) in precompiles {
        let returned_code: String = client
            .request("eth_getCode", rpc_params!(address))
            .await
            .expect("Failed to fetch runtime code");
        assert_eq!(Bytes::from_hex(returned_code), Ok(genesis_account.code.unwrap()));

        if address == itel_address {
            let returned_bal: String = client
                .request("eth_getBalance", rpc_params!(address))
                .await
                .expect("Failed to fetch iTEL balance");
            let returned_bal = returned_bal.trim_start_matches("0x");
            assert_eq!(U256::from_str_radix(returned_bal, 16)?, itel_bal);
        }
        if genesis_account.storage.is_some() {
            for (slot, value) in genesis_account.storage.unwrap().iter() {
                let returned_storage: String = client
                    .request("eth_getStorageAt", rpc_params!(address, slot.to_string(), "latest"))
                    .await
                    .expect("Failed to fetch storage slot");
                assert_eq!(returned_storage, value.to_string());
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_precompile_genesis_accounts() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));
    // check that all addresses in expected_deployments are present in precompiles
    let is_address_present = |address: &str, genesis_config: Vec<(Address, GenesisAccount)>| {
        genesis_config
            .iter()
            .any(|(precompile_address, _)| precompile_address.to_string() == address)
    };
    let expected_deployments = RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, None)?;

    // assert all interchain token service precompile configs are present
    let its_addresses = expected_deployments.get("its").and_then(|v| v.as_object()).unwrap();
    let itel_address =
        Address::from_hex(its_addresses.get("InterchainTEL").and_then(|v| v.as_str()).unwrap())
            .unwrap();

    let some_bal = U256::try_from(parse_ether("95_000_000_000").unwrap()).unwrap();
    let precompiles = NetworkGenesis::fetch_precompile_genesis_accounts(itel_address, some_bal)
        .expect("its precompiles not found");
    let addresses_with_storage: Vec<&str> = [
        "InterchainTEL",
        "InterchainTELImpl",
        "AxelarAmplifierGateway",
        "GasService",
        "InterchainTokenService",
        "InterchainTokenFactory",
        "SafeImpl",
        "Safe",
    ]
    .iter()
    .filter_map(|&key| its_addresses.get(key).and_then(Value::as_str))
    .collect();
    its_addresses
        .iter()
        .filter_map(|(key, value)| value.as_str().map(|address| (key, address)))
        .for_each(|(key, address)| {
            assert!(
                is_address_present(address, precompiles.clone()),
                "{key} is not present in precompiles"
            );

            if addresses_with_storage.contains(&address) {
                if let Some((_, genesis_account)) = precompiles
                    .iter()
                    .find(|precompile| precompile.0 == Address::from_hex(address).unwrap())
                {
                    assert!(
                        genesis_account.storage.is_some(),
                        "Storage should be present for {address}"
                    );

                    if key == "InterchainTEL" {
                        let governance_bal = U256::try_from(parse_ether("10").unwrap()).unwrap();
                        let expected_bal = some_bal - governance_bal;
                        assert!(
                            genesis_account.balance == expected_bal,
                            "ITEL balance should be 100 billion TEL minus genesis validator stake"
                        );
                    }
                }
            }
        });

    Ok(())
}

#[tokio::test]
async fn test_genesis_with_consensus_registry() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));
    // fetch registry impl bytecode from compiled output in tn-contracts
    let json_val = RethEnv::fetch_value_from_json_str(
        CONSENSUS_REGISTRY_JSON,
        Some("deployedBytecode.object"),
    )?;
    let registry_deployed_bytecode = json_val.as_str().ok_or_eyre("Couldn't fetch bytecode")?;

    // spawn testnet for RPC calls
    let temp_path =
        tempfile::TempDir::with_suffix("genesis_with_consensus_registry").expect("tempdir is okay");
    spawn_local_testnet(
        temp_path.path(),
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
        None,
    )
    .expect("failed to spawn testnet");
    // allow time for nodes to start
    tokio::time::sleep(Duration::from_secs(10)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    // sanity check onchain spawned in genesis
    let returned_impl_code: String = client
        .request("eth_getCode", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
        .await
        .expect("Failed to fetch registry impl bytecode");

    // trim `0x` prefix
    assert_eq!(&returned_impl_code, registry_deployed_bytecode);

    let tx_factory = TransactionFactory::default();
    let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse().expect("rpc url parse error"));

    // test rpc calls for registry in genesis - this is not the one deployed for the test
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());
    let current_epoch_info =
        consensus_registry.getCurrentEpochInfo().call().await.expect("get current epoch result");
    let expected_epoch_issuance = clap_u256_parser_to_18_decimals("25_806")?; // CLI default

    debug!(target: "bundle", "consensus_registry: {:#?}", current_epoch_info);
    let ConsensusRegistry::EpochInfo {
        committee,
        epochIssuance,
        blockHeight,
        epochDuration,
        stakeVersion,
    } = current_epoch_info;
    assert_eq!(blockHeight, 0);
    assert_eq!(epochDuration, 86400);
    assert_eq!(epochIssuance, expected_epoch_issuance);
    assert_eq!(stakeVersion, 0);

    let validators = consensus_registry
        .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
        .call()
        .await
        .expect("failed active validators read");

    let validator_addresses: Vec<_> = validators.iter().map(|v| v.validatorAddress).collect();
    assert_eq!(committee, validator_addresses);
    debug!(target: "bundle", "active validators??\n{:?}", validators);

    Ok(())
}

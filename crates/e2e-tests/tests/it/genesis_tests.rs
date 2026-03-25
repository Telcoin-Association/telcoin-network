//! Tests with RPC calls for ConsensusRegistry in genesis (through CLI).
//!
//! NOTE: this test contains code for executing a proxy/impl pre-genesis
//! however, the RPC calls don't work. The beginning of the test is left
//! because the proxy version may be re-prioritized later.
use alloy::{network::EthereumWallet, providers::ProviderBuilder};
use core::panic;
use e2e_tests::{spawn_local_testnet, verify_all_transports};
use eyre::OptionExt;
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use std::{collections::HashMap, time::Duration};
use telcoin_network_cli::args::clap_u256_parser_to_18_decimals;
use tn_config::{
    NetworkGenesis, BLSG1_JSON, CONSENSUS_REGISTRY_JSON, DEPLOYMENTS_JSON,
    GENESIS_ACCOUNT_STATE_YAML, ISSUANCE_ADDRESS, ISSUANCE_JSON,
};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethEnv,
};
use tn_types::{Address, Bytes, FromHex, GenesisAccount};
use tracing::debug;

async fn wait_for_rpc(url: &str) -> eyre::Result<HttpClient> {
    let client = HttpClientBuilder::default().build(url)?;
    let max_attempts = 120;
    let mut last_err = None;
    for attempt in 0..max_attempts {
        match client.request::<String, _>("eth_blockNumber", rpc_params!()).await {
            Ok(_) => return Ok(client),
            Err(e) => {
                if attempt % 20 == 0 {
                    eprintln!("[wait_for_rpc] attempt {attempt}/{max_attempts} for {url}: {e}");
                }
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    eyre::bail!(
        "RPC at {url} not available after {max_attempts} attempts ({}s). Last error: {}",
        max_attempts / 2,
        last_err.map(|e| e.to_string()).unwrap_or_default()
    )
}

#[tokio::test]
async fn test_precompile_genesis_accounts() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    //
    // TODO: Issue #584: LZ adapter + safe
    //

    // fetch precompile accounts (no ITS args needed now)
    let precompile_accounts = NetworkGenesis::fetch_precompile_genesis_accounts()?;

    // parse expected directly from the YAML
    let expected: HashMap<Address, GenesisAccount> =
        serde_yaml::from_str(GENESIS_ACCOUNT_STATE_YAML).expect("yaml parsing failure");

    // verify count matches (currently 9 precompile accounts)
    assert_eq!(
        precompile_accounts.len(),
        expected.len(),
        "precompile account count mismatch: got {} expected {}",
        precompile_accounts.len(),
        expected.len()
    );

    // verify each expected account is present with correct fields
    for (address, account) in &precompile_accounts {
        let expected_account = expected
            .get(address)
            .unwrap_or_else(|| panic!("missing expected precompile address: {address}"));

        assert_eq!(account.nonce, expected_account.nonce, "nonce mismatch for {address}");
        assert_eq!(account.balance, expected_account.balance, "balance mismatch for {address}");
        assert_eq!(account.code, expected_account.code, "code mismatch for {address}");
        assert_eq!(account.storage, expected_account.storage, "storage mismatch for {address}");
    }

    Ok(())
}

#[tokio::test]
async fn test_genesis_with_consensus_registry_accounts() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // fetch registry, blsg1, and issuance bytecodes
    let registry_runtimecode_binding = RethEnv::fetch_value_from_json_str(
        CONSENSUS_REGISTRY_JSON,
        Some("deployedBytecode.object"),
    )?;
    let unlinked_runtimecode =
        registry_runtimecode_binding.as_str().ok_or_eyre("Couldn't fetch bytecode")?;
    let tao_address_binding = RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, Some("Safe"))?;
    let tao_address =
        Address::from_hex(tao_address_binding.as_str().ok_or_eyre("Safe owner address")?)?;
    let blsg1_address = tao_address.create(0).to_string();
    let registry_deployed_bytecode =
        RethEnv::link_solidity_library(unlinked_runtimecode, &blsg1_address)?;

    let blsg1_runtimecode_binding =
        RethEnv::fetch_value_from_json_str(BLSG1_JSON, Some("deployedBytecode.object"))?;
    let blsg1_deployed_bytecode =
        blsg1_runtimecode_binding.as_str().ok_or_eyre("invalid blsg1 json")?;

    let issuance_json_val =
        RethEnv::fetch_value_from_json_str(ISSUANCE_JSON, Some("deployedBytecode.object"))?;
    let issuance_deployed_bytecode =
        issuance_json_val.as_str().ok_or_eyre("fetch issuance runtime code")?;

    // spawn testnet for RPC calls
    let temp_path = tempfile::TempDir::with_suffix("genesis_reg").expect("tempdir is okay");
    let endpoints = spawn_local_testnet(temp_path.path(), None)?;
    let rpc_url = endpoints[0].http_url.clone();
    let client = wait_for_rpc(&rpc_url).await?;

    // verify all three transports (HTTP, WS, IPC) are reachable
    verify_all_transports(&endpoints[0]).await?;

    // sanity check onchain spawned both registry & issuance in genesis
    let returned_registry_bytecode: String = client
        .request("eth_getCode", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
        .await
        .expect("Failed to fetch registry bytecode");
    let returned_issuance_bytecode: String = client
        .request("eth_getCode", rpc_params!(ISSUANCE_ADDRESS))
        .await
        .expect("Failed to fetch issuance bytecode");
    let returned_blsg1_bytecode: String = client
        .request("eth_getCode", rpc_params!(blsg1_address))
        .await
        .expect("Failed to fetch BLS G1 bytecode");
    assert_eq!(
        Bytes::from_hex(&returned_registry_bytecode)?,
        Bytes::from(registry_deployed_bytecode)
    );
    assert_eq!(
        Bytes::from_hex(&returned_issuance_bytecode)?,
        Bytes::from_hex(issuance_deployed_bytecode)?
    );
    assert_eq!(
        Bytes::from_hex(&returned_blsg1_bytecode)?,
        Bytes::from_hex(blsg1_deployed_bytecode)?
    );

    // verify all precompile-config.yaml accounts are present in genesis on-chain
    let precompile_accounts =
        NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompile fetch error");
    for (address, expected_account) in &precompile_accounts {
        let addr_str = format!("{address:#x}");
        // check code
        if expected_account.code.is_some() {
            let code: String = client
                .request("eth_getCode", rpc_params!(addr_str.clone(), "latest"))
                .await
                .unwrap_or_else(|e| panic!("eth_getCode failed for {addr_str}: {e}"));
            assert!(
                code != "0x" && code != "0x0",
                "expected code at precompile address {addr_str} but got empty"
            );
        }
        // check balance
        if expected_account.balance != tn_types::U256::ZERO {
            let bal_hex: String = client
                .request("eth_getBalance", rpc_params!(addr_str.clone(), "latest"))
                .await
                .unwrap_or_else(|e| panic!("eth_getBalance failed for {addr_str}: {e}"));
            let bal = tn_types::U256::from_str_radix(bal_hex.trim_start_matches("0x"), 16)
                .expect("parse balance hex");
            assert_eq!(bal, expected_account.balance, "balance mismatch for precompile {addr_str}");
        }
        // check nonce
        if expected_account.nonce.is_some() {
            let nonce_hex: String = client
                .request("eth_getTransactionCount", rpc_params!(addr_str.clone(), "latest"))
                .await
                .unwrap_or_else(|e| panic!("eth_getTransactionCount failed for {addr_str}: {e}"));
            let nonce = u64::from_str_radix(nonce_hex.trim_start_matches("0x"), 16)
                .expect("parse nonce hex");
            assert_eq!(
                Some(nonce),
                expected_account.nonce,
                "nonce mismatch for precompile {addr_str}"
            );
        }
    }

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

    debug!(target: "genesis-test", "consensus_registry: {:#?}", current_epoch_info);
    let ConsensusRegistry::EpochInfo {
        committee,
        epochIssuance,
        blockHeight,
        epochId: _,
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
    debug!(target: "genesis-test", "active validators??\n{:?}", validators);

    Ok(())
}

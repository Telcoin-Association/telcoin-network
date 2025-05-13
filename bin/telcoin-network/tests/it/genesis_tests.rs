//! Tests with RPC calls for ConsensusRegistry in genesis (through CLI).
//!
//! NOTE: this test contains code for executing a proxy/impl pre-genesis
//! however, the RPC calls don't work. The beginning of the test is left
//! because the proxy version may be re-prioritized later.
use crate::util::spawn_local_testnet;
use alloy::{
    network::EthereumWallet,
    primitives::utils::parse_ether,
    providers::ProviderBuilder,
    sol_types::{SolCall, SolConstructor},
};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_config::{fetch_file_content_relative_to_manifest, NetworkGenesis, DEPLOYMENTS_JSON};
use tn_reth::{
    system_calls::{
        ConsensusRegistry::{self, getCurrentEpochInfoReturn, getValidatorsReturn},
        CONSENSUS_REGISTRY_ADDRESS,
    },
    CallRequest, CreateRequest, RethChainSpec, RethEnv,
};
use tn_test_utils::TransactionFactory;
use tn_types::{
    adiri_genesis, hex, sol, Address, BlsKeypair, Bytes, FromHex, Genesis, GenesisAccount,
    TaskManager, U256,
};
use tracing::debug;

#[tokio::test]
async fn test_genesis_with_its() -> eyre::Result<()> {
    // create genesis with a proxy
    let genesis = adiri_genesis();

    // spawn testnet for RPC calls
    spawn_local_testnet(
        genesis,
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
    )
    .expect("failed to spawn testnet");
    // allow time for nodes to start
    tokio::time::sleep(Duration::from_secs(10)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    let itel_address =
        NetworkGenesis::fetch_from_json_str(DEPLOYMENTS_JSON, Some("its.InterchainTEL"))?
            .as_str()
            .map(|hex_str| Address::from_hex(hex_str).unwrap())
            .unwrap();
    let tel_supply = U256::try_from(parse_ether("100_000_000_000").unwrap()).unwrap();
    // 4 million tel staked at genesis for 4 validators
    let itel_bal = tel_supply - U256::try_from(parse_ether("4_000_000").unwrap()).unwrap();

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
                .expect("Failed to fetch RWTEL balance");
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
    // check that all addresses in expected_deployments are present in precompiles
    let is_address_present = |address: &str, genesis_config: Vec<(Address, GenesisAccount)>| {
        genesis_config
            .iter()
            .any(|(precompile_address, _)| precompile_address.to_string() == address)
    };
    let expected_deployments = NetworkGenesis::fetch_from_json_str(DEPLOYMENTS_JSON, None)?;

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
                        assert!(
                            genesis_account.balance == some_bal,
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
    // fetch registry impl bytecode from compiled output in tn-contracts
    let registry_impl_json = fetch_file_content_relative_to_manifest(
        "../../tn-contracts/artifacts/ConsensusRegistry.json",
    );
    let registry_impl_deployed_bytecode =
        RethEnv::parse_deployed_bytecode_from_json_str(&registry_impl_json)?;

    // create genesis with a proxy
    let genesis = genesis_with_proxy(registry_impl_deployed_bytecode.clone())?;

    // spawn testnet for RPC calls
    spawn_local_testnet(
        genesis,
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
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
    assert_eq!(returned_impl_code[2..], hex::encode(registry_impl_deployed_bytecode));

    let tx_factory = TransactionFactory::default();
    let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet)
        .on_http(rpc_url.parse().expect("rpc url parse error"));

    // test rpc calls for registry in genesis - this is not the one deployed for the test
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());
    let getCurrentEpochInfoReturn { currentEpochInfo } =
        consensus_registry.getCurrentEpochInfo().call().await.expect("get current epoch result");

    debug!(target: "bundle", "consensus_registry: {:#?}", currentEpochInfo);
    let ConsensusRegistry::EpochInfo { committee, blockHeight, epochDuration } = currentEpochInfo;
    assert_eq!(blockHeight, 0);
    assert_eq!(epochDuration, 86400);

    let getValidatorsReturn { _0: validators } = consensus_registry
        .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
        .call()
        .await
        .expect("failed active validators read");

    let validator_addresses: Vec<_> = validators.iter().map(|v| v.validatorAddress).collect();
    assert_eq!(committee, validator_addresses);
    debug!(target: "bundle", "active validators??\n{:?}", validators);

    // NOTE: test proxy failed active validators read - AbiError(SolTypes(Overrun))
    // this is expected to be important soon
    //
    // let consensus_proxy = ConsensusRegistry::new(registry_proxy_address, provider.clone());
    // let getValidatorsReturn { _0: validators } = consensus_proxy
    //     .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
    //     .call()
    //     .await
    //     .expect("failed active validators read");
    // debug!(target: "bundle", "proxy\n{:?}", validators);

    Ok(())
}

/// This executes a proxy/impl for inclusion in genesis.
/// This is not currently used in the test, but is expected to become useful soon.
fn genesis_with_proxy(registry_impl_deployed_bytecode: Vec<u8>) -> eyre::Result<Genesis> {
    // ERC1967Proxy interface
    sol!(
        // #[sol(rpc)]
        contract ERC1967Proxy {
            constructor(address implementation, bytes memory _data);
        }
    );

    // the consensus registry impl clone for test
    let test_cr_address: Address = Address::random();

    // create proxy transaction
    let registry_proxy_json =
        fetch_file_content_relative_to_manifest("../../tn-contracts/artifacts/ERC1967Proxy.json");
    let registry_proxy_bytecode = RethEnv::parse_bytecode_from_json_str(&registry_proxy_json)?;
    let registry_proxy_deployed_bytecode =
        RethEnv::parse_deployed_bytecode_from_json_str(&registry_proxy_json)?;

    let constructor_args =
        ERC1967Proxy::constructorCall { implementation: test_cr_address, _data: Bytes::new() }
            .abi_encode();

    let mut create_proxy = registry_proxy_bytecode.clone();
    create_proxy.extend(constructor_args);

    // construct array of 4 validators with 1-indexed `validatorIndex`
    let active_status = ConsensusRegistry::ValidatorStatus::Active;
    let initial_validators: Vec<ConsensusRegistry::ValidatorInfo> = (1..=4)
        .map(|i| {
            // generate deterministic values
            let byte = i * 52;
            let mut rng = ChaCha8Rng::seed_from_u64(byte as u64);
            let bls_keypair = BlsKeypair::generate(&mut rng);
            let bls_pubkey = bls_keypair.public().to_bytes();
            let addr = Address::from_slice(&[byte; 20]);

            ConsensusRegistry::ValidatorInfo {
                blsPubkey: bls_pubkey.into(),
                validatorAddress: addr,
                activationEpoch: 0,
                exitEpoch: 0,
                currentStatus: active_status,
                isRetired: false,
                isDelegated: false,
                stakeVersion: 0,
            }
        })
        .collect();

    let epoch_duration = 60 * 60 * 24; // 24-hours
    let stake_amount = U256::try_from(parse_ether("1_000_000").unwrap()).unwrap();
    let initial_stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: stake_amount,
        minWithdrawAmount: U256::try_from(parse_ether("1_000").unwrap()).unwrap(),
        epochIssuance: U256::try_from(parse_ether("20_000_000").unwrap())
            .unwrap()
            .checked_div(U256::from(28))
            .expect("u256 div checked"),
        epochDuration: epoch_duration,
    };

    // generate calldata to initialize proxy
    let owner = Address::random();

    // create proxy for test
    // create temporary reth env for execution
    let task_manager = TaskManager::new("Test Task Manager");
    let tmp_dir = TempDir::new().unwrap();

    // deploy bytecode to execute constructor/init calls
    let registry_proxy_address = owner.create(0);
    debug!(target: "bundle", "expected proxy address: {:?}", registry_proxy_address);

    let tmp_genesis = adiri_genesis().extend_accounts([(
        test_cr_address,
        GenesisAccount::default().with_code(Some(registry_impl_deployed_bytecode.clone().into())),
    )]);

    let tmp_chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
    let reth_env =
        RethEnv::new_for_test_with_chain(tmp_chain.clone(), tmp_dir.path(), &task_manager)?;

    // now init the registry impl
    let init_data = ConsensusRegistry::initializeCall {
        iTEL_: Address::random(),
        genesisConfig_: initial_stake_config,
        initialValidators_: initial_validators.clone(),
        owner_: Address::random(),
    };

    let init_calldata: Bytes = init_data.abi_encode().into();
    let txs = vec![
        // constructor
        CreateRequest::new(owner, create_proxy.into()).into(),
        // init
        CallRequest::new(registry_proxy_address, Address::random(), init_calldata).into(),
    ];

    let bundle = reth_env
        .execute_call_tx_for_test_bypass_evm_checks(&tmp_chain.sealed_genesis_header(), txs)?;
    let proxy_storage = bundle.state.get(&registry_proxy_address).map(|account| {
        account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
    });

    // assert storage is set
    assert!(proxy_storage.as_ref().map(|tree: &BTreeMap<_, _>| !tree.is_empty()).unwrap());

    // perform canonical adiri chain genesis with fetched storage
    let genesis_accounts = [
        (
            test_cr_address,
            GenesisAccount::default()
                .with_code(Some(registry_impl_deployed_bytecode.clone().into())),
        ),
        (
            registry_proxy_address,
            GenesisAccount::default()
                .with_code(Some(registry_proxy_deployed_bytecode.clone().into()))
                .with_balance(
                    U256::from(4)
                        .checked_mul(stake_amount)
                        .expect("U256 checked mul for total stake"),
                )
                .with_storage(proxy_storage),
        ),
    ];

    let real_genesis = adiri_genesis();
    let genesis = real_genesis.extend_accounts(genesis_accounts);
    Ok(genesis)
}

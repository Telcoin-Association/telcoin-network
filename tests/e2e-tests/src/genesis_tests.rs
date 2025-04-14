//! Tests for setting up genesis.

use crate::util::spawn_local_testnet;
use alloy::{network::EthereumWallet, primitives::Uint, providers::ProviderBuilder};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use rand::{rngs::StdRng, SeedableRng};
use reth_chainspec::ChainSpec;
use std::{sync::Arc, time::Duration};
use tn_config::{test_fetch_file_content_relative_to_manifest, ContractStandardJson};
use tn_test_utils::{get_contract_state_for_genesis, TransactionFactory};
use tn_types::{
    adiri_genesis, hex, sol, Address, BlsKeypair, Bytes, GenesisAccount, NetworkKeypair, SolValue,
    U256,
};

#[tokio::test]
async fn test_genesis_with_consensus_registry() {
    let network_genesis = adiri_genesis();
    let tmp_chain: Arc<ChainSpec> = Arc::new(network_genesis.into());

    // fetch registry impl bytecode from compiled output in tn-contracts
    let registry_standard_json = test_fetch_file_content_relative_to_manifest(
        "../../tn-contracts/artifacts/ConsensusRegistry.json".into(),
    );
    let registry_contract: ContractStandardJson =
        serde_json::from_str(&registry_standard_json).expect("json parsing failure");
    let registry_impl_bytecode = hex::decode(registry_contract.deployed_bytecode.object)
        .expect("invalid bytecode hexstring");
    let registry_impl_address = Address::random();
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();

    // deploy impl and fund `factory_address`
    let tmp_genesis = tmp_chain.genesis.clone().extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                registry_impl_address,
                GenesisAccount::default().with_code(Some(registry_impl_bytecode.clone().into())),
            ),
        ]
        .into_iter(),
    );

    // ERC1967Proxy interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract ERC1967Proxy {
            constructor(address implementation, bytes memory _data);
        }
    );

    // fetch and construct registry proxy deployment transaction
    let registry_proxy_json = test_fetch_file_content_relative_to_manifest(
        "../../tn-contracts/artifacts/ERC1967Proxy.json".into(),
    );
    let registry_proxy_contract: ContractStandardJson =
        serde_json::from_str(&registry_proxy_json).expect("json parsing failure");
    let registry_proxy_initcode =
        hex::decode(registry_proxy_contract.bytecode.object).expect("invalid bytecode hexstring");
    let constructor_params = (registry_impl_address, Bytes::default()).abi_encode_params();
    let registry_create_data =
        [registry_proxy_initcode.as_slice(), &constructor_params[..]].concat();

    // ConsensusRegistry interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract ConsensusRegistry {
            enum ValidatorStatus {
                Undefined,
                PendingActivation,
                Active,
                PendingExit,
                Exited
            }
            struct ValidatorInfo {
                bytes blsPubkey;
                bytes32 ed25519Pubkey;
                address ecdsaPubkey;
                uint32 activationEpoch;
                uint32 exitEpoch;
                uint24 validatorIndex;
                ValidatorStatus currentStatus;
            }
            struct EpochInfo {
                address[] committee;
                uint64 blockHeight;
            }
            function initialize(
                address rwTEL_,
                uint256 stakeAmount_,
                uint256 minWithdrawAmount_,
                ValidatorInfo[] memory initialValidators_,
                address owner_
            );
            function getValidators(uint8 status) public view returns (ValidatorInfo[] memory);
            function getEpochInfo(uint32 epoch) public view returns (EpochInfo memory epochInfo);
        }
    );

    let registry_init_selector = [97, 175, 158, 105];
    let activation_epoch = u32::default();
    let exit_epoch = u32::default();
    let active_status = ConsensusRegistry::ValidatorStatus::Active;

    // construct array of 4 validators with 1-indexed `validatorIndex`
    let initial_validators: Vec<ConsensusRegistry::ValidatorInfo> = (1..=4)
        .map(|i| {
            // generate random bls, ed25519, and ecdsa keys for each validator
            let mut rng = StdRng::from_entropy();
            let bls_keypair = BlsKeypair::generate(&mut rng);
            let bls_pubkey = bls_keypair.public().to_bytes().to_vec();
            let ed_25519_keypair = NetworkKeypair::generate_ed25519();
            let ecdsa_pubkey = Address::random();

            ConsensusRegistry::ValidatorInfo {
                blsPubkey: bls_pubkey.clone().into(),
                ed25519Pubkey: ed_25519_keypair
                    .public()
                    .try_into_ed25519()
                    .expect("is an ed_25519")
                    .to_bytes()
                    .into(),
                ecdsaPubkey: ecdsa_pubkey,
                activationEpoch: activation_epoch,
                exitEpoch: exit_epoch,
                validatorIndex: Uint::<24, 1>::from(i),
                currentStatus: active_status,
            }
        })
        .collect();

    let registry_init_params = (
        Address::random(),
        U256::from(1_000_000e18),
        U256::from(10_000e18),
        initial_validators.clone(),
        Address::random(),
    )
        .abi_encode_params();
    let init_call = [&registry_init_selector, &registry_init_params[..]].concat();

    // construct proxy deployment and initialize txs
    let gas_price = 7;
    let gas_limit = 3_000_000;
    let pre_genesis_chain: Arc<ChainSpec> = Arc::new(tmp_genesis.into());
    let registry_tx_raw = tx_factory.create_eip1559_encoded(
        tmp_chain.clone(),
        Some(gas_limit),
        gas_price,
        None,
        U256::ZERO,
        registry_create_data.clone().into(),
    );
    // registry deployment will be `factory_address`'s first tx
    let registry_proxy_address = factory_address.create(0);
    let initialize_tx_raw = tx_factory.create_eip1559_encoded(
        tmp_chain.clone(),
        Some(gas_limit),
        gas_price,
        Some(registry_proxy_address),
        U256::ZERO,
        init_call.clone().into(),
    );
    let raw_txs = vec![registry_tx_raw.clone(), initialize_tx_raw];

    // fetch storage changes from pre-genesis for actual genesis
    let execution_outcome = get_contract_state_for_genesis(pre_genesis_chain.clone(), raw_txs)
        .await
        .expect("unable to fetch contract state");
    let execution_bundle = execution_outcome.bundle;
    let execution_storage_registry = &execution_bundle
        .state
        .get(&registry_proxy_address)
        .expect("registry address missing from bundle state")
        .storage;
    let proxy_json = test_fetch_file_content_relative_to_manifest(
        "../../tn-contracts/artifacts/ERC1967Proxy.json".into(),
    );
    let proxy_contract: ContractStandardJson =
        serde_json::from_str(&proxy_json).expect("json parsing failure");
    let proxy_bytecode =
        hex::decode(proxy_contract.deployed_bytecode.object).expect("invalid bytecode hexstring");

    // perform canonical adiri chain genesis with fetched storage
    let genesis_accounts = vec![
        (
            registry_impl_address,
            GenesisAccount::default().with_code(Some(registry_impl_bytecode.clone().into())),
        ),
        (
            registry_proxy_address,
            GenesisAccount::default().with_code(Some(proxy_bytecode.into())).with_storage(Some(
                execution_storage_registry
                    .iter()
                    .map(|(k, v)| ((*k).into(), v.present_value.into()))
                    .collect(),
            )),
        ),
    ];
    let real_genesis = adiri_genesis();
    let genesis = real_genesis.extend_accounts(genesis_accounts.into_iter());
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    spawn_local_testnet(chain, "0x0000000000000000000000000000000000000000")
        .expect("failed to spawn testnet");
    // allow time for nodes to start
    tokio::time::sleep(Duration::from_secs(15)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    // sanity check onchain reads
    let returned_impl_code: String = client
        .request("eth_getCode", rpc_params!(registry_impl_address))
        .await
        .expect("Failed to fetch registry impl bytecode");
    // trim `0x` prefix
    assert_eq!(returned_impl_code[2..], hex::encode(registry_impl_bytecode));

    let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .with_recommended_fillers()
        .wallet(wallet)
        .on_http(rpc_url.parse().expect("rpc url parse error"));
    let consensus_registry = ConsensusRegistry::new(registry_proxy_address, provider.clone());

    let active_validators =
        consensus_registry.getValidators(2).call().await.expect("failed active validators read");
    assert_eq!(active_validators._0.abi_encode(), initial_validators.abi_encode());

    // assert committees for first 3 epochs comprise all genesis validators
    for i in 0..3 {
        let epoch_info =
            consensus_registry.getEpochInfo(i).call().await.expect("failed epoch read").epochInfo;
        for (j, _) in initial_validators.iter().enumerate() {
            assert_eq!(epoch_info.committee[j], initial_validators[j].ecdsaPubkey);
        }
    }
}

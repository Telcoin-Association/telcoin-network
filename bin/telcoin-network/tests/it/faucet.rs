//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

use crate::util::{create_validator_info, IT_TEST_MUTEX};
use alloy::{network::EthereumWallet, providers::ProviderBuilder, sol, sol_types::SolValue};
use clap::Parser;
use gcloud_sdk::{
    google::cloud::kms::v1::{
        key_management_service_client::KeyManagementServiceClient, GetPublicKeyRequest,
    },
    GoogleApi, GoogleAuthMiddleware, GoogleEnvironment,
};
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use k256::{elliptic_curve::sec1::ToEncodedPoint, pkcs8::DecodePublicKey, PublicKey as PubKey};
use narwhal_test_utils::{default_test_execution_node, CommandParser};
use reth::{
    providers::ExecutionOutcome,
    tasks::{TaskExecutor, TaskManager},
    CliContext,
};
use reth_chainspec::ChainSpec;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{public_key_to_address, Address, GenesisAccount, SealedHeader, B256, U256};
use reth_tracing::init_test_tracing;
use secp256k1::PublicKey;
use std::{str::FromStr, sync::Arc, time::Duration};
use telcoin_network::{genesis::GenesisArgs, node::NodeCommand};
use tn_faucet::FaucetArgs;
use tn_node::launch_node;
use tn_types::{
    adiri_genesis,
    test_utils::{
        contract_artifacts::{
            ERC1967PROXY_INITCODE, ERC1967PROXY_RUNTIMECODE, STABLECOINMANAGER_RUNTIMECODE,
            STABLECOIN_RUNTIMECODE,
        },
        execution_outcome_for_tests, TransactionFactory,
    },
    TransactionSigned, WorkerBlock,
};
use tokio::{runtime::Handle, task::JoinHandle, time::timeout};
use tracing::{debug, error, info};

#[tokio::test]
async fn test_faucet_transfers_tel_and_xyz_with_google_kms_e2e() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();

    // create google env and temp chain spec for state initialization
    let (tmp_chain, kms_address) = prepare_google_kms_env().await?;

    // faucet interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract StablecoinManager {
            struct StablecoinManagerInitParams {
                address admin_;
                address maintainer_;
                address[] tokens_;
                uint256 initMaxLimit;
                uint256 initMinLimit;
                address[] authorizedFaucets_;
                uint256 dripAmount_;
                uint256 nativeDripAmount_;
            }

            function initialize(StablecoinManagerInitParams calldata initParams) external;
            function grantRole(bytes32 role, address account) external;
        }
    );

    // stablecoin interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract Stablecoin {
            function initialize(
                string memory name_,
                string memory symbol_,
                uint8 decimals_
            ) external;
            function decimals() external view returns (uint8);
            function balanceOf(address account) external view returns (uint256);
            function mint(uint256 value) external;
            function mintTo(
                address account,
                uint256 value
            ) external;
            function burn(uint256 value) external;
            function burnFrom(
                address account,
                uint256 value
            ) external;
        }
    );

    // extend genesis accounts to fund factory_address, etch bytecodes, and construct proxy creation
    // txs
    let faucet_impl_address = Address::random();
    let stablecoin_impl_address = Address::random();
    let faucet_bytecode = *STABLECOINMANAGER_RUNTIMECODE;
    let stablecoin_impl_bytecode = STABLECOIN_RUNTIMECODE.as_slice();
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();
    let tmp_genesis = tmp_chain.genesis.clone().extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                faucet_impl_address,
                GenesisAccount::default().with_code(Some(faucet_bytecode.into())),
            ),
            (
                stablecoin_impl_address,
                GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.into())),
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

    // get data for faucet proxy deployment w/ initdata
    let faucet_init_selector = [22, 173, 166, 177];
    let deployed_token_bytes = vec![];
    let init_max_limit = U256::MAX;
    let init_min_limit = U256::from(1_000);
    let kms_faucets = vec![kms_address];
    let xyz_amount = U256::from(10).checked_pow(U256::from(6)).expect("1e6 doesn't overflow U256"); // 1 $XYZ
    let tel_amount =
        U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256"); // 1 $TEL

    // encode initialization struct (prevents stack too deep)
    let init_params = StablecoinManager::StablecoinManagerInitParams {
        admin_: factory_address,
        maintainer_: factory_address,
        tokens_: deployed_token_bytes,
        initMaxLimit: init_max_limit,
        initMinLimit: init_min_limit,
        authorizedFaucets_: kms_faucets,
        dripAmount_: xyz_amount,
        nativeDripAmount_: tel_amount,
    }
    .abi_encode();

    // construct create data for faucet proxy address
    let init_call = [&faucet_init_selector, &init_params[..]].concat();
    let constructor_params = (faucet_impl_address, init_call.clone()).abi_encode_params();
    let faucet_init_code = ERC1967PROXY_INITCODE.as_slice();
    let faucet_create_data = [faucet_init_code, &constructor_params[..]].concat();

    // construct `grantRole(faucet)` data
    let grant_role_selector = [47, 47, 241, 93];
    let grant_role_params = (
        B256::from_str("0xaecf5761d3ba769b4631978eb26cb84eae66bcaca9c3f0f4ecde3feb2f4cf144")?,
        kms_address,
    )
        .abi_encode_params();

    let grant_role_call = [&grant_role_selector, &grant_role_params[..]].concat().into();

    // construct create data for stablecoin proxy
    let stablecoin_init_selector = [22, 36, 246, 198];
    let stablecoin_init_params = ("name", "symbol", 6).abi_encode_params();
    let stablecoin_init_call = [&stablecoin_init_selector, &stablecoin_init_params[..]].concat();
    let stablecoin_constructor_params =
        (stablecoin_impl_address, stablecoin_init_call.clone()).abi_encode_params();
    let stablecoin_create_data =
        [ERC1967PROXY_INITCODE.as_slice(), &stablecoin_constructor_params[..]].concat();

    // faucet deployment will be `factory_address`'s first tx, stablecoin will be second tx
    let faucet_proxy_address = factory_address.create(0);
    let stablecoin_address = factory_address.create(1);

    // construct `updateXYZ()` data
    let updatexyz_selector = [233, 174, 163, 150];
    let updatexyz_params = (stablecoin_address, true, U256::MAX, U256::ZERO).abi_encode_params();
    let updatexyz_call = [&updatexyz_selector, &updatexyz_params[..]].concat().into();

    // construct `grantRole(minter_role)` data
    let minter_role_params = (
        B256::from_str("0x9f2df0fed2c77648de5860a4cc508cd0818c85b8b8a1ab4ceeef8d981c8956a6")?,
        faucet_proxy_address,
    )
        .abi_encode_params();
    let minter_role_call = [&grant_role_selector, &minter_role_params[..]].concat().into();

    // assemble eip1559 transactions using constructed datas
    let pre_genesis_chain: Arc<ChainSpec> = Arc::new(tmp_genesis.into());
    let gas_price = 100;
    let faucet_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        None,
        U256::ZERO,
        faucet_create_data.clone().into(),
    );

    let stablecoin_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        None,
        U256::ZERO,
        stablecoin_create_data.clone().into(),
    );

    let role_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        grant_role_call,
    );

    let updatexyz_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        updatexyz_call,
    );

    let minter_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(stablecoin_address),
        U256::ZERO,
        minter_role_call,
    );

    let raw_txs =
        vec![faucet_tx_raw, stablecoin_tx_raw, role_tx_raw, updatexyz_tx_raw, minter_tx_raw];

    // fetch state to be set on the faucet proxy address
    let execution_outcome = get_contract_state_for_genesis(pre_genesis_chain, raw_txs).await?;
    let execution_bundle = execution_outcome.bundle;
    let execution_storage_faucet = &execution_bundle
        .state
        .get(&faucet_proxy_address)
        .expect("faucet address missing from bundle state")
        .storage;
    // fetch state to be set on the stablecoin address
    let execution_storage_stablecoin = &execution_bundle
        .state
        .get(&stablecoin_address)
        .expect("stablecoin address missing from bundle state")
        .storage;

    let faucet_proxy_bytecode = *ERC1967PROXY_RUNTIMECODE;

    // real genesis: configure genesis accounts for proxy deployment & faucet_role
    let genesis_accounts = vec![
        (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        (kms_address, GenesisAccount::default().with_balance(U256::MAX)),
        (
            stablecoin_impl_address,
            GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.into())),
        ),
        (
            stablecoin_address,
            GenesisAccount::default()
                .with_code(Some(ERC1967PROXY_RUNTIMECODE.into()))
                .with_storage(Some(
                    execution_storage_stablecoin
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                )),
        ),
        (faucet_impl_address, GenesisAccount::default().with_code(Some(faucet_bytecode.into()))),
        // convert U256 HashMap to B256 for BTreeMap
        (
            faucet_proxy_address,
            GenesisAccount::default()
                .with_code(Some(faucet_proxy_bytecode.into()))
                .with_balance(U256::MAX)
                .with_storage(Some(
                    execution_storage_faucet
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                )),
        ),
    ];

    // start canonical adiri chain with fetched storage
    let real_genesis = adiri_genesis();
    let genesis = real_genesis.extend_accounts(genesis_accounts.into_iter());
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // task manager
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    // create and launch validator nodes on local network,
    // use expected faucet contract address from `TransactionFactory::default` with nonce == 0
    spawn_local_testnet(&task_executor, chain.clone(), &faucet_proxy_address.to_string()).await?;

    info!(target: "faucet-test", "nodes started - sleeping for 10s...");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url)?;

    // assert deployer starting balance is properly seeded
    let tx_factory = TransactionFactory::new();
    let default_deployer_address = tx_factory.address();
    let deployer_balance: String =
        client.request("eth_getBalance", rpc_params!(default_deployer_address)).await?;
    debug!(target: "faucet-test", "Deployer starting balance: {deployer_balance:?}");
    assert_eq!(U256::from_str(&deployer_balance)?, U256::MAX);

    // note: response is different each time bc KMS
    //
    // assert starting balance is 0
    let mut random_tx_factory = TransactionFactory::new_random();
    let random_address = random_tx_factory.address();
    let starting_tel_balance: String =
        client.request("eth_getBalance", rpc_params!(random_address)).await?;
    debug!(target: "faucet-test", "starting balance: {starting_tel_balance:?}");
    assert_eq!(U256::from_str(&starting_tel_balance)?, U256::ZERO);

    let tel_tx_hash: String =
        client.request("faucet_transfer", rpc_params![random_address]).await?;
    info!(target: "faucet-test", ?tel_tx_hash, "valid faucet transfer tx hash");

    // more than enough time for the nodes to launch RPCs
    let duration = Duration::from_secs(30);

    // ensure account balance increased
    let expected_tel_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_tel_balance),
    )
    .await?
    .expect("expected balance timeout");

    // duplicate request is err
    assert!(client
        .request::<String, _>("faucet_transfer", rpc_params![random_address])
        .await
        .is_err());

    // NOW:
    // submit another tx from the account that just got dripped
    // so the the worker's watch channel updates to a new block that doesn't have
    // the faucet's address in the state
    //
    // this creates scenario for faucet to rely on provider.latest() for accuracy
    let tx = random_tx_factory.create_eip1559(
        chain,
        1_000_000_000,
        Some(Address::random()),
        U256::from_str("0xaffffffffffffff").expect("U256 from str for tx factory"),
        Default::default(),
    );

    info!(target: "faucet-test", ?tx, "submitting new tx to clear worker's watch channel...");

    // submit tx through rpc
    let tx_bytes = tx.envelope_encoded();
    let tx_hash: String = client.request("eth_sendRawTransaction", rpc_params![tx_bytes]).await?;
    info!(target: "faucet-test", ?tx_hash, "tx submitted :D");

    // ensure account balance decreased
    let expected_balance = U256::from_str("0x2e0b6b3a761c1c9")?;
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_balance),
    )
    .await?
    .expect("expected balance timeout");

    // request another faucet drip for random address
    //
    // assert starting balance is 0
    info!(target: "faucet-test", ?expected_balance, "account balance decreased. requesting from faucet again...");
    let new_random_address = Address::random();
    let starting_balance: String =
        client.request("eth_getBalance", rpc_params!(new_random_address)).await?;
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    let tx_hash: String =
        client.request("faucet_transfer", rpc_params![new_random_address]).await?;
    info!(target: "faucet-test", ?tx_hash, "new random faucet request success. waiting for balance increase...");

    // ensure account balance increased
    //
    // account balance is only updates on final execution
    // finding the expected balance in time means the faucet successfully used the correct nonce
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, new_random_address, expected_tel_balance),
    )
    .await?
    .expect("expected balance random account timeout");

    // duplicate request is err
    info!(target: "faucet-test", "account balance updated. submitting duplicate request and shutting down...");
    assert!(client
        .request::<String, _>("faucet_transfer", rpc_params![new_random_address])
        .await
        .is_err());

    // assert starting stablecoin balance is 0
    let signer = random_tx_factory.get_default_signer()?;
    let wallet = EthereumWallet::from(signer);
    let provider =
        ProviderBuilder::new().with_recommended_fillers().wallet(wallet).on_http(rpc_url.parse()?);
    let stablecoin_contract = Stablecoin::new(stablecoin_address, provider.clone());
    let starting_xyz_balance: U256 =
        U256::from(stablecoin_contract.balanceOf(new_random_address).call().await?._0);
    debug!(target: "faucet-test", "starting balance: {starting_xyz_balance:?}");
    assert_eq!(starting_xyz_balance, U256::ZERO);

    // drip XYZ to new_random_address
    let xyz_tx_hash: String = client
        .request("faucet_transfer", rpc_params![new_random_address, stablecoin_address])
        .await?;
    info!(target: "faucet-test", ?xyz_tx_hash, "valid faucet XYZ transfer tx hash");

    // ensure account balance increased
    let expected_xyz_balance = U256::from(1_000_000); // 1e6 (1 XYZ)

    let result = timeout(duration, async {
        loop {
            let actual_xyz_balance: U256 =
                stablecoin_contract.balanceOf(new_random_address).call().await?._0;
            debug!(target: "faucet-test", "actual balance: {:?}", actual_xyz_balance);

            if actual_xyz_balance == expected_xyz_balance {
                return Ok::<_, eyre::Report>(actual_xyz_balance);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    match result {
        Ok(Ok(balance)) => {
            info!(target: "faucet-test", "Balance check completed successfully: {}", balance);
        }
        Ok(Err(e)) => {
            panic!("Error while checking balance: {:?}", e);
        }
        Err(_) => {
            panic!("Balance check timed out");
        }
    }

    // duplicate XYZ request is err
    info!(target: "faucet-test", "account balance updated. submitting duplicate request and shutting down...");
    assert!(client
        .request::<String, _>(
            "faucet_transfer",
            rpc_params![new_random_address, stablecoin_address]
        )
        .await
        .is_err());

    Ok(())
}

/// Retrieve the public key from KMS.
///
/// This simulates what the startup script should do on deployed nodes:
/// - set an env variable to the PEM formatted key.
async fn set_google_kms_public_key_env_var() -> eyre::Result<()> {
    // Detect Google project ID using environment variables PROJECT_ID/GCP_PROJECT_ID
    // or GKE metadata server when the app runs inside GKE
    let google_project_id = GoogleEnvironment::detect_google_project_id().await
        .expect("No Google Project ID detected. Please specify it explicitly using env variable: PROJECT_ID");

    let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
        GoogleApi::from_function(
            KeyManagementServiceClient::new,
            "https://cloudkms.googleapis.com",
            None,
        )
        .await?;

    // retrieve api information from env
    let locations = std::env::var("KMS_KEY_LOCATIONS")
        .expect("KMS_KEY_LOCATIONS must be set in the environment");
    let key_rings =
        std::env::var("KMS_KEY_RINGS").expect("KMS_KEY_RINGS must be set in the environment");
    let crypto_keys =
        std::env::var("KMS_CRYPTO_KEYS").expect("KMS_CRYPTO_KEYS must be set in the environment");
    let crypto_key_versions = std::env::var("KMS_CRYPTO_KEY_VERSIONS")
        .expect("KMS_CRYPTO_KEY_VERSIONS must be set in the environment");

    // construct api endpoint for Google KMS requests
    let name = format!(
        "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}/cryptoKeyVersions/{}",
        google_project_id, locations, key_rings, crypto_keys, crypto_key_versions
    );

    // request KMS public key
    let kms_pubkey_response =
        kms_client.get().get_public_key(GetPublicKeyRequest { name: name.clone() }).await?;

    // convert pem pubkey format
    let kms_pem_pubkey = kms_pubkey_response.into_inner().pem;
    // store to env
    std::env::set_var("FAUCET_PUBLIC_KEY", kms_pem_pubkey);

    Ok(())
}

/// Use Google KMS credentials json to fetch public key, seed account at genesis, and set env vars
/// for faucet signature requests.
async fn prepare_google_kms_env() -> eyre::Result<(Arc<ChainSpec>, Address)> {
    // set application credentials for accessing Google KMS API
    std::env::set_var(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "../../crates/execution/faucet/gcloud-credentials.json",
    );
    // set Project ID for google_sdk
    std::env::set_var("PROJECT_ID", "telcoin-network");
    // set env vars for faucet cli
    std::env::set_var("KMS_KEY_LOCATIONS", "global");
    std::env::set_var("KMS_KEY_RINGS", "tests");
    std::env::set_var("KMS_CRYPTO_KEYS", "key-for-unit-tests");
    std::env::set_var("KMS_CRYPTO_KEY_VERSIONS", "1");

    // fetch kms address from google and set env
    set_google_kms_public_key_env_var().await?;
    let kms_pem_pubkey = std::env::var("FAUCET_PUBLIC_KEY")?;
    // k256 public key to convert from pem
    let pubkey_from_pem = PubKey::from_public_key_pem(&kms_pem_pubkey)?;
    // secp256k1 public key from uncompressed k256 variation
    let public_key = PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())?;
    // calculate address from uncompressed public key
    let kms_address = public_key_to_address(public_key);

    // create genesis and fund relevant accounts
    let genesis = adiri_genesis();
    let faucet_account = vec![(kms_address, GenesisAccount::default().with_balance(U256::MAX))];
    let default_deployer_address = TransactionFactory::default().address();
    let default_deployer_account =
        vec![(default_deployer_address, GenesisAccount::default().with_balance(U256::MAX))];

    let accounts_to_fund = faucet_account.into_iter().chain(default_deployer_account.into_iter());
    let genesis = genesis.extend_accounts(accounts_to_fund);

    Ok((Arc::new(genesis.into()), kms_address))
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
async fn spawn_local_testnet(
    task_executor: &TaskExecutor,
    chain: Arc<ChainSpec>,
    contract_address: &str,
) -> eyre::Result<Vec<JoinHandle<()>>> {
    // create temp path for test
    let temp_path = tempfile::TempDir::new().expect("tempdir is okay").into_path();

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    // create validator info and copy to shared genesis dir
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");
        // init genesis ceremony to create committee / worker_cache files
        create_validator_info(datadir, "0").await?;

        // copy to shared genesis dir
        let copy = dir.join("genesis/validators");
        for config in std::fs::read_dir(copy)? {
            let entry = config?;
            std::fs::copy(entry.path(), copy_path.join(entry.file_name()))?;
        }
    }

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "create-committee",
        "--datadir",
        shared_genesis_dir.to_str().expect("shared genesis dir"),
    ]);
    create_committee_command.args.execute().await?;

    let mut node_handles = Vec::with_capacity(validators.len());
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");

        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/worker_cache.yaml"),
            dir.join("genesis/worker_cache.yaml"),
        )?;

        let instance = v.chars().last().expect("validator instance").to_string();

        let mut command = NodeCommand::<FaucetArgs>::parse_from([
            "tn",
            "--dev",
            "--datadir",
            datadir,
            //
            // TODO: debug max-block doesn't work
            //
            // "--debug.max-block",
            // "5",
            // "--debug.terminate",
            "--chain",
            "adiri",
            "--instance",
            &instance,
            "--google-kms",
            "--contract-address",
            contract_address,
        ]);

        let cli_ctx = CliContext { task_executor: task_executor.clone() };

        // update genesis with seeded accounts
        command.chain = chain.clone();

        // collect join handles
        node_handles.push(task_executor.spawn_critical(
            v,
            Box::pin(async move {
                let err = command
                    .execute(
                        cli_ctx,
                        false, // don't overwrite chain with the default
                        |mut builder, faucet_args, tn_datadir| async move {
                            builder.opt_faucet_args = Some(faucet_args);
                            let evm_config = EthEvmConfig::default();
                            let executor = EthExecutorProvider::new(
                                std::sync::Arc::clone(&builder.node_config.chain),
                                evm_config,
                            );
                            launch_node(builder, executor, evm_config, &tn_datadir).await
                        },
                    )
                    .await;
                error!("{:?}", err);
            }),
        ));
    }

    Ok(node_handles)
}

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
async fn ensure_account_balance_infinite_loop(
    client: &HttpClient,
    address: Address,
    expected_bal: U256,
) -> eyre::Result<U256> {
    while let Ok(bal) = client.request::<String, _>("eth_getBalance", rpc_params!(address)).await {
        debug!(target: "faucet-test", "bal: {bal:?}");
        let balance = U256::from_str(&bal)?;

        // return Ok if expected bal
        if balance == expected_bal {
            return Ok(balance);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}

/// Test utility to get desired state changes from a temporary genesis for a subsequent one
///
/// NOTE: makes use of `execution_outcome_from_test_batch` below
async fn get_contract_state_for_genesis(
    chain: Arc<ChainSpec>,
    raw_txs_to_execute: Vec<TransactionSigned>,
) -> eyre::Result<ExecutionOutcome> {
    // create execution components
    let manager = TaskManager::current();
    let executor = manager.executor();
    let execution_node = default_test_execution_node(Some(chain.clone()), None, executor)?;
    let provider = execution_node.get_provider().await;
    let block_executor = execution_node.get_block_executor().await;

    // execute batch
    let batch = WorkerBlock::new(raw_txs_to_execute, SealedHeader::default());
    let parent = chain.sealed_genesis_header();
    let execution_outcome = execution_outcome_for_tests(
        &batch,
        &parent,
        Default::default(),
        &provider,
        &block_executor,
    );

    Ok(execution_outcome)
}

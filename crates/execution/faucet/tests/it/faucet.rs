//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

use alloy::{
    sol,
    sol_types::{SolType, SolValue},
};
use gcloud_sdk::{
    google::cloud::kms::v1::{
        key_management_service_client::KeyManagementServiceClient, GetPublicKeyRequest,
    },
    GoogleApi, GoogleAuthMiddleware, GoogleEnvironment,
};
use jsonrpsee::{core::client::ClientT, rpc_params};
use k256::{elliptic_curve::sec1::ToEncodedPoint, pkcs8::DecodePublicKey, PublicKey as PubKey};
use narwhal_network::client::NetworkClient;
use narwhal_test_utils::{default_test_execution_node, faucet_test_execution_node};
use narwhal_typed_store::open_db;
use narwhal_worker::{
    metrics::WorkerMetrics,
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    BlockProvider,
};
use reth_chainspec::ChainSpec;
use reth_primitives::{
    alloy_primitives::U160, public_key_to_address, Address, GenesisAccount, SealedHeader, B256,
    U256,
};
use reth_provider::ExecutionOutcome;
use reth_tasks::TaskManager;
use reth_tracing::init_test_tracing;
use reth_transaction_pool::TransactionPool;
use secp256k1::PublicKey;
use std::{str::FromStr, sync::Arc, time::Duration};
use tempfile::TempDir;
use tn_faucet::Drip;
use tn_types::{
    adiri_genesis,
    error::BlockSealError,
    test_utils::{
        contract_artifacts::{
            ERC1967PROXY_INITCODE, ERC1967PROXY_RUNTIMECODE, STABLECOINMANAGER_RUNTIMECODE,
            STABLECOIN_RUNTIMECODE,
        },
        execution_outcome_for_tests, TransactionFactory,
    },
    TransactionSigned, WorkerBlock,
};
use tokio::{
    sync::{mpsc::Sender, oneshot},
    time,
};
use tracing::debug;

#[derive(Clone, Debug)]
struct TestChanQuorumWaiter(Sender<WorkerBlock>);
impl QuorumWaiterTrait for TestChanQuorumWaiter {
    fn verify_block(
        &self,
        block: WorkerBlock,
        _timeout: Duration,
    ) -> tokio::task::JoinHandle<Result<(), QuorumWaiterError>> {
        let chan = self.0.clone();
        tokio::spawn(async move {
            chan.send(block).await.unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn test_faucet_transfers_tel_with_google_kms() -> eyre::Result<()> {
    init_test_tracing();

    // set application credentials for accessing Google KMS API
    std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "./gcloud-credentials.json");
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

    // create genesis and fund account
    let tmp_genesis = adiri_genesis();

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

    // extend genesis accounts to fund factory_address and etch impl bytecode on faucet_impl
    let faucet_impl_address = Address::random();
    let faucet_bytecode = *STABLECOINMANAGER_RUNTIMECODE;
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();
    let tmp_genesis = tmp_genesis.extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                faucet_impl_address,
                GenesisAccount::default().with_code(Some(faucet_bytecode.into())),
            ),
        ]
        .into_iter(),
    );

    // get data for faucet proxy deployment w/ initdata
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract ERC1967Proxy {
            constructor(address implementation, bytes memory _data);
        }
    );
    let faucet_init_selector = [22, 173, 166, 177];
    let deployed_token_bytes = vec![];
    let init_max_limit = U256::MAX;
    let init_min_limit = U256::from(1_000);
    let kms_faucets = vec![kms_address];
    let xyz_amount = U256::from(10).checked_pow(U256::from(6)).expect("1e18 doesn't overflow U256"); // 100 $XYZ
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

    // faucet deployment will be `factory_address`'s first transaction
    let faucet_proxy_address = factory_address.create(0);
    let role_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        grant_role_call,
    );

    let raw_txs = vec![faucet_tx_raw, role_tx_raw];

    // fetch state to be set on the faucet proxy address
    let execution_outcome = get_contract_state_for_genesis(pre_genesis_chain, raw_txs).await?;
    let execution_bundle = execution_outcome.bundle;
    let execution_storage = &execution_bundle
        .state
        .get(&faucet_proxy_address)
        .expect("faucet address missing from bundle state")
        .storage;

    let faucet_proxy_bytecode = *ERC1967PROXY_RUNTIMECODE;

    // real genesis: configure genesis accounts for proxy deployment & faucet_role
    let genesis_accounts = vec![
        (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        (kms_address, GenesisAccount::default().with_balance(U256::MAX)),
        (faucet_impl_address, GenesisAccount::default().with_code(Some(faucet_bytecode.into()))),
        // convert U256 HashMap to B256 for BTreeMap
        (
            faucet_proxy_address,
            GenesisAccount::default()
                .with_code(Some(faucet_proxy_bytecode.into()))
                .with_balance(U256::MAX)
                .with_storage(Some(
                    execution_storage
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

    let manager = TaskManager::current();
    let executor = manager.executor();

    // create engine node
    let execution_node = faucet_test_execution_node(
        true,
        Some(chain.clone()),
        None,
        executor,
        faucet_proxy_address,
    )?;

    let worker_id = 0;
    let (to_worker, mut next_batch) = tokio::sync::mpsc::channel(1);
    let client = NetworkClient::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());
    let qw = TestChanQuorumWaiter(to_worker);
    let node_metrics = WorkerMetrics::default();
    let timeout = Duration::from_secs(5);
    let block_provider =
        BlockProvider::new(0, qw.clone(), Arc::new(node_metrics), client, store.clone(), timeout);

    // start batch maker
    execution_node.start_block_builder(worker_id, block_provider.blocks_rx()).await?;

    // create client
    let client = execution_node.worker_http_client(&worker_id).await?.expect("worker rpc client");
    tracing::info!("got client: {:?}", client);

    // assert starting balance is 0
    let address = Address::random();
    let starting_balance: String = client.request("eth_getBalance", rpc_params!(address)).await?;
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    // // note: response is different each time bc KMS
    let tx_hash: String = client.request("faucet_transfer", rpc_params![address]).await?;

    // more than enough time for the next block
    let duration = Duration::from_secs(15);

    // wait for canon event or timeout
    let new_block: WorkerBlock =
        time::timeout(duration, next_batch.recv()).await?.expect("batch received");

    let batch_txs = new_block.transactions();
    let tx = batch_txs.first().expect("first batch tx from faucet");

    // assert recovered transaction
    assert_eq!(tx_hash, tx.hash_ref().to_string());
    assert_eq!(tx.transaction.to(), Some(faucet_proxy_address));

    // ensure duplicate request is error
    let response = client.request::<String, _>("faucet_transfer", rpc_params![address]).await;
    assert!(response.is_err());

    debug!("requesting second valid transaction....");
    let random_address = Address::random();
    let tx_str =
        client.request::<String, _>("faucet_transfer", rpc_params![random_address]).await?;
    let tx_hash = B256::from_str(&tx_str)?;

    // try to submit another valid request
    //
    // at this point:
    // - no pending txs in pool
    // - batch is not final (stored in db)
    // - faucet must obtain correct nonce from worker's pending block watch channel
    //
    // NOTE: new batch won't come bc tx is not in pending pool due to nonce gap
    // so query the tx pool directly
    let tx_pool = execution_node.get_worker_transaction_pool(&worker_id).await?;
    let pool_tx = tx_pool.get(&tx_hash).expect("tx in pool");
    let recovered = pool_tx.transaction.transaction();
    assert_eq!(&tx_hash, recovered.hash_ref());
    assert_eq!(recovered.transaction.to(), Some(faucet_proxy_address));
    Ok(assert_eq!(recovered.transaction.nonce(), 1))
}

#[tokio::test]
async fn test_faucet_transfers_stablecoin_with_google_kms() -> eyre::Result<()> {
    init_test_tracing();

    // set application credentials for accessing Google KMS API
    std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "./gcloud-credentials.json");
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

    // create genesis and fund account
    let tmp_genesis = adiri_genesis();

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

    // extend genesis accounts to fund factory_address, etch, stablecoin bytecode on stablecoin,
    // etch impl bytecode on faucet_impl
    let faucet_impl_address = Address::random();
    let stablecoin_address = Address::random();
    let faucet_bytecode = STABLECOINMANAGER_RUNTIMECODE.as_slice();
    let stablecoin_bytecode = STABLECOIN_RUNTIMECODE.as_slice();
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();
    let tmp_genesis = tmp_genesis.extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                faucet_impl_address,
                GenesisAccount::default().with_code(Some(faucet_bytecode.into())),
            ),
            (
                stablecoin_address,
                GenesisAccount::default().with_code(Some(stablecoin_bytecode.into())),
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
    let xyz_amount = U256::from(10).checked_pow(U256::from(6)).expect("1e18 doesn't overflow U256"); // 100 $XYZ
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

    // construct `grantRole(faucet_role)` data
    let grant_role_selector = [47, 47, 241, 93];
    let grant_role_params = (
        B256::from_str("0xaecf5761d3ba769b4631978eb26cb84eae66bcaca9c3f0f4ecde3feb2f4cf144")?,
        kms_address,
    )
        .abi_encode_params();
    let grant_role_call = [&grant_role_selector, &grant_role_params[..]].concat().into();

    // faucet deployment will be `factory_address`'s first transaction
    let faucet_proxy_address = factory_address.create(0);

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

    let role_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        grant_role_call,
    );

    let minter_tx_raw = tx_factory.create_eip1559(
        pre_genesis_chain.clone(),
        gas_price,
        Some(stablecoin_address),
        U256::ZERO,
        minter_role_call,
    );

    let raw_txs = vec![faucet_tx_raw, role_tx_raw, minter_tx_raw];

    // fetch state to be set on the faucet proxy address
    let execution_outcome = get_contract_state_for_genesis(pre_genesis_chain, raw_txs).await?;
    let execution_bundle = execution_outcome.bundle;
    let execution_storage = &execution_bundle
        .state
        .get(&faucet_proxy_address)
        .expect("faucet address missing from bundle state")
        .storage;

    let faucet_proxy_bytecode = *ERC1967PROXY_RUNTIMECODE;

    // real genesis: configure genesis accounts for proxy deployment & faucet_role
    let genesis_accounts = vec![
        (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        (kms_address, GenesisAccount::default().with_balance(U256::MAX)),
        (stablecoin_address, GenesisAccount::default().with_code(Some(stablecoin_bytecode.into()))),
        (faucet_impl_address, GenesisAccount::default().with_code(Some(faucet_bytecode.into()))),
        // convert U256 HashMap to B256 for BTreeMap
        (
            faucet_proxy_address,
            GenesisAccount::default()
                .with_code(Some(faucet_proxy_bytecode.into()))
                .with_balance(U256::MAX)
                .with_storage(Some(
                    execution_storage
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                )),
        ),
    ];

    // start canonical adiri chain with fetched storage
    let real_genesis = adiri_genesis();
    let genesis = real_genesis.extend_accounts(genesis_accounts.into_iter());
    let chain = Arc::new(genesis.into());

    let manager = TaskManager::current();
    let executor = manager.executor();

    // create engine node
    let execution_node =
        faucet_test_execution_node(true, Some(chain), None, executor, faucet_proxy_address)?;

    // start batch maker
    let worker_id = 0;
    let (to_worker, mut next_batch) = tokio::sync::mpsc::channel(2);
    execution_node.start_block_builder(worker_id, to_worker).await?;

    let user_address = Address::random();
    let client = execution_node.worker_http_client(&worker_id).await?.expect("worker rpc client");

    // assert starting balance is 0
    let starting_balance: String =
        client.request("eth_getBalance", rpc_params!(user_address)).await?;
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    let contract_address = Address::from(U160::from(12345678));

    // note: response is different each time bc KMS
    let tx_hash: String =
        client.request("faucet_transfer", rpc_params![user_address, contract_address]).await?;

    // more than enough time for the next block
    let duration = Duration::from_secs(15);

    // wait for canon event or timeout
    // let new_block: WorkerBlock =
    let (new_block, ack): (WorkerBlock, oneshot::Sender<Result<(), BlockSealError>>) =
        time::timeout(duration, next_batch.recv()).await?.expect("batch received");

    // send ack
    let _ = ack.send(Ok(()));
    let batch_txs = new_block.transactions();
    let tx = batch_txs.first().expect("first batch tx from faucet");

    let contract_params: Vec<u8> = Drip::abi_encode_params(&(&contract_address, &user_address));

    // keccak256("Drip(address,address)")[0..4]
    let selector = [235, 56, 57, 167];
    let expected_input = [&selector, &contract_params[..]].concat();

    // assert recovered transaction
    let expected_tx_hash = tx.hash_ref().to_string();
    assert_eq!(tx_hash, expected_tx_hash);
    assert_eq!(tx.transaction.input(), &expected_input);

    // ensure duplicate request is error
    let dup_request = client
        .request::<String, _>("faucet_transfer", rpc_params![user_address, contract_address])
        .await;
    assert!(dup_request.is_err());

    // ensure user can request a different stablecoin
    let contract_address = Address::from(U160::from(87654321));
    let response = client
        .request::<String, _>("faucet_transfer", rpc_params![user_address, contract_address])
        .await;
    assert!(response.is_ok());

    // ensure duplicate request is error
    let dup_request = client
        .request::<String, _>("faucet_transfer", rpc_params![user_address, contract_address])
        .await;
    assert!(dup_request.is_err());

    // sleep for 11s so request is cleared from pending cache
    // NOTE: pending cache is hard-coded to 10s and this test doesn't update the DB
    tokio::time::sleep(Duration::from_secs(11)).await;

    let ok_dup_request = client
        .request::<String, _>("faucet_transfer", rpc_params![user_address, contract_address])
        .await;

    Ok(assert!(ok_dup_request.is_ok()))
}

/// Keys obtained from google kms calling:
///
/// ```
/// let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
///   GoogleApi::from_function(
///     KeyManagementServiceClient::new,
///     "https://cloudkms.googleapis.com",
///     None,
///   )
///   .await?;

/// let validators = [
///   "validator-1",
///   "validator-2",
///   "validator-3",
///   "validator-4",
/// ];

/// let locations = "global";
/// let key_rings = "adiri-testnet";
/// // let crypto_keys = "validator-1";
/// let crypto_key_versions = "1";

/// for v in validators {
///   let name = format!(
///     "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}/cryptoKeyVersions/{}",
///     google_project_id, locations, key_rings, v, crypto_key_versions
///   );

///   let pubkey = kms_client
///     .get()
///     .get_public_key(tonic::Request::new(GetPublicKeyRequest {
///         name,
///         ..Default::default()
///     }))
///     .await?;

///   println!("{v} public key:\n {:?}", pubkey.into_inner().pem);
/// }
/// ```

#[test]
#[ignore = "this is only useful for debugging purposes"]
fn test_print_kms_wallets() -> eyre::Result<()> {
    let keys = [
        "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEqzv8pSIJXo3PJZsGv+feaCZJFQoG3ed5\ngl0o/dpBKtwT+yajMYTCravDiqW/g62W+PNVzLoCbaot1WdlwXcp4Q==\n-----END PUBLIC KEY-----\n",
        "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEPinA/JiDrvzRDhDEpQU5KysaPZy/d2mv\noJ7fxS90m4tB4weUrBgsy1GeFKSU0TDSW7p9CE+l+36DQiwhkdPyIg==\n-----END PUBLIC KEY-----\n",
        "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwKznkyaaqb/gPgKnmKLzE4EVKOatDdbB\nDrfCV1ofBFFmAGrUxN78HQ27YCKRHhakqRFrIEgnsuIe4KdWhhpoig==\n-----END PUBLIC KEY-----\n",
        "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEqUWECyml/Lkvr1QePfIQUH17t/QnSXoj\nNA0o9zB2geizkPIvhMsWB7lzooHI01s1UiPWAGThJb5KvhjNWsqP2g==\n-----END PUBLIC KEY-----\n",
    ];

    for key in keys {
        // k256 public key to convert from pem
        let pubkey_from_pem = PubKey::from_public_key_pem(key)?;
        // secp256k1 public key from uncompressed k256 variation
        let public_key = PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())?;
        // calculate address from uncompressed public key
        let wallet_address = public_key_to_address(public_key);
        println!("{wallet_address:?}");
    }

    // res: wallets added to genesis
    // 0xe626ce81714cb7777b1bf8ad2323963fb3398ad5
    // 0xb3fabbd1d2edde4d9ced3ce352859ce1bebf7907
    // 0xa3478861957661b2d8974d9309646a71271d98b9
    // 0xe69151677e5aec0b4fc0a94bfcaf20f6f0f975eb

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
    let execution_outcome =
        execution_outcome_for_tests(&batch, &parent, &provider, &block_executor);

    Ok(execution_outcome)
}

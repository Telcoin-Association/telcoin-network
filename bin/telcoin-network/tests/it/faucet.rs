//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

use crate::util::create_validator_info;
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
use narwhal_test_utils::CommandParser;
use reth::{
    tasks::{TaskExecutor, TaskManager},
    CliContext,
};
use reth_chainspec::ChainSpec;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{public_key_to_address, Address, GenesisAccount, U256};
use reth_tracing::init_test_tracing;
use secp256k1::PublicKey;
use std::{str::FromStr, sync::Arc, time::Duration};
use telcoin_network::{genesis::GenesisArgs, node::NodeCommand};
use tn_faucet::FaucetArgs;
use tn_node::launch_node;
use tn_types::{
    adiri_genesis,
    test_utils::{deploy_contract_faucet_initialize, TransactionFactory},
};
use tokio::{runtime::Handle, task::JoinHandle, time::timeout};
use tracing::{debug, error, info};

#[tokio::test]
async fn test_faucet_transfers_tel_with_google_kms_e2e() -> eyre::Result<()> {
    init_test_tracing();

    // task manager
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    // create google env and chain spec
    let (chain, kms_address) = prepare_google_kms_env().await?;

    // create and launch validator nodes on local network,
    // use expected faucet contract address from `TransactionFactory::default` with nonce == 0
    spawn_local_testnet(
        &task_executor,
        chain.clone(),
        "0x8a345995579C09F45a5288b4858467920Af27301",
    )
    .await?;

    info!(target: "faucet-test", "nodes started - sleeping for 10s...");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url)?;

    // assert deployer starting balance is properly seeded
    let mut tx_factory = TransactionFactory::new();
    let default_deployer_address = tx_factory.address();
    let deployer_balance: String =
        client.request("eth_getBalance", rpc_params!(default_deployer_address)).await?;

    debug!(target: "faucet-test", "Deployer starting balance: {deployer_balance:?}");
    assert_eq!(U256::from_str(&deployer_balance)?, U256::MAX);
    info!(target: "faucet-test", "deploying contract and initializing faucet...");

    // deploy faucet contracts and initialize
    let empty_tokens_array = vec![];
    let _faucet_contract = deploy_contract_faucet_initialize(
        chain.clone(),
        &rpc_url,
        kms_address,
        empty_tokens_array,
        &mut tx_factory,
    )
    .await?;
    info!(target: "faucet-test", "faucet initialization complete!");

    // note: response is different each time bc KMS
    //
    // assert starting balance is 0
    let mut random_tx_factory = TransactionFactory::new_random();
    let random_address = random_tx_factory.address();
    let starting_balance: String =
        client.request("eth_getBalance", rpc_params!(random_address)).await?;
    debug!(target: "faucet-test", "starting balance: {starting_balance:?}");
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    let tx_hash: String = client.request("faucet_transfer", rpc_params![random_address]).await?;
    info!(target: "faucet-test", ?tx_hash, "valid faucet transfer tx hash");

    // more than enough time for the nodes to launch RPCs
    let duration = Duration::from_secs(30);

    // ensure account balance increased
    let expected_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_balance),
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
    let random_address = Address::random();
    let starting_balance: String =
        client.request("eth_getBalance", rpc_params!(random_address)).await?;
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    let tx_hash: String = client.request("faucet_transfer", rpc_params![random_address]).await?;
    info!(target: "faucet-test", ?tx_hash, "new random faucet request success. waiting for balance increase...");

    // ensure account balance increased
    //
    // account balance is only updates on final execution
    // finding the expected balance in time means the faucet successfully used the correct nonce
    let expected_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_balance),
    )
    .await?
    .expect("expected balance random account timeout");

    // duplicate request is err
    info!(target: "faucet-test", "account balance updated. submitting duplicate request and shutting down...");
    assert!(client
        .request::<String, _>("faucet_transfer", rpc_params![random_address])
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
    let wallet_address = public_key_to_address(public_key);

    // create genesis and fund relevant accounts
    let genesis = adiri_genesis();
    let faucet_account = vec![(wallet_address, GenesisAccount::default().with_balance(U256::MAX))];
    let default_deployer_address = TransactionFactory::default().address();
    let default_deployer_account =
        vec![(default_deployer_address, GenesisAccount::default().with_balance(U256::MAX))];

    let accounts_to_fund = faucet_account.into_iter().chain(default_deployer_account.into_iter());
    let genesis = genesis.extend_accounts(accounts_to_fund);

    Ok((Arc::new(genesis.into()), wallet_address))
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

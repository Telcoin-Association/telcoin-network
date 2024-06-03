//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

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
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{
    alloy_primitives::U160, public_key_to_address, Address, ChainSpec, GenesisAccount, U256,
};
use reth_tracing::init_test_tracing;
use secp256k1::PublicKey;
use std::{str::FromStr, sync::Arc, time::Duration};
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use tn_faucet::FaucetArgs;
use tn_node::launch_node;
use tn_types::adiri_genesis;
use tokio::{runtime::Handle, task::JoinHandle, time::timeout};
use tracing::{error, info};

// TODO: need to test stablecoin requests for faucet
// but not sure how to compile/deploy contracts.
#[tokio::test]
async fn test_faucet_transfers_tel_with_google_kms_e2e() -> eyre::Result<()> {
    init_test_tracing();

    // task manager
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    // create google env and chain spec
    let chain = prepare_google_kms_env().await?;

    // create and launch validator nodes on local network
    spawn_local_testnet(&task_executor, chain.clone()).await?;

    info!("nodes started");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let address = Address::from(U160::from(8991));
    let client = HttpClientBuilder::default().build("http://127.0.0.1:8545")?;

    // assert starting balance is 0
    let starting_balance: String = client.request("eth_getBalance", rpc_params!(address)).await?;
    println!("starting balance: {starting_balance:?}");
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    // note: response is different each time bc KMS
    let tx_hash: String = client.request("faucet_transfer", rpc_params![address]).await?;
    info!(target: "faucet-transaction", ?tx_hash);

    // more than enough time for the nodes to launch RPCs
    let duration = Duration::from_secs(15);

    // ensure account balance increased
    let balance = timeout(duration, ensure_account_balance(&client, address))
        .await?
        .expect("balance timeout");
    let expected_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    assert_eq!(balance, expected_balance);

    // duplicate request is err
    assert!(client.request::<String, _>("faucet_transfer", rpc_params![address]).await.is_err());

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
async fn prepare_google_kms_env() -> eyre::Result<Arc<ChainSpec>> {
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

    // create genesis and fund account
    let genesis = adiri_genesis();
    let faucet_account = vec![(wallet_address, GenesisAccount::default().with_balance(U256::MAX))];
    let genesis = genesis.extend_accounts(faucet_account.into_iter());

    Ok(Arc::new(genesis.into()))
}

/// Execute genesis ceremony inside tempdir
async fn create_validator_info(datadir: &str) -> eyre::Result<()> {
    // init genesis
    let init_command =
        CommandParser::<GenesisArgs>::parse_from(["tn", "init", "--datadir", datadir]);
    init_command.args.execute().await?;

    // keytool
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "generate",
        "validator",
        "--datadir",
        datadir,
        "--address",
        "0",
    ]);
    keys_command.args.execute().await?;

    // add validator
    let add_validator_command =
        CommandParser::<GenesisArgs>::parse_from(["tn", "add-validator", "--datadir", datadir]);
    add_validator_command.args.execute().await
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
async fn spawn_local_testnet(
    task_executor: &TaskExecutor,
    chain: Arc<ChainSpec>,
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
        create_validator_info(datadir).await?;

        // copy to shared genesis dir
        let copy = dir.join("genesis/validators");
        for config in std::fs::read_dir(copy)? {
            let entry = config?;
            std::fs::copy(entry.path(), &copy_path.join(entry.file_name()))?;
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
            &shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            &shared_genesis_dir.join("genesis/worker_cache.yaml"),
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
        ]);

        let cli_ctx = CliContext { task_executor: task_executor.clone() };

        // update genesis with seeded accounts
        command.chain = chain.clone();

        // collect join handles
        node_handles.push(task_executor.spawn_critical(
            v,
            Box::pin(async move {
                let err = command
                    .execute(cli_ctx, |mut builder, faucet_args, tn_datadir| async move {
                        builder.opt_faucet_args = Some(faucet_args);
                        let executor = EthExecutorProvider::new(std::sync::Arc::clone(&builder.node_config.chain), EthEvmConfig::default());
                        launch_node(builder, executor, tn_datadir).await
                    })
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
async fn ensure_account_balance(client: &HttpClient, address: Address) -> eyre::Result<U256> {
    while let Ok(bal) = client.request::<String, _>("eth_getBalance", rpc_params!(address)).await {
        println!("bal: {bal:?}");
        let balance = U256::from_str(&bal)?;
        if balance > U256::ZERO {
            return Ok(balance);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}

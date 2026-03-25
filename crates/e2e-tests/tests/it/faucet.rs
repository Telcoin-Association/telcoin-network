//! Integration test for faucet drip functionality.
//!
//! Tests calling the StablecoinManager's permissionless `drip()` function directly
//! via `eth_sendRawTransaction`. The contract mints TEL via the precompile at 0x7e1
//! and stablecoins via ERC20 mint. Rate limiting is handled by the contract's `_checkDrip()`.

use alloy::{network::EthereumWallet, providers::ProviderBuilder, sol_types::SolCall};
use e2e_tests::{ensure_account_balance_infinite_loop, spawn_local_testnet, verify_all_transports};
use futures::{stream::FuturesUnordered, StreamExt};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use std::{str::FromStr, sync::Arc, time::Duration};
use tn_config::{fetch_file_content_relative_to_manifest, Config, ConfigFmt, ConfigTrait};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec, RethEnv};
use tn_types::{
    adiri_genesis, hex, sol, Address, Encodable2718 as _, Genesis, GenesisAccount, SolValue,
    TaskManager, B256, U256,
};
use tokio::{task::JoinHandle, time::timeout};
use tracing::{debug, info};

#[tokio::test]
async fn test_faucet_drip_tel_and_xyz_e2e() -> eyre::Result<()> {
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
                uint256 dripAmount_;
                uint256 nativeDripAmount_;
            }

            function initialize(StablecoinManagerInitParams calldata initParams) external;
            function drip(address token, address recipient) external;
        }
    );

    // stablecoin interface
    sol!(
        #[sol(rpc)]
        contract Stablecoin {
            function initialize(
                string memory name_,
                string memory symbol_,
                uint8 decimals_
            ) external;
            function decimals() external view returns (uint8);
            function balanceOf(address account) external view returns (uint256);
        }
    );

    // set random addresses on which to etch contract bytecodes
    let faucet_impl_address = Address::random();
    let stablecoin_impl_address = Address::random();
    // fetch bytecode attributes from compiled jsons in tn-contracts repo
    let faucet_standard_json = fetch_file_content_relative_to_manifest(
        "../../tn-contracts/artifacts/StablecoinManager.json",
    );
    let faucet_deployed_bytecode =
        RethEnv::fetch_value_from_json_str(&faucet_standard_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;
    let stablecoin_json =
        fetch_file_content_relative_to_manifest("../../tn-contracts/artifacts/Stablecoin.json");
    let stablecoin_impl_bytecode =
        RethEnv::fetch_value_from_json_str(&stablecoin_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;

    // extend genesis accounts to fund factory_address, etch bytecodes
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();
    let genesis = adiri_genesis();
    let tmp_genesis = genesis.clone().extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                faucet_impl_address,
                GenesisAccount::default().with_code(Some(faucet_deployed_bytecode.clone().into())),
            ),
            (
                stablecoin_impl_address,
                GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.clone().into())),
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
    let xyz_amount = U256::from(10).checked_pow(U256::from(6)).expect("1e6 doesn't overflow U256"); // 1 $XYZ
    let tel_amount =
        U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256"); // 1 $TEL

    // encode initialization struct
    let init_params = StablecoinManager::StablecoinManagerInitParams {
        admin_: factory_address,
        maintainer_: factory_address,
        tokens_: deployed_token_bytes,
        initMaxLimit: init_max_limit,
        initMinLimit: init_min_limit,
        dripAmount_: xyz_amount,
        nativeDripAmount_: tel_amount,
    }
    .abi_encode();

    // construct create data for faucet proxy address
    let init_call = [&faucet_init_selector, &init_params[..]].concat();
    let constructor_params = (faucet_impl_address, init_call.clone()).abi_encode_params();
    let proxy_json =
        fetch_file_content_relative_to_manifest("../../tn-contracts/artifacts/ERC1967Proxy.json");
    let proxy_initcode = RethEnv::fetch_value_from_json_str(&proxy_json, Some("bytecode.object"))?
        .as_str()
        .map(hex::decode)
        .unwrap()?;
    let proxy_bytecode =
        RethEnv::fetch_value_from_json_str(&proxy_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;
    let faucet_create_data = [proxy_initcode.clone().as_slice(), &constructor_params[..]].concat();

    // construct create data for stablecoin proxy
    let stablecoin_init_selector = [22, 36, 246, 198];
    let stablecoin_init_params = ("name", "symbol", 6).abi_encode_params();
    let stablecoin_init_call = [&stablecoin_init_selector, &stablecoin_init_params[..]].concat();
    let stablecoin_constructor_params =
        (stablecoin_impl_address, stablecoin_init_call.clone()).abi_encode_params();
    let stablecoin_create_data =
        [proxy_initcode.as_slice(), &stablecoin_constructor_params[..]].concat();

    // faucet deployment will be `factory_address`'s first tx, stablecoin will be second tx
    let faucet_proxy_address = factory_address.create(0);
    let stablecoin_address = factory_address.create(1);

    // construct `updateXYZ()` data
    let updatexyz_selector = [233, 174, 163, 150];
    let updatexyz_params = (stablecoin_address, true, U256::MAX, U256::ZERO).abi_encode_params();
    let updatexyz_call = [&updatexyz_selector, &updatexyz_params[..]].concat().into();

    // construct `grantRole(minter_role)` on stablecoin to faucet proxy
    let grant_role_selector = [47, 47, 241, 93];
    let minter_role_params = (
        B256::from_str("0x9f2df0fed2c77648de5860a4cc508cd0818c85b8b8a1ab4ceeef8d981c8956a6")?,
        faucet_proxy_address,
    )
        .abi_encode_params();
    let minter_role_call = [&grant_role_selector, &minter_role_params[..]].concat().into();

    // assemble eip1559 transactions using constructed datas
    let pre_genesis_chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
    let gas_price = 100;
    let faucet_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        None,
        U256::ZERO,
        faucet_create_data.clone().into(),
    );

    let stablecoin_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        None,
        U256::ZERO,
        stablecoin_create_data.clone().into(),
    );

    let updatexyz_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        updatexyz_call,
    );

    let minter_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        Some(stablecoin_address),
        U256::ZERO,
        minter_role_call,
    );

    let raw_txs = vec![faucet_tx_raw, stablecoin_tx_raw, updatexyz_tx_raw, minter_tx_raw];

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let task_manager = TaskManager::new("Temp Task Manager");
    let tmp_reth_env = RethEnv::new_for_temp_chain(
        pre_genesis_chain.clone(),
        tmp_dir.path(),
        &task_manager,
        None,
    )?;
    // fetch state to be set on the faucet proxy address
    let execution_bundle = tmp_reth_env
        .execution_outcome_for_tests(raw_txs, &pre_genesis_chain.sealed_genesis_header());
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

    // real genesis: configure genesis accounts for proxy deployment
    let genesis_accounts = vec![
        (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        (
            stablecoin_impl_address,
            GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.into())),
        ),
        (
            stablecoin_address,
            GenesisAccount::default().with_code(Some(proxy_bytecode.clone().into())).with_storage(
                Some(
                    execution_storage_stablecoin
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                ),
            ),
        ),
        (
            faucet_impl_address,
            GenesisAccount::default().with_code(Some(faucet_deployed_bytecode.into())),
        ),
        // convert U256 HashMap to B256 for BTreeMap
        (
            faucet_proxy_address,
            GenesisAccount::default()
                .with_code(Some(proxy_bytecode.into()))
                .with_balance(U256::MAX)
                .with_storage(Some(
                    execution_storage_faucet
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                )),
        ),
    ];

    // create and launch validator nodes on local network
    let faucet_tmp_dir = tempfile::TempDir::new().unwrap();
    let endpoints = spawn_local_testnet(faucet_tmp_dir.path(), Some(genesis_accounts))?;
    let genesis_file = faucet_tmp_dir.path().join("shared-genesis/genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&genesis_file, ConfigFmt::YAML)?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());

    info!(target: "faucet-test", "nodes started - sleeping for 15s...");

    tokio::time::sleep(Duration::from_secs(15)).await;

    // verify all three transports (HTTP, WS, IPC) are reachable
    verify_all_transports(&endpoints[0]).await?;

    let rpc_url = endpoints[0].http_url.clone();
    let client = HttpClientBuilder::default().build(&rpc_url)?;

    // assert deployer starting balance is properly seeded
    let mut caller = TransactionFactory::new();
    let deployer_address = caller.address();
    let deployer_balance: String =
        client.request("eth_getBalance", rpc_params!(deployer_address)).await?;
    debug!(target: "faucet-test", "Deployer starting balance: {deployer_balance:?}");
    assert_eq!(U256::from_str(&deployer_balance)?, U256::MAX);

    // assert starting balance is 0
    let recipient = Address::random();
    let starting_tel_balance: String =
        client.request("eth_getBalance", rpc_params!(recipient)).await?;
    debug!(target: "faucet-test", "starting balance: {starting_tel_balance:?}");
    assert_eq!(U256::from_str(&starting_tel_balance)?, U256::ZERO);

    // call drip(address(0), recipient) for TEL via the contract directly
    let drip_calldata =
        StablecoinManager::dripCall { token: Address::ZERO, recipient }.abi_encode();
    let drip_tx = caller.create_eip1559(
        chain.clone(),
        None,
        1_000_000_000,
        Some(faucet_proxy_address),
        U256::ZERO,
        drip_calldata.into(),
    );
    let drip_tx_bytes = drip_tx.encoded_2718();
    let tel_tx_hash: String =
        client.request("eth_sendRawTransaction", rpc_params![drip_tx_bytes]).await?;
    info!(target: "faucet-test", ?tel_tx_hash, "drip TEL tx submitted");

    // more than enough time for the nodes to process
    let duration = Duration::from_secs(30);

    // ensure account balance increased
    let expected_tel_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, recipient, expected_tel_balance),
    )
    .await?
    .expect("expected TEL balance timeout");

    // call drip(stablecoin, new_recipient) for XYZ
    let new_recipient = Address::random();
    let xyz_drip_calldata =
        StablecoinManager::dripCall { token: stablecoin_address, recipient: new_recipient }
            .abi_encode();
    let xyz_drip_tx = caller.create_eip1559(
        chain.clone(),
        None,
        1_000_000_000,
        Some(faucet_proxy_address),
        U256::ZERO,
        xyz_drip_calldata.into(),
    );
    let xyz_drip_tx_bytes = xyz_drip_tx.encoded_2718();
    let xyz_tx_hash: String =
        client.request("eth_sendRawTransaction", rpc_params![xyz_drip_tx_bytes]).await?;
    info!(target: "faucet-test", ?xyz_tx_hash, "drip XYZ tx submitted");

    // check XYZ balance via contract call
    let signer = caller.get_default_signer()?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url.parse()?);
    let stablecoin_contract = Stablecoin::new(stablecoin_address, provider.clone());
    let expected_xyz_balance = U256::from(1_000_000); // 1e6 (1 XYZ)

    let result = timeout(duration, async {
        loop {
            let actual_xyz_balance: U256 =
                stablecoin_contract.balanceOf(new_recipient).call().await?;
            debug!(target: "faucet-test", "actual XYZ balance: {:?}", actual_xyz_balance);

            if actual_xyz_balance == expected_xyz_balance {
                return Ok::<_, eyre::Report>(actual_xyz_balance);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    match result {
        Ok(Ok(balance)) => {
            info!(target: "faucet-test", "XYZ balance check completed successfully: {}", balance);
        }
        Ok(Err(e)) => {
            panic!("Error while checking XYZ balance: {e:?}");
        }
        Err(_) => {
            panic!("XYZ balance check timed out");
        }
    }

    // submit concurrent drip calls for TEL to multiple random addresses
    // prepare all txs from the funded caller (nonces increment sequentially)
    let random_addresses: Vec<Address> = (0..10).map(|_| Address::random()).collect();
    let drip_txs: Vec<_> = random_addresses
        .iter()
        .map(|&address| {
            let drip_calldata =
                StablecoinManager::dripCall { token: Address::ZERO, recipient: address }
                    .abi_encode();
            let tx = caller.create_eip1559(
                chain.clone(),
                None,
                1_000_000_000,
                Some(faucet_proxy_address),
                U256::ZERO,
                drip_calldata.into(),
            );
            tx.encoded_2718()
        })
        .collect();

    // send all txs concurrently via RPC
    let mut requests: FuturesUnordered<JoinHandle<String>> = drip_txs
        .into_iter()
        .map(|tx_bytes| {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request::<String, _>("eth_sendRawTransaction", rpc_params![tx_bytes])
                    .await
                    .expect("request successful")
            })
        })
        .collect();

    while let Some(res) = requests.next().await {
        assert!(res.is_ok());
    }

    // wait for all account balances to update
    let mut check_account_balances: FuturesUnordered<JoinHandle<()>> = random_addresses
        .into_iter()
        .map(|address| {
            tokio::spawn({
                let client = client.clone();
                async move {
                    let _ = timeout(
                        duration,
                        ensure_account_balance_infinite_loop(
                            &client,
                            address,
                            expected_tel_balance,
                        ),
                    )
                    .await
                    .expect("account balance okay")
                    .expect("expected balance random account timeout");
                }
            })
        })
        .collect();

    while let Some(res) = check_account_balances.next().await {
        assert!(res.is_ok());
    }

    Ok(())
}

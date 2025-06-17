use crate::util::{config_local_testnet, IT_TEST_MUTEX};
use alloy::primitives::address;
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use jsonrpsee::{
    core::{client::ClientT, DeserializeOwned},
    http_client::HttpClientBuilder,
    rpc_params,
};
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};
use tn_types::{get_available_tcp_port, keccak256, test_utils::init_test_tracing, Address};
use tokio::runtime::Builder;
use tracing::{error, info};

/// One unit of TEL (10^18) measured in wei.
const WEI_PER_TEL: u128 = 1_000_000_000_000_000_000;

/// Send SIGTERM to child, can use this to pre-send TERM to all children when shutting down.
fn send_term(child: &mut Child) {
    if let Err(e) = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM) {
        error!(target: "restart-test", ?e, "error killing child");
    }
}

/// Helper function to shutdown child processes and log errors.
fn kill_child(child: &mut Child) {
    send_term(child);

    for _ in 0..5 {
        match child.try_wait() {
            Ok(Some(_)) => {
                info!(target: "restart-test", "child exited");
                return;
            }
            Ok(None) => {}
            Err(e) => error!(target: "restart-test", "error waiting on child to exit: {e}"),
        }
        std::thread::sleep(Duration::from_millis(1200));
    }
    // The child is not exiting...
    // The code below will send SIGKILL without the use of nix.
    if let Err(e) = child.kill() {
        error!(target: "restart-test", ?e, "error killing child");
    }
    // Hopefully it will exit now...
    if let Err(e) = child.wait() {
        error!(target: "restart-test", ?e, "error waiting for child to die");
    }
}

fn send_and_confirm(
    node: &str,
    node_test: &str,
    key: &str,
    to_account: Address,
    nonce: u128,
) -> eyre::Result<()> {
    let basefee_address = address!("0x9999999999999999999999999999999999999999");
    let current = get_balance(node, &to_account.to_string(), 1)?;
    let current_basefee = get_balance(node, &basefee_address.to_string(), 1)?;
    let amount = 10 * WEI_PER_TEL; // 10 TEL
    let expected = current + amount;
    send_tel(node, key, to_account, amount, 250, 21000, nonce)?;

    // sleep
    std::thread::sleep(Duration::from_millis(1000));
    info!(target: "restart-test", "calling get_positive_balance_with_retry...");

    // get positive bal and kill child2 if error
    let bal = get_balance_above_with_retry(node_test, &to_account.to_string(), expected - 1)?;

    if expected != bal {
        error!(target: "restart-test", "{expected} != {bal} - returning error!");
        return Err(Report::msg(format!("Expected a balance of {expected} got {bal}!")));
    }
    let bal =
        get_balance_above_with_retry(node_test, &basefee_address.to_string(), current_basefee)?;
    if nonce > 0 && bal != current_basefee + (current_basefee / (nonce)) {
        error!(target: "restart-test", "basefee error!");
        return Err(Report::msg("Expected a basefee increment!".to_string()));
    }
    Ok(())
}

/// Run the first part tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests1(
    client_urls: &[String; 4],
    child2: &mut Child,
    exe_path: &Path,
    temp_path: &Path,
    rpc_port2: u16,
    delay_secs: u64,
) -> eyre::Result<Child> {
    network_advancing(client_urls).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to advance network in restart_tests1");
    })?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    info!(target: "restart-test", "testing blocks same first time in restart_tests1");
    test_blocks_same(client_urls)?;
    // Try once more then fail test.
    send_and_confirm(&client_urls[1], &client_urls[2], &key, to_account, 0).inspect_err(|e| {
        kill_child(child2);
        error!(target: "restart-test", ?e, "failed to send and confirm in restart_tests1");
    })?;

    info!(target: "restart-test", "killing child2...");
    kill_child(child2);
    info!(target: "restart-test", "child2 dead :D sleeping...");
    std::thread::sleep(Duration::from_secs(delay_secs));

    // This validator should be down now, confirm.
    if get_balance(&client_urls[2], &to_account.to_string(), 5).is_ok() {
        error!(target: "restart-test", "tests1: get_balancer worked for shutdown validator - returning error!");
        return Err(Report::msg("Validator not down!".to_string()));
    }

    info!(target: "restart-test", "restarting child2...");
    // Restart
    let mut child2 = start_validator(2, exe_path, temp_path, rpc_port2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e, "failed to get positive balance with retry in restart_tests1");
        })?;
    if 10 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_TEL != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }
    // Try once more then fail test.
    send_and_confirm(&client_urls[0], &client_urls[2], &key, to_account, 1).inspect_err(|e| {
        error!(target: "restart-test", ?e, "send and confirm nonce 1 failed - killing child2...");
        kill_child(&mut child2);
    })?;

    info!(target: "restart-test", "testing blocks same again in restart_tests1");

    test_blocks_same(client_urls).inspect_err(|e| {
        error!(target: "restart-test", ?e, "test blocks same failed - killing child2...");
        kill_child(&mut child2);
    })?;
    Ok(child2)
}

/// Run the second part of tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests2(client_urls: &[String; 4]) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.
    test_blocks_same(client_urls)?; // Starting from a solid position after a restart?
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    for (i, uri) in client_urls.iter().enumerate().take(4) {
        let bal = get_positive_balance_with_retry(uri, &to_account.to_string())?;
        if 20 * WEI_PER_TEL != bal {
            return Err(Report::msg(format!(
                "Expected a balance of {} got {bal} for node {i}!",
                20 * WEI_PER_TEL
            )));
        }
    }
    let number_start = get_block_number(&client_urls[3])?;
    if let Err(e) = send_and_confirm(&client_urls[0], &client_urls[3], &key, to_account, 2) {
        let number_0 = get_block_number(&client_urls[0])?;
        let number_1 = get_block_number(&client_urls[1])?;
        let number_2 = get_block_number(&client_urls[2])?;
        let number_3 = get_block_number(&client_urls[3])?;
        if number_start == number_3 {
            return Err(eyre::eyre!(
                "Stuck on block {number_3}, other nodes {number_0}, {number_1}, {number_2}, error: {e}"
            ));
        }
        return Err(e);
    }
    test_blocks_same(client_urls)?;
    Ok(())
}

fn network_advancing(client_urls: &[String; 4]) -> eyre::Result<()> {
    fn max_start(client_urls: &[String; 4]) -> eyre::Result<u64> {
        let mut start_num = get_block_number(&client_urls[0])?;
        start_num = start_num.max(get_block_number(&client_urls[1])?);
        start_num = start_num.max(get_block_number(&client_urls[2])?);
        start_num = start_num.max(get_block_number(&client_urls[3])?);
        Ok(start_num)
    }
    let start_num = max_start(client_urls)?;
    let mut next_num = start_num;
    let mut i = 0;
    // Wait until a node is advancing agian, network should be back now.
    while next_num <= start_num {
        std::thread::sleep(Duration::from_secs(1));
        next_num = max_start(client_urls)?;
        i += 1;
        if i > 45 {
            return Err(eyre::eyre!(
                "Network not advancing past {next_num} within 45 seconds after restart!"
            ));
        }
    }
    Ok(())
}

fn do_restarts(delay: u64) -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();
    info!(target: "restart-test", "do_restarts, delay: {delay}");
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let mut exe_path =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"));
    exe_path.push("../../target/debug/telcoin-network");
    let mut children: [Option<Child>; 4] = [None, None, None, None];
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0, 0, 0, 0];
    for (i, child) in children.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        *child = Some(start_validator(i, &exe_path, &temp_path, rpc_port));
    }

    // pass &mut to `run_restart_tests1` to shutdown child in case of error
    let mut child2 = children[2].take().expect("missing child 2");

    info!(target: "restart-test", "Running restart tests 1");
    // run restart tests1
    let res1 =
        run_restart_tests1(&client_urls, &mut child2, &exe_path, &temp_path, rpc_ports[2], delay);
    info!(target: "restart-test", "Ran restart tests 1: {res1:?}");
    let is_ok = res1.is_ok();

    // kill new child2 if successfully restarted
    let assert_str = match res1 {
        Ok(mut child2_restarted) => {
            kill_child(&mut child2_restarted);
            "".to_string()
        }
        Err(err) => {
            // run_restart_tests1 shutsdown child2 on error
            tracing::error!(target: "restart-test", "Got error: {err}");
            err.to_string()
        }
    };

    // send SIGTERM to all children (child2 should already be dead)
    // This lets them start shutting down in parrallel.
    for (i, child) in children.iter_mut().enumerate() {
        if i != 2 {
            let child = child.as_mut().expect("missing a child");
            send_term(child);
        }
    }

    // kill all children (child2 should already be dead)
    for (i, child) in children.iter_mut().enumerate() {
        // Best effort to kill all the other nodes.
        if i != 2 {
            let child = child.as_mut().expect("missing a child");
            kill_child(child);
            info!(target: "restart-test", "kill and wait on child{i} complete");
        }
    }

    // Make sure we shutdown nodes even if an error in first testing.
    assert!(is_ok, "{}", assert_str);
    let to_account = address_from_word("testing");
    // The validators should be down now, confirm.
    assert!(get_balance(&client_urls[0], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[1], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[3], &to_account.to_string(), 5).is_err());

    info!(target: "restart-test", "all nodes shutdown...restarting network");
    // Restart network
    for (i, child) in children.iter_mut().enumerate() {
        *child = Some(start_validator(i, &exe_path, &temp_path, rpc_ports[i]));
    }

    info!(target: "restart-test", "Running restart tests 2");
    let res2 = run_restart_tests2(&client_urls);
    info!(target: "restart-test", "Ran restart tests 2: {res2:?}");

    // SIGTERM children so they can shutdown in parrellel.
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        send_term(child);
    }

    // kill children before returning final_result
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        kill_child(child);
        info!(target: "restart-test", "kill and wait on child complete for final result");
    }
    res2
}

/// Test a restart case with a short delay, the stopped node should rejoin consensus.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restartstt() -> eyre::Result<()> {
    do_restarts(2)
}

/// Run some test to make sure an observer is participating in the network.
fn run_observer_tests(client_urls: &[String; 4], obs_url: &str) -> eyre::Result<()> {
    network_advancing(client_urls)?;
    std::thread::sleep(Duration::from_secs(2)); // Advancing, so pause so that upcoming checks will fail if a node is lagging.

    let key = get_key("test-source");
    let to_account = address_from_word("testing");

    test_blocks_same(client_urls)?;
    // Send to observer, validator confirms.
    send_and_confirm(obs_url, &client_urls[2], &key, to_account, 0)?;
    // Send to observer, validator confirms- second time.
    send_and_confirm(obs_url, &client_urls[3], &key, to_account, 1)?;

    // Send to a validator, observer sees transfer.
    send_and_confirm(&client_urls[0], obs_url, &key, to_account, 2)?;

    test_blocks_same(client_urls)?;
    Ok(())
}

/// Test an observer node can submit txns.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_observer() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    init_test_tracing();
    info!(target: "restart-test", "do_restarts_observer");
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    {
        config_local_testnet(&temp_path, Some("restart_test".to_string()), None)
            .expect("failed to config");
    }
    let mut exe_path =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"));
    exe_path.push("../../target/debug/telcoin-network");
    let mut children: [Option<Child>; 4] = [None, None, None, None];
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    let mut rpc_ports: [u16; 4] = [0, 0, 0, 0];
    for (i, child) in children.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        *child = Some(start_validator(i, &exe_path, &temp_path, rpc_port));
    }
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for child!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let mut obs_child = start_observer(4, &exe_path, &temp_path, obs_rpc_port);
    let res = run_observer_tests(&client_urls, &obs_url);

    // SIGTERM children so they can shutdown in parrellel.
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        send_term(child);
    }
    send_term(&mut obs_child);

    // kill children before returning final_result
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        kill_child(child);
        info!(target: "restart-test", "kill and wait on child complete for final result");
    }
    kill_child(&mut obs_child);
    res
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_restarts_delayed() -> eyre::Result<()> {
    do_restarts(70)
}

/// Start a process running a validator node.
fn start_validator(instance: usize, exe_path: &Path, base_dir: &Path, mut rpc_port: u16) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    // The instance option will still change a set port so account for that.
    rpc_port += instance as u16;
    let mut command = Command::new(exe_path);

    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--instance")
        .arg(format!("{}", instance + 1))
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"));

    #[cfg(feature = "faucet")]
    command
        .arg("--public-key") // If the binary is built with the faucet need this to start...
        .arg("0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99");

    command.spawn().expect("failed to execute")
}

/// Start a process running an observer node.
fn start_observer(instance: usize, exe_path: &Path, base_dir: &Path, mut rpc_port: u16) -> Child {
    let data_dir = base_dir.join("observer");
    // The instance option will still change a set port so account for that.
    rpc_port += instance as u16;
    let mut command = Command::new(exe_path);
    command
        .env("TN_BLS_PASSPHRASE", "restart_test")
        .arg("node")
        .arg("--observer")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--instance")
        .arg(format!("{}", instance + 1))
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"));
    command.spawn().expect("failed to execute")
}

fn test_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
    info!(target: "restart-test", "calling get_block for {:?}", &client_urls[0]);
    let block0 = get_block(&client_urls[0], None)?;
    let number = u64::from_str_radix(&block0["number"].as_str().unwrap_or("0x100_000")[2..], 16)?;
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[1]);
    let block = get_block(&client_urls[1], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[2]);
    let block = get_block(&client_urls[2], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same! block0: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    info!(target: "restart-test", ?number, "success - now calling get_block for {:?}", &client_urls[3]);
    let block = get_block(&client_urls[3], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    info!(target: "restart-test", "all rpcs returned same block hash");
    Ok(())
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the TEL and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let res_str: String =
        call_rpc(node, "eth_getBalance", rpc_params!(address, "latest"), retries)?;
    info!(target: "restart-test", "get_balance for {node}: parsing string {res_str}");
    let tel = u128::from_str_radix(&res_str[2..], 16)?;
    info!(target: "restart-test", "get_balance for {node}: {tel:?}");
    Ok(tel)
}

/// Retry up to 10 times to retrieve an account balance > 0.
fn get_positive_balance_with_retry(node: &str, address: &str) -> eyre::Result<u128> {
    get_balance_above_with_retry(node, address, 0)
}

/// Retry up to 30 times to retrieve an account balance > above.
///
/// Max time to get balance is 1min.
fn get_balance_above_with_retry(node: &str, address: &str, above: u128) -> eyre::Result<u128> {
    let mut bal = get_balance(node, address, 5)?;
    let mut i = 0;
    while i < 45 && bal <= above {
        std::thread::sleep(Duration::from_millis(1200));
        i += 1;
        bal = get_balance(node, address, 5)?;
    }
    if i == 45 && bal <= above {
        error!(target:"restart-test", "get_balance_above_with_retry i == 30 - returning error!!");
        Err(Report::msg(format!("Failed to get a balance {bal} for {address} above {above}")))
    } else {
        Ok(bal)
    }
}

/// If key starts with 0x then return it otherwise generate the key from the key string.
fn get_key(key: &str) -> String {
    if key.starts_with("0x") {
        key.to_string()
    } else {
        let (_, _, key) = account_from_word(key);
        key
    }
}

fn get_block(node: &str, block_number: Option<u64>) -> eyre::Result<HashMap<String, Value>> {
    let params = if let Some(block_number) = block_number {
        rpc_params!(format!("0x{block_number:x}"), true)
    } else {
        rpc_params!("latest", true)
    };
    call_rpc(node, "eth_getBlockByNumber", params.clone(), 10)
}

fn get_block_number(node: &str) -> eyre::Result<u64> {
    let block = get_block(node, None)?;
    Ok(u64::from_str_radix(&block["number"].as_str().unwrap_or("0x100_000")[2..], 16)?)
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
fn address_from_word(key_word: &str) -> Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (_, public_key) = secp.generate_keypair(&mut rand);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    Address::from_slice(&hash[12..])
}

/// Return the (account, public key, secret key) generated from key_word.
fn account_from_word(key_word: &str) -> (String, String, String) {
    let seed = keccak256(key_word.as_bytes());
    let mut rand =
        <secp256k1::rand::rngs::StdRng as secp256k1::rand::SeedableRng>::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand);
    let keypair = Keypair::from_secret_key(&secp, &secret_key);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
    let address = Address::from_slice(&hash[12..]);
    let pubkey = keypair.public_key().serialize();
    let secret = keypair.secret_bytes();
    (address.to_string(), const_hex::encode(pubkey), const_hex::encode(secret))
}

/// Create, sign and submit a TXN to transfer TEL from key's account to to_account.
fn send_tel(
    node: &str,
    key: &str,
    to_account: Address,
    amount: u128,
    gas_price: u128,
    gas: u128,
    nonce: u128,
) -> eyre::Result<()> {
    let mut to_addr = [0_u8; 20];
    //const_hex::decode_to_slice(to_account, &mut to_addr[..])?;
    to_addr.copy_from_slice(to_account.as_slice());
    let (from_account, _, _) = decode_key(key)?;
    let new_transaction = LegacyTransaction {
        chain: 0x7e1,
        nonce,
        to: Some(to_addr),
        value: amount,
        gas_price,
        gas,
        data: vec![/* contract code or other data */],
    };
    let secret_key = SecretKey::from_slice(&const_hex::decode(key)?)?;
    let ecdsa = new_transaction
        .ecdsa(&secret_key.secret_bytes())
        .map_err(|_| Report::msg("Failed to get ecdsa"))?;
    let transaction_bytes = new_transaction.sign(&ecdsa);
    let res_str: String = call_rpc(
        node,
        "eth_sendRawTransaction",
        rpc_params!(const_hex::encode(transaction_bytes)),
        1,
    )?;
    info!(target: "restart-test", "Submitted TEL transfer from {from_account} to {to_account} for {amount}: {res_str}");
    Ok(())
}

/// Decode a secret key into it's public key and account.
/// Returns a tuple of (account, public_key, public_key_long) as hex encoded strings.
fn decode_key(key: &str) -> eyre::Result<(String, String, String)> {
    match const_hex::decode(key) {
        Ok(key) => {
            match SecretKey::from_slice(&key) {
                Ok(secret_key) => {
                    let secp = Secp256k1::new();
                    let keypair = Keypair::from_secret_key(&secp, &secret_key);
                    let public_key = keypair.public_key();
                    // strip out the first byte because that should be the
                    // SECP256K1_TAG_PUBKEY_UNCOMPRESSED tag returned by
                    // libsecp's uncompressed pubkey serialization
                    let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
                    let address = Address::from_slice(&hash[12..]);
                    Ok((
                        address.to_string(),
                        const_hex::encode(public_key.serialize()),
                        const_hex::encode(public_key.serialize_uncompressed()),
                    ))
                }
                Err(err) => Err(Report::msg(err.to_string())),
            }
        }
        Err(err) => Err(Report::msg(err.to_string())),
    }
}

/// Make an RPC call to node with command and params.
/// Wraps any Eyre otherwise returns the result as a String.
/// This is for testing and will try up to retries times at one second intervals to send the
/// request.
fn call_rpc<R, Params>(node: &str, command: &str, params: Params, retries: usize) -> eyre::Result<R>
where
    R: DeserializeOwned,
    Params: jsonrpsee::core::traits::ToRpcParams + Send + Clone,
{
    // jsonrpsee is async AND tokio specific so give it a runtime (and can't use a crate like
    // pollster)...
    let runtime = Builder::new_current_thread().enable_io().enable_time().build()?;

    let resp = runtime.block_on(async move {
        let client = HttpClientBuilder::default().build(node).expect("couldn't build rpc client");
        let mut resp = client.request(command, params.clone()).await;
        let mut i = 0;
        while i < retries && resp.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let client =
                HttpClientBuilder::default().build(node).expect("couldn't build rpc client");
            resp = client.request(command, params.clone()).await;
            i += 1;
        }
        resp
    });
    Ok(resp?)
}

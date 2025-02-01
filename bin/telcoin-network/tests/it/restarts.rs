use crate::util::{config_local_testnet, IT_TEST_MUTEX};
use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use rand::{rngs::StdRng, SeedableRng};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::{value::RawValue, Value};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};
use tn_types::{get_available_tcp_port, keccak256, Address};
use tokio::runtime::Runtime;
use tracing::{error, info};

/// One unit of TEL (10^18) measured in wei.
const WEI_PER_TEL: u128 = 1_000_000_000_000_000_000;

/// Helper function to shutdown child processes and log errors.
fn kill_child(child: &mut Child) {
    // The code below will send SIGKILL without the use of nix.
    //if let Err(e) = child.kill() {
    //    error!(target: "restart-test", ?e, "error killing child");
    //}
    if let Err(e) = signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM) {
        error!(target: "restart-test", ?e, "error killing child");
    }

    if let Err(e) = child.wait() {
        error!(target: "restart-test", ?e, "error waiting for child to die");
    }
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
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    // send tel and kill child2 if error
    send_tel(&client_urls[1], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 0).inspect_err(
        |e| {
            kill_child(child2);
            error!(target: "restart-test", ?e);
        },
    )?;

    // sleep
    std::thread::sleep(Duration::from_millis(1000));
    info!(target: "restart-test", "calling get_positive_balance_with_retry in tests1...");

    // get positive bal and kill child2 if error
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())
        .inspect_err(|e| {
            kill_child(child2);
            error!(target: "restart-test", ?e);
        })?;

    if 10 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1: 10 * WEI_PER_TEL != bal - returning error!");
        kill_child(child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }

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
            error!(target: "restart-test", ?e);
        })?;
    if 10 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1 after restart: 10 * WEI_PER_TEL != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 10 * WEI_PER_TEL)));
    }
    send_tel(&client_urls[0], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 1).inspect_err(
        |e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e);
        },
    )?;
    let bal = get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), bal)
        .inspect_err(|e| {
            kill_child(&mut child2);
            error!(target: "restart-test", ?e);
        })?;
    if 20 * WEI_PER_TEL != bal {
        error!(target: "restart-test", "tests1 after restart: 20 * WEI_PER_TEL != bal - returning error!");
        kill_child(&mut child2);
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 20 * WEI_PER_TEL)));
    }
    test_blocks_same(client_urls).inspect_err(|e| {
        kill_child(&mut child2);
        error!(target: "restart-test", ?e);
    })?;
    Ok(child2)
}

/// Run the second part of tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests2(client_urls: &[String; 4]) -> eyre::Result<()> {
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())?;
    if 20 * WEI_PER_TEL != bal {
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 20 * WEI_PER_TEL)));
    }
    send_tel(&client_urls[0], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 2)?;
    let bal = get_balance_above_with_retry(&client_urls[3], &to_account.to_string(), bal)?;
    if 30 * WEI_PER_TEL != bal {
        return Err(Report::msg(format!("Expected a balance of {} got {bal}!", 30 * WEI_PER_TEL)));
    }
    test_blocks_same(client_urls)?;
    Ok(())
}

fn do_restarts(delay: u64) -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    info!(target: "restart-test", "do_restarts, delay: {delay}");
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    let rt = Runtime::new()?;
    rt.block_on(config_local_testnet(temp_path.clone())).expect("failed to config");
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
    // Restart network
    for (i, child) in children.iter_mut().enumerate() {
        *child = Some(start_validator(i, &exe_path, &temp_path, rpc_ports[i]));
    }

    info!(target: "restart-test", "Running restart tests 2");
    let res2 = run_restart_tests2(&client_urls);
    info!(target: "restart-test", "Ran restart tests 2: {res2:?}");

    // kill children before returnin final_result
    for child in children.iter_mut() {
        let child = child.as_mut().expect("missing a child");
        kill_child(child);
        info!(target: "restart-test", "kill and wait on child complete for final result");
    }

    res2
}

/// Test a restart case with a short delay, the stopped node should rejoin consensus.
#[test]
fn test_restartstt() -> eyre::Result<()> {
    tn_test_utils::init_test_tracing();
    do_restarts(2)
}

/// Test a restart case with a long delay, the stopped node should not rejoin consensus but follow
/// the consensus chain.
#[test]
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
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--chain")
        .arg("adiri")
        .arg("--disable-discovery")
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

fn test_blocks_same(client_urls: &[String; 4]) -> eyre::Result<()> {
    let block0 = get_block(&client_urls[0], None)?;
    let number = u64::from_str_radix(&block0["number"].as_str().unwrap_or("0x100_000")[2..], 16)?;
    let block = get_block(&client_urls[1], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    let number = u64::from_str_radix(&block["number"].as_str().unwrap_or_default()[2..], 16)?;
    let block = get_block(&client_urls[2], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg(format!(
            "Blocks between validators not the same! block0: {:?} - block: {:?}",
            block0["hash"], block["hash"]
        )));
    }
    let number = u64::from_str_radix(&block["number"].as_str().unwrap_or_default()[2..], 16)?;
    let block = get_block(&client_urls[3], Some(number))?;
    if block0["hash"] != block["hash"] {
        return Err(Report::msg("Blocks between validators not the same!".to_string()));
    }
    Ok(())
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the TEL and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let params = RawValue::from_string(format!("[\"{address}\", \"latest\"]"))?;
    let res_str = call_rpc(node, "eth_getBalance", Some(&params), retries)?;
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
    let mut bal = get_balance(node, address, 5).unwrap_or(0);
    let mut i = 0;
    while i < 35 && bal <= above {
        std::thread::sleep(Duration::from_millis(1200));
        i += 1;
        bal = get_balance(node, address, 5).unwrap_or(0);
    }
    if i == 30 && bal <= above {
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
        RawValue::from_string(format!("[\"0x{block_number:x}\", true]"))?
    } else {
        RawValue::from_string("[\"latest\", true]".to_string())?
    };
    let mut block = call_rpc(node, "eth_getBlockByNumber", Some(&params), 5)?;
    while block.is_empty() {
        block = call_rpc(node, "eth_getBlockByNumber", Some(&params), 5)?;
    }
    Ok(serde_json::from_str(&block)?)
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similiar functionality in the test client to allow easy testing using simple strings
/// for accounts.
fn address_from_word(key_word: &str) -> Address {
    let seed = keccak256(key_word.as_bytes());
    let mut rand = <StdRng as SeedableRng>::from_seed(seed.0);
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
    let mut rand = rand::rngs::StdRng::from_seed(seed.0);
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
    let params = RawValue::from_string(format!("[\"{}\"]", const_hex::encode(transaction_bytes)))?;
    let res_str = call_rpc(node, "eth_sendRawTransaction", Some(&params), 5)?;
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
fn call_rpc(
    node: &str,
    command: &str,
    params: Option<&RawValue>,
    retries: usize,
) -> eyre::Result<String> {
    tracing::debug!(target: "restart-test", ?node, ?command, "call rpc");
    match jsonrpc::simple_http::SimpleHttpTransport::builder().url(node) {
        Ok(trans) => {
            let client = jsonrpc::Client::with_transport(trans.build());
            let req = client.build_request(command, params);
            let mut resp = client.send_request(req.clone());
            let mut i = 1;
            while i < retries && resp.is_err() {
                std::thread::sleep(Duration::from_millis(1000));
                resp = client.send_request(req.clone());
                i += 1;
            }
            let resp = resp?;
            if let Some(err) = resp.error {
                Err(Report::msg(format!("RPC error: {}", err.message)))
            } else if let Ok(res) = resp.result() {
                Ok(res)
            } else if let Some(result) = resp.result {
                Ok(result.get().to_string())
            } else {
                Ok("".to_string())
            }
        }
        Err(e) => Err(Report::msg(format!("Error connecting to {node}: {e}"))),
    }
}

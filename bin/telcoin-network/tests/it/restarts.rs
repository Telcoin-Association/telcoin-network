use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use ethereum_tx_sign::{LegacyTransaction, Transaction};
use eyre::Report;
use rand::{rngs::StdRng, SeedableRng};
use reth_primitives::{alloy_primitives, keccak256, Address};
use secp256k1::{Keypair, Secp256k1, SecretKey};
use serde_json::value::RawValue;
use tn_types::utils::get_available_tcp_port;
use tokio::runtime::Runtime;

use crate::util::config_local_testnet;

const WEI_PER_TEL: u128 = 1_000_000_000_000_000_000;

/// Run the first part tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests1(
    client_urls: &[String; 4],
    mut child2: Child,
    exe_path: &Path,
    temp_path: &Path,
    rpc_port2: u16,
) -> eyre::Result<Child> {
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    send_tel(&client_urls[1], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 0)?;
    std::thread::sleep(Duration::from_millis(1000));
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())?;
    assert_eq!(10 * WEI_PER_TEL, bal);
    child2.kill()?;
    child2.wait()?;
    std::thread::sleep(Duration::from_millis(3000));
    // This validator should be down now, confirm.
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 5).is_err());
    // Restart
    let child2 = start_validator(2, &exe_path, &temp_path, rpc_port2);
    let bal = get_positive_balance_with_retry(&client_urls[2], &to_account.to_string())?;
    assert_eq!(10 * WEI_PER_TEL, bal);
    send_tel(&client_urls[0], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 1)?;
    let bal = get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), bal)?;
    assert_eq!(20 * WEI_PER_TEL, bal);
    Ok(child2)
}

/// Run the second part of tests, broken up like this to allow more robust node shutdown.
fn run_restart_tests2(client_urls: &[String; 4]) -> eyre::Result<()> {
    let key = get_key("test-source");
    let to_account = address_from_word("testing");
    let bal =
        get_balance_above_with_retry(&client_urls[2], &to_account.to_string(), 20 * WEI_PER_TEL)?;
    assert_eq!(20 * WEI_PER_TEL, bal);
    send_tel(&client_urls[0], &key, to_account, 10 * WEI_PER_TEL, 250, 21000, 2)?;
    let bal = get_balance_above_with_retry(&client_urls[3], &to_account.to_string(), bal)?;
    assert_eq!(30 * WEI_PER_TEL, bal);
    Ok(())
}

#[test]
fn test_restarts() {
    // the tmp dir should be removed once tmp_quard is dropped
    let tmp_guard = tempfile::TempDir::new().expect("tempdir is okay");
    // create temp path for test
    let temp_path = tmp_guard.path().to_path_buf();
    let rt = Runtime::new().expect("tokio failed");
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
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child {i}!");
        rpc_ports[i] = rpc_port;
        client_urls[i].push_str(&format!(":{rpc_port}"));
        children[i] = Some(start_validator(i, &exe_path, &temp_path, rpc_port));
    }

    let res1 = run_restart_tests1(
        &client_urls,
        children[2].take().expect("missing child 2"),
        &exe_path,
        &temp_path,
        rpc_ports[2],
    );
    let is_ok = res1.is_ok();
    match res1 {
        Ok(mut child2) => {
            let _ = child2.kill();
            let _ = child2.wait();
        }
        Err(err) => {
            println!("Got error: {err}");
        }
    }
    for i in 0..4 {
        // Best effort to kill all the nodes.
        if i != 2 {
            let child = children[i].as_mut().expect("missing a child");
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    // Make sure we shutdown nodes even if an error in first testing.
    assert!(is_ok);
    let to_account = address_from_word("testing");
    // The validators should be down now, confirm.
    assert!(get_balance(&client_urls[0], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[1], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[2], &to_account.to_string(), 5).is_err());
    assert!(get_balance(&client_urls[3], &to_account.to_string(), 5).is_err());
    // Restart network
    for i in 0..4 {
        children[i] = Some(start_validator(i, &exe_path, &temp_path, rpc_ports[i]));
    }

    let res2 = run_restart_tests2(&client_urls);

    for i in 0..4 {
        let child = children[i].as_mut().expect("missing a child");
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(res2.is_ok());
}

/// Start a process running a validator node.
fn start_validator(instance: usize, exe_path: &Path, base_dir: &Path, mut rpc_port: u16) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    // The instance option will still change a set port so account for that.
    rpc_port += instance as u16;
    Command::new(exe_path)
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
        .arg(&format!("{rpc_port}"))
        .arg("--public-key") // If the binary is built with the faucet need this to start...
        .arg("0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99")
        .spawn()
        .expect("failed to execute")
}

/// Send an RPC call to node to get the latest balance for address.
/// Return a tuple of the TEL and remainder (any value left after dividing by 1_e18).
/// Note, balance is in wei and must fit in an u128.
fn get_balance(node: &str, address: &str, retries: usize) -> eyre::Result<u128> {
    let params = RawValue::from_string(format!("[\"{address}\", \"latest\"]"))
        .expect("Failed to create params for balance query!");
    let res_str = call_rpc(node, "eth_getBalance", Some(&params), retries)?;
    let tel = u128::from_str_radix(&res_str[2..], 16)?;
    Ok(tel)
}

/// Retry up to 10 times to retrieve an account balance > 0.
fn get_positive_balance_with_retry(node: &str, address: &str) -> eyre::Result<u128> {
    get_balance_above_with_retry(node, address, 0)
}

/// Retry up to 10 times to retrieve an account balance > above.
fn get_balance_above_with_retry(node: &str, address: &str, above: u128) -> eyre::Result<u128> {
    let mut bal = get_balance(node, address, 5).unwrap_or(0);
    let mut i = 0;
    while i < 10 && bal <= above {
        std::thread::sleep(Duration::from_millis(1000));
        i += 1;
        bal = get_balance(node, address, 5).unwrap_or(0);
    }
    Ok(bal)
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
    let seed = alloy_primitives::keccak256(key_word.as_bytes());
    let mut rand = rand::rngs::StdRng::from_seed(seed.0);
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand);
    let keypair = Keypair::from_secret_key(&secp, &secret_key);
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = alloy_primitives::keccak256(&public_key.serialize_uncompressed()[1..]);
    let address = alloy_primitives::Address::from_slice(&hash[12..]);
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
    let params = RawValue::from_string(format!("[\"{}\"]", const_hex::encode(transaction_bytes)))
        .expect("Failed to create params for balance query!");
    let res_str = call_rpc(node, "eth_sendRawTransaction", Some(&params), 5)?;
    println!("Submitted TEL transfer from {from_account} to {to_account} for {amount}: {res_str}");
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
                    let hash =
                        alloy_primitives::keccak256(&public_key.serialize_uncompressed()[1..]);
                    let address = alloy_primitives::Address::from_slice(&hash[12..]);
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

//! Cryptographic boundary cases for the self-installation policy.

use crate::{
    batch_allowlisted_authorities, batch_allowlisted_signed_authorities, Address, Authorization,
    EthSignature, PooledTransaction, SignableTransaction as _, TxEip1559, TxEip7702, U256,
};
use alloy::signers::{local::PrivateKeySigner, SignerSync as _};

/// Valid self and foreign signatures, including a foreign tuple after a valid self tuple.
#[test]
fn self_authorization_policy_checks_every_recoverable_tuple() -> Result<(), &'static str> {
    let alice = PrivateKeySigner::from_slice(&[1; 32]).map_err(|_| "Alice key")?;
    let bob = PrivateKeySigner::from_slice(&[2; 32]).map_err(|_| "Bob key")?;
    let authorization =
        Authorization { chain_id: U256::from(2017), address: Address::ZERO, nonce: 1 };
    let alice_signature =
        alice.sign_hash_sync(&authorization.signature_hash()).map_err(|_| "Alice signature")?;
    let bob_signature =
        bob.sign_hash_sync(&authorization.signature_hash()).map_err(|_| "Bob signature")?;
    let own = authorization.clone().into_signed(alice_signature);
    let foreign = authorization.clone().into_signed(bob_signature);
    let mut tx =
        TxEip7702 { chain_id: 2017, authorization_list: vec![own.clone()], ..Default::default() };
    assert!(batch_allowlisted_authorities(&tx, alice.address()));
    assert!(!batch_allowlisted_authorities(&tx, bob.address()));
    tx.authorization_list = vec![own.clone(), foreign.clone()];
    assert!(!batch_allowlisted_authorities(&tx, alice.address()));
    tx.authorization_list = vec![foreign, own.clone()];
    assert!(!batch_allowlisted_authorities(&tx, alice.address()));
    tx.authorization_list = vec![own.clone(), own];
    assert!(batch_allowlisted_authorities(&tx, alice.address()));
    // An unrecoverable tuple is inert under EIP-7702 and still pays its intrinsic gas.
    tx.authorization_list.push(authorization.into_signed(EthSignature::new(
        U256::ZERO,
        U256::ZERO,
        false,
    )));
    assert!(batch_allowlisted_authorities(&tx, alice.address()));
    let signature = alice.sign_hash_sync(&tx.signature_hash()).map_err(|_| "outer signature")?;
    assert!(batch_allowlisted_signed_authorities(&PooledTransaction::Eip7702(
        tx.clone().into_signed(signature)
    )));
    let invalid = PooledTransaction::Eip7702(tx.into_signed(EthSignature::new(
        U256::ZERO,
        U256::ZERO,
        false,
    )));
    assert!(!batch_allowlisted_signed_authorities(&invalid));
    assert!(batch_allowlisted_authorities(&TxEip1559::default(), alice.address()));
    Ok(())
}

/// A foreign authority stays forbidden for wildcard chain IDs and currently inert tuples.
#[test]
fn foreign_authorizations_cannot_hide_behind_chain_id_or_nonce() -> Result<(), &'static str> {
    let authority = PrivateKeySigner::from_slice(&[1; 32]).map_err(|_| "authority key")?;
    [0_u64, 2017, 2018].into_iter().try_for_each(|chain_id| {
        let authorization = Authorization {
            chain_id: U256::from(chain_id),
            address: Address::ZERO,
            nonce: u64::MAX,
        };
        let signature =
            authority.sign_hash_sync(&authorization.signature_hash()).map_err(|_| "signature")?;
        let tx = TxEip7702 {
            chain_id: 2017,
            authorization_list: vec![authorization.into_signed(signature)],
            ..Default::default()
        };
        assert!(!batch_allowlisted_authorities(&tx, Address::ZERO));
        Ok(())
    })
}

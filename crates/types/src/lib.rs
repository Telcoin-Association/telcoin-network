// SPDX-License-Identifier: Apache-2.0
#![allow(missing_docs)]

// Used in tests
#[cfg(test)]
use proptest as _;
#[cfg(test)]
use tn_storage as _;
#[cfg(test)]
use tn_test_utils_committee as _;

mod canonical_reader;
mod codec;
#[allow(clippy::mutable_key_type)]
mod committee;
pub mod consensus_chain_traits;
mod crypto;
pub mod database_traits;
pub mod gas_accumulator;
mod genesis;
mod helpers;
mod notifier;
mod primary;
mod serde;
mod sync;
mod task_manager;
mod worker;
#[macro_use]
pub mod error;
pub mod forks;

pub use canonical_reader::*;
pub use codec::*;
pub use committee::*;
pub use consensus_chain_traits::*;
pub use crypto::*;
pub use database_traits::*;
pub use genesis::*;
pub use helpers::*;
pub use notifier::*;
pub use primary::*;
pub use sync::*;
pub use task_manager::*;
pub use worker::*;
#[cfg(feature = "test-utils")]
pub mod test_utils;

// re-exports for easier maintainability
pub use alloy::{
    consensus::{
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        transaction::TransactionMeta,
        BlockHeader, Header as ExecHeader, SignableTransaction, Transaction as TransactionTrait,
        TxEip1559, TxEip7702,
    },
    eips::{
        eip1559::{ETHEREUM_BLOCK_GAS_LIMIT_30M, MIN_PROTOCOL_BASE_FEE},
        eip2718::{Decodable2718, Encodable2718, Typed2718},
        eip4844::{env_settings::EnvKzgSettings, BlobAndProofV1, BlobTransactionSidecar},
        eip7702::Authorization,
        BlockHashOrNumber, BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{
        address, hex_literal, keccak256, Address, BlockHash, BlockNumber, Bloom, Bytes, Log,
        LogData, Sealable, TxHash, TxKind, TxNumber, B256, U160, U256,
    },
    rpc::types::{AccessList, Withdrawals},
    signers::Signature as EthSignature,
    sol,
    sol_types::{SolCall, SolType, SolValue},
};
pub use libp2p::{multiaddr::Protocol, Multiaddr};

/// Whether a transaction's EIP-2718 type is on the batch executable allowlist
/// (legacy, EIP-2930, EIP-1559, EIP-7702).
///
/// One predicate shared by the batch validator, the batch builder, and the
/// worker gateway so producers and validators can never disagree on the set.
/// Deliberately fork-blind: uniform across chain configurations.
///
/// EIP-4844 stays excluded: batches carry raw transaction bytes with no
/// transport for blob sidecars or KZG proofs, so a blob transaction could
/// never be verified or executed from a batch.
///
/// # LOCKSTEP FLEET UPGRADE (EIP-7702)
///
/// Admitting type `0x04` activates the 7702-aware gas-penalty math in the EVM
/// handler's `reimburse_caller` on live balances. A validator running an older
/// binary — one without the two-argument authorization-intrinsic subtraction —
/// computes a different post-execution balance for the same certified batch
/// and forks state at the live edge. Chain history is clean (no 7702
/// transaction has ever been certified), so there is no replay risk; the risk
/// window is only a mixed-version fleet during rollout. Every validator must
/// run a binary at or beyond this change before the first 7702 transaction is
/// admitted.
pub fn batch_allowlisted_tx_type<T: Typed2718>(tx: &T) -> bool {
    tx.is_legacy() || tx.is_eip2930() || tx.is_eip1559() || tx.is_eip7702()
}

/// Whether a transaction's EIP-7702 authorization list is within the batch
/// executable bound: `true` for any non-7702 transaction; for a type-0x04
/// transaction, `true` iff the list length is in
/// `1..=max_tx_authorizations(epoch)`.
///
/// One predicate shared by the batch validator, the batch builder, the worker
/// gateway, and the pool wrap so producers and validators can never disagree
/// on the bound. See [`max_tx_authorizations`] for the derivation and the
/// no-false-rejection argument: an over-cap transaction can never execute
/// inside a batch, so rejecting it refuses only garbage.
///
/// The empty list is excluded because EIP-7702 declares an empty
/// authorization list invalid (revm rejects it at execution; reth's pool
/// rejects it at admission), so a batch carrying one wastes certified work.
pub fn batch_allowlisted_authorization_list<T: TransactionTrait>(tx: &T, epoch: Epoch) -> bool {
    !tx.is_eip7702()
        || tx.authorization_list().is_some_and(|list| {
            u64::try_from(list.len())
                .is_ok_and(|len| (1..=max_tx_authorizations(epoch)).contains(&len))
        })
}
pub use reth_primitives::{
    Account, Block, BlockBody, EthPrimitives, NodePrimitives, PooledTransaction, Receipt,
    Recovered, RecoveredBlock, SealedBlock, SealedHeader, Transaction, TransactionSigned,
};

#[cfg(test)]
mod allowlist_tests {
    use super::*;

    /// A bare EIP-2718 type byte: the allowlist consults nothing else.
    struct TypeByte(u8);

    impl Typed2718 for TypeByte {
        fn ty(&self) -> u8 {
            self.0
        }
    }

    #[test]
    fn allowlist_admits_legacy_2930_1559_and_7702_only() {
        for admitted in [0x00, 0x01, 0x02, 0x04] {
            assert!(
                batch_allowlisted_tx_type(&TypeByte(admitted)),
                "type {admitted:#04x} must be admitted"
            );
        }
        for rejected in [0x03, 0x05, 0x7e] {
            assert!(
                !batch_allowlisted_tx_type(&TypeByte(rejected)),
                "type {rejected:#04x} must be rejected"
            );
        }
    }

    /// Build a type-0x04 transaction carrying `n` dummy-signed authorization
    /// tuples. The predicate only reads the list length, so dummy signatures
    /// suffice.
    fn tx_7702_with_tuples(n: usize) -> TxEip7702 {
        let dummy_signature = EthSignature::new(U256::from(1), U256::from(1), false);
        let authorization_list = (0..n)
            .map(|_| {
                Authorization { chain_id: U256::from(2017), address: Address::ZERO, nonce: 1 }
                    .into_signed(dummy_signature)
            })
            .collect();
        TxEip7702 {
            chain_id: 2017,
            nonce: 0,
            gas_limit: 100_000,
            max_fee_per_gas: 100,
            max_priority_fee_per_gas: 0,
            to: Address::ZERO,
            value: U256::ZERO,
            access_list: Default::default(),
            authorization_list,
            input: Default::default(),
        }
    }

    #[test]
    fn max_tx_authorizations_is_pinned_to_1199() {
        // (30,000,000 - 21,000) / 25,000 = 1199. A change here changes what
        // every enforcement site admits — it must be deliberate.
        assert_eq!(max_tx_authorizations(0), 1199);
    }

    #[test]
    fn authorization_list_predicate_truth_table() {
        // Non-7702 transactions always pass: the bound applies to type 0x04 only.
        let non_7702 = TxEip1559 {
            chain_id: 2017,
            nonce: 0,
            gas_limit: 100_000,
            max_fee_per_gas: 100,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Default::default(),
        };
        assert!(batch_allowlisted_authorization_list(&non_7702, 0), "non-7702 must pass");

        // 7702: empty is invalid per EIP-7702, 1..=cap passes, cap+1 fails.
        let cap = usize::try_from(max_tx_authorizations(0)).expect("cap fits usize");
        assert!(!batch_allowlisted_authorization_list(&tx_7702_with_tuples(0), 0), "empty list");
        assert!(batch_allowlisted_authorization_list(&tx_7702_with_tuples(1), 0), "one tuple");
        assert!(batch_allowlisted_authorization_list(&tx_7702_with_tuples(cap), 0), "at cap");
        assert!(
            !batch_allowlisted_authorization_list(&tx_7702_with_tuples(cap + 1), 0),
            "over cap"
        );
    }
}

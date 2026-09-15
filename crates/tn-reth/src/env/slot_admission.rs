//! Pinned transaction admission for ordered sender slots.

use super::RethEnv;
use crate::{error::TnRethError, recover_raw_transaction};
use alloy_evm::IntoTxEnv as _;
use reth_evm::{ConfigureEvm as _, EvmFactory as _};
use reth_provider::ProviderError;
use reth_revm::context::result::EVMError;
use std::fmt;
use tn_types::{
    batch_allowlisted_tx_type, max_batch_gas, BatchSlotError, BatchSlotMessage, BatchSlots,
    SignedBatchSlotRecord, Typed2718 as _, B256,
};

impl RethEnv {
    /// Validate sender ownership and transaction eligibility at the slot's canonical opening.
    ///
    /// The private EVM journal advances each sender's nonce and reserves the full maximum cost.
    /// It never executes bytecode or publishes state. Archive mode and the restore-floor guard
    /// are required, as for all other pinned execution reads.
    pub fn validate_slot_transactions(
        &self,
        slots: &BatchSlots,
        record: &SignedBatchSlotRecord,
    ) -> Result<(), BatchSlotAdmissionError> {
        record.authenticate(slots).map_err(BatchSlotAdmissionError::Protocol)?;
        match record.message() {
            BatchSlotMessage::Timeout { .. } => Ok(()),
            BatchSlotMessage::Proposal { position, batch } => {
                if batch.transactions.is_empty() {
                    Err(BatchSlotAdmissionError::EmptyProposal)
                } else {
                    let parent = position.parent().execution();
                    let header = self
                        .sealed_header_by_hash(parent)
                        .map_err(BatchSlotAdmissionError::Provider)?
                        .ok_or(BatchSlotAdmissionError::MissingOpening(parent))?;
                    let mut database = self
                        .read_only_state_db(&header)
                        .map_err(|error| BatchSlotAdmissionError::Provider(error.into()))?;
                    let mut environment = self
                        .evm_config()
                        .evm_env(&header)
                        .map_err(|error| BatchSlotAdmissionError::Environment(error.to_string()))?;
                    environment.block_env.basefee = batch.base_fee_per_gas;
                    environment.block_env.gas_limit = max_batch_gas(batch.epoch);
                    environment.block_env.beneficiary = batch.beneficiary;
                    let mut evm =
                        self.evm_config().evm_factory().create_evm(&mut database, environment);
                    batch.transactions.iter().try_for_each(|bytes| {
                        let transaction = recover_raw_transaction(bytes)
                            .map_err(BatchSlotAdmissionError::Provider)?;
                        match () {
                            () if !batch_allowlisted_tx_type(transaction.inner()) => {
                                Err(BatchSlotAdmissionError::UnsupportedType(transaction.ty()))
                            }
                            () if slots.bucket(transaction.signer()) != position.bucket() => {
                                Err(BatchSlotAdmissionError::WrongBucket)
                            }
                            () => evm
                                .admit_slot_transaction(transaction.into_tx_env())
                                .map_err(BatchSlotAdmissionError::Transaction),
                        }
                    })
                }
            }
        }
    }
}

/// Invalid slot transactions or unavailable canonical admission state.
#[derive(Debug)]
pub enum BatchSlotAdmissionError {
    /// Signed record authentication failed.
    Protocol(BatchSlotError),
    /// The opening header cannot be found in canonical execution storage.
    MissingOpening(B256),
    /// Execution storage or signed transaction recovery failed.
    Provider(TnRethError),
    /// The execution configuration cannot construct the pinned environment.
    Environment(String),
    /// Revm rejected an environment, intrinsic-gas, nonce, code or balance check.
    Transaction(EVMError<reth_revm::db::bal::EvmDatabaseError<ProviderError>>),
    /// An execution proposal must contain at least one transaction.
    EmptyProposal,
    /// A transaction's sender belongs to another bucket.
    WrongBucket,
    /// Transaction type is outside TN's executable allowlist.
    UnsupportedType(u8),
}

impl fmt::Display for BatchSlotAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "batch slot admission failed: {self:?}")
    }
}

impl std::error::Error for BatchSlotAdmissionError {}

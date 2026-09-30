//! Defense-in-depth regression for issue #933.
//!
//! The certified-output execution path (`RethEnv::build_block_from_batch_payload`) must not
//! fatally halt the engine on a transaction that fails signer recovery. A certified sub-DAG is
//! fixed and identical on every honest node, so an unrecoverable transaction is dropped
//! deterministically (mirroring the `InvalidTx` tolerance of the execute phase) rather than
//! aborting the whole block. Aborting would return `Err` into the engine loop, halting the
//! network and, on restart-replay of the same certified output, crash-looping every node.

use crate::pipeline_helpers::PipelineTestEnv;

/// Bytes that cannot be decoded as an EIP-2718 typed transaction, so
/// `reth_recover_raw_transaction` fails on them.
const UNDECODABLE_TX: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

/// A batch whose only transaction is undecodable builds an (empty) block instead of returning a
/// fatal error. Before the fix, the recover phase's fallible `collect` aborted the whole block on
/// the first unrecoverable transaction, propagating an error that stopped the engine loop.
#[test]
fn undecodable_only_batch_builds_empty_block_instead_of_halting() {
    let mut env = PipelineTestEnv::new();
    let block = env
        .execute_block(vec![UNDECODABLE_TX.to_vec()])
        .expect("undecodable-only batch must not fatally halt block building");
    assert!(
        block.execution_output.result.receipts.is_empty(),
        "the undecodable transaction must be dropped, leaving an empty block"
    );
}

/// A batch mixing an undecodable transaction with a valid one drops only the undecodable one and
/// still executes the valid transaction, so a single bad transaction cannot suppress the rest of a
/// certified batch.
#[test]
fn undecodable_tx_is_dropped_valid_tx_survives() {
    let mut env = PipelineTestEnv::new();
    let valid_tx = env.user_precompile_tx(Vec::new());
    let block = env
        .execute_block(vec![UNDECODABLE_TX.to_vec(), valid_tx])
        .expect("a batch with one undecodable tx must still build");
    assert_eq!(
        block.execution_output.result.receipts.len(),
        1,
        "only the undecodable transaction should be dropped; the valid one must execute"
    );
}

//! Example ExEx: Transaction Lifecycle Tracker
//!
//! This ExEx demonstrates every notification type in TN's ExEx system:
//! 1. **CertificateAccepted** — a header is certified (own or peer)
//! 2. **ConsensusCommitted** — a sub-DAG is committed by Bullshark
//! 3. **ChainExecuted** — blocks are executed and finalized
//! 4. **Lagged** — the ExEx fell behind and notifications were dropped
//!
//! It logs each stage so you can see transactions flow through the full lifecycle.
//!
//! An ExEx cannot run standalone — it needs a [`TnExExContext`] handed to it by a
//! running node — so this is a library crate rather than a binary. Register it
//! with `TnBuilder::install_exex` (see [`lifecycle_tracker_exex`]).

use tn_exex::{TnExExContext, TnExExEvent, TnExExNotification};
use tn_types::BlockHeader as _;
use tracing::{debug, info, warn};

/// The lifecycle tracker ExEx.
///
/// Receives notifications covering the full transaction lifecycle and logs each
/// stage. In a real application, this could feed a block explorer, analytics
/// pipeline, or bridge monitoring system.
///
/// # Registering the ExEx
///
/// `lifecycle_tracker_exex` matches the signature `TnBuilder::install_exex`
/// expects, so it can be registered directly:
///
/// ```
/// use exex_lifecycle::lifecycle_tracker_exex;
/// use tn_exex::TnExExContext;
///
/// // In node setup this is simply:
/// //     builder.install_exex("lifecycle-tracker", lifecycle_tracker_exex);
/// //
/// // which requires the function to satisfy this bound:
/// fn assert_installable<F, Fut>(_f: F)
/// where
///     F: FnOnce(TnExExContext) -> Fut,
///     Fut: std::future::Future<Output = eyre::Result<()>>,
/// {
/// }
/// assert_installable(lifecycle_tracker_exex);
/// ```
pub async fn lifecycle_tracker_exex(mut ctx: TnExExContext) -> eyre::Result<()> {
    info!(target: "exex::lifecycle", "Lifecycle tracker ExEx started");

    while let Some(notification) = ctx.notifications.recv().await {
        match notification {
            TnExExNotification::CertificateAccepted { certificate, is_own } => {
                // Stage 1: A header has been certified.
                // The certificate's batches are now included in the consensus DAG.
                let origin = if is_own { "own" } else { "peer" };
                info!(
                    target: "exex::lifecycle",
                    round = certificate.round(),
                    origin = origin,
                    num_batches = certificate.header().payload().len(),
                    "Certificate accepted"
                );
            }

            TnExExNotification::ConsensusCommitted { sub_dag } => {
                // Stage 2: A sub-DAG has been committed by Bullshark.
                // The certificates are now ordered and will be executed.
                info!(
                    target: "exex::lifecycle",
                    leader_round = sub_dag.leader().round(),
                    num_certificates = sub_dag.len(),
                    "Consensus committed sub-DAG"
                );
            }

            TnExExNotification::ChainExecuted { new } => {
                // Stage 3: Blocks have been executed and added to the canonical chain.
                // This is the final stage — transactions are now finalized.
                let mut total_txs = 0u64;
                let mut total_gas = 0u64;

                for block in new.blocks().values() {
                    let tx_count = block.body().transactions().count() as u64;
                    let gas_used = block.header().gas_used();
                    total_txs += tx_count;
                    total_gas += gas_used;
                    debug!(
                        target: "exex::lifecycle",
                        block_number = block.number(),
                        tx_count,
                        gas_used,
                        "Block executed"
                    );
                }

                let tip = new.tip();
                let tip_number = tip.number();
                info!(
                    target: "exex::lifecycle",
                    tip_number,
                    num_blocks = new.blocks().len(),
                    total_txs,
                    total_gas,
                    "Chain executed"
                );

                // Report finished height so the node knows we've processed up to here
                ctx.events.send(TnExExEvent::FinishedHeight(tip_number))?;
            }

            TnExExNotification::Lagged { missed } => {
                // The ExEx fell behind and the manager dropped `missed`
                // notifications (it never blocks consensus). A stateful indexer
                // should reconcile the gap by replaying from its last processed
                // height via `ctx.replay_from(..)`. Here we just log it.
                warn!(
                    target: "exex::lifecycle",
                    missed,
                    "Lagged: notifications dropped; a stateful ExEx should replay to reconcile"
                );
            }
        }
    }

    info!(target: "exex::lifecycle", "Lifecycle tracker ExEx shutting down");
    Ok(())
}

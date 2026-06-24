//! Example ExEx: Transaction Lifecycle Tracker
//!
//! This ExEx demonstrates every notification type in TN's ExEx system. All of
//! them are produced by the consensus-following (observer) path, not the
//! validator hot path:
//! 1. **CertificateAccepted** — a gossip-verified header is certified
//! 2. **ConsensusOutput** — a full consensus output is reconstructed for execution
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
            TnExExNotification::CertificateAccepted { certificate } => {
                // Stage 1: A gossip-verified header has been certified on the
                // consensus-following path. Its batches are now included in the
                // consensus DAG. Followers have no certificates of their own, so
                // there is no own/peer distinction.
                info!(
                    target: "exex::lifecycle",
                    round = certificate.round(),
                    num_batches = certificate.header().payload().len(),
                    "Certificate accepted"
                );
            }

            TnExExNotification::ConsensusOutput { output } => {
                // Stage 2: A full consensus output was reconstructed from a
                // verified consensus header. Its certificates are ordered and
                // will be executed.
                let sub_dag = output.sub_dag();
                info!(
                    target: "exex::lifecycle",
                    leader_round = sub_dag.leader().round(),
                    num_certificates = sub_dag.len(),
                    "Consensus output reconstructed"
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

                // Report finished height so the node knows we've processed up to
                // here. `try_send` is non-blocking; `FinishedHeight` is
                // latest-wins, so a dropped report on a momentarily-full channel
                // is harmless (the next report carries a higher height).
                let _ = ctx.events.try_send(TnExExEvent::FinishedHeight(tip_number));
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

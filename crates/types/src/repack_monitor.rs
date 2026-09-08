//! Rolling cross-producer duplicate-transaction monitor (issue #1259).
//!
//! Batch contents travel in plaintext gossip before certification. A validator that receives
//! another validator's batch can extract its transactions, pack them into its own batch, and set
//! itself as the fee recipient. If its copy is ordered first, it collects the priority fees
//! (issue #1259). Sealing batch contents until ordering (commit-reveal or threshold encryption)
//! is the only full fix; until then the accepted posture is to keep the residual documented and
//! watch for poaching patterns in practice.
//!
//! This module is that watch. [`RepackMonitor`] records, over a bounded window of recent
//! consensus outputs, which producer's batch first carried each transaction hash. A transaction
//! that reappears in a batch from a different producer is reported to the caller, which logs it
//! and counts it in metrics. The first-ordered copy is the one execution credits, so repeated
//! wins by one producer over transactions first gossiped by others is the poaching pattern
//! operators watch for.
//!
//! A flagged transaction is an indicator, not proof: a sender that submits the same transaction
//! to two validators' RPC endpoints produces the same signature. The monitor therefore only
//! observes; it never rejects a batch or changes execution.

use crate::{keccak256, Address, B256};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

/// Number of recent consensus outputs the monitor retains.
///
/// A poached copy races the victim's copy through certification, so the two copies land in the
/// same output or a nearby one. The engine buffers at most `MAX_QUEUED_OUTPUTS` (8) outputs, so
/// a 16-output window covers the full backlog plus the same span again for slower races.
pub const REPACK_WINDOW_OUTPUTS: usize = 16;

/// Hard cap on transaction hashes retained across the whole window.
///
/// Bounds memory regardless of batch sizes: 65,536 entries cost a few megabytes. When the cap is
/// exceeded the oldest outputs are evicted first, which shrinks the effective window under
/// sustained full batches. A producer that floods tiny transactions can shrink it deliberately;
/// that only blinds telemetry, never execution, and the flood itself is visible in batch
/// metrics.
pub const REPACK_WINDOW_MAX_TXS: usize = 65_536;

/// A transaction observed in a batch from a producer that is not the one whose batch first
/// carried it inside the window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepackedTx {
    /// Hash of the duplicated transaction.
    pub tx_hash: B256,
    /// Producer credited for the first-ordered batch that carried the transaction.
    pub first_producer: Address,
    /// Consensus output number of that first-ordered batch.
    pub first_consensus_number: u64,
}

/// Transaction hashes first seen in one retained consensus output.
struct OutputEntry {
    /// The output's consensus number.
    consensus_number: u64,
    /// Hashes whose first observation was in this output.
    tx_hashes: Vec<B256>,
}

/// First observation of a transaction hash inside the window.
struct FirstSeen {
    /// Producer of the first-ordered batch that carried the transaction.
    producer: Address,
    /// Consensus number of that batch's output.
    consensus_number: u64,
}

/// Interior state behind the monitor's lock.
#[derive(Default)]
struct RepackWindow {
    /// Retained outputs, oldest first. Eviction pops from the front.
    outputs: VecDeque<OutputEntry>,
    /// First observation per transaction hash. Keys are exactly the union of the retained
    /// outputs' `tx_hashes` lists, which are disjoint, so eviction removes a key exactly once.
    seen: HashMap<B256, FirstSeen>,
    /// Total retained hashes; kept equal to `seen.len()` and compared against
    /// [`REPACK_WINDOW_MAX_TXS`].
    total_txs: usize,
    /// Cross-producer duplicates reported since construction. Monotonic; not evicted.
    repacked_total: u64,
}

/// Shared, cloneable monitor for cross-producer transaction re-packing.
///
/// Monitoring is opt-in: [`RepackMonitor::default`] is disabled, holds no window, and hashes
/// nothing, so a node that has not opted in pays nothing. [`RepackMonitor::enabled`] builds
/// the active monitor.
///
/// The engine owns one instance and passes a clone into each consensus-output
/// execution (the same flow as `GasAccumulator`). Observation happens in execution order on the
/// single engine thread, so the first record for a hash is the first-ordered batch, which is the
/// copy that collects the fees. The lock recovers from poisoning because the state is telemetry:
/// a panicked writer can at worst lose its own in-flight observation.
#[derive(Clone, Debug, Default)]
pub struct RepackMonitor {
    /// Window state shared across clones. `None` = monitoring disabled: observations return
    /// nothing without building a window or hashing a transaction.
    inner: Option<Arc<Mutex<RepackWindow>>>,
}

impl std::fmt::Debug for RepackWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepackWindow")
            .field("outputs", &self.outputs.len())
            .field("total_txs", &self.total_txs)
            .field("repacked_total", &self.repacked_total)
            .finish()
    }
}

impl RepackMonitor {
    /// Build an active monitor with an empty window.
    pub fn enabled() -> Self {
        Self { inner: Some(Arc::new(Mutex::new(RepackWindow::default()))) }
    }

    /// Whether this monitor observes batches. The default-constructed monitor does not.
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Record one batch's transactions for `producer` inside output `consensus_number` and
    /// return the transactions first carried by a different producer's batch.
    ///
    /// `raw_txs` are the batch's encoded transactions; each hashes to its canonical transaction
    /// hash. Batches must be observed in execution (ordering) order. A duplicate from the same
    /// producer is not reported: re-gossip of a producer's own batch is not poaching. A reported
    /// duplicate stays attributed to its first producer and is not re-inserted.
    ///
    /// A disabled monitor returns an empty list and hashes nothing.
    pub fn observe_batch(
        &self,
        consensus_number: u64,
        producer: Address,
        raw_txs: &[Vec<u8>],
    ) -> Vec<RepackedTx> {
        self.inner.as_ref().map_or_else(Vec::new, |inner| {
            Self::observe(inner, consensus_number, producer, raw_txs)
        })
    }

    /// Record one batch into the active window. See [`Self::observe_batch`].
    fn observe(
        inner: &Mutex<RepackWindow>,
        consensus_number: u64,
        producer: Address,
        raw_txs: &[Vec<u8>],
    ) -> Vec<RepackedTx> {
        let mut window = inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let RepackWindow { outputs, seen, total_txs, repacked_total } = &mut *window;

        // Reuse the entry for this output when the previous batch was in the same output,
        // otherwise start a new one.
        let mut current = outputs
            .pop_back()
            .and_then(|last| {
                if last.consensus_number == consensus_number {
                    Some(last)
                } else {
                    outputs.push_back(last);
                    None
                }
            })
            .unwrap_or_else(|| OutputEntry { consensus_number, tx_hashes: Vec::new() });

        let retained_before = current.tx_hashes.len();
        let duplicates: Vec<RepackedTx> = raw_txs
            .iter()
            .filter_map(|raw| {
                let tx_hash = keccak256(raw);
                match seen.entry(tx_hash) {
                    Entry::Occupied(first) => {
                        let FirstSeen { producer: first_producer, consensus_number: first_number } =
                            *first.get();
                        (first_producer != producer).then_some(RepackedTx {
                            tx_hash,
                            first_producer,
                            first_consensus_number: first_number,
                        })
                    }
                    Entry::Vacant(slot) => {
                        slot.insert(FirstSeen { producer, consensus_number });
                        current.tx_hashes.push(tx_hash);
                        None
                    }
                }
            })
            .collect();
        *total_txs += current.tx_hashes.len().saturating_sub(retained_before);
        *repacked_total += u64::try_from(duplicates.len()).unwrap_or(u64::MAX);
        outputs.push_back(current);

        // Evict the oldest outputs down to both bounds. The fold walks front to back and counts
        // the prefix that must go for the tx cap to hold; the length bound is a direct
        // subtraction. Eviction may drop the output just pushed when a single output alone
        // exceeds the cap; the cap is hard.
        let over_len = outputs.len().saturating_sub(REPACK_WINDOW_OUTPUTS);
        let (over_cap, _) =
            outputs.iter().fold((0usize, *total_txs), |(drop_n, remaining), entry| {
                if remaining > REPACK_WINDOW_MAX_TXS {
                    (drop_n.saturating_add(1), remaining.saturating_sub(entry.tx_hashes.len()))
                } else {
                    (drop_n, remaining)
                }
            });
        outputs.drain(..over_len.max(over_cap)).for_each(|evicted| {
            evicted.tx_hashes.iter().for_each(|hash| {
                seen.remove(hash);
            });
            *total_txs = total_txs.saturating_sub(evicted.tx_hashes.len());
        });

        duplicates
    }

    /// Cross-producer duplicates reported since construction (monotonic, never evicted).
    ///
    /// A disabled monitor reports zero.
    pub fn total_repacked(&self) -> u64 {
        self.inner.as_ref().map_or(0, |inner| {
            inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).repacked_total
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Distinct raw transaction bytes for index `n`.
    fn raw_tx(n: u32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    /// Address filled with byte `b`.
    fn addr(b: u8) -> Address {
        Address::repeat_byte(b)
    }

    /// The default monitor is disabled: it observes nothing and reports nothing, so an
    /// un-flagged node does no re-packing work at all.
    #[test]
    fn test_default_monitor_is_disabled() {
        let monitor = RepackMonitor::default();
        assert!(!monitor.is_enabled());
        assert!(monitor.observe_batch(1, addr(0xaa), &[raw_tx(1)]).is_empty());
        // The same tx from another producer is not reported: nothing was recorded.
        assert!(monitor.observe_batch(2, addr(0xbb), &[raw_tx(1)]).is_empty());
        assert_eq!(monitor.total_repacked(), 0);
        assert!(RepackMonitor::enabled().is_enabled());
    }

    #[test]
    fn test_cross_producer_duplicate_flagged_with_first_attribution() {
        let monitor = RepackMonitor::enabled();
        assert!(monitor.observe_batch(1, addr(0xaa), &[raw_tx(1), raw_tx(2)]).is_empty());
        let dups = monitor.observe_batch(2, addr(0xbb), &[raw_tx(2), raw_tx(3)]);
        assert_eq!(
            dups,
            vec![RepackedTx {
                tx_hash: keccak256(raw_tx(2)),
                first_producer: addr(0xaa),
                first_consensus_number: 1,
            }]
        );
        // A third producer's copy stays attributed to the first producer, not the second.
        let dups = monitor.observe_batch(3, addr(0xcc), &[raw_tx(2)]);
        assert_eq!(
            dups,
            vec![RepackedTx {
                tx_hash: keccak256(raw_tx(2)),
                first_producer: addr(0xaa),
                first_consensus_number: 1,
            }]
        );
        assert_eq!(monitor.total_repacked(), 2);
    }

    #[test]
    fn test_same_producer_duplicate_not_flagged() {
        let monitor = RepackMonitor::enabled();
        // Twice in one batch, then again in a later output, all from one producer.
        assert!(monitor.observe_batch(1, addr(0xaa), &[raw_tx(1), raw_tx(1)]).is_empty());
        assert!(monitor.observe_batch(2, addr(0xaa), &[raw_tx(1)]).is_empty());
        assert_eq!(monitor.total_repacked(), 0);
    }

    #[test]
    fn test_same_output_cross_producer_flagged() {
        let monitor = RepackMonitor::enabled();
        assert!(monitor.observe_batch(7, addr(0xaa), &[raw_tx(1)]).is_empty());
        let dups = monitor.observe_batch(7, addr(0xbb), &[raw_tx(1)]);
        assert_eq!(
            dups,
            vec![RepackedTx {
                tx_hash: keccak256(raw_tx(1)),
                first_producer: addr(0xaa),
                first_consensus_number: 7,
            }]
        );
    }

    #[test]
    fn test_output_window_eviction_forgets_first_seen() {
        let monitor = RepackMonitor::enabled();
        assert!(monitor.observe_batch(0, addr(0xaa), &[raw_tx(0)]).is_empty());
        // Fill enough later outputs to evict output 0 from the window.
        let window = u64::try_from(REPACK_WINDOW_OUTPUTS).expect("window fits in u64");
        (1..=window).for_each(|n| {
            let filler = u32::try_from(n).expect("filler index fits in u32");
            assert!(monitor.observe_batch(n, addr(0xaa), &[raw_tx(filler)]).is_empty());
        });
        // Output 0 is evicted, so producer B's copy is a fresh first observation.
        assert!(monitor.observe_batch(100, addr(0xbb), &[raw_tx(0)]).is_empty());
        // Producer A's own original transaction now flags against B's fresh attribution.
        let dups = monitor.observe_batch(101, addr(0xaa), &[raw_tx(0)]);
        assert_eq!(
            dups,
            vec![RepackedTx {
                tx_hash: keccak256(raw_tx(0)),
                first_producer: addr(0xbb),
                first_consensus_number: 100,
            }]
        );
    }

    #[test]
    fn test_tx_cap_evicts_oldest_outputs() {
        let monitor = RepackMonitor::enabled();
        // Two outputs that together exceed the tx cap: the older one is evicted whole.
        let first: Vec<Vec<u8>> = (0..40_000_u32).map(raw_tx).collect();
        let second: Vec<Vec<u8>> = (100_000..130_000_u32).map(raw_tx).collect();
        assert!(monitor.observe_batch(1, addr(0xaa), &first).is_empty());
        assert!(monitor.observe_batch(2, addr(0xbb), &second).is_empty());
        // A transaction from the evicted output is a fresh observation, not a flag.
        assert!(monitor.observe_batch(3, addr(0xbb), &[raw_tx(0)]).is_empty());
        // A transaction from the retained output still flags.
        let dups = monitor.observe_batch(3, addr(0xcc), &[raw_tx(100_000)]);
        assert_eq!(dups.len(), 1);
        assert_eq!(monitor.total_repacked(), 1);
    }
}

//! Compare cumulative allocations of the former merge path and production sorted runs.

use reth_primitives_traits::Account;
use reth_trie::{updates::TrieUpdatesSorted, HashedPostState, HashedPostStateSorted, Nibbles};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tn_types::{B256, U256};

#[path = "../src/env/output_overlay/sorted_runs.rs"]
mod sorted_runs;
use sorted_runs::SortedTrieRuns;

/// Enable allocation accounting only during one measured accumulation.
static MEASURING: AtomicBool = AtomicBool::new(false);
/// Sum of requested allocation and reallocation sizes during accumulation.
static BYTES: AtomicUsize = AtomicUsize::new(0);
/// System allocator with a process-local byte counter, used by this standalone binary only.
struct CountingAllocator;

// SAFETY: every operation delegates to System with the caller's original pointer and layout.
// The counter neither accesses allocated memory nor changes allocation behavior.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: GlobalAlloc's caller supplies a valid layout, passed through unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: this allocator forwards the original System pointer and allocation layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            BYTES.fetch_add(size, Ordering::Relaxed);
        }
        // SAFETY: the caller's pointer, layout, and new size satisfy GlobalAlloc's contract.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

/// Allocator used only by this standalone measurement binary.
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Workloads separate the general sorted merge from Reth's append fast path.
#[derive(Clone, Copy, Debug)]
enum Workload {
    /// Disjoint keys whose ranges overlap across blocks.
    Interleaved,
    /// Identical keys overwritten on every block.
    Repeated,
    /// Every block's keys sort after the preceding block's keys.
    AppendOnly,
}

/// Encode ordered input keys.
fn key(value: u64) -> B256 {
    B256::from(U256::from(value).to_be_bytes::<32>())
}

/// Construct shared block deltas outside the measured region.
fn inputs(
    workload: Workload,
    blocks: u64,
    width: u64,
) -> Vec<(Arc<HashedPostStateSorted>, Arc<TrieUpdatesSorted>)> {
    (0..blocks)
        .map(|block| {
            let keys = (0..width)
                .map(|entry| {
                    key(match workload {
                        Workload::Interleaved => entry * blocks + block,
                        Workload::Repeated => entry,
                        Workload::AppendOnly => block * width + entry,
                    })
                })
                .collect::<Vec<_>>();
            let state = HashedPostState::default()
                .with_accounts(
                    keys.iter()
                        .copied()
                        .map(|key| (key, Some(Account { nonce: block, ..Default::default() }))),
                )
                .into_sorted();
            let nodes = TrieUpdatesSorted::new(
                keys.into_iter().map(|key| (Nibbles::unpack(key), None)).collect(),
                Default::default(),
            );
            (Arc::new(state), Arc::new(nodes))
        })
        .collect()
}

/// Measure cumulative requested bytes and elapsed time, including cursor-free accumulation only.
fn measure(run: impl FnOnce()) -> (usize, u128) {
    BYTES.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let started = Instant::now();
    run();
    let elapsed = started.elapsed().as_micros();
    MEASURING.store(false, Ordering::Relaxed);
    (BYTES.load(Ordering::Relaxed), elapsed)
}

/// Print comparable workloads; timing is descriptive, while allocations expose cumulative work.
fn main() {
    println!("workload,blocks,entries_per_vector,implementation,allocated_bytes,elapsed_us");
    [Workload::Interleaved, Workload::Repeated, Workload::AppendOnly].into_iter().for_each(
        |workload| {
            [64, 128, 256, 512, 1024].into_iter().for_each(|blocks| {
                let deltas = inputs(workload, blocks, 32);
                let old = measure(|| {
                    let mut state = Arc::new(HashedPostStateSorted::default());
                    let mut nodes = Arc::new(TrieUpdatesSorted::default());
                    deltas.iter().for_each(|(incoming_state, incoming_nodes)| {
                        Arc::make_mut(&mut state).extend_ref_and_sort(incoming_state);
                        Arc::make_mut(&mut nodes).extend_ref_and_sort(incoming_nodes);
                    });
                    black_box((state, nodes));
                });
                let geometric = measure(|| {
                    let mut runs = SortedTrieRuns::default();
                    deltas
                        .iter()
                        .for_each(|(state, nodes)| runs.extend(state.clone(), nodes.clone()));
                    black_box(
                        runs.iter()
                            .map(|run| run.state().accounts.len() + run.nodes().total_len())
                            .sum::<usize>(),
                    );
                });
                println!("{workload:?},{blocks},32,former,{},{}", old.0, old.1);
                println!("{workload:?},{blocks},32,geometric,{},{}", geometric.0, geometric.1);
            });
        },
    );
}

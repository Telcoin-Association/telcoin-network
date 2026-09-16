//! A `Database` backed by pack files, each keyed by a sorted [`BtreeIndex`] (work in progress; the
//! store fields are not wired up yet).

use std::sync::Arc;

use dashmap::DashMap;
use tn_types::ValueT;

use crate::archive::{btree_index::BtreeIndex, pack::Pack};

/// One named table: a pack data log plus its sorted B+tree index.
#[derive(Debug)]
#[allow(dead_code)] // WIP: populated once the Database impl is wired up.
struct TnTable<V>
where
    V: ValueT,
{
    data: Pack<V>,
    idx: BtreeIndex,
}

type StoreType = DashMap<&'static str, Arc<TnTable<Vec<u8>>>>;

/// Implement the Database trait with a pack file backed store.
#[derive(Clone, Debug)]
pub struct TnDatabase {
    #[allow(dead_code)] // WIP: read once the Database impl is wired up.
    store: Arc<StoreType>,
}

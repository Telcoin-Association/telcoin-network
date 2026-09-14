use std::sync::Arc;

use dashmap::DashMap;
use tn_types::{KeyT, ValueT};

use crate::archive::{btree_index::BtreeIndex, pack::Pack};

#[derive(Debug)]
struct TnTable<const KSIZE: usize, V>
where
    V: ValueT,
{
    data: Pack<V>,
    idx: BtreeIndex<KSIZE>,
}

type StoreType = DashMap<&'static str, Arc<TnTable<Vec<u8>, Vec<u8>>>>;

/// Implement the Database trait with a pack file backed store.
#[derive(Clone, Debug)]
pub struct TnDatabase {
    store: Arc<StoreType>,
}

//! TN(TN-0204): a scope in which one thread's transaction reads share the batch they resolve.
//!
//! A Telcoin Network datadir can store a transaction as its position in the consensus batch its
//! block executed (TN-0201), so every read of such a transaction resolves that batch (the
//! provider's `TxBodyResolver`, TN-0202). [`append_matching_block_logs`] reads the transaction of
//! each matching receipt by id, one call each, inside a [`TxReadScope`]: the resolver keeps the
//! batch it resolved in this thread's scope ([`TxReadScope::keep`]) and answers the block's later
//! reads from it ([`TxReadScope::get`]), so a block's batch is read at most once per call whatever
//! the resolver's own cache holds. The scope holds one value, and drops it when it ends. Outside a
//! scope nothing is kept, and a provider that resolves no batch never touches it.
//!
//! [`append_matching_block_logs`]: crate::logs_utils::append_matching_block_logs

use alloy_primitives::B256;
use std::{any::Any, cell::RefCell, marker::PhantomData, sync::Arc};

/// A value a scope keeps: the resolver's own type, behind [`Any`].
pub type ScopedValue = Arc<dyn Any + Send + Sync>;

thread_local! {
    /// `None` outside a scope; inside one, the value it keeps and its key, if any.
    static SCOPE: RefCell<Option<Option<(B256, ScopedValue)>>> = const { RefCell::new(None) };
}

/// This thread's transaction read scope, from [`TxReadScope::enter`] until the guard drops. A
/// scope entered inside another shares the outer one's value.
#[derive(Debug)]
#[must_use = "the scope ends when the guard drops"]
pub struct TxReadScope {
    /// Whether this guard entered the scope, and so ends it.
    outermost: bool,
    /// The scope is the entering thread's: the guard stays on it.
    _thread: PhantomData<*const ()>,
}

impl TxReadScope {
    /// Enter a scope on this thread, or join the one it is in.
    pub fn enter() -> Self {
        let outermost = SCOPE.with(|scope| {
            let mut scope = scope.borrow_mut();
            let outermost = scope.is_none();
            if outermost {
                *scope = Some(None);
            }
            outermost
        });
        Self { outermost, _thread: PhantomData }
    }

    /// The value this thread's scope keeps under `key`, if the thread is in a scope that keeps
    /// one under `key` and it is a `T`.
    pub fn get<T: Any + Send + Sync>(key: &B256) -> Option<Arc<T>> {
        SCOPE.with(|scope| match &*scope.borrow() {
            Some(Some((kept, value))) if kept == key => value.clone().downcast().ok(),
            _ => None,
        })
    }

    /// Keep `value` under `key` in this thread's scope, in place of what it kept. Outside a scope
    /// this does nothing.
    pub fn keep(key: B256, value: ScopedValue) {
        let replaced = SCOPE
            .with(|scope| scope.borrow_mut().as_mut().and_then(|kept| kept.replace((key, value))));
        // the value it kept drops here, outside the borrow
        drop(replaced);
    }
}

impl Drop for TxReadScope {
    fn drop(&mut self) {
        if self.outermost {
            let kept = SCOPE.with(|scope| scope.borrow_mut().take());
            drop(kept);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(n: u64) -> ScopedValue {
        Arc::new(n)
    }

    #[test]
    fn nothing_is_kept_outside_a_scope() {
        TxReadScope::keep(B256::with_last_byte(1), value(1));
        assert_eq!(TxReadScope::get::<u64>(&B256::with_last_byte(1)), None);
    }

    #[test]
    fn a_scope_keeps_one_value_until_it_ends() {
        let (one, two) = (B256::with_last_byte(1), B256::with_last_byte(2));
        {
            let _scope = TxReadScope::enter();
            assert_eq!(TxReadScope::get::<u64>(&one), None, "a new scope keeps nothing");
            TxReadScope::keep(one, value(1));
            assert_eq!(TxReadScope::get::<u64>(&one).as_deref(), Some(&1));
            assert_eq!(TxReadScope::get::<u64>(&two), None, "another key");
            assert_eq!(TxReadScope::get::<u32>(&one), None, "another type");
            TxReadScope::keep(two, value(2));
            assert_eq!(TxReadScope::get::<u64>(&one), None, "replaced");
            assert_eq!(TxReadScope::get::<u64>(&two).as_deref(), Some(&2));
        }
        assert_eq!(TxReadScope::get::<u64>(&two), None, "the scope ended");
        let _scope = TxReadScope::enter();
        assert_eq!(TxReadScope::get::<u64>(&two), None, "the next scope starts empty");
    }

    #[test]
    fn an_inner_scope_shares_the_outer_one() {
        let key = B256::with_last_byte(3);
        let kept = Arc::new(3_u64);
        let outer = TxReadScope::enter();
        {
            let _inner = TxReadScope::enter();
            TxReadScope::keep(key, kept.clone());
        }
        assert_eq!(TxReadScope::get::<u64>(&key).as_deref(), Some(&3), "the inner end keeps it");
        drop(outer);
        assert_eq!(TxReadScope::get::<u64>(&key), None);
        assert_eq!(Arc::strong_count(&kept), 1, "the end of the scope dropped its value");
    }

    #[test]
    fn a_scope_is_its_threads_own() {
        let key = B256::with_last_byte(4);
        let _scope = TxReadScope::enter();
        TxReadScope::keep(key, value(4));
        std::thread::spawn(move || {
            assert_eq!(TxReadScope::get::<u64>(&key), None, "another thread's scope");
            TxReadScope::keep(key, value(5));
        })
        .join()
        .expect("the other thread");
        assert_eq!(TxReadScope::get::<u64>(&key).as_deref(), Some(&4));
    }
}

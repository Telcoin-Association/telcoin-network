//! Trait that defines an index (key to file position).

use crate::archive::error::{commit::CommitError, fetch::FetchError, insert::AppendError};

/// Trait that any archive pack file can use for an index.
pub trait Index<K, V> {
    /// Save the file pos for key into the index.
    fn save(&mut self, key: K, record_pos: V) -> Result<(), AppendError>;
    /// Load the file pos for key from the index.
    fn load(&mut self, key: K) -> Result<V, FetchError>;
    /// Flush and sync all the index data to disk.
    fn sync(&mut self) -> Result<(), CommitError>;
    /// True if the index contains the key.
    fn contains(&mut self, key: K) -> bool {
        self.load(key).is_ok()
    }
}

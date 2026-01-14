//! Trait that defines an index (key to file position).

use crate::archive::error::{fetch::FetchError, insert::AppendError};

/// Trait that any archive pack file can use for an index.
pub trait Index<K> {
    /// Save the file pos for key into the index.
    fn save(&mut self, key: K, record_pos: u64) -> Result<(), AppendError>;
    /// Load the file pos for key from the index.
    fn load(&mut self, key: K) -> Result<u64, FetchError>;
}

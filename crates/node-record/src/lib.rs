//! Signed node records and protocol names shared by Telcoin nodes and external readers.
//!
//! Records are keyed by the validator's raw compressed BLS public key. Verification must
//! reconstruct the expected chain and role domain locally. A valid signature alone does not
//! check a DHT record's publisher or the advertised address count; consumers must check both.

mod protocol;
mod record;

pub use protocol::{gossip_protocol_id_prefix, NetworkType};
pub use record::{NetworkInfo, NodeRecord, RecordDomain};
pub use tn_types::RpcInfo;

/// Maximum addresses advertised in one signed node record or retained for one peer.
///
/// Each primary or worker swarm advertises exactly one address. Readers reject records
/// exceeding this bound, and the node's per-peer address store derives its cap from it.
pub const MAX_ADVERTISED_MULTIADDRS: usize = 1;

#[cfg(test)]
mod tests;

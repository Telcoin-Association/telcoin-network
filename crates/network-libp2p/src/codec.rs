//! Codec types for encoding/decoding consensus network messages.

use crate::PeerExchangeMap;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

/// Convenience type for all traits implemented for messages used for TN network messaging.
pub trait TNMessage:
    Send + Sync + Serialize + DeserializeOwned + Clone + fmt::Debug + From<PeerExchangeMap> + 'static
{
    /// Function to intercept peer exchange messages at the network layer before passing to the
    /// application layer. Only the network layer needs peer exchange messages.
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap>;
}

//! Telcoin Network hardfork definitions.

use core::fmt;
use reth_revm::primitives::hardfork::SpecId;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Telcoin Network hardfork variants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TelcoinHardfork {
    /// Genesis hardfork - maps to Prague spec.
    #[default]
    Genesis,
}

impl fmt::Display for TelcoinHardfork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Genesis => write!(f, "Genesis"),
        }
    }
}

impl FromStr for TelcoinHardfork {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "genesis" => Ok(Self::Genesis),
            _ => Err(format!("unknown telcoin hardfork: '{s}'")),
        }
    }
}

impl From<TelcoinHardfork> for SpecId {
    fn from(fork: TelcoinHardfork) -> Self {
        match fork {
            TelcoinHardfork::Genesis => SpecId::PRAGUE,
        }
    }
}

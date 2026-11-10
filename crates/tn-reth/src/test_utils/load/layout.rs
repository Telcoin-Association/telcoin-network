//! The reth storage layout a bench datadir holds, [`StorageLayout`], and the one place the bench
//! selects it for a new datadir ([`StorageLayout::select`]) and reads it back from an open one
//! ([`StorageLayout::of`]).
//!
//! The layout is written once, when the genesis is (`init_genesis` stores reth's
//! `StorageSettings`), and an existing datadir opens in the layout it holds. So the bench selects
//! it when it builds a base, keys its cached bases by it (`base_dir_name`), and checks every env it
//! opens against it ([`open_production_env_with_layout`](super::open_production_env_with_layout)):
//! a run never measures another layout than the one it reports.
//!
//! **Selection.** [`StorageLayout::select`] sets the env's choice for a new datadir
//! ([`RethConfig::with_new_datadir_layout`]) for v1 too, so the env's default for new datadirs
//! (`env::StorageLayout::for_new_datadirs`) never decides a bench's layout. v2 needs the tn-lmdb
//! build (the env refuses it on MDBX), so [`StorageLayout::selectable`] refuses it on the MDBX
//! build before the bench builds anything.

use crate::{RethConfig, RethEnv};
use reth_provider::{
    DatabaseProviderFactory as _, MetadataProvider as _, StorageSettings, StorageSettingsCache as _,
};
use serde::{Deserialize, Serialize};

/// reth's storage layout: where receipts, senders, changesets and history live, and whether the
/// plain or the hashed state is canonical.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageLayout {
    /// Everything in the database but headers, transactions and receipts (`StorageSettings::v1`).
    #[default]
    V1,
    /// Receipts, senders and changesets in static files; the hashed state canonical
    /// (`StorageSettings::v2`).
    V2,
}

impl StorageLayout {
    /// Every layout.
    pub const ALL: [Self; 2] = [Self::V1, Self::V2];

    /// Its knob value and report label: `v1` or `v2`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }

    /// The layout labelled `label`.
    pub fn parse(label: &str) -> eyre::Result<Self> {
        Self::ALL
            .into_iter()
            .find(|layout| layout.label() == label)
            .ok_or_else(|| eyre::eyre!("{label:?} is not a storage layout: v1 or v2"))
    }

    /// The reth settings of this layout.
    pub const fn settings(self) -> StorageSettings {
        match self {
            Self::V1 => StorageSettings::v1(),
            Self::V2 => StorageSettings::v2(),
        }
    }

    /// The env's layout of the same name.
    pub const fn env_layout(self) -> crate::env::StorageLayout {
        match self {
            Self::V1 => crate::env::StorageLayout::V1,
            Self::V2 => crate::env::StorageLayout::V2,
        }
    }

    /// Whether this build can make a new datadir in this layout: an error naming why not. v2 is
    /// tn-lmdb only.
    pub fn selectable(self) -> eyre::Result<()> {
        match self {
            _ if self.env_layout().is_supported() => Ok(()),
            Self::V1 => Ok(()),
            Self::V2 => eyre::bail!(
                "storage layout v2 runs on tn-lmdb only: build without the `mdbx` feature, or \
                 run layout v1"
            ),
        }
    }

    /// Select this layout for a new datadir opened with `config` (see the module docs), refused
    /// unless [`Self::selectable`].
    pub fn select(self, config: &mut RethConfig) -> eyre::Result<()> {
        self.selectable()?;
        *config = config.clone().with_new_datadir_layout(self.env_layout());
        Ok(())
    }

    /// The layout `env`'s datadir holds: its stored storage settings, which must be the ones its
    /// provider runs with and one of [`Self::ALL`]'s.
    pub fn of(env: &RethEnv) -> eyre::Result<Self> {
        let provider = env.blockchain_provider().database_provider_ro()?;
        let stored = provider
            .storage_settings()?
            .ok_or_else(|| eyre::eyre!("the datadir stores no storage settings"))?;
        let running = provider.cached_storage_settings();
        eyre::ensure!(
            stored == running,
            "the datadir stores storage settings {stored:?}, its provider runs {running:?}"
        );
        Self::ALL.into_iter().find(|layout| layout.settings() == stored).ok_or_else(|| {
            eyre::eyre!("the datadir's storage settings {stored:?} are neither v1 nor v2")
        })
    }
}

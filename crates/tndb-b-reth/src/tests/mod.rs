//! The crate's tests: reth's MDBX tests ported (`mdbx_port`), the adapter's own (`adapter`),
//! the dup-sort tables (`dup`), random scripts run on MDBX too (`differential`), and a child
//! process killed mid-commit, then reopened and checked (`crash`, Unix only).

use crate::{env::PACKED_SMALL_RETH, TndbArgs, TndbEnv, TndbEnvKind};
use parking_lot::RwLock;
use reth_db_api::DatabaseError;
use std::{
    io,
    path::Path,
    process::{Child, Command},
};

mod adapter;
#[cfg(unix)]
mod crash;
mod differential;
mod dup;
mod mdbx_port;

/// Keeps the tests' opens of a database apart from the spawn of a child process (`crash`), which
/// otherwise fails a test running beside it with `MDBX_BUSY` now and then. A child spawned while
/// another thread opens a database holds copies of that thread's descriptors until it execs, and
/// with them the open file description by which the open's `create` locked the new file, so the
/// open's own lock of that file fails though no handle has it. Opens share the lock; a spawn takes
/// it alone, and once `spawn` returns the child has exec'd, which closed its copies.
static SPAWN_LOCK: RwLock<()> = RwLock::new(());

/// A form of the nested dup layouts' tables (`PlainStorageState`, `HashedStorages`,
/// `StoragesTrie`) a test runs on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Form {
    /// Engine dup tables packing a key's rows up to the most the page size allows (production).
    Packed,
    /// Engine dup tables packing at most [`PACKED_SMALL_RETH`] bytes of a key's rows, so a key
    /// promotes at its second or third row ([`TndbArgs::packed_dup_max`]).
    PackedSmall,
    /// Composite keys in plain tables ([`TndbArgs::composite_dup_tables`]), the changesets' path.
    Composite,
}

impl Form {
    /// Every form, production's first.
    pub(crate) const ALL: [Self; 3] = [Self::Packed, Self::PackedSmall, Self::Composite];

    /// [`Self::Packed`] if `nested`, else [`Self::Composite`].
    pub(crate) const fn nested(nested: bool) -> Self {
        if nested {
            Self::Packed
        } else {
            Self::Composite
        }
    }

    /// The arguments that open a new file in this form.
    pub(crate) fn args(self) -> TndbArgs {
        let args = TndbArgs::default();
        match self {
            Self::Packed => args,
            Self::PackedSmall => args.packed_dup_max(PACKED_SMALL_RETH),
            Self::Composite => args.composite_dup_tables(),
        }
    }

    /// The `packed_max` the engine records for a `PlainStorageState` (20-byte keys) of this form
    /// at 4 KiB pages; `None` for a plain table.
    pub(crate) const fn plain_storage_packed_max(self) -> Option<u16> {
        match self {
            Self::Packed => Some(987),
            Self::PackedSmall => Some(PACKED_SMALL_RETH),
            Self::Composite => None,
        }
    }

    /// The form's name, for messages.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Packed => "packed",
            Self::PackedSmall => "packed-small",
            Self::Composite => "composite",
        }
    }
}

/// [`TndbEnv::open`], never while a child is being spawned ([`SPAWN_LOCK`]).
pub(crate) fn open_env(
    path: &Path,
    kind: TndbEnvKind,
    args: TndbArgs,
) -> Result<TndbEnv, DatabaseError> {
    let _open = SPAWN_LOCK.read();
    TndbEnv::open(path, kind, args)
}

/// `command.spawn()`, never while a test opens a database ([`SPAWN_LOCK`]).
pub(crate) fn spawn(command: &mut Command) -> io::Result<Child> {
    let _spawn = SPAWN_LOCK.write();
    command.spawn()
}

//! The crate's tests: reth's MDBX tests ported (`mdbx_port`), the adapter's own (`adapter`),
//! the dup-sort tables (`dup`), random scripts run on MDBX too (`differential`), and a child
//! process killed mid-commit, then reopened and checked (`crash`, Unix only).

use crate::{TndbArgs, TndbEnv, TndbEnvKind};
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

// SPDX-License-Identifier: MIT or Apache-2.0

//! Read-only inspection of a tndb database (the `db tndb-stat` command): what its log and
//! indexes hold, read from the files and changing none of them. It replays the log as an open
//! would, without writing anything, and refuses a database another handle has open (the node's),
//! so it never maps a log a writer may trim or replace. Live monitoring is the metrics' job.

use std::{fs, path::Path};

use eyre::{bail, WrapErr as _};

use super::{
    compact::{read_base, FRAME},
    layout::{gen_dir, list_gens, lock_dir_shared},
    manifest::{Entry, Manifest},
    split::{ENVIRONMENTS, MDBX_MIGRATED},
    store::{idx_dir, open_log_read_only, orphan_indexes, replay, RowsSink},
};
use crate::archive::{
    btree_index::BtreeIndex,
    pack::{DataHeader, DATA_HEADER_BYTES},
};

/// What one database (one environment of a [`super::SplitDatabase`]) holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvReport {
    /// The current generation (one more after each compaction).
    pub generation: u64,
    /// Older generations still on disk: left by a crash right after a switch (the next open
    /// deletes them once the current one replays).
    pub older_generations: Vec<u64>,
    /// Unfinished `compact-*` builds left by a close or a crash (the next open deletes them).
    pub unfinished_builds: usize,
    /// Index directories (`idx-<id>`) of tables the manifest does not name, or whose key size it
    /// has not fixed: a manifest rolled back to an older copy. Each forces a full replay at the
    /// next open, which fails if its table has records left, and is deleted once that passes.
    pub orphan_indexes: Vec<String>,
    /// The log was closed cleanly (false: after a crash, the next open replays it).
    pub clean: bool,
    /// The log file's length on disk (its data, plus preallocated room).
    pub file_bytes: u64,
    /// Where the log's last whole batch ends.
    pub data_end: u64,
    /// The log's length right after the compaction that made this generation (0 if none did).
    pub base: u64,
    /// The bytes of the live rows' records.
    pub live_bytes: u64,
    /// The bytes of every other record (removed, overwritten and cleared rows, and removals):
    /// what a compaction would give back.
    pub dead_bytes: u64,
    /// The tables, in id order.
    pub tables: Vec<TableReport>,
}

/// One table of an [`EnvReport`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableReport {
    /// The table's name.
    pub name: String,
    /// The table's id in the log's records.
    pub id: u16,
    /// The encoded key size (0 until the table's first row fixes it).
    pub key_size: u16,
    /// Live rows.
    pub rows: u64,
    /// The bytes of the live rows' records.
    pub live_bytes: u64,
    /// Its index, as the next open would find it.
    pub index: IndexState,
}

/// A table index's state, as the next open would find it from the index's header and root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexState {
    /// Closed cleanly, covering the whole log, with a sound root: the next open uses it, unless
    /// its open-time walk of the internal pages (which a writable open runs, and an inspection
    /// cannot) finds damage below the root, which rebuilds it.
    Current,
    /// Closed cleanly but covering a different length of the log: rebuilt at the next open.
    Stale,
    /// Not closed cleanly (a crash): rebuilt at the next open.
    Unclean,
    /// Its directory is missing: rebuilt at the next open.
    Missing,
    /// It could not be read (damage): rebuilt at the next open.
    Unreadable,
    /// The table has no rows yet, so no key size and no index.
    NoKeySize,
}

impl IndexState {
    /// A short name for a report.
    pub fn label(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Stale => "stale (rebuilt at open)",
            Self::Unclean => "unclean (rebuilt at open)",
            Self::Missing => "missing (rebuilt at open)",
            Self::Unreadable => "unreadable (rebuilt at open)",
            Self::NoKeySize => "none (no rows yet)",
        }
    }
}

/// What a [`super::SplitDatabase`] holds: each environment present, in `epoch`, `kad`, `cache`
/// order, with its report or why it could not be read.
#[derive(Debug)]
pub struct SplitReport {
    /// Each environment's name and report.
    pub envs: Vec<(&'static str, eyre::Result<EnvReport>)>,
    /// The node's MDBX consensus tables were copied in (see `migrate_from_mdbx`).
    pub migrated: bool,
}

/// Inspect the database at `root` (see the module docs). Fails if another handle has it open, if
/// it was never opened as a database, or if its log is damaged (as its next open would).
pub fn inspect(root: &Path) -> eyre::Result<EnvReport> {
    let _lock = lock_dir_shared(root)?;
    let manifest = Manifest::read(root)?;
    // As at open, orphans are reckoned only against a manifest: with it lost, any record fails
    // the replay.
    let manifest_lost = manifest.is_none();
    let manifest = manifest.unwrap_or_default();
    let gens = list_gens(root)?;
    let Some((&generation, older)) = gens.split_last() else {
        bail!("tndb: {} holds no generation", root.display());
    };
    let unfinished_builds = fs::read_dir(root)?
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
        .count();
    let gen_path = gen_dir(root, generation);
    let orphan_indexes = if manifest_lost {
        Vec::new()
    } else {
        orphan_indexes(&gen_path, &manifest)?
            .into_iter()
            .filter_map(|dir| dir.file_name().map(|name| name.to_string_lossy().into_owned()))
            .collect()
    };
    let log = open_log_read_only(&gen_path)
        .wrap_err_with(|| format!("tndb: open generation {generation} of {}", root.display()))?;
    let header = *log.header();
    let mut sink = RowsSink::wanting(manifest.tables.iter().map(|t| t.id));
    let data_end = replay(&log, &manifest, &mut sink)
        .wrap_err_with(|| format!("tndb: replay generation {generation} of {}", root.display()))?
        .end;
    let rows = sink.0;
    let mut tables = Vec::with_capacity(manifest.tables.len());
    for entry in &manifest.tables {
        let positions = rows.get(&entry.id);
        let mut live_bytes = 0;
        for &pos in positions.into_iter().flat_map(|rows| rows.values()) {
            live_bytes += log.record_bytes(pos)?.len() as u64 + FRAME;
        }
        tables.push(TableReport {
            name: entry.name.clone(),
            id: entry.id,
            key_size: entry.ksize,
            rows: positions.map_or(0, |rows| rows.len() as u64),
            live_bytes,
            index: index_state(&gen_path, entry, &header, log.file_len()),
        });
    }
    let live_bytes = tables.iter().map(|table| table.live_bytes).sum();
    Ok(EnvReport {
        generation,
        older_generations: older.to_vec(),
        unfinished_builds,
        orphan_indexes,
        clean: !log.opened_unclean(),
        file_bytes: fs::metadata(gen_path.join("log"))?.len(),
        data_end,
        base: read_base(&gen_path),
        live_bytes,
        dead_bytes: data_end.saturating_sub(DATA_HEADER_BYTES as u64 + live_bytes),
        tables,
    })
}

/// Inspect the [`super::SplitDatabase`] at `root`: each environment present (see [`inspect`]).
pub fn inspect_split(root: &Path) -> SplitReport {
    let envs = ENVIRONMENTS
        .into_iter()
        .filter(|env| root.join(env).is_dir())
        .map(|env| (env, inspect(&root.join(env))))
        .collect();
    SplitReport { envs, migrated: root.join(MDBX_MIGRATED).exists() }
}

/// The state of `entry`'s index in the generation at `gen_path`, whose log is `log_len` long,
/// opened read-only (so without the writable open's walk: see [`IndexState::Current`]).
fn index_state(gen_path: &Path, entry: &Entry, header: &DataHeader, log_len: u64) -> IndexState {
    if entry.ksize == 0 {
        return IndexState::NoKeySize;
    }
    let dir = idx_dir(gen_path, entry.id);
    if !dir.exists() {
        return IndexState::Missing;
    }
    match BtreeIndex::open_btx_file(&dir, header, entry.ksize, true) {
        Ok(idx) if idx.opened_unclean() => IndexState::Unclean,
        Ok(idx) if idx.data_file_length() != log_len => IndexState::Stale,
        Ok(_) => IndexState::Current,
        Err(_) => IndexState::Unreadable,
    }
}

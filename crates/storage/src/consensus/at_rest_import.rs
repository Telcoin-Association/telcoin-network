//! Re-import one past epoch's pack at rest from a copy (`telcoin-network db import-epoch`).
//!
//! The copy is a pack's data log: this node's backup of `epoch-N/`, or another node's sealed
//! `epoch-N/`. Its bytes are untrusted. They go through the import a peer's stream takes
//! ([`ConsensusChain::import_sealed`], which authenticates the epoch's final output against the
//! local record of the epoch and every earlier one by its parent links), and only a copy that
//! verified, sealed and read back sealed is installed, by the install that peer imports use
//! ([`ConsensusChain::install_imported_epoch_dir`], rename-aside). Nothing outside the import's
//! own `import-N` directory is written before that, and a dry run leaves nothing behind.
//!
//! It runs with the node stopped and opens no [`ConsensusChain`]: a chain's open writes (it opens
//! the live epoch for append and puts back interrupted installs), and the chain's import keeps a
//! pack in place that still serves the epoch's final output, wherever else its log is damaged.

use std::{
    fmt::{self, Display},
    path::{Path, PathBuf},
    time::Duration,
};

use tn_types::{Epoch, EpochRecord};
use tokio::io::AsyncReadExt as _;

use super::{ConsensusChain, ConsensusChainError};
use crate::{
    consensus_pack::{
        legacy_migration_dry_run, pack_unsealed_version, wal_consistent_end, ConsensusPack,
        DATA_NAME, SENTINEL_MIN_VERSION,
    },
    epoch_records::{EpochRecordDb, CERTS_NAME, RECORDS_NAME},
    pack_validate::{classify_physical_corruption, validate_pack_file, Verdict},
};

/// What held the place of the epoch's pack when [`ConsensusChain::import_epoch_at_rest`] ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochPackState {
    /// Neither `epoch-N` nor `epoch-N.replaced`.
    Missing,
    /// `epoch-N`, whose data log does not validate (why).
    Damaged(String),
    /// An interrupted install: `epoch-N.replaced` with no `epoch-N`, whose data log does not
    /// validate (why). The install puts the aside back first, then replaces it, as every install
    /// does.
    InterruptedInstall(String),
}

impl Display for EpochPackState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(f, "a missing pack"),
            Self::Damaged(why) => write!(f, "a damaged pack ({why})"),
            Self::InterruptedInstall(why) => {
                write!(f, "an interrupted install whose last copy is damaged ({why})")
            }
        }
    }
}

/// A verified import of [`ConsensusChain::import_epoch_at_rest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochImport {
    /// The epoch imported.
    pub epoch: Epoch,
    /// What held the epoch's place.
    pub replaces: EpochPackState,
    /// The bytes of the copy's data log that were imported.
    pub bytes: u64,
    /// Whether the verified copy was installed as `epoch-N` (else a dry run, which left nothing).
    pub installed: bool,
}

/// Why [`ConsensusChain::import_epoch_at_rest`] refused, or failed.
#[derive(Debug)]
pub enum EpochImportError {
    /// Epoch 0's pack is verified against the pre-genesis descriptor, which no copy carries.
    EpochZero,
    /// Not a past epoch: `resume` is the epoch the node opens for append when it starts (the
    /// latest-consensus hint, else the newest `epoch-N`), and its pack is the node's to write.
    NotPast {
        /// The epoch asked for.
        epoch: Epoch,
        /// The epoch the node resumes.
        resume: Epoch,
    },
    /// The local epoch records lack what the copy is verified against: the record of the epoch
    /// and its certificate, and the record before it (why).
    Records(String),
    /// The epoch's pack (or an interrupted install's copy of it) validates and holds the epoch's
    /// certified final output: nothing to import.
    NothingToImport {
        /// The epoch asked for.
        epoch: Epoch,
        /// The directory holding the pack that validates.
        dir: PathBuf,
    },
    /// The copy does not read as a data log of the epoch's pack (why).
    Source(String),
    /// The copy failed the import's verification, or the import failed; nothing was installed.
    Import(ConsensusChainError),
    /// The copy verified, but its install as `epoch-N` failed, possibly part way (see the
    /// message): the node's next start settles what it left (`recover_incomplete_installs`).
    Install {
        /// The epoch asked for.
        epoch: Epoch,
        /// Why the install failed.
        error: ConsensusChainError,
    },
}

impl Display for EpochImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EpochZero => write!(
                f,
                "epoch 0 cannot be imported: its pack is verified against the pre-genesis \
                 descriptor, which a copy does not carry; restore `epoch-0` from a backup"
            ),
            Self::NotPast { epoch, resume } => write!(
                f,
                "epoch {epoch} is not a past epoch: the node resumes epoch {resume}, whose pack \
                 it writes (a restart or `db repair --epoch {resume} --force` repairs it); only \
                 an epoch below {resume} can be imported"
            ),
            Self::Records(why) => write!(f, "cannot verify the copy: {why}"),
            Self::NothingToImport { epoch, dir } => write!(
                f,
                "nothing to import: the pack of epoch {epoch} at {} validates and ends with the \
                 epoch's certified final output (`db repair --epoch {epoch}` repairs its indexes \
                 and puts back an interrupted install)",
                dir.display()
            ),
            Self::Source(why) => write!(f, "{why}"),
            Self::Import(e) => write!(f, "the copy did not import, and nothing was installed: {e}"),
            Self::Install { epoch, error } => write!(
                f,
                "the copy verified, but its install did not complete ({error}). It may have \
                 stopped part way: `epoch-{epoch}` may already be the verified copy, with the pack \
                 it replaced at `epoch-{epoch}.replaced`, or `epoch-{epoch}` may be missing, with \
                 that pack at `epoch-{epoch}.replaced`. The node's next start settles either \
                 (`recover_incomplete_installs` keeps `epoch-{epoch}` and removes the aside, or \
                 puts the aside back), and so does a rerun of this import, which puts an \
                 interrupted install back first and finds nothing to import once the copy is in \
                 place"
            ),
        }
    }
}

impl std::error::Error for EpochImportError {}

impl ConsensusChain {
    /// Re-import the pack of past epoch `epoch` under the epochs directory `base_path` from the
    /// copy at `source` (a pack's `data` file, or a directory holding one), with the node
    /// stopped (see the [module docs](self)). Without `install` it is a dry run: the copy is
    /// imported and verified in `import-N`, which is then removed, and nothing else is written.
    ///
    /// Refuses, before anything is written: a copy inside `base_path`, whose directories the
    /// import and the node's start remove ([`EpochImportError::Source`]); epoch 0
    /// ([`EpochImportError::EpochZero`]); an
    /// epoch the node does not only read ([`EpochImportError::NotPast`]); local records that
    /// lack the record of `epoch`, its certificate, or the record of `epoch - 1` it chains to
    /// ([`EpochImportError::Records`]); and a pack in place that validates and ends with the
    /// epoch's certified final output, as `epoch-N` or as an interrupted install's
    /// `epoch-N.replaced` ([`EpochImportError::NothingToImport`]; each pack version judged by its
    /// own rule, see `pack_damage`). What it replaces is a missing pack, a damaged or short one,
    /// or an interrupted install whose copy is damaged ([`EpochPackState`]).
    ///
    /// The stream is the copy's data log up to the end of its last complete output (a sealed log
    /// ends with its clean-close sentinel, a live one with padding, neither of which a peer
    /// serves), and it stops before free space on the filesystem drops below `min_free`, as a peer
    /// import does: the node's stores share it. Pass the node's own floor
    /// ([`import_min_free`](crate::consensus_pack::import_min_free) of its execution database's
    /// growth step; [`IMPORT_MIN_FREE_BYTES`](crate::consensus_pack::IMPORT_MIN_FREE_BYTES) for the
    /// default step).
    pub async fn import_epoch_at_rest(
        base_path: &Path,
        epoch: Epoch,
        source: &Path,
        install: bool,
        timeout: Duration,
        min_free: u64,
    ) -> Result<EpochImport, EpochImportError> {
        if let Some(why) = source_inside(base_path, source) {
            return Err(EpochImportError::Source(why));
        }
        if epoch == 0 {
            return Err(EpochImportError::EpochZero);
        }
        let resume = Self::resume_epoch(base_path).or_else(|| {
            ConsensusPack::epoch_dirs(base_path).ok().and_then(|epochs| epochs.last().copied())
        });
        match resume {
            Some(resume) if epoch < resume => {}
            resume => {
                return Err(EpochImportError::NotPast { epoch, resume: resume.unwrap_or_default() })
            }
        }
        let (record, previous) = local_records(base_path, epoch)?;
        let replaces = pack_in_place(base_path, &record, &previous)?;

        let data = if source.is_dir() { source.join(DATA_NAME) } else { source.to_path_buf() };
        let len = {
            let data = data.clone();
            tokio::task::spawn_blocking(move || wal_consistent_end(&data, epoch))
                .await
                .map_err(|e| EpochImportError::Source(format!("the copy's check stopped: {e}")))?
        }
        .map_err(|e| {
            EpochImportError::Source(format!(
                "{} does not read as a data log of epoch {epoch}'s pack: {e}",
                data.display()
            ))
        })?;
        let file = tokio::fs::File::open(&data).await.map_err(|e| {
            EpochImportError::Source(format!("cannot open {}: {e}", data.display()))
        })?;
        let Some((import_path, path)) = Self::import_sealed(
            base_path,
            file.take(len),
            &record,
            &previous,
            timeout,
            None,
            min_free,
        )
        .await
        .map_err(EpochImportError::Import)?
        else {
            return Err(EpochImportError::Import(ConsensusChainError::IO(std::io::Error::other(
                format!("this process is already importing epoch {epoch}"),
            ))));
        };
        if install {
            Self::install_imported_epoch_dir(
                base_path,
                epoch,
                &path.join(format!("epoch-{epoch}")),
            )
            .map_err(|error| EpochImportError::Install { epoch, error })?;
        }
        drop(import_path);
        Ok(EpochImport { epoch, replaces, bytes: len, installed: install })
    }
}

/// Why the copy at `source` cannot be read where it is: inside the epochs directory `base_path`,
/// whose directories the import and the node's start remove or replace (the import removes a
/// leftover `import-N` before it reads the copy, and the start sweeps `import-*` and `staging-*`
/// and settles `epoch-N.replaced`). Both are compared resolved (symlinks and `..` followed); a
/// copy that does not exist is left to the read that reports it, which runs before anything is
/// removed.
fn source_inside(base_path: &Path, source: &Path) -> Option<String> {
    let (Ok(source_real), Ok(base_real)) = (source.canonicalize(), base_path.canonicalize()) else {
        return None;
    };
    source_real.starts_with(&base_real).then(|| {
        format!(
            "the copy at {} is inside the epochs directory {}, whose directories the import and \
             the node remove or replace: copy it outside the datadir's consensus database and \
             pass that path",
            source.display(),
            base_path.display()
        )
    })
}

/// The local records of `epoch` and `epoch - 1`, read from the records log at rest (no index,
/// nothing written): the record of `epoch` must carry a certificate in the local certificates
/// log, and chain to the record before it.
fn local_records(
    base_path: &Path,
    epoch: Epoch,
) -> Result<(EpochRecord, EpochRecord), EpochImportError> {
    let unreadable = |what: &str, path: PathBuf, e: &dyn Display| {
        EpochImportError::Records(format!(
            "the local epoch {what} at {} do not read ({e}); with the node stopped, `db repair \
             --force` heals them",
            path.display()
        ))
    };
    let records_path = base_path.join(RECORDS_NAME);
    let records = EpochRecordDb::read_records_from_pack(&records_path)
        .map_err(|e| unreadable("records", records_path, &e))?;
    let find = |epoch: Epoch| {
        records.iter().find(|record| record.epoch == epoch).cloned().ok_or_else(|| {
            EpochImportError::Records(format!("this node holds no record of epoch {epoch}"))
        })
    };
    let (record, previous) = (find(epoch)?, find(epoch - 1)?);
    if record.parent_hash != previous.digest() {
        return Err(EpochImportError::Records(format!(
            "the local record of epoch {epoch} does not chain to the record of epoch {}",
            epoch - 1
        )));
    }
    let certs_path = base_path.join(CERTS_NAME);
    let certs = EpochRecordDb::read_certs_from_pack(&certs_path)
        .map_err(|e| unreadable("certificates", certs_path, &e))?;
    if !certs.iter().any(|cert| cert.epoch_hash == record.digest()) {
        return Err(EpochImportError::Records(format!(
            "the local record of epoch {epoch} is not certified yet"
        )));
    }
    Ok((record, previous))
}

/// What holds the place of the pack of `record`'s epoch under `base_path`; refused when it
/// validates and holds the epoch's certified final output.
fn pack_in_place(
    base_path: &Path,
    record: &EpochRecord,
    previous: &EpochRecord,
) -> Result<EpochPackState, EpochImportError> {
    let epoch = record.epoch;
    let live = base_path.join(format!("epoch-{epoch}"));
    let aside = base_path.join(format!("epoch-{epoch}.replaced"));
    let (dir, state): (_, fn(String) -> EpochPackState) = if live.exists() {
        (live, EpochPackState::Damaged)
    } else if aside.exists() {
        (aside, EpochPackState::InterruptedInstall)
    } else {
        return Ok(EpochPackState::Missing);
    };
    match pack_damage(&dir.join(DATA_NAME), record, previous) {
        Some(why) => Ok(state(why)),
        None => Err(EpochImportError::NothingToImport { epoch, dir }),
    }
}

/// Why the data log at `data` of the past epoch of `record` (certified; `previous` is the record
/// of the epoch before it) is not a whole, healthy pack of the epoch, or `None` when it is: it
/// validates as `db validate` judges it, as the node reads a pack of its version, and it holds
/// the epoch's certified final output (`record.final_consensus`) as its last. Read-only.
///
/// Each pack version is judged by its own rule. A v2 pack carries the clean-close sentinel, and a
/// past epoch's pack was sealed when the next epoch began, so one without it is damaged. A legacy
/// pack (v0 or v1, [`SENTINEL_MIN_VERSION`]) predates the sentinel, so its absence says nothing:
/// the node reads such a pack only through its migration to v2, which for a past epoch refuses
/// any damage in the log, so the dry run of that migration judges it. A v0 (batches-first) pack
/// is read by nothing else, so its outputs are counted against the record's; a v1 pack has v2's
/// layout and is then walked as a v2 pack is.
fn pack_damage(data: &Path, record: &EpochRecord, previous: &EpochRecord) -> Option<String> {
    let epoch = record.epoch;
    if !data.exists() {
        return Some("no data log".to_string());
    }
    let Some((version, unsealed)) = pack_unsealed_version(data, epoch) else {
        return Some("its data log does not open".to_string());
    };
    if version >= SENTINEL_MIN_VERSION {
        if unsealed {
            return Some("its data log is not sealed".to_string());
        }
    } else {
        match legacy_migration_dry_run(data, epoch) {
            Err(why) => return Some(format!("its migration to v2 refuses: {why}")),
            // the epoch's outputs run from the one after the previous epoch's final to its own
            Ok(outputs) if version == 0 => {
                let expected =
                    record.final_consensus.number.saturating_sub(previous.final_consensus.number);
                return (outputs != expected).then(|| {
                    format!(
                        "it holds {outputs} output(s), but the epoch's certified record has \
                         {expected}"
                    )
                });
            }
            Ok(_) => {}
        }
    }
    match classify_physical_corruption(data, epoch) {
        Err(e) => return Some(format!("its data log does not read: {e}")),
        Ok(Some(corruption)) => {
            return Some(format!("{:?} at byte {}", corruption.kind, corruption.offset))
        }
        Ok(None) => {}
    }
    let report = match validate_pack_file(data, epoch, Some(previous)) {
        Ok(report) if report.verdict == Verdict::Invalid => {
            return Some(format!("`db validate --epoch {epoch}` finds it INVALID"))
        }
        Ok(report) => report,
        Err(e) => return Some(format!("its validation failed: {e}")),
    };
    // A pack whose last outputs are gone (a backup taken before the epoch closed, then sealed by
    // `db repair`) still validates as far as it goes: its final output must be the certified one.
    let last = (report.last_consensus_number, report.last_consensus_digest);
    let certified = record.final_consensus;
    (last != (Some(certified.number), Some(certified.hash))).then(|| match last {
        (Some(number), _) => format!(
            "it ends at output {number}, not at the epoch's certified final output {}",
            certified.number
        ),
        _ => format!(
            "it holds no output, not the epoch's certified final output {}",
            certified.number
        ),
    })
}

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        test_genesis, BlsSignature, Committee, ConsensusHeaderDigest, ConsensusNumHash,
        ConsensusOutput, EpochCertificate, EpochRecord,
    };

    use super::{EpochImportError, EpochPackState};
    use crate::{
        consensus::ConsensusChain,
        consensus_pack::{test::make_test_output, ConsensusPack, DATA_NAME, IMPORT_MIN_FREE_BYTES},
        mem_db::MemDatabase,
    };

    const TIMEOUT: Duration = Duration::from_secs(30);

    /// Epoch 1 of [`closed_epochs`]: what its pack is built from.
    struct Epoch1 {
        /// Its three outputs.
        outputs: Vec<ConsensusOutput>,
        /// The record of epoch 0, which its pack's meta chains to.
        previous: EpochRecord,
        /// Its committee.
        committee: Committee,
    }

    /// A node's epochs directory, closed: epochs 0 and 1 sealed with three outputs each, the
    /// live epoch 2 the latest-consensus hint names, and the records of epochs 0 and 1, each
    /// certified. Returns what epoch 1's pack is built from.
    async fn closed_epochs(dir: &Path) -> Epoch1 {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let spec: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committees = [
            fixture.committee(),
            fixture.committee().advance_epoch_for_test(1),
            fixture.committee().advance_epoch_for_test(2),
        ];
        let keys = |epoch: usize| committees[epoch].bls_keys().iter().copied().collect::<Vec<_>>();
        std::fs::create_dir_all(dir).unwrap();
        let chain = ConsensusChain::new(dir.to_owned(), committees[0].clone()).unwrap();
        let mut previous = EpochRecord {
            epoch: 0,
            committee: keys(0),
            next_committee: keys(0),
            ..Default::default()
        };
        chain.new_epoch(previous.clone(), committees[0].clone()).await.unwrap();
        let mut parent = ConsensusHeaderDigest::default();
        let mut epoch1 = Vec::new();
        let mut record0 = None;
        for epoch in 0..2_usize {
            for n in (epoch as u64 * 3 + 1)..=(epoch as u64 * 3 + 3) {
                let output =
                    make_test_output(&committees[epoch], n as usize % 4, spec.clone(), n, parent);
                parent = output.consensus_header_hash();
                if epoch == 1 {
                    epoch1.push(output.clone());
                }
                chain.save_consensus_output(output).await.unwrap();
            }
            chain.persist_current().await.unwrap();
            let record = EpochRecord {
                epoch: epoch as u32,
                committee: keys(epoch),
                next_committee: keys(epoch + 1),
                parent_hash: previous.digest(),
                final_consensus: ConsensusNumHash { number: epoch as u64 * 3 + 3, hash: parent },
                ..Default::default()
            };
            let cert = EpochCertificate {
                epoch_hash: record.digest(),
                signature: BlsSignature::default(),
                signed_authorities: Default::default(),
            };
            chain.epochs().save(record.clone(), cert).await.unwrap();
            chain.new_epoch(record.clone(), committees[epoch + 1].clone()).await.unwrap();
            if epoch == 0 {
                record0 = Some(record.clone());
            }
            previous = record;
        }
        chain.epochs().persist().await.unwrap();
        chain.close().await;
        ConsensusChain::write_latest_consensus_hint(dir, 2, 6).unwrap();
        Epoch1 {
            outputs: epoch1,
            previous: record0.expect("epoch 0 closed"),
            committee: committees[1].clone(),
        }
    }

    /// Replace epoch 1's pack under `base` with one of on-disk `version` holding the first `keep`
    /// of its outputs, closed as a pack of that version was: v2 sealed with its clean-close
    /// sentinel, v0 and v1 (which predate it) without one.
    async fn rebuild_epoch1(base: &Path, epoch1: &Epoch1, version: u16, keep: usize) {
        std::fs::remove_dir_all(base.join("epoch-1")).unwrap();
        let pack = ConsensusPack::open_append_version(
            base,
            epoch1.previous.clone(),
            epoch1.committee.clone(),
            version,
        )
        .unwrap();
        for output in &epoch1.outputs[..keep] {
            pack.save_consensus_output(output.clone()).await.unwrap();
        }
        pack.persist().await.unwrap();
        pack.close().await;
        if version < crate::consensus_pack::SENTINEL_MIN_VERSION {
            let data = base.join("epoch-1").join(DATA_NAME);
            let len = std::fs::metadata(&data).unwrap().len();
            let file = std::fs::OpenOptions::new().write(true).open(&data).unwrap();
            file.set_len(len - crate::archive::data_file::SENTINEL_LEN).unwrap();
        }
    }

    /// Every file under `dir` with its bytes, for proving a run left the directory as it found it.
    fn tree(dir: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        let mut tree = BTreeMap::new();
        let mut pending = vec![dir.to_path_buf()];
        while let Some(at) = pending.pop() {
            for entry in std::fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                let relative = path.strip_prefix(dir).unwrap().to_path_buf();
                if path.is_dir() {
                    tree.insert(relative, None);
                    pending.push(path);
                } else {
                    tree.insert(relative, Some(std::fs::read(&path).unwrap()));
                }
            }
        }
        tree
    }

    /// Copy the files of directory `from` into a new directory `to`.
    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &to.join(entry.file_name()));
            } else {
                std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
            }
        }
    }

    /// Flip one byte in the middle of the file at `path`.
    fn flip_middle_byte(path: &Path) {
        let mut bytes = std::fs::read(path).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xFF;
        std::fs::write(path, bytes).unwrap();
    }

    /// Epoch 1's installed pack holds exactly `outputs`, read back as the node reads a past epoch.
    async fn assert_holds(base: &Path, outputs: &[ConsensusOutput]) {
        let pack = ConsensusPack::open_static(base, 1).expect("the installed pack opens sealed");
        for output in outputs {
            let read = pack.get_consensus_output(output.number()).await.expect("output reads");
            crate::consensus_pack::test::compare_outputs(&read, output);
        }
        pack.close().await;
        assert!(ConsensusChain::pack_data_at_start(base, 1).unwrap(), "a start finds the pack");
        for leftover in ["import-1", "epoch-1.replaced"] {
            assert!(!base.join(leftover).exists(), "{leftover} left behind");
        }
    }

    /// The import refuses, before writing anything, what it cannot verify or need not import:
    /// epoch 0, the live epoch and later, local records missing the epoch or its certificate, a
    /// pack in place that validates (as `epoch-N` or an interrupted install's aside), and a copy
    /// of another epoch, one with a flipped byte, or one that is not a pack.
    #[tokio::test(flavor = "multi_thread")]
    async fn import_epoch_at_rest_refuses_what_it_cannot_verify_or_need_not_import() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("epochs");
        closed_epochs(&base).await;
        let copy = tmp.path().join("copy");
        copy_dir(&base.join("epoch-1"), &copy);
        let import = |epoch, source: PathBuf| {
            let base = base.clone();
            async move {
                ConsensusChain::import_epoch_at_rest(
                    &base,
                    epoch,
                    &source,
                    true,
                    TIMEOUT,
                    IMPORT_MIN_FREE_BYTES,
                )
                .await
            }
        };
        let before = tree(&base);

        let err = import(0, copy.clone()).await.unwrap_err();
        assert!(matches!(err, EpochImportError::EpochZero), "{err}");
        for epoch in [2, 3] {
            let err = import(epoch, copy.clone()).await.unwrap_err();
            assert!(
                matches!(err, EpochImportError::NotPast { epoch: e, resume: 2 } if e == epoch),
                "{err}"
            );
        }
        let err = import(1, copy.clone()).await.unwrap_err();
        assert!(matches!(err, EpochImportError::NothingToImport { epoch: 1, .. }), "{err}");
        std::fs::rename(base.join("epoch-1"), base.join("epoch-1.replaced")).unwrap();
        let err = import(1, copy.clone()).await.unwrap_err();
        assert!(matches!(err, EpochImportError::NothingToImport { epoch: 1, .. }), "{err}");
        std::fs::rename(base.join("epoch-1.replaced"), base.join("epoch-1")).unwrap();
        assert_eq!(tree(&base), before, "a refusal wrote nothing");

        // the copy, with epoch 1's pack missing: another epoch's, a flipped byte, not a pack
        let aside = tmp.path().join("aside");
        std::fs::rename(base.join("epoch-1"), &aside).unwrap();
        let before = tree(&base);
        let err = import(1, base.join("epoch-0")).await.unwrap_err();
        assert!(matches!(err, EpochImportError::Source(_)), "another epoch's pack: {err}");
        let flipped = tmp.path().join("flipped");
        copy_dir(&copy, &flipped);
        flip_middle_byte(&flipped.join(DATA_NAME));
        let err = import(1, flipped).await.unwrap_err();
        assert!(
            matches!(err, EpochImportError::Import(_) | EpochImportError::Source(_)),
            "a flipped byte: {err}"
        );
        let err = import(1, tmp.path().join("no-such-copy")).await.unwrap_err();
        assert!(matches!(err, EpochImportError::Source(_)), "{err}");
        assert_eq!(tree(&base), before, "a refused copy left nothing behind");

        // the records: the epoch's certificate, then the records themselves
        let certs = base.join(crate::epoch_records::CERTS_NAME);
        let certs_bytes = std::fs::read(&certs).unwrap();
        std::fs::remove_file(&certs).unwrap();
        let err = import(1, copy.clone()).await.unwrap_err();
        assert!(matches!(err, EpochImportError::Records(_)), "{err}");
        std::fs::write(&certs, certs_bytes).unwrap();
        std::fs::remove_file(base.join(crate::epoch_records::RECORDS_NAME)).unwrap();
        let err = import(1, copy.clone()).await.unwrap_err();
        assert!(matches!(err, EpochImportError::Records(_)), "{err}");
    }

    /// The local records must hold the epoch, certified, and the epoch before it, linked.
    #[tokio::test]
    async fn import_epoch_at_rest_needs_the_certified_record_and_its_parent() {
        let tmp = TempDir::new().unwrap();
        let db = crate::epoch_records::EpochRecordDb::open(tmp.path()).unwrap();
        let record0 = EpochRecord::default();
        let record1 = EpochRecord { epoch: 1, parent_hash: record0.digest(), ..Default::default() };
        let cert = |record: &EpochRecord| EpochCertificate {
            epoch_hash: record.digest(),
            signature: BlsSignature::default(),
            signed_authorities: Default::default(),
        };
        db.save(record0.clone(), cert(&record0)).await.unwrap();
        db.save_record(record1.clone()).await.unwrap();
        db.persist().await.unwrap();
        db.close().await;
        let err = super::local_records(tmp.path(), 1).unwrap_err();
        assert!(err.to_string().contains("not certified"), "{err}");
        let err = super::local_records(tmp.path(), 3).unwrap_err();
        assert!(err.to_string().contains("no record of epoch 3"), "{err}");
        let db = crate::epoch_records::EpochRecordDb::open(tmp.path()).unwrap();
        db.save_certificate(record1.digest(), cert(&record1)).await.unwrap();
        // certified, but not the child of the record before it
        let record2 = EpochRecord { epoch: 2, parent_hash: record0.digest(), ..Default::default() };
        db.save(record2.clone(), cert(&record2)).await.unwrap();
        db.persist().await.unwrap();
        db.close().await;
        assert_eq!(super::local_records(tmp.path(), 1).unwrap(), (record1, record0));
        let err = super::local_records(tmp.path(), 2).unwrap_err();
        assert!(err.to_string().contains("does not chain"), "{err}");
    }

    /// A dry run verifies a copy and leaves the epochs directory byte-identical; the install puts
    /// a verified copy in place of a missing pack, a damaged one, and an interrupted install whose
    /// aside is damaged, and the node then reads every output of the epoch.
    #[tokio::test(flavor = "multi_thread")]
    async fn import_epoch_at_rest_installs_a_verified_copy() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("epochs");
        let outputs = closed_epochs(&base).await.outputs;
        let copy = tmp.path().join("copy");
        copy_dir(&base.join("epoch-1"), &copy);
        let import = |source: &Path, install| {
            let (base, source) = (base.clone(), source.to_path_buf());
            async move {
                ConsensusChain::import_epoch_at_rest(
                    &base,
                    1,
                    &source,
                    install,
                    TIMEOUT,
                    IMPORT_MIN_FREE_BYTES,
                )
                .await
            }
        };

        // missing: the dry run leaves nothing, the install puts it back
        std::fs::remove_dir_all(base.join("epoch-1")).unwrap();
        let before = tree(&base);
        // the floor it is given: one no filesystem meets stops the import, writing nothing
        let err =
            ConsensusChain::import_epoch_at_rest(&base, 1, &copy, true, TIMEOUT, u64::MAX / 2)
                .await
                .expect_err("an unmet floor stops the import");
        assert!(format!("{err}").contains("free"), "{err}");
        assert_eq!(tree(&base), before, "a stopped import changed the epochs directory");
        let dry = import(&copy, false).await.expect("the copy verifies");
        assert_eq!((dry.replaces.clone(), dry.installed), (EpochPackState::Missing, false));
        // what a peer serves: the sealed log without its clean-close sentinel
        let sealed = std::fs::metadata(copy.join(DATA_NAME)).unwrap().len();
        assert_eq!(dry.bytes, sealed - crate::archive::data_file::SENTINEL_LEN);
        assert_eq!(tree(&base), before, "a dry run changed the epochs directory");
        let done = import(&copy.join(DATA_NAME), true).await.expect("installed");
        assert_eq!(
            (done.replaces, done.installed, done.bytes),
            (EpochPackState::Missing, true, dry.bytes)
        );
        assert_holds(&base, &outputs).await;

        // damaged in place: replaced
        flip_middle_byte(&base.join("epoch-1").join(DATA_NAME));
        let before = tree(&base);
        let dry = import(&copy, false).await.expect("the copy verifies");
        assert!(matches!(dry.replaces, EpochPackState::Damaged(_)), "{:?}", dry.replaces);
        assert_eq!(tree(&base), before, "a dry run changed the epochs directory");
        let done = import(&copy, true).await.expect("installed");
        assert!(matches!(done.replaces, EpochPackState::Damaged(_)), "{:?}", done.replaces);
        assert_holds(&base, &outputs).await;

        // an interrupted install whose aside is damaged: put back, then replaced
        std::fs::rename(base.join("epoch-1"), base.join("epoch-1.replaced")).unwrap();
        flip_middle_byte(&base.join("epoch-1.replaced").join(DATA_NAME));
        let done = import(&copy, true).await.expect("installed");
        assert!(
            matches!(done.replaces, EpochPackState::InterruptedInstall(_)),
            "{:?}",
            done.replaces
        );
        assert_holds(&base, &outputs).await;
    }

    /// Each pack version is judged by its own rule (`pack_damage`): a whole legacy pack (v0 or
    /// v1, which predate the clean-close sentinel) and a whole sealed v2 pack are refused as
    /// nothing to import, while a pack of each version that lost its last output, though it reads
    /// cleanly as far as it goes, is replaced by the verified copy.
    #[tokio::test(flavor = "multi_thread")]
    async fn import_epoch_at_rest_judges_each_version_and_needs_the_certified_final() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("epochs");
        let epoch1 = closed_epochs(&base).await;
        let copy = tmp.path().join("copy");
        copy_dir(&base.join("epoch-1"), &copy);
        let import = || {
            ConsensusChain::import_epoch_at_rest(
                &base,
                1,
                &copy,
                true,
                TIMEOUT,
                IMPORT_MIN_FREE_BYTES,
            )
        };
        for version in [0_u16, 1, 2] {
            rebuild_epoch1(&base, &epoch1, version, 3).await;
            let err = import().await.unwrap_err();
            assert!(
                matches!(err, EpochImportError::NothingToImport { epoch: 1, .. }),
                "a whole v{version} pack: {err}"
            );
            rebuild_epoch1(&base, &epoch1, version, 2).await;
            let done = import().await.expect("a short pack is replaced");
            assert!(
                matches!(done.replaces, EpochPackState::Damaged(_)),
                "a short v{version} pack: {:?}",
                done.replaces
            );
            assert_holds(&base, &epoch1.outputs).await;
        }
    }

    /// An install that fails after the copy verified is reported as an install failure, naming
    /// what it may have left and that the node's next start settles it, never as a copy that
    /// changed nothing; a rerun then installs the copy.
    #[tokio::test]
    async fn import_epoch_at_rest_reports_a_failed_install_as_one() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("epochs");
        let epoch1 = closed_epochs(&base).await;
        let copy = tmp.path().join("copy");
        copy_dir(&base.join("epoch-1"), &copy);
        flip_middle_byte(&base.join("epoch-1").join(DATA_NAME));
        // the install runs on this (current-thread) test's thread
        crate::consensus_pack::FAIL_NEXT_INSTALL.with(|fail| fail.set(true));
        let err = ConsensusChain::import_epoch_at_rest(
            &base,
            1,
            &copy,
            true,
            TIMEOUT,
            IMPORT_MIN_FREE_BYTES,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, EpochImportError::Install { epoch: 1, .. }), "{err}");
        let text = err.to_string();
        for named in ["recover_incomplete_installs", "`epoch-1.replaced`", "part way"] {
            assert!(text.contains(named), "{named}: {text}");
        }
        assert!(!text.contains("nothing was installed"), "{text}");
        let done = ConsensusChain::import_epoch_at_rest(
            &base,
            1,
            &copy,
            true,
            TIMEOUT,
            IMPORT_MIN_FREE_BYTES,
        )
        .await
        .expect("a rerun installs the copy");
        assert!(matches!(done.replaces, EpochPackState::Damaged(_)), "{:?}", done.replaces);
        assert_holds(&base, &epoch1.outputs).await;
    }

    /// A copy inside the epochs directory is refused before anything is read or removed: the
    /// import's own `import-N`, which it would remove before reading the copy, or any other
    /// directory there, however the path is spelled.
    #[tokio::test(flavor = "multi_thread")]
    async fn import_epoch_at_rest_refuses_a_copy_inside_the_epochs_directory() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("epochs");
        closed_epochs(&base).await;
        std::fs::rename(base.join("epoch-1"), base.join("import-1")).unwrap();
        copy_dir(&base.join("import-1"), &base.join("copy-1"));
        let before = tree(&base);
        let spelled = tmp.path().join("epochs").join("..").join("epochs").join("copy-1");
        for (source, install) in [
            (base.join("import-1"), false),
            (base.join("import-1").join(DATA_NAME), true),
            (spelled, true),
        ] {
            let err = ConsensusChain::import_epoch_at_rest(
                &base,
                1,
                &source,
                install,
                TIMEOUT,
                IMPORT_MIN_FREE_BYTES,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, EpochImportError::Source(why) if why.contains("inside the epochs")),
                "{}: {err}",
                source.display()
            );
        }
        assert_eq!(tree(&base), before, "a refused copy was touched");
    }
}

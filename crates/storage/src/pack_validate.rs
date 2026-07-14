//! Read-only validation of consensus epoch pack files.
//!
//! [`validate_pack_file`] mirrors the integrity checks performed by
//! [`Inner::stream_import`](crate::consensus_pack) — the importer a syncing node runs against a
//! streamed epoch pack — but instead of bailing on the first error it walks the entire `data`
//! stream and collects *every* problem into a [`PackValidationReport`]. This makes it possible to
//! diagnose a pack offline (e.g. reproduce a node's `MissingBatches` failure) and, crucially, to
//! classify each missing batch as either:
//!
//! - [`BatchClass::Absent`] — the digest appears *nowhere* in the file → a genuine data gap that
//!   needs an external source to fix (re-fetch / regenerate the pack), or
//! - [`BatchClass::Misordered`] — the digest *is* present in the file, just not inside the
//!   consensus header's group → a pack-construction ordering bug, fixable without external data.
//!
//! ## Why the `data` stream alone is enough
//!
//! `stream_import` only consumes the record stream: it tracks a per-consensus-header set of batch
//! digests seen so far and checks every digest referenced by a consensus header against it. The
//! `idx`/`hash`/`bhash` sidecar indexes are only needed to *use* a pack, not to judge its
//! integrity, so validation runs against the bare `data` file.
//!
//! ## The cleared-set subtlety
//!
//! The per-header `batches` set is cleared after **every** consensus header
//! (`consensus_pack.rs`), so a batch referenced by header *N* must appear as a `Batch` record
//! within *N*'s group (after header *N-1*, at/before header *N*). A batch present elsewhere in the
//! file but cleared before *N* still triggers `MissingBatches` — that is exactly the
//! Absent-vs-Misordered distinction this validator surfaces.

use std::{
    collections::{BTreeSet, HashSet},
    fmt::{self, Display},
    path::Path,
};

use tn_types::{BlockHash, ConsensusHeader, ConsensusHeaderDigest, Epoch, EpochRecord};

use crate::{
    archive::{
        error::fetch::FetchError,
        pack::{Pack, PackCompression},
    },
    consensus_pack::{verify_epoch_meta, PackError, PackRecord, PACK_VERSION},
};

/// Classification of a referenced-but-missing batch digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchClass {
    /// The digest appears nowhere in the file — a real data gap. Repair needs an external source.
    Absent,
    /// The digest is present in the file, but not within its consensus header's group — a
    /// pack-construction ordering bug, repairable by re-emitting it into the right group.
    Misordered,
}

impl Display for BatchClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchClass::Absent => write!(f, "ABSENT"),
            BatchClass::Misordered => write!(f, "MISORDERED"),
        }
    }
}

/// A single integrity problem found while walking a pack file.
#[derive(Debug, Clone)]
pub enum PackIssue {
    /// A consensus header's `parent_hash` does not match the previous header's digest.
    ChainBreak {
        /// Consensus number of the offending header.
        number: u64,
        /// The parent digest we expected (previous header's digest, or epoch anchor).
        expected_parent: ConsensusHeaderDigest,
        /// The `parent_hash` actually recorded on the header.
        found_parent: ConsensusHeaderDigest,
    },
    /// A consensus header references a batch digest not present in its group.
    MissingBatch {
        /// Consensus number of the referencing header.
        number: u64,
        /// The referenced batch digest.
        digest: BlockHash,
        /// Whether the digest is absent from the whole file or merely misordered.
        class: BatchClass,
    },
    /// A `Batch` record present in a group but referenced by no header in that group.
    ExtraBatch {
        /// Consensus number that closed the group containing the orphan batch.
        number: u64,
        /// The unreferenced batch digest.
        digest: BlockHash,
    },
    /// A v1 group's `Batch` records are present and correct as a set, but not in the ascending
    /// digest order the v1 importer ([`iter_to_output`](crate::consensus_pack)) requires. Only
    /// emitted when the group has no missing/extra batch, so it isolates a pure ordering defect —
    /// distinct from [`BatchClass::Misordered`], which means a batch in the *wrong group*. (v1
    /// only; v0 does not constrain intra-group batch order.)
    UnsortedBatches {
        /// Consensus number of the group whose batches are out of order.
        number: u64,
    },
    /// A consensus header's `number` is not the next sequential value.
    ///
    /// Mirrors the importer's [`Inner::save_consensus_output`](crate::consensus_pack) check, which
    /// rejects any output whose number is not exactly `start_consensus_number +
    /// headers_written_so_far`. `expected` is position-based (derived from how many headers
    /// preceded this one, not from the previous header's recorded number), so a single bad
    /// header does not cascade into spurious issues for every following header.
    NonSequentialConsensusNumber {
        /// The number this header should have carried, given its position in the stream.
        expected: u64,
        /// The `number` actually recorded on the header.
        found: u64,
    },
    /// The `EpochMeta` record failed cross-checks (epoch mismatch, or full linkage when a previous
    /// [`EpochRecord`] is supplied).
    EpochMetaMismatch {
        /// Human-readable description of the mismatch.
        detail: String,
    },
}

/// Overall verdict for a pack file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// No issues found — the pack would import cleanly.
    Valid,
    /// One or more issues found — the pack would be rejected by `stream_import`.
    Invalid,
}

impl Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Valid => write!(f, "VALID"),
            Verdict::Invalid => write!(f, "INVALID"),
        }
    }
}

/// The result of validating a pack `data` file.
#[derive(Debug, Clone)]
pub struct PackValidationReport {
    /// Epoch the pack was validated as.
    pub epoch: Epoch,
    /// First consensus number of the epoch, taken from the `EpochMeta` record.
    pub start_consensus_number: u64,
    /// Total `Batch` records found anywhere in the file.
    pub batch_count: u64,
    /// Total `Consensus` (header) records found in the file.
    pub consensus_count: u64,
    /// Consensus number of the first header in the file, if any.
    pub first_consensus_number: Option<u64>,
    /// Consensus number of the last header in the file, if any.
    pub last_consensus_number: Option<u64>,
    /// Every issue found, in file order.
    pub issues: Vec<PackIssue>,
    /// `Valid` iff `issues` is empty.
    pub verdict: Verdict,
}

impl PackValidationReport {
    /// Number of [`PackIssue::MissingBatch`] entries with the given class.
    pub fn missing_batch_count(&self, class: BatchClass) -> usize {
        self.issues
            .iter()
            .filter(|i| matches!(i, PackIssue::MissingBatch { class: c, .. } if *c == class))
            .count()
    }
}

/// Validate a consensus epoch pack `data` file without using its sidecar indexes.
///
/// `path` must point at the pack's `data` stream file (the same bytes streamed over the wire).
/// `epoch` is the epoch the pack belongs to; it is cross-checked against the file header (the
/// header `uid` derives from the epoch, so the wrong epoch fails to open) and against the
/// `EpochMeta` record.
///
/// When `previous` (the previous epoch's [`EpochRecord`]) is supplied, the full
/// [`verify_epoch_meta`] linkage checks run and the first header's `parent_hash` is anchored to the
/// previous epoch's final consensus header. With no previous record those linkage checks and the
/// first-header parent check are skipped (everything else still runs).
pub fn validate_pack_file(
    path: &Path,
    epoch: Epoch,
    previous: Option<&EpochRecord>,
) -> Result<PackValidationReport, PackError> {
    // Read-only open of just the data file — `Pack::open` loads/cross-checks the header (the wrong
    // epoch fails here with an open error) and needs no sidecar index files.
    let pack =
        Pack::<PackRecord>::open(path, epoch as u64, true, PackCompression::ZStd, PACK_VERSION)?;

    // ---- Single pass: mirror `Inner::stream_import`, but collect every issue instead of bailing.
    let mut issues = Vec::new();

    let mut iter = pack.raw_iter().map_err(|e| PackError::ReadError(e.to_string()))?;

    // The first record must be the EpochMeta.
    let epoch_meta = match iter.next() {
        Some(record) => match record? {
            PackRecord::EpochMeta(meta) => meta,
            _ => return Err(PackError::NotEpoch),
        },
        None => return Err(PackError::NotEpoch),
    };

    // Cross-check the epoch stored in the meta record against the epoch we opened with.
    if epoch_meta.epoch != epoch {
        issues.push(PackIssue::EpochMetaMismatch {
            detail: format!(
                "data file opened as epoch {epoch} but EpochMeta record says epoch {}",
                epoch_meta.epoch
            ),
        });
    }
    // Optional full linkage check against the previous epoch's record.
    if let Some(previous) = previous {
        if let Err(e) = verify_epoch_meta(epoch, previous, &epoch_meta) {
            issues.push(PackIssue::EpochMetaMismatch { detail: e.to_string() });
        }
    }

    let start_consensus_number = epoch_meta.start_consensus_number;

    // Expected `parent_hash` of the *next* consensus header. `None` = "no anchor, skip the check":
    // a bare file with no previous record cannot verify its first header's parent.
    let expected_parent: Option<ConsensusHeaderDigest> = if epoch == 0 {
        Some(ConsensusHeader::default().digest())
    } else {
        previous.map(|p| p.final_consensus.hash)
    };

    if pack.version() == 0 {
        verify_v0_data(&mut iter, epoch, expected_parent, start_consensus_number, issues)
    } else {
        verify_v1_data(&mut iter, epoch, expected_parent, start_consensus_number, issues)
    }
}

fn verify_v0_data(
    iter: &mut impl Iterator<Item = Result<PackRecord, FetchError>>,
    epoch: Epoch,
    mut expected_parent: Option<ConsensusHeaderDigest>,
    start_consensus_number: u64,
    mut issues: Vec<PackIssue>,
) -> Result<PackValidationReport, PackError> {
    let mut batch_count: u64 = 0;
    let mut consensus_count: u64 = 0;
    let mut first_consensus_number: Option<u64> = None;
    let mut last_consensus_number: Option<u64> = None;
    // Per-group sets, cleared after every consensus header exactly like `stream_import`.
    let mut batches: HashSet<BlockHash> = HashSet::new();
    let mut referenced_batches: HashSet<BlockHash> = HashSet::new();

    // Persistent, never-cleared set of every batch digest seen anywhere in the file. This is what
    // lets us tell an *absent* batch (a real data gap) apart from a *misordered* one (present,
    // wrong group). Classification is deferred until end-of-loop, when this set is complete.
    let mut all_batch_digests: HashSet<BlockHash> = HashSet::new();

    for record in iter {
        match record? {
            PackRecord::EpochMeta(_) => {
                // A second EpochMeta is the same failure `stream_import` rejects.
                issues.push(PackIssue::EpochMetaMismatch {
                    detail: "epoch meta data found more than once".to_string(),
                });
            }
            PackRecord::Batch(batch) => {
                batch_count += 1;
                // Compute the (re-encode + hash) digest once and record it in both the per-group
                // set and the persistent global set.
                let digest = batch.digest();
                batches.insert(digest);
                all_batch_digests.insert(digest);
            }
            PackRecord::Consensus(consensus_header) => {
                consensus_count += 1;
                let number = consensus_header.number;
                first_consensus_number.get_or_insert(number);
                last_consensus_number = Some(number);

                // 0. Sequential numbering, mirroring `Inner::save_consensus_output`. The expected
                // number is position-based: `start + (headers seen before this one)`. Because the
                // header `number` is hashed into the digest, a *missing/reordered* header normally
                // trips the `parent_hash` chain check below — but a corrupted number on the final
                // header has no successor to catch it, and the importer rejects any non-sequential
                // number outright, so check it explicitly here. Keeping `expected` position-based
                // (not "previous number + 1") means one bad header doesn't cascade into spurious
                // issues for every following header.
                let expected_number = start_consensus_number + (consensus_count - 1);
                if number != expected_number {
                    issues.push(PackIssue::NonSequentialConsensusNumber {
                        expected: expected_number,
                        found: number,
                    });
                }

                // 1. Chain continuity (skip when we have no anchor yet).
                if let Some(parent) = expected_parent {
                    if consensus_header.parent_hash != parent {
                        issues.push(PackIssue::ChainBreak {
                            number,
                            expected_parent: parent,
                            found_parent: consensus_header.parent_hash,
                        });
                    }
                }

                // 2. Every referenced batch must be present in *this* header's group. The global
                // set is not yet complete here (a referenced batch may appear later in the file),
                // so record the issue with a placeholder class and resolve it after the loop.
                for header in consensus_header.sub_dag.headers() {
                    for (digest, _) in header.payload().iter() {
                        if batches.contains(digest) {
                            referenced_batches.insert(*digest);
                        } else {
                            issues.push(PackIssue::MissingBatch {
                                number,
                                digest: *digest,
                                class: BatchClass::Absent,
                            });
                        }
                    }
                }

                // 3. Any present-but-unreferenced batch in this group is an extra.
                // `referenced_batches` only ever holds digests that were also in `batches`, so the
                // difference is exactly the orphans (mirrors stream_import's `len()` comparison).
                for digest in batches.difference(&referenced_batches) {
                    issues.push(PackIssue::ExtraBatch { number, digest: *digest });
                }

                // Group boundary: clear, exactly like `stream_import`.
                batches.clear();
                referenced_batches.clear();
                expected_parent = Some(consensus_header.digest());
            }
        }
    }

    Ok(finalize_report(
        epoch,
        start_consensus_number,
        batch_count,
        consensus_count,
        first_consensus_number,
        last_consensus_number,
        issues,
        &all_batch_digests,
    ))
}

/// Validate the `data` record stream of a **v1** pack (header-first layout).
///
/// v1 writes each `Consensus` header *before* the `Batch` records it references (the reverse of
/// v0), and those batches arrive in ascending digest order (`collect_batches` uses a `BTreeMap`;
/// [`iter_to_output`](crate::consensus_pack) rejects any out-of-order batch). So a group's batches
/// are exactly the `Batch` records between a header and the next header. We hold the open header
/// and the batches seen since it, resolving the group when the next header (or EOF) closes it.
///
/// The per-header sequential-number and chain-continuity checks are identical to
/// [`verify_v0_data`]; only the batch grouping differs, plus the v1-only intra-group ordering check
/// performed in [`close_v1_group`].
fn verify_v1_data(
    iter: &mut impl Iterator<Item = Result<PackRecord, FetchError>>,
    epoch: Epoch,
    mut expected_parent: Option<ConsensusHeaderDigest>,
    start_consensus_number: u64,
    mut issues: Vec<PackIssue>,
) -> Result<PackValidationReport, PackError> {
    let mut batch_count: u64 = 0;
    let mut consensus_count: u64 = 0;
    let mut first_consensus_number: Option<u64> = None;
    let mut last_consensus_number: Option<u64> = None;

    // Persistent, never-cleared set of every batch digest seen anywhere in the file — same role as
    // in `verify_v0_data`: it lets the deferred `MissingBatch` classification tell an *absent*
    // digest (a real gap) apart from a *misordered* one (present, wrong group).
    let mut all_batch_digests: HashSet<BlockHash> = HashSet::new();

    // The currently open consensus header and the batch digests seen since it, in arrival order
    // (a `Vec`, not a set, because the v1 ordering check needs the sequence). A batch that appears
    // before the first header — a malformed pack the real writer never produces and the importer
    // rejects outright — is attributed to the first group; an acceptable diagnostic.
    let mut open_header: Option<Box<ConsensusHeader>> = None;
    let mut collected: Vec<BlockHash> = Vec::new();

    for record in iter {
        match record? {
            PackRecord::EpochMeta(_) => {
                // A second EpochMeta is the same failure `stream_import` rejects.
                issues.push(PackIssue::EpochMetaMismatch {
                    detail: "epoch meta data found more than once".to_string(),
                });
            }
            PackRecord::Batch(batch) => {
                batch_count += 1;
                let digest = batch.digest();
                collected.push(digest);
                all_batch_digests.insert(digest);
            }
            PackRecord::Consensus(consensus_header) => {
                // A new header means the previous group's batches have all arrived — close it.
                if let Some(prev) = open_header.take() {
                    close_v1_group(&prev, &collected, &mut issues);
                    collected.clear();
                }

                consensus_count += 1;
                let number = consensus_header.number;
                first_consensus_number.get_or_insert(number);
                last_consensus_number = Some(number);

                // Sequential numbering and chain continuity, identical to `verify_v0_data`. See the
                // comments there for why `expected` is position-based and why the trailing header
                // needs the explicit sequential check.
                let expected_number = start_consensus_number + (consensus_count - 1);
                if number != expected_number {
                    issues.push(PackIssue::NonSequentialConsensusNumber {
                        expected: expected_number,
                        found: number,
                    });
                }
                if let Some(parent) = expected_parent {
                    if consensus_header.parent_hash != parent {
                        issues.push(PackIssue::ChainBreak {
                            number,
                            expected_parent: parent,
                            found_parent: consensus_header.parent_hash,
                        });
                    }
                }
                expected_parent = Some(consensus_header.digest());
                open_header = Some(consensus_header);
            }
        }
    }
    // Close the final group at EOF.
    if let Some(prev) = open_header.take() {
        close_v1_group(&prev, &collected, &mut issues);
    }

    Ok(finalize_report(
        epoch,
        start_consensus_number,
        batch_count,
        consensus_count,
        first_consensus_number,
        last_consensus_number,
        issues,
        &all_batch_digests,
    ))
}

/// Resolve a closed v1 group: append the batch-presence, extra-batch and ordering issues for the
/// header whose group just ended. `collected` is the group's batch digests in arrival (file) order.
///
/// Mirrors the per-header batch checks `verify_v0_data` performs inline, plus the v1-only ordering
/// check. `MissingBatch` is recorded with a placeholder [`BatchClass::Absent`]; the final class is
/// resolved in [`finalize_report`] once every digest in the file is known.
fn close_v1_group(header: &ConsensusHeader, collected: &[BlockHash], issues: &mut Vec<PackIssue>) {
    let number = header.number;
    let collected_set: HashSet<BlockHash> = collected.iter().copied().collect();

    // Every referenced batch must be present in this header's group.
    let mut referenced: BTreeSet<BlockHash> = BTreeSet::new();
    let mut missing = false;
    for sub_header in header.sub_dag.headers() {
        for (digest, _) in sub_header.payload().iter() {
            referenced.insert(*digest);
            if !collected_set.contains(digest) {
                missing = true;
                issues.push(PackIssue::MissingBatch {
                    number,
                    digest: *digest,
                    class: BatchClass::Absent,
                });
            }
        }
    }

    // Any collected batch the header does not reference is an extra.
    let mut extra = false;
    for digest in &collected_set {
        if !referenced.contains(digest) {
            extra = true;
            issues.push(PackIssue::ExtraBatch { number, digest: *digest });
        }
    }

    // Ordering is only meaningful when the set is otherwise correct: with a missing or extra batch
    // already reported, an order complaint would be redundant noise. The v1 writer emits batches in
    // ascending digest order (the same order the importer's `BTreeSet` yields), so the expected
    // sequence is the referenced digests sorted.
    if !missing && !extra {
        let expected: Vec<BlockHash> = referenced.into_iter().collect();
        if collected != expected.as_slice() {
            issues.push(PackIssue::UnsortedBatches { number });
        }
    }
}

/// Resolve every deferred [`PackIssue::MissingBatch`] class against the now-complete
/// `all_batch_digests` set — a digest present anywhere in the file is [`BatchClass::Misordered`],
/// otherwise it is a genuine [`BatchClass::Absent`] gap — then build the final report. This is a
/// cheap pass over `issues` (bounded by the number of missing references), not another file
/// traversal. Shared by [`verify_v0_data`] and [`verify_v1_data`].
#[allow(clippy::too_many_arguments)]
fn finalize_report(
    epoch: Epoch,
    start_consensus_number: u64,
    batch_count: u64,
    consensus_count: u64,
    first_consensus_number: Option<u64>,
    last_consensus_number: Option<u64>,
    mut issues: Vec<PackIssue>,
    all_batch_digests: &HashSet<BlockHash>,
) -> PackValidationReport {
    for issue in issues.iter_mut() {
        if let PackIssue::MissingBatch { digest, class, .. } = issue {
            *class = if all_batch_digests.contains(digest) {
                BatchClass::Misordered
            } else {
                BatchClass::Absent
            };
        }
    }

    let verdict = if issues.is_empty() { Verdict::Valid } else { Verdict::Invalid };
    PackValidationReport {
        epoch,
        start_consensus_number,
        batch_count,
        consensus_count,
        first_consensus_number,
        last_consensus_number,
        issues,
        verdict,
    }
}

impl Display for PackValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Number of detail rows to print before truncating (a pathological pack can have
        // thousands).
        const MAX_ROWS: usize = 50;

        let mut chain_breaks = 0usize;
        let mut missing_absent = 0usize;
        let mut missing_misordered = 0usize;
        let mut extra = 0usize;
        let mut unsorted = 0usize;
        let mut non_sequential = 0usize;
        let mut meta = 0usize;
        for issue in &self.issues {
            match issue {
                PackIssue::ChainBreak { .. } => chain_breaks += 1,
                PackIssue::MissingBatch { class: BatchClass::Absent, .. } => missing_absent += 1,
                PackIssue::MissingBatch { class: BatchClass::Misordered, .. } => {
                    missing_misordered += 1
                }
                PackIssue::ExtraBatch { .. } => extra += 1,
                PackIssue::UnsortedBatches { .. } => unsorted += 1,
                PackIssue::NonSequentialConsensusNumber { .. } => non_sequential += 1,
                PackIssue::EpochMetaMismatch { .. } => meta += 1,
            }
        }

        writeln!(f, "=== consensus pack validation report ===")?;
        writeln!(f, "epoch:                  {}", self.epoch)?;
        writeln!(f, "start consensus number: {}", self.start_consensus_number)?;
        match (self.first_consensus_number, self.last_consensus_number) {
            (Some(a), Some(b)) => {
                writeln!(f, "consensus headers:      {} ({a} ..= {b})", self.consensus_count)?
            }
            _ => writeln!(f, "consensus headers:      {}", self.consensus_count)?,
        }
        writeln!(f, "batch records:          {}", self.batch_count)?;
        writeln!(f, "verdict:                {}", self.verdict)?;
        writeln!(f)?;
        writeln!(f, "issues: {} total", self.issues.len())?;
        writeln!(f, "  chain breaks:           {chain_breaks}")?;
        writeln!(
            f,
            "  missing batches:        {} (absent: {missing_absent}, misordered: {missing_misordered})",
            missing_absent + missing_misordered
        )?;
        writeln!(f, "  extra batches:          {extra}")?;
        writeln!(f, "  unsorted batch groups:  {unsorted}")?;
        writeln!(f, "  non-sequential numbers: {non_sequential}")?;
        writeln!(f, "  epoch meta mismatches:  {meta}")?;

        if self.issues.is_empty() {
            return Ok(());
        }

        writeln!(f)?;
        let shown = self.issues.len().min(MAX_ROWS);
        writeln!(f, "details (showing {shown} of {}):", self.issues.len())?;
        for issue in self.issues.iter().take(MAX_ROWS) {
            match issue {
                PackIssue::ChainBreak { number, expected_parent, found_parent } => writeln!(
                    f,
                    "  consensus {number}  CHAIN BREAK     expected parent {expected_parent}, found {found_parent}"
                )?,
                PackIssue::MissingBatch { number, digest, class } => {
                    writeln!(f, "  consensus {number}  MISSING BATCH  {digest}  {class}")?
                }
                PackIssue::ExtraBatch { number, digest } => {
                    writeln!(f, "  consensus {number}  EXTRA BATCH    {digest}")?
                }
                PackIssue::UnsortedBatches { number } => {
                    writeln!(f, "  consensus {number}  UNSORTED BATCHES")?
                }
                PackIssue::NonSequentialConsensusNumber { expected, found } => {
                    writeln!(f, "  consensus {found}  NON-SEQUENTIAL  (expected {expected})")?
                }
                PackIssue::EpochMetaMismatch { detail } => writeln!(f, "  EPOCH META     {detail}")?,
            }
        }
        if self.issues.len() > MAX_ROWS {
            writeln!(f, "  ... and {} more", self.issues.len() - MAX_ROWS)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{path::Path, sync::Arc};

    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{test_genesis, BlockHash, Committee, ConsensusHeader, ConsensusOutput, Hash};

    use super::{validate_pack_file, BatchClass, PackIssue, Verdict};
    use crate::{
        archive::pack::{Pack, PackCompression},
        consensus_pack::{test::make_test_output, EpochMeta, PackRecord},
        mem_db::MemDatabase,
    };

    /// Build a chained run of `n` consensus outputs for epoch 0 (parent anchored at the default
    /// consensus header digest, exactly like a real epoch-0 pack).
    fn make_outputs(
        committee: &Committee,
        chain: Arc<RethChainSpec>,
        n: u64,
    ) -> Vec<ConsensusOutput> {
        let mut parent = ConsensusHeader::default().digest();
        let mut outputs = Vec::new();
        for i in 0..n {
            let output =
                make_test_output(committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest().into();
            outputs.push(output);
        }
        outputs
    }

    /// The `EpochMeta` an epoch-0 pack starts with.
    fn epoch0_meta(committee: &Committee) -> EpochMeta {
        EpochMeta {
            epoch: 0,
            committee: committee.clone(),
            start_consensus_number: 1,
            genesis_exec_state: Default::default(),
            genesis_consensus: Default::default(),
        }
    }

    /// Build the `data` record stream for the given on-disk `version` (EpochMeta first, then one
    /// group per output). The two layouts differ only in intra-group record order:
    ///
    /// - **v0:** each output's batches, then its consensus header.
    /// - **v1:** the consensus header, then its batches in ascending digest order (matching the
    ///   real writer, so a clean v1 pack has no `UnsortedBatches`).
    ///
    /// Also returns the batch digests per group. The returned digests are in the same file order
    /// as the emitted `Batch` records, so v1 groups come back sorted.
    fn build_records(
        meta: EpochMeta,
        outputs: &[ConsensusOutput],
        version: u16,
    ) -> (Vec<PackRecord>, Vec<Vec<BlockHash>>) {
        let mut records = vec![PackRecord::EpochMeta(meta)];
        let mut group_batches = Vec::new();
        for output in outputs {
            let mut batches: Vec<_> =
                output.batches().iter().flat_map(|cb| cb.batches.iter().cloned()).collect();
            if version > 0 {
                // v1 writes batches in ascending digest order (see `collect_batches`).
                batches.sort_by_key(|b| b.digest());
            }
            let digests = batches.iter().map(|b| b.digest()).collect();
            let header = PackRecord::Consensus(Box::new(output.consensus_header()));
            let batch_records = batches.into_iter().map(PackRecord::Batch);
            if version == 0 {
                records.extend(batch_records);
                records.push(header);
            } else {
                records.push(header);
                records.extend(batch_records);
            }
            group_batches.push(digests);
        }
        (records, group_batches)
    }

    /// Write a record stream to a bare epoch-0 `data` pack file, stamped with `version`.
    fn write_records(path: &Path, records: &[PackRecord], version: u16) {
        let mut pack = Pack::<PackRecord>::open(path, 0, false, PackCompression::ZStd, version)
            .expect("open pack");
        for record in records {
            pack.append(record).expect("append record");
        }
        pack.commit().expect("commit pack");
    }

    /// Index of a `Batch` record in the stream whose digest matches `target`.
    fn find_batch(records: &[PackRecord], target: BlockHash) -> usize {
        records
            .iter()
            .position(|r| matches!(r, PackRecord::Batch(b) if b.digest() == target))
            .expect("target batch present in stream")
    }

    fn setup() -> (TempDir, Committee, Arc<RethChainSpec>) {
        let temp_dir = TempDir::with_prefix("pack_validate").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let committee = fixture.committee();
        (temp_dir, committee, chain)
    }

    /// A well-formed pack validates clean, in both the v0 (batches-first) and v1 (header-first)
    /// layouts.
    #[test]
    fn test_validate_clean_pack() {
        for version in [0u16, 1] {
            let (temp_dir, committee, chain) = setup();
            let outputs = make_outputs(&committee, chain, 5);
            let (records, _) = build_records(epoch0_meta(&committee), &outputs, version);
            let path = temp_dir.path().join("data");
            write_records(&path, &records, version);

            let report = validate_pack_file(&path, 0, None).expect("validate");
            assert_eq!(
                report.verdict,
                Verdict::Valid,
                "v{version}: unexpected issues: {:?}",
                report.issues
            );
            assert!(report.issues.is_empty(), "v{version}");
            assert_eq!(report.consensus_count, 5, "v{version}");
            assert_eq!(report.first_consensus_number, Some(1), "v{version}");
            assert_eq!(report.last_consensus_number, Some(5), "v{version}");
        }
    }

    /// Dropping a batch record that no other group carries → reported Absent for the exact digest,
    /// in both layouts.
    #[test]
    fn test_validate_absent_batch() {
        for version in [0u16, 1] {
            let (temp_dir, committee, chain) = setup();
            let outputs = make_outputs(&committee, chain, 5);
            let (mut records, group_batches) =
                build_records(epoch0_meta(&committee), &outputs, version);

            // Target the first batch of group index 2 → referenced by consensus header number 3.
            let target = group_batches[2][0];
            let pos = find_batch(&records, target);
            records.remove(pos);

            let path = temp_dir.path().join("data");
            write_records(&path, &records, version);

            let report = validate_pack_file(&path, 0, None).expect("validate");
            assert_eq!(report.verdict, Verdict::Invalid, "v{version}");
            let absent = report.issues.iter().any(|i| {
                matches!(i,
                    PackIssue::MissingBatch { digest, class: BatchClass::Absent, number }
                    if *digest == target && *number == 3)
            });
            assert!(
                absent,
                "v{version}: expected Absent missing batch at consensus 3; issues: {:?}",
                report.issues
            );
            // Must not be misclassified as misordered.
            assert_eq!(report.missing_batch_count(BatchClass::Misordered), 0, "v{version}");
        }
    }

    /// v0: moving a batch into a later group → reported Misordered where it belongs, and ExtraBatch
    /// where it now (wrongly) sits. In the v0 (batches-first) layout the batch lands in group 5 by
    /// being spliced just *before* consensus header 5.
    #[test]
    fn test_validate_misordered_batch() {
        let (temp_dir, committee, chain) = setup();
        let outputs = make_outputs(&committee, chain, 6);
        let (mut records, group_batches) = build_records(epoch0_meta(&committee), &outputs, 0);

        // Take the first batch of group 2 (consensus number 3) and splice it into group 4's records
        // (just before consensus header number 5).
        let target = group_batches[2][0];
        let from = find_batch(&records, target);
        let moved = records.remove(from);
        let insert_at = records
            .iter()
            .position(|r| matches!(r, PackRecord::Consensus(h) if h.number == 5))
            .expect("consensus header 5 present");
        records.insert(insert_at, moved);

        let path = temp_dir.path().join("data");
        write_records(&path, &records, 0);

        let report = validate_pack_file(&path, 0, None).expect("validate");
        assert_eq!(report.verdict, Verdict::Invalid);
        // Present in the file but not in group 3 → Misordered at consensus 3.
        let misordered = report.issues.iter().any(|i| {
            matches!(i,
                PackIssue::MissingBatch { digest, class: BatchClass::Misordered, number }
                if *digest == target && *number == 3)
        });
        assert!(misordered, "expected Misordered at consensus 3; issues: {:?}", report.issues);
        // Now an orphan inside group 5 → ExtraBatch at consensus 5.
        let extra = report.issues.iter().any(|i| {
            matches!(i, PackIssue::ExtraBatch { digest, number } if *digest == target && *number == 5)
        });
        assert!(extra, "expected ExtraBatch at consensus 5; issues: {:?}", report.issues);
        // Nothing should be classified Absent — the batch is still in the file.
        assert_eq!(report.missing_batch_count(BatchClass::Absent), 0);
    }

    /// v1: same corruption in the header-first layout. To land the moved batch in group 5, splice it
    /// just *after* consensus header 5 (a v1 group's batches follow its header). Present-but-wrong-
    /// group → Misordered at 3; orphan in group 5 → ExtraBatch at 5.
    #[test]
    fn test_validate_misordered_batch_v1() {
        let (temp_dir, committee, chain) = setup();
        let outputs = make_outputs(&committee, chain, 6);
        let (mut records, group_batches) = build_records(epoch0_meta(&committee), &outputs, 1);

        let target = group_batches[2][0];
        let from = find_batch(&records, target);
        let moved = records.remove(from);
        let header5 = records
            .iter()
            .position(|r| matches!(r, PackRecord::Consensus(h) if h.number == 5))
            .expect("consensus header 5 present");
        records.insert(header5 + 1, moved);

        let path = temp_dir.path().join("data");
        write_records(&path, &records, 1);

        let report = validate_pack_file(&path, 0, None).expect("validate");
        assert_eq!(report.verdict, Verdict::Invalid);
        let misordered = report.issues.iter().any(|i| {
            matches!(i,
                PackIssue::MissingBatch { digest, class: BatchClass::Misordered, number }
                if *digest == target && *number == 3)
        });
        assert!(misordered, "expected Misordered at consensus 3; issues: {:?}", report.issues);
        let extra = report.issues.iter().any(|i| {
            matches!(i, PackIssue::ExtraBatch { digest, number } if *digest == target && *number == 5)
        });
        assert!(extra, "expected ExtraBatch at consensus 5; issues: {:?}", report.issues);
        assert_eq!(report.missing_batch_count(BatchClass::Absent), 0);
    }

    /// v1-only: a group whose batches are all present and correct as a set, but written out of the
    /// ascending digest order v1 requires, is flagged `UnsortedBatches` for that group — with no
    /// spurious MissingBatch/ExtraBatch (the set is intact).
    #[test]
    fn test_validate_unsorted_batches_v1() {
        let (temp_dir, committee, chain) = setup();
        let outputs = make_outputs(&committee, chain, 5);
        let (mut records, _) = build_records(epoch0_meta(&committee), &outputs, 1);

        // Swap the first two Batch records that follow consensus header 3 (its group has 4 batches),
        // breaking the ascending-digest order without changing the set.
        let header3 = records
            .iter()
            .position(|r| matches!(r, PackRecord::Consensus(h) if h.number == 3))
            .expect("consensus header 3 present");
        assert!(
            matches!(records[header3 + 1], PackRecord::Batch(_))
                && matches!(records[header3 + 2], PackRecord::Batch(_)),
            "expected header 3 to be followed by at least two batch records"
        );
        records.swap(header3 + 1, header3 + 2);

        let path = temp_dir.path().join("data");
        write_records(&path, &records, 1);

        let report = validate_pack_file(&path, 0, None).expect("validate");
        assert_eq!(report.verdict, Verdict::Invalid, "issues: {:?}", report.issues);
        let unsorted: Vec<_> = report
            .issues
            .iter()
            .filter(|i| matches!(i, PackIssue::UnsortedBatches { .. }))
            .collect();
        assert_eq!(unsorted.len(), 1, "expected one UnsortedBatches; issues: {:?}", report.issues);
        assert!(matches!(unsorted[0], PackIssue::UnsortedBatches { number: 3 }));
        // The set is intact, so no missing or extra batch should be reported.
        assert!(
            !report.issues.iter().any(|i| matches!(
                i,
                PackIssue::MissingBatch { .. } | PackIssue::ExtraBatch { .. }
            )),
            "unexpected missing/extra batch issues: {:?}",
            report.issues
        );
    }

    /// Overwrite the `number` field of the consensus header that currently carries `current`.
    fn set_consensus_number(records: &mut [PackRecord], current: u64, new: u64) {
        let rec = records
            .iter_mut()
            .find(|r| matches!(r, PackRecord::Consensus(h) if h.number == current))
            .expect("consensus header present");
        if let PackRecord::Consensus(h) = rec {
            h.number = new;
        }
    }

    /// A corrupted `number` on the *final* header has no successor to trip the `parent_hash` chain
    /// check, so only the explicit sequential-number check (mirroring the importer) catches it.
    #[test]
    fn test_validate_non_sequential_trailing_header() {
        for version in [0u16, 1] {
            let (temp_dir, committee, chain) = setup();
            let outputs = make_outputs(&committee, chain, 5);
            let (mut records, _) = build_records(epoch0_meta(&committee), &outputs, version);

            // The 5th (last) header should carry number 5; corrupt it to 99.
            set_consensus_number(&mut records, 5, 99);

            let path = temp_dir.path().join("data");
            write_records(&path, &records, version);

            let report = validate_pack_file(&path, 0, None).expect("validate");
            assert_eq!(report.verdict, Verdict::Invalid, "v{version}");
            let non_seq = report.issues.iter().any(|i| {
                matches!(i, PackIssue::NonSequentialConsensusNumber { expected: 5, found: 99 })
            });
            assert!(
                non_seq,
                "v{version}: expected NonSequentialConsensusNumber(expected 5, found 99); issues: {:?}",
                report.issues
            );
            // The chain check alone misses this: the final header has no successor whose parent_hash
            // would mismatch, so no ChainBreak fires — the sequential check is what catches it.
            let chain_breaks =
                report.issues.iter().filter(|i| matches!(i, PackIssue::ChainBreak { .. })).count();
            assert_eq!(
                chain_breaks, 0,
                "v{version}: trailing-number corruption should not trip ChainBreak"
            );
        }
    }

    /// A corrupted middle header number fires exactly one sequential-number issue: `expected` is
    /// position-based, so the corruption does not cascade into a spurious issue on every following
    /// header.
    #[test]
    fn test_validate_non_sequential_middle_header() {
        for version in [0u16, 1] {
            let (temp_dir, committee, chain) = setup();
            let outputs = make_outputs(&committee, chain, 5);
            let (mut records, _) = build_records(epoch0_meta(&committee), &outputs, version);

            // The header at position 3 should carry number 3; corrupt it to 7.
            set_consensus_number(&mut records, 3, 7);

            let path = temp_dir.path().join("data");
            write_records(&path, &records, version);

            let report = validate_pack_file(&path, 0, None).expect("validate");
            assert_eq!(report.verdict, Verdict::Invalid, "v{version}");
            let non_seq: Vec<_> = report
                .issues
                .iter()
                .filter(|i| matches!(i, PackIssue::NonSequentialConsensusNumber { .. }))
                .collect();
            assert_eq!(
                non_seq.len(),
                1,
                "v{version}: expected exactly one non-sequential issue (no cascade); issues: {:?}",
                report.issues
            );
            assert!(matches!(
                non_seq[0],
                PackIssue::NonSequentialConsensusNumber { expected: 3, found: 7 }
            ));
        }
    }
}

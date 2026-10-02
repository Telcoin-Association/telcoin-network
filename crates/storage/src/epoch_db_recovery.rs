//! Startup check that the headers stored in the epoch database decode with this binary.
//!
//! The epoch database holds the working consensus state of the epoch in progress (the
//! `TableHint::Epoch` tables in [`crate::tables`]), and the node clears it at every epoch
//! boundary. Opening it loads every row into memory through the panicking `decode` that all typed
//! read paths use. `Header` is the only fork-gated type these tables store, directly in
//! `LastProposed` and inside a `Certificate` in `Certificates` and `ProposedCertificates`, so the
//! check here decodes those three tables with a fallible decode before that load.
//!
//! Exactly one kind of undecodable row is removed: the header a validator proposed for a
//! sub-second fork epoch while it still ran a binary without the sub-second layout, found when it
//! restarts on the upgraded binary. Any other undecodable row stops the open with an error that
//! names it, because `LastProposed` and `ProposedCertificates` are records the proposer and the
//! certifier stop the node for rather than run without.

use tn_types::{
    encoded_size, forks::subsecond_timestamp_active, try_decode, try_decode_key,
    AuthorityIdentifier, Database, DbTxMut as _, Epoch, Header, Round, Table,
};

use crate::tables::{Certificates, LastProposed, ProposedCertificates};

/// Remove the seconds-only header this node proposed for a sub-second fork epoch from
/// `LastProposed`, and fail on any other row of a header-bearing epoch table that this binary
/// cannot decode.
///
/// Must run on the raw backend before it is wrapped in a
/// [`LayeredDatabase`](crate::layered_db::LayeredDatabase), whose full-memory load panics on the
/// first undecodable row. All three tables are checked before anything is removed, so an error
/// leaves the database as it was. Returns the removed rows, logged at error level, or `None` when
/// nothing was removed.
///
/// # Errors
///
/// Returns an error naming the table, key, epoch and decode error of the first undecodable row
/// that is not such a header, for example after disk corruption or a serialization bug.
pub(crate) fn discard_seconds_only_proposal<DB: RawRows>(
    epoch_db: &DB,
) -> eyre::Result<Option<UndecodableTable>> {
    let stale = scan_table::<LastProposed, _>(epoch_db, is_seconds_only_fork_header)?;
    // a stale binary never stores a fork-epoch certificate (its own needs votes from peers that
    // cannot decode its header, and theirs do not decode on it), so nothing here is removed. an
    // undecodable row here would stop the full-memory load as well; checking first names the row
    // in the error and keeps the `LastProposed` removal from happening before that stop
    scan_table::<Certificates, _>(epoch_db, |_| false)?;
    scan_table::<ProposedCertificates, _>(epoch_db, |_| false)?;
    let Some(Discard { report, keys }) = stale else {
        return Ok(None);
    };

    tracing::error!(
        target: "tn::storage",
        table = %report.table,
        epoch = ?report.epoch,
        key = %report.key,
        rows = report.rows,
        error = %report.error,
        "epoch table holds headers this binary cannot decode, likely written by a binary with a \
         different header layout for this epoch (a validator that ran past a fork epoch before \
         upgrading)"
    );

    // removing the row cannot make the node equivocate. it holds the header this node proposed
    // for a fork-epoch round before it upgraded, and that header's digest hashes the
    // seconds-only bytes. no peer on the sub-second layout can decode it, so none of them voted
    // for it. a peer still on the older binary may have, and once upgraded that peer's `Votes`
    // guard refuses any other header from this author for the round, so at most one header for
    // the round is ever certified, at worst costing this author the round. the proposer also
    // runs only once the node is an active committee member again, by which time catch-up has
    // normally moved `primary_round` past that round.
    let mut txn = epoch_db.write_txn()?;
    for key in &keys {
        txn.remove::<LastProposed>(key)?;
    }
    txn.commit()?;

    tracing::warn!(
        target: "tn::storage",
        table = LastProposed::NAME,
        rows = keys.len(),
        "discarded undecodable epoch state: removed the header this node proposed for a \
         sub-second fork epoch before upgrading, which no peer on this binary's layout can decode"
    );
    Ok(Some(report))
}

/// Undecodable rows of an epoch table, described by the first of them.
#[derive(Debug)]
pub(crate) struct UndecodableTable {
    /// The table's name.
    pub(crate) table: &'static str,
    /// The key of the first row, decoded if possible, else its raw bytes.
    pub(crate) key: String,
    /// The epoch of the header the first row starts with, if its leading fields decode.
    pub(crate) epoch: Option<Epoch>,
    /// The decode error of the first row.
    pub(crate) error: String,
    /// The number of rows.
    pub(crate) rows: usize,
}

/// Access to a backend's stored row bytes, so rows can be checked without the panicking decode.
pub(crate) trait RawRows: Database {
    /// Call `visit` with the stored key and value bytes of every row of `T`, in key order.
    fn for_each_raw_row<T: Table>(&self, visit: impl FnMut(&[u8], &[u8])) -> eyre::Result<()>;

    /// Store `value` under `key` in `T` verbatim, as another binary may have written them.
    #[cfg(test)]
    fn insert_raw_row<T: Table>(&self, key: &[u8], value: &[u8]) -> eyre::Result<()>;
}

/// Undecodable rows of a table that may be removed.
struct Discard<K> {
    /// The rows, for the log.
    report: UndecodableTable,
    /// The keys of the rows.
    keys: Vec<K>,
}

/// Open `T` in `db` and scan its rows with a fallible decode.
///
/// An undecodable row whose key decodes and whose value bytes satisfy `discardable` is returned
/// for removal; any other undecodable row is an error. `T`'s values must start with a header,
/// which is where the reported epoch is read from.
fn scan_table<T: Table, DB: RawRows>(
    db: &DB,
    discardable: impl Fn(&[u8]) -> bool,
) -> eyre::Result<Option<Discard<T::Key>>> {
    // a fresh database has no tables yet and the raw scan needs them
    db.open_table::<T>()?;
    let mut discard: Option<Discard<T::Key>> = None;
    let mut refused: Option<UndecodableTable> = None;
    db.for_each_raw_row::<T>(|key, value| {
        let decoded_key = try_decode_key::<T::Key>(key);
        let error = match (&decoded_key, try_decode::<T::Value>(value)) {
            (Ok(_), Ok(_)) => return,
            (Err(e), _) => e.to_string(),
            (Ok(_), Err(e)) => e.to_string(),
        };
        let row = || UndecodableTable {
            table: T::NAME,
            key: decoded_key
                .as_ref()
                .map_or_else(|_| format!("{key:02x?}"), |decoded| format!("{decoded:?}")),
            epoch: leading_header_epoch(value),
            error,
            rows: 1,
        };
        match &decoded_key {
            Ok(decoded) if discardable(value) => match discard.as_mut() {
                Some(found) => {
                    found.report.rows += 1;
                    found.keys.push(decoded.clone());
                }
                None => discard = Some(Discard { report: row(), keys: vec![decoded.clone()] }),
            },
            _ => match refused.as_mut() {
                Some(first) => first.rows += 1,
                None => refused = Some(row()),
            },
        }
    })?;

    if let Some(row) = refused {
        let epoch = row.epoch.map_or_else(|| "unknown".to_owned(), |epoch| epoch.to_string());
        eyre::bail!(
            "epoch table {} holds {} row(s) this binary cannot decode, the first at key {} with \
             header epoch {} ({}); only a header this node proposed for a sub-second fork epoch \
             on a binary without that layout is discarded at startup and this row is not one, so \
             the node stops instead of running without it (the epoch database may be corrupt)",
            row.table,
            row.rows,
            row.key,
            epoch,
            row.error
        );
    }
    Ok(discard)
}

/// Whether `value` is a header of a sub-second fork epoch in the seconds-only layout, as a binary
/// from before that fork stores it.
///
/// The sub-second layout is the seconds-only one plus a trailing two-byte `created_at_millis`,
/// written and read only when [`subsecond_timestamp_active`] holds for the header's own epoch
/// (`HeaderRef::serialize` and the `Header` decoder in `tn_types::primary::header`). Such a
/// header therefore decodes once two bytes are appended. The epoch check is what separates it
/// from a header of an earlier epoch that lost two trailing zero bytes, which decodes the same
/// way because that epoch's decoder never reads the field.
fn is_seconds_only_fork_header(value: &[u8]) -> bool {
    leading_header_epoch(value).is_some_and(subsecond_timestamp_active)
        && try_decode::<Header>(&[value, [0; 2].as_slice()].concat()).is_ok()
}

/// The epoch of the header `value` starts with, read from the leading author, round and epoch
/// fields that every header layout shares.
///
/// Header bytes are their fields in order with no framing, and a certificate's bytes start with
/// its header's, so this reads the epoch of a stored `Header` or `Certificate` whose later fields
/// do not decode.
fn leading_header_epoch(value: &[u8]) -> Option<Epoch> {
    type Prefix = (AuthorityIdentifier, Round, Epoch);
    let len = encoded_size::<Prefix>(&(AuthorityIdentifier::from_bytes([0; 32]), 0, 0)).ok()?;
    let (_, _, epoch) = try_decode::<Prefix>(value.get(..len)?).ok()?;
    Some(epoch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tables::{CertificateDigestByOrigin, CertificateDigestByRound, Votes},
        ReDB, LAST_PROPOSAL_KEY,
    };
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_types::{
        encode, encode_key, BlsKeypair, Certificate, HeaderBuilder, HeaderDigest, Signer as _,
        TimestampMs, VoteDigest, VoteInfo,
    };

    /// An epoch whose headers carry the sub-second layout in this build: 10 where the fork is
    /// live from genesis, the dormant adiri fork point otherwise.
    fn subsecond_epoch() -> Epoch {
        [10, Epoch::MAX]
            .into_iter()
            .find(|epoch| subsecond_timestamp_active(*epoch))
            .expect("the sub-second layout is active from some epoch")
    }

    /// A header for `epoch` with a real seed signature and a non-zero sub-second part.
    fn header(epoch: Epoch) -> Header {
        let keypair = BlsKeypair::generate(&mut StdRng::seed_from_u64(7));
        HeaderBuilder::default()
            .author(AuthorityIdentifier::from_bytes([3; 32]))
            .round(1)
            .epoch(epoch)
            .created_at_ms(TimestampMs::from_millis(1_700_000_000_250))
            .seed_signature(keypair.sign(b"epoch-db-recovery"))
            .build()
    }

    fn certificate(header: Header) -> Certificate {
        let mut certificate = Certificate::default();
        certificate.update_header_for_test(header);
        certificate
    }

    /// The bytes a pre-fork binary stores for `value`, whose encoding starts with `header`'s.
    ///
    /// The sub-second layout appends the two-byte `created_at_millis` to the earlier header
    /// layout, so removing those two bytes gives the earlier one.
    fn pre_fork_bytes(value: &impl serde::Serialize, header: &Header) -> Vec<u8> {
        let millis_end = encode(header).len();
        let mut bytes = encode(value);
        bytes.drain(millis_end - 2..millis_end);
        bytes
    }

    /// The leading author, round and epoch fields of a sub-second fork-epoch header followed by
    /// bytes that do not decode as the rest of a header in any layout.
    ///
    /// Its epoch passes the fork check, so only the layout check stands between it and removal.
    fn fork_epoch_garbage() -> Vec<u8> {
        let header = header(subsecond_epoch());
        let prefix = encoded_size(&(header.author().clone(), header.round(), header.epoch()))
            .expect("prefix size");
        let mut bytes = encode(&header);
        bytes.truncate(prefix);
        bytes.extend([0xff; 24]);
        assert_eq!(leading_header_epoch(&bytes), Some(subsecond_epoch()));
        assert!(try_decode::<Header>(&[bytes.as_slice(), [0; 2].as_slice()].concat()).is_err());
        bytes
    }

    fn vote_info(epoch: Epoch) -> VoteInfo {
        VoteInfo { epoch, round: 1, vote_digest: VoteDigest::new([5; 32]) }
    }

    fn raw_rows<T: Table, DB: RawRows>(db: &DB) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = Vec::new();
        db.for_each_raw_row::<T>(|key, value| rows.push((key.to_vec(), value.to_vec()))).unwrap();
        rows
    }

    /// The error the check returns for `db`, as text.
    fn refusal<DB: RawRows>(db: &DB) -> String {
        discard_seconds_only_proposal(db)
            .expect_err("an undecodable row other than a seconds-only fork-epoch proposal")
            .to_string()
    }

    /// A pre-fork `LastProposed` header is reported with its table and epoch and removed, while
    /// `Votes` and the decodable `Certificates` row are kept.
    fn assert_discards_pre_fork_header<DB: RawRows>(db: &DB) {
        let epoch = subsecond_epoch();
        let header = header(epoch);
        let bytes = pre_fork_bytes(&header, &header);
        assert!(try_decode::<Header>(&bytes).is_err(), "pre-fork bytes must not decode");
        let certificate = certificate(header.clone());
        let author = AuthorityIdentifier::from_bytes([4; 32]);
        db.open_table::<LastProposed>().unwrap();
        db.open_table::<Votes>().unwrap();
        db.open_table::<Certificates>().unwrap();
        db.insert::<Votes>(&author, &vote_info(epoch)).unwrap();
        db.insert::<Certificates>(&header.digest(), &certificate).unwrap();
        db.insert_raw_row::<LastProposed>(&encode_key(&LAST_PROPOSAL_KEY), &bytes).unwrap();

        let report = discard_seconds_only_proposal(db).unwrap().expect("the header is removed");

        assert_eq!(report.table, LastProposed::NAME);
        assert_eq!(report.epoch, Some(epoch));
        assert_eq!(report.rows, 1);
        assert_eq!(report.key, format!("{LAST_PROPOSAL_KEY:?}"));
        assert!(raw_rows::<LastProposed, _>(db).is_empty());
        assert_eq!(db.get::<Votes>(&author).unwrap(), Some(vote_info(epoch)));
        assert_eq!(db.get::<Certificates>(&header.digest()).unwrap(), Some(certificate));
        assert!(discard_seconds_only_proposal(db).unwrap().is_none());
    }

    /// A pre-fork certificate in `T` is refused with its table, key and epoch, and nothing is
    /// removed: not the row, the digest indexes or the decodable `LastProposed` header.
    fn assert_refuses_pre_fork_certificate<T, DB>(db: &DB)
    where
        T: Table<Key = HeaderDigest, Value = Certificate>,
        DB: RawRows,
    {
        let epoch = subsecond_epoch();
        let header = header(epoch);
        let bytes = pre_fork_bytes(&certificate(header.clone()), &header);
        assert!(try_decode::<Certificate>(&bytes).is_err(), "pre-fork bytes must not decode");
        let digest = header.digest();
        let author = header.author().clone();
        db.open_table::<LastProposed>().unwrap();
        db.open_table::<CertificateDigestByRound>().unwrap();
        db.open_table::<CertificateDigestByOrigin>().unwrap();
        db.open_table::<T>().unwrap();
        db.insert::<LastProposed>(&LAST_PROPOSAL_KEY, &header).unwrap();
        db.insert::<CertificateDigestByRound>(&(1, author.clone()), &digest).unwrap();
        db.insert::<CertificateDigestByOrigin>(&(author, 1), &digest).unwrap();
        db.insert_raw_row::<T>(&encode_key(&digest), &bytes).unwrap();

        let error = refusal(db);

        let expected = format!("epoch table {} holds 1 row(s) this binary cannot decode", T::NAME);
        assert!(error.starts_with(&expected), "{error}");
        assert!(error.contains(&format!("key {digest:?} with header epoch {epoch} (")), "{error}");
        assert_eq!(raw_rows::<T, _>(db), vec![(encode_key(&digest), bytes)]);
        assert_eq!(raw_rows::<CertificateDigestByRound, _>(db).len(), 1);
        assert_eq!(raw_rows::<CertificateDigestByOrigin, _>(db).len(), 1);
        assert_eq!(db.get::<LastProposed>(&LAST_PROPOSAL_KEY).unwrap(), Some(header));
    }

    /// Undecodable `LastProposed` rows that are not a seconds-only fork-epoch header are refused
    /// and kept: a fork-epoch header prefix followed by junk, and bytes too short to hold a
    /// header prefix at all.
    fn assert_refuses_undecodable_last_proposed<DB: RawRows>(db: &DB) {
        let key = encode_key(&LAST_PROPOSAL_KEY);
        db.open_table::<LastProposed>().unwrap();
        for (bytes, epoch) in [
            (fork_epoch_garbage(), subsecond_epoch().to_string()),
            (vec![0x5a; 7], "unknown".to_owned()),
        ] {
            db.insert_raw_row::<LastProposed>(&key, &bytes).unwrap();

            let error = refusal(db);

            assert!(
                error.starts_with("epoch table last_proposed holds 1 row(s)"),
                "{epoch}: {error}"
            );
            let expected = format!("key {LAST_PROPOSAL_KEY:?} with header epoch {epoch} (");
            assert!(error.contains(&expected), "{error}");
            assert_eq!(raw_rows::<LastProposed, _>(db), vec![(key.clone(), bytes)]);
        }
    }

    /// A removable `LastProposed` header stays when another table holds a row the check refuses,
    /// because every table is checked before anything is removed.
    fn assert_refuses_before_removing<DB: RawRows>(db: &DB) {
        let header = header(subsecond_epoch());
        let proposal = pre_fork_bytes(&header, &header);
        let certified = pre_fork_bytes(&certificate(header.clone()), &header);
        let key = encode_key(&LAST_PROPOSAL_KEY);
        db.open_table::<LastProposed>().unwrap();
        db.open_table::<ProposedCertificates>().unwrap();
        db.insert_raw_row::<LastProposed>(&key, &proposal).unwrap();
        db.insert_raw_row::<ProposedCertificates>(&encode_key(&header.digest()), &certified)
            .unwrap();

        let error = refusal(db);

        assert!(error.starts_with("epoch table proposed_certificates holds 1 row(s)"), "{error}");
        assert_eq!(raw_rows::<LastProposed, _>(db), vec![(key, proposal)]);
    }

    /// Decodable rows in every scanned table are kept.
    fn assert_keeps_decodable_rows<DB: RawRows>(db: &DB) {
        let header = header(subsecond_epoch());
        let certificate = certificate(header.clone());
        let digest = header.digest();
        db.open_table::<LastProposed>().unwrap();
        db.open_table::<Certificates>().unwrap();
        db.open_table::<ProposedCertificates>().unwrap();
        db.insert::<LastProposed>(&LAST_PROPOSAL_KEY, &header).unwrap();
        db.insert::<Certificates>(&digest, &certificate).unwrap();
        db.insert::<ProposedCertificates>(&digest, &certificate).unwrap();

        assert!(discard_seconds_only_proposal(db).unwrap().is_none());

        assert_eq!(db.get::<LastProposed>(&LAST_PROPOSAL_KEY).unwrap(), Some(header));
        assert_eq!(db.get::<Certificates>(&digest).unwrap(), Some(certificate.clone()));
        assert_eq!(db.get::<ProposedCertificates>(&digest).unwrap(), Some(certificate));
    }

    /// A header of an epoch before the sub-second fork that lost two trailing zero bytes is
    /// refused, although it decodes once two bytes are appended, the way a seconds-only
    /// fork-epoch header does.
    ///
    /// Only builds with epochs before the sub-second fork can hold one. Under adiri, epoch 10
    /// also predates the seed-signature fork, so its header ends with the execution block hash.
    #[cfg(feature = "adiri")]
    fn assert_refuses_truncated_pre_fork_epoch_header<DB: RawRows>(db: &DB) {
        use tn_types::{BlockNumHash, B256};

        let epoch = 10;
        assert!(!subsecond_timestamp_active(epoch), "epoch {epoch} must predate the fork");
        let mut hash = [0x11; 32];
        hash[30..].fill(0);
        let header = HeaderBuilder::default()
            .author(AuthorityIdentifier::from_bytes([3; 32]))
            .round(1)
            .epoch(epoch)
            .latest_execution_block(BlockNumHash::new(5, B256::from(hash)))
            .build();
        let mut bytes = encode(&header);
        assert_eq!(bytes.split_off(bytes.len() - 2), [0, 0], "the header must end in zeros");
        assert!(try_decode::<Header>(&bytes).is_err(), "truncated bytes must not decode");
        assert!(
            try_decode::<Header>(&[bytes.as_slice(), [0; 2].as_slice()].concat()).is_ok(),
            "the truncated header must pass the layout check, leaving the epoch check to refuse it"
        );
        let key = encode_key(&LAST_PROPOSAL_KEY);
        db.open_table::<LastProposed>().unwrap();
        db.insert_raw_row::<LastProposed>(&key, &bytes).unwrap();

        let error = refusal(db);

        assert!(error.starts_with("epoch table last_proposed holds 1 row(s)"), "{error}");
        assert!(error.contains(&format!("with header epoch {epoch} (")), "{error}");
        assert_eq!(raw_rows::<LastProposed, _>(db), vec![(key, bytes)]);
    }

    #[cfg(feature = "reth-libmdbx")]
    fn open_raw_mdbx(path: &std::path::Path) -> crate::mdbx::MdbxDatabase {
        use crate::mdbx::database::MEGABYTE;

        crate::mdbx::MdbxDatabase::open(path, 8, 16 * MEGABYTE, 4 * MEGABYTE).unwrap()
    }

    fn open_raw_redb(dir: &TempDir) -> ReDB {
        ReDB::open(dir.path().join("epoch")).unwrap()
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_discards_pre_fork_header() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_header(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_refuses_pre_fork_certificate() {
        let dir = TempDir::new().unwrap();
        assert_refuses_pre_fork_certificate::<Certificates, _>(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_refuses_pre_fork_proposed_certificate() {
        let dir = TempDir::new().unwrap();
        assert_refuses_pre_fork_certificate::<ProposedCertificates, _>(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_refuses_undecodable_last_proposed() {
        let dir = TempDir::new().unwrap();
        assert_refuses_undecodable_last_proposed(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_refuses_before_removing() {
        let dir = TempDir::new().unwrap();
        assert_refuses_before_removing(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_keeps_decodable_rows() {
        let dir = TempDir::new().unwrap();
        assert_keeps_decodable_rows(&open_raw_mdbx(dir.path()));
    }

    #[cfg(all(feature = "reth-libmdbx", feature = "adiri"))]
    #[test]
    fn test_mdbx_refuses_truncated_pre_fork_epoch_header() {
        let dir = TempDir::new().unwrap();
        assert_refuses_truncated_pre_fork_epoch_header(&open_raw_mdbx(dir.path()));
    }

    #[test]
    fn test_redb_discards_pre_fork_header() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_header(&open_raw_redb(&dir));
    }

    #[test]
    fn test_redb_refuses_pre_fork_certificate() {
        let dir = TempDir::new().unwrap();
        assert_refuses_pre_fork_certificate::<Certificates, _>(&open_raw_redb(&dir));
    }

    #[test]
    fn test_redb_refuses_pre_fork_proposed_certificate() {
        let dir = TempDir::new().unwrap();
        assert_refuses_pre_fork_certificate::<ProposedCertificates, _>(&open_raw_redb(&dir));
    }

    #[test]
    fn test_redb_refuses_undecodable_last_proposed() {
        let dir = TempDir::new().unwrap();
        assert_refuses_undecodable_last_proposed(&open_raw_redb(&dir));
    }

    #[test]
    fn test_redb_refuses_before_removing() {
        let dir = TempDir::new().unwrap();
        assert_refuses_before_removing(&open_raw_redb(&dir));
    }

    #[test]
    fn test_redb_keeps_decodable_rows() {
        let dir = TempDir::new().unwrap();
        assert_keeps_decodable_rows(&open_raw_redb(&dir));
    }

    #[cfg(feature = "adiri")]
    #[test]
    fn test_redb_refuses_truncated_pre_fork_epoch_header() {
        let dir = TempDir::new().unwrap();
        assert_refuses_truncated_pre_fork_epoch_header(&open_raw_redb(&dir));
    }

    /// The raw epoch database [`crate::open_db`] opens in this build.
    #[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
    fn open_raw_epoch_db(store: &std::path::Path) -> crate::mdbx::MdbxDatabase {
        open_raw_mdbx(&store.join("epoch"))
    }

    /// The raw epoch database [`crate::open_db`] opens in this build.
    #[cfg(feature = "redb")]
    fn open_raw_epoch_db(store: &std::path::Path) -> ReDB {
        ReDB::open(store.join("epoch")).unwrap()
    }

    /// The node's startup path: a pre-fork `LastProposed` header in the epoch database opens
    /// without a panic, the header is gone, and the `Votes` guards survive.
    #[cfg(any(feature = "reth-libmdbx", feature = "redb"))]
    #[test]
    fn test_open_db_discards_pre_fork_last_proposed() {
        let dir = TempDir::new().unwrap();
        let epoch = subsecond_epoch();
        let header = header(epoch);
        let author = AuthorityIdentifier::from_bytes([4; 32]);
        {
            let raw = open_raw_epoch_db(dir.path());
            raw.open_table::<LastProposed>().unwrap();
            raw.open_table::<Votes>().unwrap();
            raw.insert::<Votes>(&author, &vote_info(epoch)).unwrap();
            raw.insert_raw_row::<LastProposed>(
                &encode_key(&LAST_PROPOSAL_KEY),
                &pre_fork_bytes(&header, &header),
            )
            .unwrap();
        }

        let db = crate::open_db(dir.path());

        assert!(db.is_empty::<LastProposed>());
        assert_eq!(db.get::<Votes>(&author).unwrap(), Some(vote_info(epoch)));
        // the removal is durable, so the next start finds nothing to discard
        drop(db);
        let raw = open_raw_epoch_db(dir.path());
        assert!(discard_seconds_only_proposal(&raw).unwrap().is_none());
        assert!(raw_rows::<LastProposed, _>(&raw).is_empty());
    }

    /// The node's startup path stops on an undecodable `LastProposed` row that is not a
    /// seconds-only fork-epoch header, names the table, and leaves the row in place.
    #[cfg(any(feature = "reth-libmdbx", feature = "redb"))]
    #[test]
    fn test_open_db_refuses_undecodable_last_proposed() {
        let dir = TempDir::new().unwrap();
        let key = encode_key(&LAST_PROPOSAL_KEY);
        let bytes = fork_epoch_garbage();
        {
            let raw = open_raw_epoch_db(dir.path());
            raw.open_table::<LastProposed>().unwrap();
            raw.insert_raw_row::<LastProposed>(&key, &bytes).unwrap();
        }

        let store = dir.path();
        let panic =
            std::panic::catch_unwind(|| crate::open_db(store)).expect_err("open_db must stop");

        let message = panic.downcast_ref::<String>().expect("a formatted panic message");
        assert!(message.starts_with("Cannot check database (epoch)"), "{message}");
        assert!(message.contains("epoch table last_proposed holds 1 row(s)"), "{message}");
        let raw = open_raw_epoch_db(dir.path());
        assert_eq!(raw_rows::<LastProposed, _>(&raw), vec![(key, bytes)]);
    }
}

//! Startup check that the headers stored in the epoch database decode with this binary.
//!
//! The epoch database holds the working consensus state of the epoch in progress (the
//! `TableHint::Epoch` tables in [`crate::tables`]), and the node clears it at every epoch
//! boundary. Opening it loads every row into memory through the panicking `decode` that all typed
//! read paths use, so a row this binary cannot decode crash-loops the node at startup. `Header` is
//! the only fork-gated type these tables store, directly in `LastProposed` and inside a
//! `Certificate` in `Certificates` and `ProposedCertificates`. A binary with a different header
//! layout for an epoch writes rows this binary cannot decode, for example a validator that ran
//! past a fork epoch on the old binary and was upgraded afterwards. The check here finds such rows
//! with a fallible decode before that load and discards the tables that hold them.

use tn_types::{
    encoded_size, try_decode, try_decode_key, AuthorityIdentifier, Database, DbTxMut as _, Epoch,
    Round, Table,
};

use crate::tables::{
    CertificateDigestByOrigin, CertificateDigestByRound, Certificates, LastProposed,
    ProposedCertificates,
};

/// Clear each header-bearing epoch table of `epoch_db` that holds a row this binary cannot
/// decode.
///
/// Must run on the raw backend before it is wrapped in a
/// [`LayeredDatabase`](crate::layered_db::LayeredDatabase), whose full-memory load panics on the
/// first such row. Each table holding undecodable rows is logged at error level and returned; an
/// empty list means nothing was cleared.
///
/// `Certificates` is cleared together with its two digest indexes, which keeps the certificate
/// store consistent and causally complete. `Votes` is never cleared: it is the node's durable
/// record of the votes it signed, peers cannot rebuild it, and its layout is not fork-gated.
pub(crate) fn discard_undecodable_header_tables<DB: RawRows>(
    epoch_db: &DB,
) -> eyre::Result<Vec<UndecodableTable>> {
    let last_proposed = scan_table::<LastProposed, _>(epoch_db)?;
    let certificates = scan_table::<Certificates, _>(epoch_db)?;
    let proposed_certificates = scan_table::<ProposedCertificates, _>(epoch_db)?;
    let (clear_last_proposed, clear_certificates, clear_proposed_certificates) =
        (last_proposed.is_some(), certificates.is_some(), proposed_certificates.is_some());
    let reports: Vec<UndecodableTable> =
        [last_proposed, certificates, proposed_certificates].into_iter().flatten().collect();
    if reports.is_empty() {
        return Ok(reports);
    }

    for report in &reports {
        tracing::error!(
            target: "tn::storage",
            table = %report.table,
            epoch = ?report.epoch,
            key = %report.key,
            rows = report.rows,
            error = %report.error,
            "epoch table holds headers this binary cannot decode, likely written by a binary \
             with a different header layout for this epoch (a validator that ran past a fork \
             epoch before upgrading)"
        );
    }

    // discarding these rows cannot make the node equivocate:
    // - `LastProposed`: when the row is a header from a binary with a different layout for its
    //   epoch, it was never certified. a header digest hashes the epoch's wire layout and no peer
    //   on this binary's layout can decode the row, so none voted for it, and a header this binary
    //   proposes for that round is the only one those peers see. the proposer also runs only once
    //   the node is an active committee member again, by which time catch-up has normally moved
    //   `primary_round` past that round. for any other cause, a second header for the round is no
    //   worse than an equivocating author: two certificates for one author and round need two
    //   quorums, which share an honest voter whose `Votes` guard refuses the second vote.
    // - `Certificates`, its indexes and `ProposedCertificates`: certificates are quorum-signed
    //   public data. the node fetches missing ones from its peers, and the certifier re-requests
    //   votes for its own header, which voters answer with the vote they already signed.
    //
    // the unscanned index tables must exist to be cleared, and opening a table takes a write
    // transaction of its own, so this happens before the clearing one begins
    epoch_db.open_table::<CertificateDigestByRound>()?;
    epoch_db.open_table::<CertificateDigestByOrigin>()?;
    let mut cleared = Vec::new();
    let mut txn = epoch_db.write_txn()?;
    if clear_last_proposed {
        txn.clear_table::<LastProposed>()?;
        cleared.push(LastProposed::NAME);
    }
    if clear_certificates {
        txn.clear_table::<Certificates>()?;
        txn.clear_table::<CertificateDigestByRound>()?;
        txn.clear_table::<CertificateDigestByOrigin>()?;
        cleared.extend([
            Certificates::NAME,
            CertificateDigestByRound::NAME,
            CertificateDigestByOrigin::NAME,
        ]);
    }
    if clear_proposed_certificates {
        txn.clear_table::<ProposedCertificates>()?;
        cleared.push(ProposedCertificates::NAME);
    }
    txn.commit()?;

    tracing::warn!(
        target: "tn::storage",
        tables = ?cleared,
        "discarded undecodable epoch state: cleared these epoch tables, the node fetches the \
         epoch's certificates from its peers"
    );
    Ok(reports)
}

/// An epoch table that holds rows this binary cannot decode.
#[derive(Debug)]
pub(crate) struct UndecodableTable {
    /// The table's name.
    pub(crate) table: &'static str,
    /// The key of the first undecodable row, decoded if possible, else its raw bytes.
    pub(crate) key: String,
    /// The epoch of the header the first undecodable row starts with, if its leading fields
    /// decode.
    pub(crate) epoch: Option<Epoch>,
    /// The decode error of the first undecodable row.
    pub(crate) error: String,
    /// The number of undecodable rows in the table.
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

/// Open `T` in `db` and scan its rows with a fallible decode.
///
/// `T`'s values must start with a header, which is where the reported epoch is read from.
fn scan_table<T: Table, DB: RawRows>(db: &DB) -> eyre::Result<Option<UndecodableTable>> {
    // a fresh database has no tables yet and the raw scan needs them
    db.open_table::<T>()?;
    let mut report: Option<UndecodableTable> = None;
    db.for_each_raw_row::<T>(|key, value| {
        let decoded_key = try_decode_key::<T::Key>(key);
        let error = match (&decoded_key, try_decode::<T::Value>(value)) {
            (Ok(_), Ok(_)) => return,
            (Err(e), _) => e.to_string(),
            (Ok(_), Err(e)) => e.to_string(),
        };
        if let Some(first) = report.as_mut() {
            first.rows += 1;
            return;
        }
        report = Some(UndecodableTable {
            table: T::NAME,
            key: decoded_key.map_or_else(|_| format!("{key:02x?}"), |k| format!("{k:?}")),
            epoch: leading_header_epoch(value),
            error,
            rows: 1,
        });
    })?;
    Ok(report)
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
    use crate::{tables::Votes, ReDB, LAST_PROPOSAL_KEY};
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_types::{
        encode, encode_key, forks::subsecond_timestamp_active, BlsKeypair, Certificate, Header,
        HeaderBuilder, Signer as _, TimestampMs, VoteDigest, VoteInfo,
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

    fn vote_info(epoch: Epoch) -> VoteInfo {
        VoteInfo { epoch, round: 1, vote_digest: VoteDigest::new([5; 32]) }
    }

    fn row_count<T: Table, DB: RawRows>(db: &DB) -> usize {
        let mut rows = 0;
        db.for_each_raw_row::<T>(|_, _| rows += 1).unwrap();
        rows
    }

    /// A pre-fork `LastProposed` header is reported with its table and epoch and cleared, while
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

        let reports = discard_undecodable_header_tables(db).unwrap();

        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0].table, LastProposed::NAME);
        assert_eq!(reports[0].epoch, Some(epoch));
        assert_eq!(reports[0].rows, 1);
        assert_eq!(reports[0].key, format!("{LAST_PROPOSAL_KEY:?}"));
        assert_eq!(row_count::<LastProposed, _>(db), 0);
        assert_eq!(db.get::<Votes>(&author).unwrap(), Some(vote_info(epoch)));
        assert_eq!(db.get::<Certificates>(&header.digest()).unwrap(), Some(certificate));
    }

    /// A pre-fork certificate clears `Certificates` and both digest indexes, and keeps the
    /// decodable `LastProposed` header.
    fn assert_discards_pre_fork_certificate<DB: RawRows>(db: &DB) {
        let epoch = subsecond_epoch();
        let header = header(epoch);
        let bytes = pre_fork_bytes(&certificate(header.clone()), &header);
        assert!(try_decode::<Certificate>(&bytes).is_err(), "pre-fork bytes must not decode");
        let digest = header.digest();
        let author = header.author().clone();
        db.open_table::<LastProposed>().unwrap();
        db.open_table::<Certificates>().unwrap();
        db.open_table::<CertificateDigestByRound>().unwrap();
        db.open_table::<CertificateDigestByOrigin>().unwrap();
        db.insert::<LastProposed>(&LAST_PROPOSAL_KEY, &header).unwrap();
        db.insert::<CertificateDigestByRound>(&(1, author.clone()), &digest).unwrap();
        db.insert::<CertificateDigestByOrigin>(&(author, 1), &digest).unwrap();
        db.insert_raw_row::<Certificates>(&encode_key(&digest), &bytes).unwrap();

        let reports = discard_undecodable_header_tables(db).unwrap();

        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0].table, Certificates::NAME);
        assert_eq!(reports[0].epoch, Some(epoch));
        assert_eq!(reports[0].key, format!("{digest:?}"));
        assert_eq!(row_count::<Certificates, _>(db), 0);
        assert_eq!(row_count::<CertificateDigestByRound, _>(db), 0);
        assert_eq!(row_count::<CertificateDigestByOrigin, _>(db), 0);
        assert_eq!(db.get::<LastProposed>(&LAST_PROPOSAL_KEY).unwrap(), Some(header));
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

        assert!(discard_undecodable_header_tables(db).unwrap().is_empty());

        assert_eq!(db.get::<LastProposed>(&LAST_PROPOSAL_KEY).unwrap(), Some(header));
        assert_eq!(db.get::<Certificates>(&digest).unwrap(), Some(certificate.clone()));
        assert_eq!(db.get::<ProposedCertificates>(&digest).unwrap(), Some(certificate));
    }

    #[cfg(feature = "reth-libmdbx")]
    fn open_raw_mdbx(path: &std::path::Path) -> crate::mdbx::MdbxDatabase {
        use crate::mdbx::database::MEGABYTE;

        crate::mdbx::MdbxDatabase::open(path, 8, 16 * MEGABYTE, 4 * MEGABYTE).unwrap()
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_discards_pre_fork_header() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_header(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_discards_pre_fork_certificate() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_certificate(&open_raw_mdbx(dir.path()));
    }

    #[cfg(feature = "reth-libmdbx")]
    #[test]
    fn test_mdbx_keeps_decodable_rows() {
        let dir = TempDir::new().unwrap();
        assert_keeps_decodable_rows(&open_raw_mdbx(dir.path()));
    }

    #[test]
    fn test_redb_discards_pre_fork_header() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_header(&ReDB::open(dir.path().join("epoch")).unwrap());
    }

    #[test]
    fn test_redb_discards_pre_fork_certificate() {
        let dir = TempDir::new().unwrap();
        assert_discards_pre_fork_certificate(&ReDB::open(dir.path().join("epoch")).unwrap());
    }

    #[test]
    fn test_redb_keeps_decodable_rows() {
        let dir = TempDir::new().unwrap();
        assert_keeps_decodable_rows(&ReDB::open(dir.path().join("epoch")).unwrap());
    }

    /// The node's startup path: a pre-fork `LastProposed` header in the epoch database opens
    /// without a panic, the header is gone, and the `Votes` guards survive.
    #[cfg(all(feature = "reth-libmdbx", not(feature = "redb")))]
    #[test]
    fn test_open_db_discards_pre_fork_last_proposed() {
        let dir = TempDir::new().unwrap();
        let epoch = subsecond_epoch();
        let header = header(epoch);
        let author = AuthorityIdentifier::from_bytes([4; 32]);
        {
            let raw = open_raw_mdbx(&dir.path().join("epoch"));
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
        // the clear is durable, so the next start finds nothing to discard
        drop(db);
        let raw = open_raw_mdbx(&dir.path().join("epoch"));
        assert!(discard_undecodable_header_tables(&raw).unwrap().is_empty());
        assert_eq!(row_count::<LastProposed, _>(&raw), 0);
    }
}

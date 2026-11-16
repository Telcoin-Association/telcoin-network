//! The shared log's records. Each is the payload of one CRC-framed pack record:
//!
//! ```text
//! byte 0     kind (bits 0-6: PUT 1, REMOVE 2, CLEAR 3) | BATCH_END (bit 7)
//! bytes 1-2  table id (u16 LE)
//! PUT        key (the table's key size) ‖ value
//! REMOVE     key
//! CLEAR      (nothing)
//! ```
//!
//! A batch (a bare write, or a whole transaction) is a run of consecutive records whose last one
//! carries `BATCH_END`. There are no commit records: replay applies a batch only once its flagged
//! record reads back intact, so every batch end is a commit point.

use eyre::bail;

use crate::archive::error::fetch::FetchError;

use super::TableId;

pub(super) const KIND_PUT: u8 = 1;
pub(super) const KIND_REMOVE: u8 = 2;
pub(super) const KIND_CLEAR: u8 = 3;
/// The flag on a batch's last record.
pub(super) const BATCH_END: u8 = 0x80;
const KIND_MASK: u8 = 0x7f;

/// Bytes before a record's key: its kind byte and its table id.
pub(super) const HEAD: usize = 3;

/// A record's head: kind, batch-end flag, table id.
pub(super) fn head(kind: u8, table: TableId, end: bool) -> [u8; HEAD] {
    let [lo, hi] = table.to_le_bytes();
    [kind | if end { BATCH_END } else { 0 }, lo, hi]
}

/// What a record does.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Op<'a> {
    /// Insert (or overwrite) the key.
    Put(&'a [u8]),
    /// Remove the key.
    Remove(&'a [u8]),
    /// Empty the table.
    Clear,
}

/// A parsed record.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Record<'a> {
    pub(super) table: TableId,
    pub(super) op: Op<'a>,
    /// The last record of its batch.
    pub(super) end: bool,
}

/// Parse a record's payload; `ksize` gives a table's key size, or `None` for a table id the
/// database does not have. An unknown table or kind, a key size not yet fixed, or a record of
/// the wrong length is an error (damage, never a crash tail: a torn record fails its CRC first).
pub(super) fn parse(
    payload: &[u8],
    ksize: impl Fn(TableId) -> Option<u16>,
) -> eyre::Result<Record<'_>> {
    let Some((head, body)) = payload.split_at_checked(HEAD) else {
        bail!("tndb: a record shorter than its head");
    };
    let (kind, end) = (head[0] & KIND_MASK, head[0] & BATCH_END != 0);
    let table = u16::from_le_bytes([head[1], head[2]]);
    let Some(ksize) = ksize(table) else { bail!("tndb: a record of unknown table {table}") };
    let ksize = ksize as usize;
    let op = match kind {
        KIND_PUT if ksize > 0 && body.len() >= ksize => Op::Put(&body[..ksize]),
        KIND_REMOVE if ksize > 0 && body.len() == ksize => Op::Remove(body),
        KIND_CLEAR if body.is_empty() => Op::Clear,
        KIND_PUT | KIND_REMOVE | KIND_CLEAR => {
            bail!("tndb: a malformed record of kind {kind} for table {table}")
        }
        other => bail!("tndb: a record of unknown kind {other}"),
    };
    Ok(Record { table, op, end })
}

/// The value of `payload`, a record an index entry points at, checked to be table `table`'s put
/// of `key`: a damaged entry that lands on any other record is `CorruptIndex`, never another
/// row's value.
pub(super) fn put_value<'a>(
    payload: &'a [u8],
    table: TableId,
    key: &[u8],
) -> Result<&'a [u8], FetchError> {
    let value_at = HEAD + key.len();
    let row = payload.get(..value_at).is_some_and(|row| {
        row[0] & KIND_MASK == KIND_PUT && row[1..HEAD] == table.to_le_bytes() && &row[HEAD..] == key
    });
    if !row {
        return Err(FetchError::CorruptIndex(format!(
            "a record an index entry points at is not the row it indexes (table {table})"
        )));
    }
    Ok(&payload[value_at..])
}

#[cfg(test)]
mod test {
    use super::*;

    /// `put_value` returns the value only for the indexed table's put of the asked key.
    #[test]
    fn test_tndb_record_put_value_checks_the_row() {
        let mut put = head(KIND_PUT, 2, true).to_vec();
        put.extend_from_slice(b"keyXvalue");
        assert_eq!(put_value(&put, 2, b"keyX").expect("the row"), b"value");
        let corrupt = |payload: &[u8], table, key: &[u8]| {
            matches!(put_value(payload, table, key), Err(FetchError::CorruptIndex(_)))
        };
        assert!(corrupt(&put, 3, b"keyX"), "another table's record");
        assert!(corrupt(&put, 2, b"keyY"), "another key's record");
        let mut remove = head(KIND_REMOVE, 2, true).to_vec();
        remove.extend_from_slice(b"keyX");
        assert!(corrupt(&remove, 2, b"keyX"), "a removal, not a put");
        assert!(corrupt(&put[..5], 2, b"keyX"), "shorter than its key");
    }

    #[test]
    fn test_tndb_record_parse() {
        let ksize = |table| (table < 3).then_some(4);
        let mut put = head(KIND_PUT, 2, true).to_vec();
        put.extend_from_slice(b"keyXvalue");
        assert_eq!(
            parse(&put, ksize).expect("put"),
            Record { table: 2, op: Op::Put(b"keyX"), end: true }
        );
        let mut remove = head(KIND_REMOVE, 1, false).to_vec();
        remove.extend_from_slice(b"keyX");
        assert_eq!(
            parse(&remove, ksize).expect("remove"),
            Record { table: 1, op: Op::Remove(b"keyX"), end: false }
        );
        assert_eq!(
            parse(&head(KIND_CLEAR, 0, true), ksize).expect("clear"),
            Record { table: 0, op: Op::Clear, end: true }
        );
        assert!(parse(&head(KIND_CLEAR, 7, true), ksize).is_err(), "unknown table");
        assert!(parse(&head(9, 0, true), ksize).is_err(), "unknown kind");
        assert!(parse(&[KIND_PUT, 0], ksize).is_err(), "shorter than its head");
        let mut short = head(KIND_REMOVE, 0, true).to_vec();
        short.extend_from_slice(b"key");
        assert!(parse(&short, ksize).is_err(), "a removal of the wrong length");
    }
}

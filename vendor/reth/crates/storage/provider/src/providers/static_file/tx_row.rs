//! TN(TN-0201): the rows of a `Transactions` jar with tagged values (`NippyJar` version 3).
//!
//! Each stored value of such a jar is a tag byte followed by its payload (the jar appends and
//! verifies the value's checksum, which no reader here sees):
//! - tag 0, inline: the transaction's `Compact` bytes, byte for byte what a jar without tags
//!   stores;
//! - tag 1, position: the transaction's position in the consensus batch its block executed, the
//!   canonical unsigned LEB128 of a `u32` (1 to 5 bytes, the last one not a redundant zero), with
//!   nothing after it.
//!
//! Any other tag is refused. A jar without tagged values stores every transaction inline,
//! untagged, as upstream does.

use alloy_primitives::{bytes::BufMut, BlockNumber};
use reth_codecs::Compact;
use std::path::PathBuf;

/// The tag of a row that stores the transaction itself.
pub const TX_ROW_INLINE: u8 = 0;
/// The tag of a row that stores the transaction's position in its batch.
pub const TX_ROW_REF: u8 = 1;
/// The longest canonical LEB128 of a `u32`.
const MAX_POSITION_LEN: usize = 5;

/// A `Transactions` row, as [`decode_tx_row`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxRow<T> {
    /// The transaction itself.
    Inline(T),
    /// The transaction's position in the batch its block executed.
    Ref(u32),
}

/// A tagged `Transactions` value that does not decode. The value's checksum was verified before,
/// so this is a writer bug or a format this build does not know.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxRowError {
    /// The value holds no byte, so no tag.
    Empty,
    /// A tag other than [`TX_ROW_INLINE`] and [`TX_ROW_REF`].
    UnknownTag(u8),
    /// The position is missing, or its last byte says another follows.
    TruncatedPosition,
    /// The position is longer than the five bytes a `u32` takes.
    OverlongPosition,
    /// The position ends in a redundant zero byte.
    NonCanonicalPosition,
    /// The position is above `u32::MAX`.
    PositionAboveU32,
    /// This many bytes follow the position.
    TrailingBytes(usize),
}

impl core::fmt::Display for TxRowError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => write!(f, "transaction row is empty"),
            Self::UnknownTag(tag) => write!(f, "transaction row has an unknown tag {tag}"),
            Self::TruncatedPosition => write!(f, "transaction row's position ends early"),
            Self::OverlongPosition => {
                write!(f, "transaction row's position is longer than {MAX_POSITION_LEN} bytes")
            }
            Self::NonCanonicalPosition => {
                write!(f, "transaction row's position ends in a redundant zero byte")
            }
            Self::PositionAboveU32 => write!(f, "transaction row's position is above u32::MAX"),
            Self::TrailingBytes(len) => {
                write!(f, "transaction row has {len} bytes after its position")
            }
        }
    }
}

impl core::error::Error for TxRowError {}

/// Decodes a `Transactions` value. `tagged` is the jar's
/// [`has_tagged_values`](reth_nippy_jar::NippyJar::has_tagged_values): a value of a jar without
/// tags is always the transaction itself, decoded as upstream does.
pub fn decode_tx_row<T: Compact>(value: &[u8], tagged: bool) -> Result<TxRow<T>, TxRowError> {
    if !tagged {
        return Ok(TxRow::Inline(T::from_compact(value, value.len()).0))
    }
    match value.split_first() {
        None => Err(TxRowError::Empty),
        Some((&TX_ROW_INLINE, tx)) => Ok(TxRow::Inline(T::from_compact(tx, tx.len()).0)),
        Some((&TX_ROW_REF, position)) => decode_position(position).map(TxRow::Ref),
        Some((&tag, _)) => Err(TxRowError::UnknownTag(tag)),
    }
}

/// Writes the value of a row that stores `position`: [`TX_ROW_REF`] and the position's canonical
/// LEB128. Returns the bytes written (2 to 6).
pub fn encode_tx_ref<B: BufMut>(position: u32, buf: &mut B) -> usize {
    buf.put_u8(TX_ROW_REF);
    let mut rest = position;
    let mut len = 2;
    while rest >= 0x80 {
        buf.put_u8(rest as u8 | 0x80);
        rest >>= 7;
        len += 1;
    }
    buf.put_u8(rest as u8);
    len
}

/// Decodes a position's LEB128, which must be canonical and the whole of `bytes`.
fn decode_position(bytes: &[u8]) -> Result<u32, TxRowError> {
    let mut value = 0u64;
    for (i, &byte) in bytes.iter().enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            if i > 0 && byte == 0 {
                return Err(TxRowError::NonCanonicalPosition)
            }
            let position = u32::try_from(value).map_err(|_| TxRowError::PositionAboveU32)?;
            return match bytes.len() - i - 1 {
                0 => Ok(position),
                trailing => Err(TxRowError::TrailingBytes(trailing)),
            }
        }
        if i + 1 == MAX_POSITION_LEN {
            return Err(TxRowError::OverlongPosition)
        }
    }
    Err(TxRowError::TruncatedPosition)
}

/// A value the static-file writer appends to a tagged `Transactions` jar. Write only: it is read
/// back with [`decode_tx_row`].
pub(crate) enum TaggedTx<'a, T> {
    /// [`TX_ROW_INLINE`], then the transaction's `Compact` bytes.
    Inline(&'a T),
    /// [`encode_tx_ref`].
    Ref(u32),
}

impl<T: Compact> Compact for TaggedTx<'_, T> {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        match self {
            Self::Inline(tx) => {
                buf.put_u8(TX_ROW_INLINE);
                1 + tx.to_compact(buf)
            }
            Self::Ref(position) => encode_tx_ref(*position, buf),
        }
    }

    fn from_compact(_: &[u8], _: usize) -> (Self, &[u8]) {
        unreachable!("a tagged transaction value is only written; `decode_tx_row` reads it")
    }
}

/// Positions that cannot be written as a block's `Transactions` rows. The persist that met them
/// fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxPositionsError {
    /// The positions were given for another number of blocks than the call writes.
    BlockCount {
        /// Blocks written.
        blocks: usize,
        /// Blocks with an entry in the positions.
        positions: usize,
    },
    /// A block's positions are not one per transaction.
    Count {
        /// The block.
        block: BlockNumber,
        /// Its transactions.
        transactions: usize,
        /// Its positions.
        positions: usize,
    },
    /// A block's positions are not strictly increasing.
    NotIncreasing {
        /// The block.
        block: BlockNumber,
        /// The index in the block of the first position not above the one before it.
        index: usize,
        /// The position before it.
        previous: u32,
        /// The position.
        position: u32,
    },
    /// A position row was asked of a jar without tagged values.
    UntaggedJar {
        /// The jar's data file.
        path: PathBuf,
    },
}

impl core::fmt::Display for TxPositionsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BlockCount { blocks, positions } => write!(
                f,
                "transaction positions given for {positions} blocks, but {blocks} are written"
            ),
            Self::Count { block, transactions, positions } => {
                write!(f, "block {block} has {transactions} transactions but {positions} positions")
            }
            Self::NotIncreasing { block, index, previous, position } => write!(
                f,
                "block {block}'s transaction positions are not increasing: {position} at index \
                 {index} follows {previous}"
            ),
            Self::UntaggedJar { path } => write!(
                f,
                "static file {} has no tagged values, so it cannot store a transaction position",
                path.display()
            ),
        }
    }
}

impl core::error::Error for TxPositionsError {}

/// Checks that `tx_positions` has an entry for each of `blocks` blocks, or none.
pub(crate) const fn check_tx_positions_len(
    blocks: usize,
    tx_positions: &[Option<std::sync::Arc<[u32]>>],
) -> Result<(), TxPositionsError> {
    if tx_positions.is_empty() || tx_positions.len() == blocks {
        return Ok(())
    }
    Err(TxPositionsError::BlockCount { blocks, positions: tx_positions.len() })
}

/// Checks that `positions` can be the rows of block `block`, which has `transactions`
/// transactions: one position each, strictly increasing (a block keeps its batch's order).
pub(crate) fn check_tx_positions(
    block: BlockNumber,
    transactions: usize,
    positions: &[u32],
) -> Result<(), TxPositionsError> {
    if positions.len() != transactions {
        return Err(TxPositionsError::Count { block, transactions, positions: positions.len() })
    }
    if let Some(index) = positions.windows(2).position(|pair| pair[0] >= pair[1]) {
        return Err(TxPositionsError::NotIncreasing {
            block,
            index: index + 1,
            previous: positions[index],
            position: positions[index + 1],
        })
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Bytes, Signature, TxKind, U256};
    use reth_ethereum_primitives::TransactionSigned;

    fn encoded(value: TaggedTx<'_, TransactionSigned>) -> Vec<u8> {
        let mut buf = Vec::new();
        let len = value.to_compact(&mut buf);
        assert_eq!(len, buf.len());
        buf
    }

    fn decode(value: &[u8]) -> Result<TxRow<TransactionSigned>, TxRowError> {
        decode_tx_row(value, true)
    }

    /// Asserts that `row` is `tx` inline, by its encoding and hash (a decoded transaction's hash
    /// cache may differ from the original's, so not by `==`).
    fn assert_inline(row: Result<TxRow<TransactionSigned>, TxRowError>, tx: &TransactionSigned) {
        let Ok(TxRow::Inline(decoded)) = row else { panic!("not inline: {row:?}") };
        assert_eq!(decoded.encoded_2718(), tx.encoded_2718());
        assert_eq!(decoded.tx_hash(), tx.tx_hash());
    }

    fn transactions() -> Vec<TransactionSigned> {
        let legacy = TxLegacy { nonce: 3, gas_limit: 21_000, ..Default::default() };
        let call = TxEip1559 {
            chain_id: 2017,
            nonce: 9,
            gas_limit: 90_000,
            to: TxKind::Call(alloy_primitives::Address::with_last_byte(7)),
            value: U256::from(5),
            // Long enough for the Compact encoding to compress it.
            input: Bytes::from(vec![0xAB; 64]),
            ..Default::default()
        };
        vec![
            legacy.into_signed(Signature::test_signature()).into(),
            call.into_signed(Signature::test_signature()).into(),
        ]
    }

    /// An inline row is the tag, then exactly what a jar without tags stores; both decode to the
    /// transaction. A position row round-trips every length of its LEB128.
    #[test]
    fn tx_rows_round_trip() {
        for tx in transactions() {
            let mut untagged = Vec::new();
            tx.to_compact(&mut untagged);
            let inline = encoded(TaggedTx::Inline(&tx));
            assert_eq!(inline[0], TX_ROW_INLINE);
            assert_eq!(inline[1..], untagged);
            assert_inline(decode(&inline), &tx);
            assert_inline(decode_tx_row(&untagged, false), &tx);
        }

        for (position, len) in [
            (0, 2),
            (1, 2),
            (127, 2),
            (128, 3),
            (16_383, 3),
            (16_384, 4),
            ((1 << 21) - 1, 4),
            (1 << 21, 5),
            ((1 << 28) - 1, 5),
            (1 << 28, 6),
            (u32::MAX, 6),
        ] {
            let value = encoded(TaggedTx::Ref(position));
            assert_eq!(value.len(), len, "{position}");
            let mut direct = Vec::new();
            assert_eq!(encode_tx_ref(position, &mut direct), len);
            assert_eq!(direct, value);
            assert_eq!(decode(&value), Ok(TxRow::Ref(position)), "{position}");
        }
        assert_eq!(encoded(TaggedTx::Ref(300)), [TX_ROW_REF, 0xAC, 0x02]);
    }

    /// Every malformed tagged value is an error, never a transaction or another position.
    #[test]
    fn malformed_tx_rows_are_refused() {
        assert_eq!(decode(&[]), Err(TxRowError::Empty));
        for tag in 2..=u8::MAX {
            assert_eq!(decode(&[tag, 0]), Err(TxRowError::UnknownTag(tag)), "{tag}");
            assert_eq!(decode(&[tag]), Err(TxRowError::UnknownTag(tag)), "{tag}");
        }

        let cases: &[(&[u8], TxRowError)] = &[
            // No position, or one whose last byte says another follows.
            (&[], TxRowError::TruncatedPosition),
            (&[0x80], TxRowError::TruncatedPosition),
            (&[0xFF, 0xFF, 0xFF, 0xFF], TxRowError::TruncatedPosition),
            // More than five bytes.
            (&[0x80, 0x80, 0x80, 0x80, 0x80], TxRowError::OverlongPosition),
            (&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00], TxRowError::OverlongPosition),
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0x8F, 0x00], TxRowError::OverlongPosition),
            // A redundant zero byte: 0, 1 and 128 spelled one byte too long.
            (&[0x80, 0x00], TxRowError::NonCanonicalPosition),
            (&[0x81, 0x00], TxRowError::NonCanonicalPosition),
            (&[0x80, 0x81, 0x00], TxRowError::NonCanonicalPosition),
            (&[0x80, 0x80, 0x80, 0x80, 0x00], TxRowError::NonCanonicalPosition),
            // Above u32::MAX: 2^32 and the largest five bytes hold.
            (&[0x80, 0x80, 0x80, 0x80, 0x10], TxRowError::PositionAboveU32),
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F], TxRowError::PositionAboveU32),
            // Bytes after the position.
            (&[0x05, 0x00], TxRowError::TrailingBytes(1)),
            (&[0xAC, 0x02, 0x01, 0x02], TxRowError::TrailingBytes(2)),
            (&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x00], TxRowError::TrailingBytes(1)),
        ];
        for (position, err) in cases {
            let value = [&[TX_ROW_REF][..], position].concat();
            assert_eq!(decode(&value), Err(err.clone()), "{position:02x?}");
        }
        // The largest position decodes.
        assert_eq!(decode(&[TX_ROW_REF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F]), Ok(TxRow::Ref(u32::MAX)));
    }

    /// A block's positions must be one per transaction and strictly increasing.
    #[test]
    fn tx_positions_are_checked() {
        assert_eq!(check_tx_positions(4, 0, &[]), Ok(()));
        assert_eq!(check_tx_positions(4, 3, &[0, 2, 9]), Ok(()));
        assert_eq!(
            check_tx_positions(4, 3, &[0, 2]),
            Err(TxPositionsError::Count { block: 4, transactions: 3, positions: 2 })
        );
        assert_eq!(
            check_tx_positions(4, 1, &[0, 2]),
            Err(TxPositionsError::Count { block: 4, transactions: 1, positions: 2 })
        );
        assert_eq!(
            check_tx_positions(4, 3, &[0, 2, 2]),
            Err(TxPositionsError::NotIncreasing { block: 4, index: 2, previous: 2, position: 2 })
        );
        assert_eq!(
            check_tx_positions(4, 3, &[5, 1, 7]),
            Err(TxPositionsError::NotIncreasing { block: 4, index: 1, previous: 5, position: 1 })
        );
    }
}

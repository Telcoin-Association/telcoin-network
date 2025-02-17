//! Compatibility wrapper for redb

use redb::{Key, TypeName, Value};
use std::{fmt::Debug, marker::PhantomData};

use tn_types::{decode, decode_key, encode, encode_key, KeyT, ValueT};

#[derive(Debug)]
pub struct KeyWrap<K: KeyT>(PhantomData<K>);
impl<K: KeyT> Key for KeyWrap<K> {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        // Do a byte compare
        // We use encode_key/decode_key so this is fine.
        data1.cmp(data2)
    }
}

impl<K: KeyT> Value for KeyWrap<K> {
    type SelfType<'a>
        = K
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        //todo!()
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode_key(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        encode_key(value)
    }

    fn type_name() -> TypeName {
        TypeName::new(std::any::type_name::<K>())
    }
}

#[derive(Debug)]
pub struct ValWrap<V: ValueT>(PhantomData<V>);
impl<V: ValueT> Value for ValWrap<V> {
    type SelfType<'a>
        = V
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        encode(value)
    }

    fn type_name() -> redb::TypeName {
        TypeName::new(std::any::type_name::<V>())
    }
}

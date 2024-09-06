use bincode::Options as _;
use redb::{Key, TypeName, Value};
use std::{fmt::Debug, marker::PhantomData};

use crate::traits::{KeyT, ValueT};

#[derive(Debug)]
pub struct KeyWrap<K: KeyT>(PhantomData<K>);
impl<K: KeyT> Key for KeyWrap<K> {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        // If we want to do a typed compare use this:
        //let d1 = KeyWrap::<K>::from_bytes(data1);
        //let d2 = KeyWrap::<K>::from_bytes(data2);
        //d1.cmp(&d2)
        // Do a byte compare
        data1.cmp(data2)
    }
}

impl<K: KeyT> Value for KeyWrap<K> {
    type SelfType<'a> = K
    where
        Self: 'a;

    type AsBytes<'a> = Vec<u8>
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
        bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .deserialize(data)
            .expect("Invalid bytes!")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .serialize(value)
            .expect("Can not serialize!")
    }

    fn type_name() -> TypeName {
        TypeName::new(std::any::type_name::<K>())
    }
}

#[derive(Debug)]
pub struct ValWrap<V: ValueT>(PhantomData<V>);
impl<V: ValueT> Value for ValWrap<V> {
    type SelfType<'a> = V
    where
        Self: 'a;

    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .deserialize(data)
            .expect("Invalid bytes!")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .serialize(value)
            .expect("Can not serialize!")
    }

    fn type_name() -> redb::TypeName {
        TypeName::new(std::any::type_name::<V>())
    }
}

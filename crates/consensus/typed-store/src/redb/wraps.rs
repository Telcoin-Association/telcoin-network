use bincode::Options as _;
use redb::{Key, TypeName, Value};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, ops::Deref};

#[derive(Debug)]
pub struct KeyWrap<K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static>(
    pub(crate) K,
);
impl<K> Key for KeyWrap<K>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
{
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let d1 = KeyWrap::<K>::from_bytes(data1);
        let d2 = KeyWrap::<K>::from_bytes(data2);
        d1.cmp(&d2)
    }
}

impl<K> Deref for KeyWrap<K>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
{
    type Target = K;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Value for KeyWrap<K>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
{
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
pub struct ValWrap<V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static>(
    pub(crate) V,
);
impl<V> Value for ValWrap<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
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

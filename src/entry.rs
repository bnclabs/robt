use mkit::{
    cbor::{Cbor, FromCbor, IntoCbor},
    Cborize, Diff,
};

use std::io;

use crate::{vlog, Result};

const KEY_VER1: u32 = 0x0001;
const ENTRY_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub struct Key<K> {
    key: K,
}

impl<K> Key<K> {
    const ID: u32 = ENTRY_VER1;
}

impl<K> Key<K>
where
    K: IntoCbor + FromCbor,
{
    pub fn encode<W>(self, buf: &mut W) -> Result<usize>
    where
        W: io::Write,
    {
        Ok(self.into_cbor()?.encode(buf)?)
    }
    pub fn decode<R>(buf: &mut R) -> Result<(Self, usize)>
    where
        R: io::Read,
    {
        let (val, n) = Cbor::decode(buf)?;
        Ok((Key::from_cbor(val)?, n))
    }
}

#[derive(Clone, Cborize)]
pub enum Entry<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    MM {
        fpos: u64,
    },
    MZ {
        fpos: u64,
    },
    ZZ {
        seqno: u64,
        delete: bool,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<V>>,
    },
}

impl<V> Entry<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    const ID: u32 = ENTRY_VER1;

    fn new_mm(fpos: u64) -> Self {
        Entry::MM { fpos }
    }

    fn new_mz(fpos: u64) -> Self {
        Entry::MZ { fpos }
    }

    fn new_zz(
        seqno: u64,
        delete: bool,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<V>>,
    ) -> Self {
        Entry::ZZ {
            seqno,
            delete,
            value,
            deltas,
        }
    }
}

impl<V> Entry<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    pub fn encode<W>(self, buf: &mut W) -> Result<usize>
    where
        W: io::Write,
    {
        Ok(self.into_cbor()?.encode(buf)?)
    }
    pub fn decode<R>(buf: &mut R) -> Result<(Self, usize)>
    where
        R: io::Read,
    {
        let (val, n) = Cbor::decode(buf)?;
        Ok((Entry::from_cbor(val)?, n))
    }

    pub fn is_mblock(&self) -> bool {
        match self {
            Entry::MM { .. } => true,
            Entry::MZ { .. } => true,
            Entry::ZZ { .. } => false,
        }
    }

    pub fn is_zblock(&self) -> bool {
        !self.is_mblock()
    }

    pub fn is_child_zblock(&self) -> bool {
        match self {
            Entry::MZ { .. } => true,
            Entry::MM { .. } => false,
            Entry::ZZ { .. } => false,
        }
    }

    pub fn to_fpos(&self) -> Option<u64> {
        match self {
            Entry::MZ { fpos } => Some(*fpos),
            Entry::MM { fpos } => Some(*fpos),
            Entry::ZZ { .. } => None,
        }
    }
}

//#[cfg(test)]
//#[path = "entry_test.rs"]
//mod entry_test;

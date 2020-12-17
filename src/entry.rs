use mkit::{
    cbor::{Cbor, FromCbor, IntoCbor},
    Cborize, Diff,
};

use std::io;

use crate::{vlog, Result};

const ENTRY_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub enum Entry<K, V>
where
    V: Diff,
    <V as Diff>::D: IntoCbor + FromCbor,
{
    MM {
        key: K,
        fpos: u64,
    },
    MZ {
        key: K,
        fpos: u64,
    },
    ZZ {
        key: K,
        seqno: u64,
        deleted: bool,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<<V as Diff>::D>>,
    },
}

impl<K, V> Entry<K, V>
where
    V: Diff,
    <V as Diff>::D: IntoCbor + FromCbor,
{
    const ID: u32 = ENTRY_VER1;

    fn new_mm(key: K, fpos: u64) -> Self {
        Entry::MM { key, fpos }
    }

    fn new_mz(key: K, fpos: u64) -> Self {
        Entry::MZ { key, fpos }
    }

    fn new_zz(
        key: K,
        seqno: u64,
        deleted: bool,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<<V as Diff>::D>>,
    ) -> Self {
        Entry::ZZ {
            key,
            seqno,
            deleted,
            value,
            deltas,
        }
    }
}

impl<K, V> Entry<K, V>
where
    V: Diff + IntoCbor + FromCbor,
    K: IntoCbor + FromCbor,
    <V as Diff>::D: IntoCbor + FromCbor,
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
            Entry::MZ { fpos, .. } => Some(*fpos),
            Entry::MM { fpos, .. } => Some(*fpos),
            Entry::ZZ { .. } => None,
        }
    }
}

//#[cfg(test)]
//#[path = "entry_test.rs"]
//mod entry_test;

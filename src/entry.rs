use mkit::{
    cbor::{self, Cbor, FromCbor, IntoCbor},
    db, Cborize, Diff,
};

use std::convert::TryFrom;

use crate::{util, vlog, Error, Result};

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
        length: u64,
    },
    MZ {
        key: K,
        fpos: u64,
        length: u64,
    },
    ZZ {
        key: K,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<<V as Diff>::D>>,
    },
}

impl<K, V> From<db::Entry<K, V>> for Entry<K, V>
where
    V: Diff,
    <V as Diff>::D: IntoCbor + FromCbor,
{
    fn from(e: db::Entry<K, V>) -> Self {
        Entry::ZZ {
            key: e.key,
            value: e.value.into(),
            deltas: e.deltas.into_iter().map(vlog::Delta::from).collect(),
        }
    }
}

impl<K, V> Entry<K, V>
where
    V: Diff,
    <V as Diff>::D: IntoCbor + FromCbor,
{
    const ID: u32 = ENTRY_VER1;

    fn new_mm(key: K, fpos: u64, length: u64) -> Self {
        Entry::MM { key, fpos, length }
    }

    fn new_mz(key: K, fpos: u64, length: u64) -> Self {
        Entry::MZ { key, fpos, length }
    }

    fn new_zz(
        key: K,
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<<V as Diff>::D>>,
    ) -> Self {
        Entry::ZZ { key, value, deltas }
    }
}

impl<K, V> Entry<K, V>
where
    K: FromCbor + IntoCbor,
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: IntoCbor + FromCbor,
{
    pub fn encode_zz(
        self,
        mut vfpos: u64,
        value_in_vlog: bool,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        match self {
            Entry::ZZ { key, value, deltas } => {
                let (value, mut vblock) = if value_in_vlog {
                    value.encode(vfpos)?
                } else {
                    (value, vec![])
                };

                Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut vblock)?;

                vfpos += err_at!(FailConvert, u64::try_from(vblock.len()))?;

                let mut deltas_ref = vec![];
                for delta in deltas.into_iter() {
                    let (delta, data) = delta.encode(vfpos)?;
                    deltas_ref.push(delta);
                    vblock.extend_from_slice(&data);
                    vfpos += err_at!(FailConvert, u64::try_from(data.len()))?;
                }

                Cbor::try_from(cbor::SimpleValue::Break)?.encode(&mut vblock)?;

                let entry = Entry::ZZ {
                    key,
                    value,
                    deltas: deltas_ref,
                };

                let iblock = util::to_cbor_bytes(entry)?;

                Ok((iblock, vblock))
            }
            _ => unreachable!(),
        }
    }
}

impl<K, V> Entry<K, V>
where
    V: Diff + IntoCbor + FromCbor,
    K: IntoCbor + FromCbor,
    <V as Diff>::D: IntoCbor + FromCbor,
{
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

use mkit::{
    cbor::{self, Cbor, IntoCbor},
    db, Cborize,
};

use std::{borrow::Borrow, convert::TryFrom};

use crate::{util, vlog, Error, Result};

const ENTRY_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub enum Entry<K, V, D> {
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
        value: vlog::Value<V>,
        deltas: Vec<vlog::Delta<D>>,
    },
}

impl<K, V, D> From<db::Entry<K, V, D>> for Entry<K, V, D> {
    fn from(e: db::Entry<K, V, D>) -> Self {
        Entry::ZZ {
            key: e.key,
            value: e.value.into(),
            deltas: e.deltas.into_iter().map(vlog::Delta::from).collect(),
        }
    }
}

impl<K, V, D> From<Entry<K, V, D>> for db::Entry<K, V, D> {
    fn from(e: Entry<K, V, D>) -> Self {
        match e {
            Entry::ZZ { key, value, deltas } => db::Entry {
                key,
                value: value.into(),
                deltas: deltas.into_iter().map(db::Delta::from).collect(),
            },
            Entry::MZ { .. } => unreachable!(),
            Entry::MM { .. } => unreachable!(),
        }
    }
}

impl<K, V, D> Entry<K, V, D> {
    const ID: u32 = ENTRY_VER1;

    pub fn new_mm(key: K, fpos: u64) -> Self {
        Entry::MM { key, fpos }
    }

    pub fn new_mz(key: K, fpos: u64) -> Self {
        Entry::MZ { key, fpos }
    }
}

impl<K, V, D> Entry<K, V, D>
where
    K: IntoCbor,
    V: IntoCbor,
    D: IntoCbor,
{
    pub fn into_reference(self, mut vfpos: u64, vlog: bool) -> Result<(Self, Vec<u8>)> {
        match self {
            val @ Entry::MM { .. } => Ok((val, vec![])),
            val @ Entry::MZ { .. } => Ok((val, vec![])),
            Entry::ZZ { key, value, deltas } => {
                let (value, mut vblock) = if vlog {
                    value.into_reference(vfpos)?
                } else {
                    (value, vec![])
                };

                Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut vblock)?;

                vfpos += err_at!(FailConvert, u64::try_from(vblock.len()))?;

                let mut drefs = vec![];
                for delta in deltas.into_iter() {
                    let (delta, data) = delta.into_reference(vfpos)?;
                    drefs.push(delta);
                    vblock.extend_from_slice(&data);
                    vfpos += err_at!(FailConvert, u64::try_from(data.len()))?;
                }

                vblock
                    .extend_from_slice(&util::into_cbor_bytes(cbor::SimpleValue::Break)?);

                let entry = Entry::ZZ {
                    key,
                    value,
                    deltas: drefs,
                };

                Ok((entry, vblock))
            }
        }
    }
}

impl<K, V, D> Entry<K, V, D> {
    pub fn as_key(&self) -> &K {
        match self {
            Entry::MZ { key, .. } => key,
            Entry::MM { key, .. } => key,
            Entry::ZZ { key, .. } => key,
        }
    }

    pub fn to_key(&self) -> K
    where
        K: Clone,
    {
        match self {
            Entry::MZ { key, .. } => key.clone(),
            Entry::MM { key, .. } => key.clone(),
            Entry::ZZ { key, .. } => key.clone(),
        }
    }

    pub fn borrow_key<Q>(&self) -> &Q
    where
        K: Borrow<Q>,
    {
        match self {
            Entry::MZ { key, .. } => key.borrow(),
            Entry::MM { key, .. } => key.borrow(),
            Entry::ZZ { key, .. } => key.borrow(),
        }
    }
}

//#[cfg(test)]
//#[path = "entry_test.rs"]
//mod entry_test;

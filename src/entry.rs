use mkit::{
    cbor::{self, Cbor, IntoCbor},
    db, Cborize,
};

use std::convert::TryFrom;

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

impl<K, V, D> Entry<K, V, D> {
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
}

//#[cfg(test)]
//#[path = "entry_test.rs"]
//mod entry_test;

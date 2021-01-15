use mkit::{
    cbor::{self, Cbor, FromCbor, IntoCbor},
    db, Cborize,
};

use std::{borrow::Borrow, convert::TryFrom, fmt, io};

use crate::{reader::Reader, util, vlog, Error, Result};

const ENTRY_VER1: u32 = 0x0001;

#[derive(Clone, Debug, Eq, PartialEq, Cborize)]
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

impl<K, V, D> Entry<K, V, D> {
    // serialize into value-block and return the same.
    pub fn into_reference(self, mut vfpos: u64, vlog: bool) -> Result<(Self, Vec<u8>)>
    where
        V: IntoCbor,
        D: IntoCbor,
    {
        match self {
            Entry::MM { .. } => Ok((self, vec![])),
            Entry::MZ { .. } => Ok((self, vec![])),
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

    pub fn into_native<F>(self, f: &mut F, versions: bool) -> Result<Self>
    where
        V: FromCbor,
        D: FromCbor,
        F: io::Seek + io::Read,
    {
        match self {
            Entry::MM { .. } => Ok(self),
            Entry::MZ { .. } => Ok(self),
            Entry::ZZ { key, value, deltas } if versions => {
                let value = value.into_native(f)?;
                let mut native_deltas = vec![];
                for delta in deltas.into_iter() {
                    native_deltas.push(delta.into_native(f)?);
                }

                let entry = Entry::ZZ {
                    key,
                    value,
                    deltas: native_deltas,
                };

                Ok(entry)
            }
            Entry::ZZ { key, value, .. } => {
                let value = value.into_native(f)?;
                Ok(Entry::ZZ {
                    key,
                    value,
                    deltas: Vec::default(),
                })
            }
        }
    }

    pub fn print(&self, prefix: &str, reader: &mut Reader<K, V, D>) -> Result<()>
    where
        K: fmt::Debug + FromCbor,
        V: fmt::Debug + FromCbor,
        D: fmt::Debug + FromCbor,
    {
        let fd = &mut reader.index;
        let entries = match self {
            Entry::MM { key, fpos } => {
                println!("{}MM<{:?}@{}>", prefix, key, fpos);
                let fpos = io::SeekFrom::Start(*fpos);
                let block = read_file!(fd, fpos, reader.m_blocksize, "read mm-block")?;
                Some(util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0)
            }
            Entry::MZ { key, fpos } => {
                println!("{}MZ<{:?}@{}>", prefix, key, fpos);
                let fpos = io::SeekFrom::Start(*fpos);
                let block = read_file!(fd, fpos, reader.m_blocksize, "read mm-block")?;
                Some(util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0)
            }
            Entry::ZZ { key, value, deltas } => {
                println!("{}ZZ---- {:?}; {:?}; {:?}", prefix, key, value, deltas);
                None
            }
        };

        let prefix = prefix.to_string() + "  ";
        match entries {
            Some(entries) => {
                for entry in entries.into_iter() {
                    let entry = match &mut reader.vlog {
                        Some(vlog) => entry.into_native(vlog, true)?,
                        None => entry,
                    };
                    entry.print(prefix.as_str(), reader)?;
                }
            }
            None => (),
        }

        Ok(())
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

    pub fn is_zblock(&self) -> bool {
        match self {
            Entry::MZ { .. } => false,
            Entry::MM { .. } => false,
            Entry::ZZ { .. } => true,
        }
    }
}

#[cfg(test)]
#[path = "entry_test.rs"]
mod entry_test;

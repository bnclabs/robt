use mkit::{
    self,
    cbor::{Cbor, FromCbor, IntoCbor},
    Cborize, Footprint,
};

use std::{fs, io, mem};

use crate::{Error, Result};

const VALUE_VER1: u32 = 0x0001;
const DELTA_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub enum Value<V> {
    N {
        value: V,
        seqno: u64,
        deleted: bool,
    },
    R {
        seqno: u64,
        deleted: bool,
        fpos: u64,
        length: u64,
    },
}

impl<V> Value<V> {
    const ID: u32 = VALUE_VER1;
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> mkit::Result<usize> {
        let mut n = mem::size_of_val(self);
        n += match self {
            Value::N { value, .. } => value.footprint()?,
            Value::R { .. } => 0,
        };

        Ok(n)
    }
}

impl<V> Value<V> {
    pub fn new_native(value: V, seqno: u64, deleted: bool) -> Value<V> {
        Value::N {
            value,
            seqno,
            deleted,
        }
    }

    pub fn new_reference(seqno: u64, deleted: bool, fpos: u64, length: u64) -> Value<V> {
        Value::R {
            seqno,
            deleted,
            fpos,
            length,
        }
    }
}

impl<V> Value<V>
where
    V: FromCbor + IntoCbor,
{
    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        V: FromCbor,
    {
        match self {
            Value::N { .. } => Ok(self),
            Value::R {
                seqno,
                deleted,
                fpos,
                length,
            } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading value from vlog")?;
                let value = V::from_cbor(Cbor::decode(&mut block.as_slice())?.0)?;
                Ok(Value::N {
                    value,
                    seqno,
                    deleted,
                })
            }
        }
    }

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
        Ok((Value::from_cbor(val)?, n))
    }
}

#[derive(Clone, Cborize)]
pub enum Delta<D> {
    N {
        diff: D,
        seqno: u64,
        deleted: bool,
    },
    R {
        seqno: u64,
        deleted: bool,
        fpos: u64,
        length: u64,
    },
}

impl<D> Delta<D> {
    const ID: u32 = DELTA_VER1;
}

impl<D> Footprint for Delta<D>
where
    D: Footprint,
{
    fn footprint(&self) -> mkit::Result<usize> {
        let mut n = mem::size_of_val(self);
        n += match self {
            Delta::N { diff, .. } => diff.footprint()?,
            Delta::R { .. } => 0,
        };

        Ok(n)
    }
}

impl<D> Delta<D> {
    pub fn new_value(diff: D, seqno: u64, deleted: bool) -> Self {
        Delta::N {
            diff,
            seqno,
            deleted,
        }
    }

    pub fn new_reference(seqno: u64, deleted: bool, fpos: u64, length: u64) -> Self {
        Delta::R {
            seqno,
            deleted,
            fpos,
            length,
        }
    }
}

impl<D> Delta<D> {
    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        D: FromCbor,
    {
        match self {
            Delta::N { .. } => Ok(self),
            Delta::R {
                seqno,
                deleted,
                fpos,
                length,
            } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading delta from vlog")?;
                let diff = D::from_cbor(Cbor::decode(&mut block.as_slice())?.0)?;
                Ok(Delta::N {
                    diff,
                    seqno,
                    deleted,
                })
            }
        }
    }

    pub fn encode<W>(self, buf: &mut W) -> Result<usize>
    where
        W: io::Write,
        D: IntoCbor,
    {
        Ok(self.into_cbor()?.encode(buf)?)
    }

    pub fn decode<R>(buf: &mut R) -> Result<(Self, usize)>
    where
        R: io::Read,
        D: FromCbor,
    {
        let (val, n) = Cbor::decode(buf)?;
        Ok((Delta::from_cbor(val)?, n))
    }
}

//#[cfg(test)]
//#[path = "vlog_test.rs"]
//mod vlog_test;

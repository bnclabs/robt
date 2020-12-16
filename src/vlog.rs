use mkit::{
    cbor::{Cbor, FromCbor, IntoCbor},
    Cborize, Footprint,
};

use std::{fs, io, mem};

use crate::{Error, Result};

const VALUE_VER1: u32 = 0x0001;
const DELTA_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub enum Value<V> {
    N(ValueVal<V>),
    R(ValueRef),
}

impl<V> Value<V> {
    const ID: u32 = VALUE_VER1;
}

#[derive(Clone, Cborize)]
pub struct ValueVal<V> {
    value: V,
}

impl<V> ValueVal<V> {
    const ID: u32 = VALUE_VER1;
}

#[derive(Clone, Cborize)]
pub struct ValueRef {
    fpos: u64,
    length: u64,
}

impl ValueRef {
    const ID: u32 = VALUE_VER1;
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> usize {
        match self {
            Value::N(ValueVal { value }) => value.footprint(),
            Value::R(refr) => mem::size_of_val(refr),
        }
    }
}

impl<V> Value<V> {
    pub fn new_native(value: V) -> Value<V> {
        Value::N(ValueVal { value })
    }

    pub fn new_reference(fpos: u64, length: u64) -> Value<V> {
        Value::R(ValueRef { fpos, length })
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
            Value::N(_) => Ok(self),
            Value::R(ValueRef { fpos, length }) => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading value from vlog")?;
                Ok(Self::decode(&mut block.as_slice())?.0)
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
    N(DeltaVal<D>),
    R(DeltaRef),
}

impl<D> Delta<D> {
    const ID: u32 = DELTA_VER1;
}

#[derive(Clone, Cborize)]
pub struct DeltaVal<D> {
    diff: D,
}

impl<D> DeltaVal<D> {
    const ID: u32 = DELTA_VER1;
}

#[derive(Clone, Cborize)]
pub struct DeltaRef {
    fpos: u64,
    length: u64,
}

impl DeltaRef {
    const ID: u32 = DELTA_VER1;
}

impl<D> Footprint for Delta<D>
where
    D: Footprint,
{
    fn footprint(&self) -> usize {
        match self {
            Delta::N(DeltaVal { diff }) => diff.footprint(),
            Delta::R(refr) => mem::size_of_val(refr),
        }
    }
}

impl<D> Delta<D> {
    pub fn new_value(diff: D) -> Self {
        Delta::N(DeltaVal { diff })
    }

    pub fn new_reference(fpos: u64, length: u64) -> Self {
        Delta::R(DeltaRef { fpos, length })
    }
}

impl<D> Delta<D> {
    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        D: FromCbor,
    {
        match self {
            Delta::N(_) => Ok(self),
            Delta::R(DeltaRef { fpos, length }) => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading delta from vlog")?;
                Ok(Self::decode(&mut block.as_slice())?.0)
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

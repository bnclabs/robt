use mkit::{
    cbor::{Cbor, FromCbor, IntoCbor},
    Cborize, Diff, Footprint,
};

use std::{fs, io, mem};

use crate::{Error, Result};

const VALUE_VER1: u32 = 0x10001;
const DELTA_VER1: u32 = 0x20001;

#[derive(Clone)]
pub enum Value<V> {
    Nativ(ValueVal<V>),
    Refrn(ValueRef),
}

#[derive(Clone, Cborize)]
pub struct ValueVal<V> {
    value: V,
}

impl<V> ValueVal<V> {
    const ID: u32 = VALUE_VER1;
}

#[derive(Clone)]
pub struct ValueRef {
    fpos: u64,
    length: u64,
}

impl<V> Footprint for Value<V>
where
    V: Footprint,
{
    fn footprint(&self) -> usize {
        match self {
            Value::Nativ(ValueVal { value }) => value.footprint(),
            Value::Refrn(refr) => mem::size_of_val(refr),
        }
    }
}

impl<V> Value<V> {
    pub fn new_native(value: V) -> Value<V> {
        Value::Nativ(ValueVal { value })
    }

    pub fn new_reference(fpos: u64, length: u64) -> Value<V> {
        Value::Refrn(ValueRef { fpos, length })
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
            Value::Nativ(_) => Ok(self),
            Value::Refrn(ValueRef { fpos, length }) => {
                let block = read_file!(fd, fpos, length, "reading value from vlog")?;
                let (val, _) = Value::decode(&mut block.as_slice())?;
                Ok(val)
            }
        }
    }

    pub fn encode<W>(&self, buf: &mut Vec<u8>) -> Result<usize>
    where
        V: Clone,
        W: io::Write,
    {
        match self {
            Value::Nativ(ValueVal { value }) => Ok(value.clone().into_cbor()?.encode(buf)?),
            Value::Refrn(_) => err_at!(Fatal, msg: "unexpected value-reference")?,
        }
    }

    pub fn decode<R>(buf: &mut R) -> Result<(Self, usize)>
    where
        R: io::Read,
    {
        let (val, n) = Cbor::decode(buf)?;
        let val = ValueVal::from_cbor(val)?;
        Ok((Value::Nativ(val), n))
    }
}

#[derive(Clone)]
pub enum Delta<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    Nativ(DeltaVal<V>),
    Refrn(DeltaRef),
}

#[derive(Clone, Cborize)]
pub struct DeltaVal<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    diff: <V as Diff>::D,
}

impl<V> DeltaVal<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    const ID: u32 = DELTA_VER1;
}

#[derive(Clone)]
pub struct DeltaRef {
    fpos: u64,
    length: u64,
}

impl<V> Footprint for Delta<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    fn footprint(&self) -> usize {
        match self {
            Delta::Nativ(DeltaVal { diff }) => diff.footprint(),
            Delta::Refrn(refr) => mem::size_of_val(refr),
        }
    }
}

impl<V> Delta<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    pub fn new_value(diff: <V as Diff>::D) -> Self {
        Delta::Nativ(DeltaVal { diff })
    }

    pub fn new_reference(fpos: u64, length: u64) -> Self {
        Delta::Refrn(DeltaRef { fpos, length })
    }
}

impl<V> Delta<V>
where
    V: Diff + FromCbor + IntoCbor,
    <V as Diff>::D: FromCbor + IntoCbor,
{
    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        V: Diff,
        <V as Diff>::D: FromCbor,
    {
        match self {
            Delta::Nativ(_) => Ok(self),
            Delta::Refrn(DeltaRef { fpos, length }) => {
                let block = read_file!(fd, fpos, length, "reading delta from vlog")?;
                let (val, _) = Delta::decode(&mut block.as_slice())?;
                Ok(val)
            }
        }
    }

    pub fn encode<W>(self, buf: &mut W) -> Result<usize>
    where
        W: io::Write,
    {
        match self {
            Delta::Nativ(DeltaVal { diff }) => Ok(diff.into_cbor()?.encode(buf)?),
            Delta::Refrn(_) => err_at!(Fatal, msg: "unexpected delta-reference")?,
        }
    }

    pub fn decode<R>(buf: &mut R) -> Result<(Self, usize)>
    where
        R: io::Read,
    {
        let (val, n) = Cbor::decode(buf)?;
        let val = DeltaVal::from_cbor(val)?;
        Ok((Delta::Nativ(val), n))
    }
}

//#[cfg(test)]
//#[path = "vlog_test.rs"]
//mod vlog_test;

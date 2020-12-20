use mkit::{
    self,
    cbor::{FromCbor, IntoCbor},
    db, Cborize,
};

use std::{convert::TryFrom, fs, io};

use crate::{util, Error, Result};

const VALUE_VER1: u32 = 0x0001;
const DELTA_VER1: u32 = 0x0001;

#[derive(Clone, Cborize)]
pub enum Value<V> {
    N { value: db::Value<V> },
    R { fpos: u64, length: u64 },
}

impl<V> Value<V> {
    const ID: u32 = VALUE_VER1;
}

impl<V> From<db::Value<V>> for Value<V> {
    fn from(value: db::Value<V>) -> Value<V> {
        Value::N { value }
    }
}

impl<V> From<Value<V>> for db::Value<V> {
    fn from(value: Value<V>) -> db::Value<V> {
        match value {
            Value::N { value } => value,
            Value::R { .. } => unreachable!(),
        }
    }
}

impl<V> Value<V> {
    pub fn encode(self, fpos: u64) -> Result<(Self, Vec<u8>)>
    where
        V: IntoCbor,
    {
        match self {
            val @ Value::N { .. } => {
                let data = util::to_cbor_bytes(val)?;
                let length = err_at!(FailConvert, u64::try_from(data.len()))?;
                Ok((Value::R { fpos, length }, data))
            }
            val @ Value::R { .. } => Ok((val, vec![])),
        }
    }

    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        V: FromCbor,
    {
        match self {
            Value::N { .. } => Ok(self),
            Value::R { fpos, length } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading value from vlog")?;
                let value = util::from_cbor_bytes(&block)?.0;
                Ok(Value::N { value })
            }
        }
    }
}

#[derive(Clone, Cborize)]
pub enum Delta<D> {
    N { delta: db::Delta<D> },
    R { fpos: u64, length: u64 },
}

impl<D> Delta<D> {
    const ID: u32 = DELTA_VER1;
}

impl<D> From<db::Delta<D>> for Delta<D> {
    fn from(delta: db::Delta<D>) -> Delta<D> {
        Delta::N { delta }
    }
}

impl<D> From<Delta<D>> for db::Delta<D> {
    fn from(delta: Delta<D>) -> db::Delta<D> {
        match delta {
            Delta::N { delta } => delta,
            Delta::R { .. } => unreachable!(),
        }
    }
}

impl<D> Delta<D> {
    pub fn encode(self, fpos: u64) -> Result<(Self, Vec<u8>)>
    where
        D: IntoCbor,
    {
        match self {
            val @ Delta::N { .. } => {
                let data = util::to_cbor_bytes(val)?;
                let length = err_at!(FailConvert, u64::try_from(data.len()))?;
                Ok((Delta::R { fpos, length }, data))
            }
            val @ Delta::R { .. } => Ok((val, vec![])),
        }
    }

    pub fn fetch(self, fd: &mut fs::File) -> Result<Self>
    where
        D: FromCbor,
    {
        match self {
            Delta::N { .. } => Ok(self),
            Delta::R { fpos, length } => {
                let seek = io::SeekFrom::Start(fpos);
                let block = read_file!(fd, seek, length, "reading delta from vlog")?;
                let delta = util::from_cbor_bytes(&block)?.0;
                Ok(Delta::N { delta })
            }
        }
    }
}

//#[cfg(test)]
//#[path = "vlog_test.rs"]
//mod vlog_test;

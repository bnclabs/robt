use mkit::cbor::IntoCbor;

use crate::{Error, Result};

pub fn encode_to_cbor<T>(val: T) -> Result<Vec<u8>>
where
    T: IntoCbor,
{
    let mut data: Vec<u8> = vec![];
    let n = val.into_cbor()?.encode(&mut data)?;
    if n != data.len() {
        err_at!(Fatal, msg: "cbor encoding len mistmatch {} {}", n, data.len())
    } else {
        Ok(data)
    }
}

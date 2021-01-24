use mkit::cbor::{Cbor, FromCbor, IntoCbor};

use crate::{Error, Result};

pub fn into_cbor_bytes<T>(val: T) -> Result<Vec<u8>>
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

pub fn from_cbor_bytes<T>(mut data: &[u8]) -> Result<(T, usize)>
where
    T: FromCbor,
{
    let (val, n) = Cbor::decode(&mut data)?;
    Ok((T::from_cbor(val)?, n))
}

#[cfg(test)]
use ppom::Mdb;

#[cfg(test)]
pub fn load_index(seed: u128, diff: bool, n: usize, dels: usize) -> Mdb<u16, u64, u64> {
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let index = Mdb::new("testing");

    for _i in 0..n {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        match diff {
            true => index.insert(key, value).ok().map(|_| ()),
            false => index.set(key, value).ok().map(|_| ()),
        };
        // println!("{} {}", _i, key);
    }

    let mut n_deleted = dels;
    while n_deleted > 0 {
        let key: u16 = rng.gen();
        match index.get(&key) {
            Ok(entry) if !entry.is_deleted() => {
                n_deleted -= 1;
                match diff {
                    true => index.delete(&key).unwrap(),
                    false => index.remove(&key).unwrap(),
                };
            }
            _ => (),
        }
    }

    assert_eq!(dels, index.deleted_count());

    index
}

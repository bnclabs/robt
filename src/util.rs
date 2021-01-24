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
pub fn load_index(seed: u128, diff: bool, inserts: u64, dels: u64) -> Mdb<u16, u64, u64> {
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let index = Mdb::new("testing");

    let (mut i, mut d) = (inserts, dels);
    while (i + d) > 0 {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        // println!("{} {}", (i + d), key);
        match rng.gen::<u64>() % (i + d) {
            k if k < i && diff => {
                index.insert(key, value).ok();
                i -= 1;
            }
            k if k < i => {
                index.set(key, value).ok();
                i -= 1;
            }
            _ => match index.get(&key) {
                Ok(entry) if !entry.is_deleted() && diff => {
                    index.delete(&key).unwrap();
                    d -= 1;
                }
                Ok(entry) if !entry.is_deleted() => {
                    index.remove(&key).unwrap();
                    d -= 1;
                }
                _ => (),
            },
        }
    }

    if diff {
        let n_deleted: u64 = index
            .iter()
            .unwrap()
            .map(|e| {
                e.to_values()
                    .into_iter()
                    .filter(|v| v.is_deleted())
                    .map(|_| 1_u64)
                    .sum::<u64>()
            })
            .sum();
        assert_eq!(n_deleted, dels);
    }

    index
}

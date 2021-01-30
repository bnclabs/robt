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
pub fn load_index(
    seed: u128,
    sets: u64,
    inserts: u64,
    rems: u64,
    dels: u64,
    seqno: Option<u64>,
) -> Mdb<u16, u64, u64> {
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let index = Mdb::new("testing");
    seqno.map(|seqno| index.set_seqno(seqno));

    let (mut s, mut i, mut d, mut r) = (sets, inserts, dels, rems);
    while (s + i + d + r) > 0 {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        // println!("{} {}", (s + i + d + r), key);
        match rng.gen::<u64>() % (s + i + d + r) {
            k if k < s => {
                index.set(key, value).ok();
                s -= 1;
            }
            k if k < (s + i) => {
                index.insert(key, value).ok();
                i -= 1;
            }
            k => match index.get(&key) {
                Ok(entry) if !entry.is_deleted() && (k < (s + i + d)) => {
                    index.delete(&key).unwrap();
                    d -= 1;
                }
                Ok(entry) if !entry.is_deleted() => {
                    index.remove(&key).unwrap();
                    r -= 1;
                }
                _ => (),
            },
        }
    }

    index
}

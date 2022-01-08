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
use ppom::mdb::OMap;

#[cfg(test)]
pub fn load_index(
    seed: u128,
    sets: u64,
    inserts: u64,
    rems: u64,
    dels: u64,
    seqno: Option<u64>,
) -> OMap<u16, u64> {
    use rand::{rngs::StdRnd, Rng, SeedableRng};

    let mut rng = StdRnd::from_seed(seed.to_le_bytes());
    let index = OMap::new("testing");
    seqno.map(|seqno| index.set_seqno(seqno));

    let (mut se, mut it, mut ds, mut rs) = (sets, inserts, dels, rems);
    while (se + it + ds + rs) > 0 {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        // println!("{} seqno:{} {}", (se + it + ds + rs), key, index.to_seqno() + 1,);
        match rng.gen::<u64>() % (se + it + ds + rs) {
            k if k < se => {
                index.set(key, value).ok();
                se -= 1;
            }
            k if k < (se + it) => {
                index.insert(key, value).ok();
                it -= 1;
            }
            k => match index.get(&key) {
                Ok(entry) if !entry.is_deleted() && (k < (se + it + ds)) => {
                    index.delete(&key).unwrap();
                    ds -= 1;
                }
                Ok(entry) if !entry.is_deleted() => {
                    index.remove(&key).unwrap();
                    rs -= 1;
                }
                _ => (),
            },
        }
    }

    index
}

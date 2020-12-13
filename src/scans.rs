use mkit::traits::{Bloom, Entry};

use std::{cmp, convert::TryFrom, hash, marker, time};

use crate::{Error, Result};

/// Iterator wrapper, to wrap full-table scanners and generate bitmap index.
///
/// Computes a bitmap of all keys iterated over the index `I`. Bitmap type
/// is parameterised as `B`.
pub struct BitmappedScan<K, V, B, I, E>
where
    K: hash::Hash,
    B: Bloom,
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    iter: I,
    bitmap: B,
    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B, I, E> BitmappedScan<K, V, B, I, E>
where
    K: hash::Hash,
    B: Bloom,
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    pub fn new(iter: I) -> BitmappedScan<K, V, B, I, E> {
        BitmappedScan {
            iter,
            bitmap: <B as Bloom>::create(),
            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn unwrap(self) -> Result<(I, B)> {
        Ok((self.iter, self.bitmap))
    }
}

impl<K, V, B, I, E> Iterator for BitmappedScan<K, V, B, I, E>
where
    K: hash::Hash,
    B: Bloom,
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    type Item = Result<E>;

    #[inline]
    fn next(&mut self) -> Option<Result<E>> {
        match self.iter.next()? {
            Ok(entry) => {
                self.bitmap.add_key(entry.as_key());
                Some(Ok(entry))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

/// Iterator wrapper, to wrap full-table scanners and count seqno,
/// index-items, deleted items and epoch.
pub struct BuildScan<K, V, I, E>
where
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    iter: I,

    start: time::SystemTime,
    seqno: u64,
    n_count: u64,
    n_deleted: usize,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I, E> BuildScan<K, V, I, E>
where
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    fn new(iter: I, seqno: u64) -> BuildScan<K, V, I, E> {
        BuildScan {
            iter,

            start: time::SystemTime::now(),
            seqno,
            n_count: Default::default(),
            n_deleted: Default::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    fn unwrap(self) -> Result<(u64, u64, u64, u64, u64, I)> {
        let build_time = {
            let elapsed = err_at!(Fatal, self.start.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };
        let epoch = {
            let elapsed = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };
        Ok((
            build_time,
            self.seqno,
            self.n_count,
            u64::try_from(self.n_deleted).unwrap(),
            epoch,
            self.iter,
        ))
    }
}

impl<K, V, I, E> Iterator for BuildScan<K, V, I, E>
where
    I: Iterator<Item = Result<E>>,
    E: Entry<K, V>,
{
    type Item = Result<E>;

    #[inline]
    fn next(&mut self) -> Option<Result<E>> {
        match self.iter.next()? {
            Ok(entry) => {
                self.seqno = cmp::max(self.seqno, entry.to_seqno());
                self.n_count += 1;
                if entry.is_deleted() {
                    self.n_deleted += 1;
                }
                Some(Ok(entry))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

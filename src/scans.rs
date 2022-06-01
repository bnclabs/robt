use mkit::{
    self,
    db::{self, Bloom},
};

use std::{cmp, convert::TryFrom, hash, marker, time};

use crate::{Error, Result};

// Iterator wrapper, to wrap full-table scanners and count seqno,
// index-items, deleted items and epoch.
pub struct BuildScan<K, V, D, I> {
    iter: I,
    entry: Option<db::Entry<K, V, D>>,

    start: time::SystemTime,
    seqno: u64,
    n_count: u64,
    n_deleted: u64,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, D, I> BuildScan<K, V, D, I> {
    pub fn new(iter: I, seqno: u64) -> BuildScan<K, V, D, I> {
        BuildScan {
            iter,
            entry: None,

            start: time::SystemTime::now(),
            seqno,
            n_count: u64::default(),
            n_deleted: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn push(&mut self, entry: db::Entry<K, V, D>) {
        self.entry = match &self.entry {
            None => Some(entry),
            Some(_) => unreachable!(),
        }
    }

    // return (build_time, seqno, count, deleted, epoch, iter)
    pub fn unwrap(self) -> Result<(u64, u64, u64, u64, u64, I)> {
        let build_time = {
            let elapsed = err_at!(Fatal, self.start.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };
        let epoch = {
            let elapsed = err_at!(Fatal, time::UNIX_EPOCH.elapsed())?;
            err_at!(FailConvert, u64::try_from(elapsed.as_nanos()))?
        };
        Ok((build_time, self.seqno, self.n_count, self.n_deleted, epoch, self.iter))
    }
}

impl<K, V, D, I> Iterator for BuildScan<K, V, D, I>
where
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = db::Entry<K, V, D>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.entry.take() {
            Some(entry) => Some(entry),
            None => {
                let entry = self.iter.next()?;
                self.seqno = cmp::max(self.seqno, entry.to_seqno());
                self.n_count += 1;
                if entry.is_deleted() {
                    self.n_deleted += 1;
                }
                Some(entry)
            }
        }
    }
}

// Iterator wrapper, to wrap full-table scanners and generate bitmap index.
//
// Computes a bitmap of all keys iterated over the index `I`. Bitmap type
// is parameterised as `B`.
pub struct BitmappedScan<K, V, D, B, I> {
    iter: I,
    bitmap: B,
    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
    _dff: marker::PhantomData<D>,
}

impl<K, V, D, B, I> BitmappedScan<K, V, D, B, I>
where
    B: Bloom,
{
    pub fn new(iter: I, bitmap: B) -> BitmappedScan<K, V, D, B, I> {
        BitmappedScan {
            iter,
            bitmap,
            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
        }
    }

    pub fn unwrap(self) -> Result<(B, I)> {
        Ok((self.bitmap, self.iter))
    }
}

impl<K, V, D, B, I> Iterator for BitmappedScan<K, V, D, B, I>
where
    K: hash::Hash,
    B: Bloom,
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = db::Entry<K, V, D>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.iter.next()?;
        self.bitmap.add_key(&entry.key);
        Some(entry)
    }
}

// Iterator type, for continuous full table iteration filtering out
// older mutations.
pub struct CompactScan<K, V, D, I> {
    iter: I,
    cutoff: db::Cutoff,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
    _dff: marker::PhantomData<D>,
}

impl<K, V, D, I> CompactScan<K, V, D, I> {
    pub fn new(iter: I, cutoff: db::Cutoff) -> Self {
        CompactScan {
            iter,
            cutoff,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
        }
    }
}

impl<K, V, D, I> Iterator for CompactScan<K, V, D, I>
where
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = db::Entry<K, V, D>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.iter.next()?.purge(self.cutoff) {
                break Some(entry);
            }
        }
    }
}

#[cfg(test)]
#[path = "scans_test.rs"]
mod scans_test;

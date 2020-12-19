use mkit::{
    self,
    cbor::{FromCbor, IntoCbor},
    db,
    traits::{Bloom, Diff},
};

use std::{cmp, convert::TryFrom, hash, marker, time};

use crate::{Error, Result};

/// Iterator wrapper, to wrap full-table scanners and generate bitmap index.
///
/// Computes a bitmap of all keys iterated over the index `I`. Bitmap type
/// is parameterised as `B`.
pub struct BitmappedScan<K, V, B, I>
where
    K: hash::Hash,
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    B: Bloom,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    iter: I,
    bitmap: B,
    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B, I> BitmappedScan<K, V, B, I>
where
    K: hash::Hash,
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    B: Bloom,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    pub fn new(iter: I) -> BitmappedScan<K, V, B, I> {
        BitmappedScan {
            iter,
            bitmap: <B as Bloom>::create(),
            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn unwrap(self) -> Result<(B, I)> {
        Ok((self.bitmap, self.iter))
    }
}

impl<K, V, B, I> Iterator for BitmappedScan<K, V, B, I>
where
    K: hash::Hash,
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    B: Bloom,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    type Item = Result<db::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next()? {
            Ok(entry) => {
                self.bitmap.add_key(&entry.key);
                Some(Ok(entry))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

/// Iterator wrapper, to wrap full-table scanners and count seqno,
/// index-items, deleted items and epoch.
pub struct BuildScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    iter: I,
    entry: Option<db::Entry<K, V>>,

    start: time::SystemTime,
    seqno: u64,
    n_count: u64,
    n_deleted: usize,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I> BuildScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    pub fn new(iter: I, seqno: u64) -> BuildScan<K, V, I> {
        BuildScan {
            iter,
            entry: None,

            start: time::SystemTime::now(),
            seqno,
            n_count: Default::default(),
            n_deleted: Default::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn push(&mut self, entry: db::Entry<K, V>) {
        self.entry = Some(entry);
    }

    pub fn unwrap(self) -> Result<(u64, u64, u64, u64, u64, I)> {
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

impl<K, V, I> Iterator for BuildScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    type Item = Result<db::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.entry.take() {
            Some(entry) => Some(Ok(entry)),
            None => match self.iter.next()? {
                Ok(entry) => {
                    self.seqno = cmp::max(self.seqno, entry.to_seqno());
                    self.n_count += 1;
                    if entry.is_deleted() {
                        self.n_deleted += 1;
                    }
                    Some(Ok(entry))
                }
                Err(err) => Some(Err(err)),
            },
        }
    }
}

/// Iterator type, for continuous full table iteration filtering out
/// older mutations.
pub struct CompactScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    iter: I,
    cutoff: db::Cutoff,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, I> CompactScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    pub fn new(iter: I, cutoff: db::Cutoff) -> CompactScan<K, V, I> {
        CompactScan {
            iter,
            cutoff,
            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    pub fn unwrap(self) -> I {
        self.iter
    }
}

impl<K, V, I> Iterator for CompactScan<K, V, I>
where
    V: Diff,
    <V as Diff>::D: FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V>>>,
{
    type Item = Result<db::Entry<K, V>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(entry)) => match entry.purge(self.cutoff) {
                    Some(entry) => break Some(Ok(entry)),
                    None => (),
                },
                Some(Err(err)) => break Some(Err(err)),
                None => break None,
            }
        }
    }
}

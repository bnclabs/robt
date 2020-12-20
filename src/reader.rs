use mkit::{cbor::FromCbor, db};

use std::{
    borrow::Borrow,
    fs, io, marker,
    ops::{Bound, RangeBounds},
};

use crate::{entry::Entry, robt::Stats, util, Error, Result};

struct Reader<K, V, D> {
    m_blocksize: usize,
    z_blocksize: usize,
    v_blocksize: usize,

    index: fs::File,
    vlog: Option<fs::File>,
    entries: Vec<Entry<K, V, D>>,
}

impl<K, V, D> Reader<K, V, D>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    pub fn from_root(
        stats: &Stats,
        root: Vec<u8>,
        index: fs::File,
        vlog: Option<fs::File>,
    ) -> Result<Self> {
        let (entries, _) = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&root)?;
        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            v_blocksize: stats.v_blocksize,
            index,
            vlog,
            entries,
        })
    }

    pub fn find<Q>(&mut self, key: &Q) -> Result<Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let fd = &mut self.index;

        let mut entries = self.entries.clone();
        loop {
            let off = match entries.binary_search_by(|e| e.borrow_key().cmp(key)) {
                Ok(off) => off,
                Err(off) => off.saturating_sub(1),
            };
            match &entries[off] {
                Entry::MM { fpos, .. } => {
                    entries = self.read_block(*fpos)?;
                }
                Entry::MZ { fpos, .. } => {
                    let entries = self.read_block(*fpos)?;
                    match entries.binary_search_by(|x| x.borrow_key().cmp(key)) {
                        Ok(off) => break Ok(entries[off].clone()),
                        Err(off) => break err_at!(KeyNotFound, msg: "missing key"),
                    };
                }
                Entry::ZZ { .. } => unreachable!(),
            }
        }
    }

    fn read_block(&mut self, fpos: u64) -> Result<Vec<Entry<K, V, D>>> {
        let fpos = io::SeekFrom::Start(fpos);
        let block = read_file!(&mut self.index, fpos, self.m_blocksize, "read block")?;
        Ok(util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0)
    }
}

pub struct IterTill<'a, K, V, D, Q, R>
where
    K: Borrow<Q>,
    Q: Ord,
    R: RangeBounds<Q>,
{
    range: R,
    iter: Iter<'a, K, V, D>,
    fin: bool,

    _key: marker::PhantomData<Q>,
}

impl<'a, K, V, D, Q, R> IterTill<'a, K, V, D, Q, R>
where
    K: Borrow<Q>,
    Q: Ord,
    R: RangeBounds<Q>,
{
    fn new(range: R, iter: Iter<'a, K, V, D>) -> Self {
        IterTill {
            range,
            iter,
            fin: false,

            _key: marker::PhantomData,
        }
    }
}

impl<'a, K, V, D, Q, R> Iterator for IterTill<'a, K, V, D, Q, R>
where
    K: Borrow<Q> + FromCbor,
    V: FromCbor,
    D: FromCbor,
    Q: Ord,
    R: RangeBounds<Q>,
{
    type Item = Result<db::Entry<K, V, D>>;

    fn next(&mut self) -> Option<Result<db::Entry<K, V, D>>> {
        if self.fin {
            return None;
        }

        let e = try_result!(self.iter.next()?);
        let key: &Q = e.borrow_key();

        match self.range.end_bound() {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(till) if self.iter.reverse && key.ge(till) => Some(Ok(e)),
            Bound::Excluded(till) if self.iter.reverse && key.gt(till) => Some(Ok(e)),
            Bound::Included(till) if key.le(till) => Some(Ok(e)),
            Bound::Excluded(till) if key.lt(till) => Some(Ok(e)),
            _ => {
                self.fin = true;
                None
            }
        }
    }
}

pub struct Iter<'a, K, V, D> {
    reader: &'a mut Reader<K, V, D>,
    stack: Vec<Vec<Entry<K, V, D>>>,
    reverse: bool,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
    _dff: marker::PhantomData<D>,
}

impl<'a, K, V, D> Iter<'a, K, V, D> {
    fn new(
        reader: &'a mut Reader<K, V, D>,
        stack: Vec<Vec<Entry<K, V, D>>>,
        reverse: bool,
    ) -> Self {
        Iter {
            reader,
            stack,
            reverse,
            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
        }
    }
}

impl<'a, K, V, D> Iter<'a, K, V, D>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    fn next_fwd(&mut self) -> Option<Result<db::Entry<K, V, D>>> {
        match self.stack.last_mut() {
            Some(es) if es.len() > 0 => match es.remove(0) {
                entry @ Entry::ZZ { .. } => Some(Ok(entry.into())),
                Entry::MZ { key, fpos } => {
                    self.stack.push(try_result!(self.reader.read_block(fpos)));
                    self.next_fwd()
                }
                Entry::MM { key, fpos } => {
                    self.stack.push(try_result!(self.reader.read_block(fpos)));
                    self.next_fwd()
                }
            },
            Some(es) => {
                self.stack.pop();
                self.next_fwd()
            }
            None => None,
        }
    }

    fn next_rwd(&mut self) -> Option<Result<db::Entry<K, V, D>>> {
        match self.stack.last_mut() {
            Some(es) => match es.pop() {
                Some(entry @ Entry::ZZ { .. }) => Some(Ok(entry.into())),
                Some(Entry::MZ { key, fpos }) => {
                    self.stack.push(try_result!(self.reader.read_block(fpos)));
                    self.next_rwd()
                }
                Some(Entry::MM { key, fpos }) => {
                    self.stack.push(try_result!(self.reader.read_block(fpos)));
                    self.next_rwd()
                }
                None => {
                    self.stack.pop();
                    self.next_rwd()
                }
            },
            None => None,
        }
    }
}

impl<'a, K, V, D> Iterator for Iter<'a, K, V, D>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    type Item = Result<db::Entry<K, V, D>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reverse {
            false => self.next_fwd(),
            true => self.next_rwd(),
        }
    }
}

use mkit::{cbor::FromCbor, db};

use std::{
    borrow::Borrow,
    fs, io, marker,
    ops::{Bound, RangeBounds},
};

use crate::{config::Stats, entry::Entry, util, Error, Result};

pub struct Reader<K, V, D> {
    m_blocksize: usize,
    z_blocksize: usize,
    v_blocksize: usize,
    root: Vec<Entry<K, V, D>>,

    index: fs::File,
    vlog: Option<fs::File>,
}

impl<K, V, D> Reader<K, V, D>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    pub fn from_root(
        root: u64,
        stats: &Stats,
        mut index: fs::File,
        vlog: Option<fs::File>,
    ) -> Result<Self> {
        let root: Vec<Entry<K, V, D>> = {
            let fpos = io::SeekFrom::Start(root);
            let block = read_file!(&mut index, fpos, stats.m_blocksize, "read block")?;
            util::from_cbor_bytes(&block)?.0
        };

        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            v_blocksize: stats.v_blocksize,
            root,

            index,
            vlog,
        })
    }

    pub fn get<Q>(&mut self, ukey: &Q) -> Result<Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone + FromCbor,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let fd = &mut self.index;

        let mut es = self.root.clone();
        loop {
            let off = match es.binary_search_by(|e| e.borrow_key().cmp(ukey)) {
                Ok(off) => off,
                Err(off) if off == 0 => break err_at!(KeyNotFound, msg: "missing key"),
                Err(off) => off - 1,
            };
            es = match &es[off] {
                Entry::MM { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                Entry::MZ { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mz-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                e @ Entry::ZZ { .. } if e.borrow_key() == ukey => break Ok(e.clone()),
                _ => break err_at!(KeyNotFound, msg: "missing key"),
            }
        }
    }

    fn cmp_skey(key: &K, skey: Bound<&Q>, z: bool) -> cmp::Ordering {
        match skey {
            Bound::Unbounded => cmp::Ording::Less,
            Bound::Included(skey) => key.borrow().cmp(skey),
            Bound::Excluded(skey) if z => match key.borrow().cmp(skey) {
                cmp::Ordering::Equal => cmp::Ordering::Less,
                c => c,
            }
            Bound::Excluded(skey) => key.borrow().cmp(skey),
        }
    }

    pub fn iter<Q, R>(&mut self, r: R) -> IterTill<K, V, D, Q, R>
    where
        K: Clone + FromCbor + Borrow<Q>,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        R: RangeBounds<Q>,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let fd = &mut self.index;
        let mut stack = vec![];

        let skey = r.start_bound();
        let mut es = self.root.clone();
        let stack = loop {
            let z = match es.first().map(|e| e.is_zblock()) {
                Some(z) => z,
                None => break stack,
            };
            let off = match es.binary_search_by(|e| Self::cmp_skey(e.as_key(), skey, z)) {
                Ok(off) => off,
                Err(off) if off == es.len() => break stack,
                Err(off) => off.saturating_sub(1),
            };
            let rem = es[off..].to_vec();
            es = match rem.remove(0) {
                Entry::MM { fpos, .. } => {
                    stack.push(rem)
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                Entry::MZ { fpos, .. } => {
                    stack.push(rem)
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mz-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                e = Entry::ZZ { key, .. } {
                    stack.push(rem);
                    break stack
                }
                None => break stack
            }
       };

       let iter = Iter::new_fwd(self, stack);
       Ok(IterTill::new(iter, RangeFull))
    }

    //pub fn iter_from<Q>(&mut self, ukey: Bound<&Q>) -> Result<Entry<K, V, D>>
    //where
    //    K: Clone + Borrow<Q>,
    //    V: Clone,
    //    D: Clone + FromCbor,
    //    Q: Ord,
    //{
    //    let m_blocksize = self.m_blocksize;
    //    let fd = &mut self.index;
    //    let mut stack = vec![];

    //    let mut es = self.root.clone();
    //    let stack = loop {
    //        let (entry, rem) = match es.binary_search_by(|e| e.borrow_key().cmp(ukey)) {
    //            Ok(off) => (es.remove(0), es[off..].to_vec()),
    //            Err(off) if off == es.len() => break stack,
    //            Err(off) => (es.remove(0), es[off..].to_vec()),
    //        };

    //        stack.push(rem);

    //        es = match entry {
    //            Entry::MM { fpos, .. } => {
    //                let fpos = io::SeekFrom::Start(*fpos);
    //                let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
    //                util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
    //            }
    //            Entry::MZ { fpos, .. } => {
    //                let fpos = io::SeekFrom::Start(*fpos);
    //                let block = read_file!(fd, fpos, m_blocksize, "read mz-block")?;
    //                util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
    //            }
    //            e @ Entry::ZZ { .. } if e.borrow_key() == ukey => break Ok(e.clone()),
    //            _ => break err_at!(KeyNotFound, msg: "missing key"),
    //        }
    //    }
    //}
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
    fn new(iter: Iter<'a, K, V, D>, r: R) -> Self {
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

        let e = iter_result!(self.iter.next()?);
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
}

impl<'a, K, V, D> Iter<'a, K, V, D> {
    fn new_fwd(r: &'a mut Reader<K, V, D>, stack: Vec<Vec<Entry<K, V, D>>>) -> Self {
        Iter {
            reader: r,
            stack,
            reverse: false,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    fn new_rwd(r: &'a mut Reader<K, V, D>, mut stack: Vec<Vec<Entry<K, V, D>>>) -> Self {
        stack.iter_mut().map(|x| x.reverse());
        Iter {
            reader: r,
            stack,
            reverse: true,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
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
        let fd = &mut self.reader.index;
        let m_blocksize = self.reader.m_blocksize;

        match self.stack.pop() {
            Some(mut es) => match es.pop() {
                Some(entry @ Entry::ZZ { .. }) => {
                    self.stack.push(es);
                    Some(Ok(entry.into()))
                }
                Some(Entry::MM { fpos, .. }) | Some(Entry::MZ { fpos, .. }) => {
                    self.stack.push(es);

                    let entries = iter_result!(|| -> Result<Vec<Entry<K, V, D>>> {
                        let fpos = io::SeekFrom::Start(fpos);
                        let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                        Ok(util::from_cbor_bytes(&block)?.0)
                    }());
                    self.stack.push(entries);
                    self.next()
                }
                None => self.next(),
            },
            None => None,
        }
    }
}

pub fn read_entries<K, V, D>(
    fd: &mut fs::File,
    fpos: u64,
    n: usize,
) -> Result<Vec<Entry<K, V, D>>>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    let block = read_file!(fd, io::SeekFrom::Start(fpos), n, "read block")?;
    Ok(util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0)
}

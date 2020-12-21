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

    index: fs::File,
    vlog: Option<fs::File>,
    root: Vec<Entry<K, V, D>>,
}

impl<K, V, D> Reader<K, V, D>
where
    K: FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    pub fn from_root(
        root: Vec<u8>,
        stats: &Stats,
        index: fs::File,
        vlog: Option<fs::File>,
    ) -> Result<Self> {
        let (root, _) = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&root)?;
        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            v_blocksize: stats.v_blocksize,
            index,
            vlog,
            root,
        })
    }

    pub fn find<Q>(&mut self, ky: &Q) -> Result<Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let fd = &mut self.index;

        let mut es = self.root.clone();
        loop {
            let off = match es.binary_search_by(|e| e.borrow_key().cmp(ky)) {
                Ok(off) => off,
                Err(off) if off == 0 => break err_at!(KeyNotFound, msg: "missing key"),
                Err(off) => off - 1,
            };
            es = match &es[off] {
                Entry::MM { fpos, .. } => read_entries(fd, *fpos, m_blocksize)?,
                Entry::MZ { fpos, .. } => read_entries(fd, *fpos, m_blocksize)?,
                e @ Entry::ZZ { .. } if e.borrow_key() == ky => break Ok(e.clone()),
                _ => break err_at!(KeyNotFound, msg: "missing key"),
            }
        }
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
    _dff: marker::PhantomData<D>,
}

impl<'a, K, V, D> Iter<'a, K, V, D> {
    fn new_fwd(r: &'a mut Reader<K, V, D>, stack: Vec<Vec<Entry<K, V, D>>>) -> Self {
        Iter {
            reader: r,
            stack,
            reverse: false,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
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
            _dff: marker::PhantomData,
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
                    let x = iter_result!(read_entries(fd, fpos, m_blocksize));
                    self.stack.push(x);
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

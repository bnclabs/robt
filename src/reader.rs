use fs2::FileExt;
use log::error;
use mkit::{cbor::FromCbor, db};

use std::{
    borrow::Borrow,
    cmp, fmt, fs, io, marker,
    ops::{Bound, RangeBounds},
};

use crate::{config::Stats, entry::Entry, util, Error, Result};

pub struct Reader<K, V, D> {
    pub m_blocksize: usize,
    pub z_blocksize: usize,
    pub root: Vec<Entry<K, V, D>>,

    pub index: fs::File,
    pub vlog: Option<fs::File>,
}

impl<K, V, D> Drop for Reader<K, V, D> {
    fn drop(&mut self) {
        if let Err(err) = self.index.unlock() {
            error!( target: "robt", "fail to unlock reader lock for index: {}", err)
        }
        if let Some(vlog) = self.vlog.as_ref() {
            if let Err(err) = vlog.unlock() {
                error!(target: "robt", "fail to unlock reader lock for vlog: {}", err)
            }
        }
    }
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

        err_at!(IOError, index.lock_shared())?;
        if let Some(vlog) = vlog.as_ref() {
            err_at!(IOError, vlog.lock_shared())?
        }

        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            root,

            index,
            vlog,
        })
    }

    pub fn get<Q>(&mut self, ukey: &Q, versions: bool) -> Result<Entry<K, V, D>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
    {
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;
        let fd = &mut self.index;

        let mut es = self.root.clone();
        loop {
            let off = match es.binary_search_by(|e| e.borrow_key().cmp(ukey)) {
                Ok(off) => off,
                Err(off) if off == 0 => break err_at!(KeyNotFound, msg: "missing key"),
                Err(off) => off - 1,
            };
            es = match es[off].clone() {
                Entry::MM { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                Entry::MZ { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(fpos);
                    let block = read_file!(fd, fpos, z_blocksize, "read mz-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                Entry::ZZ { key, value, deltas } if key.borrow() == ukey => {
                    let deltas = if versions { deltas } else { Vec::default() };
                    let mut entry = Entry::ZZ { key, value, deltas };
                    let entry = match &mut self.vlog {
                        Some(fd) => entry.into_native(fd, versions)?,
                        None => {
                            entry.drain_deltas();
                            entry
                        }
                    };
                    break Ok(entry);
                }
                _ => break err_at!(KeyNotFound, msg: "missing key"),
            }
        }
    }

    pub fn iter<Q, R>(
        &mut self,
        range: R,
        reverse: bool,
        versions: bool,
    ) -> Result<Iter<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (stack, bound) = if reverse {
            let stack = self.rwd_stack(range.end_bound(), self.root.clone())?;
            let bound: Bound<K> = match range.start_bound() {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(q) => Bound::Included(q.to_owned()),
                Bound::Excluded(q) => Bound::Excluded(q.to_owned()),
            };
            (stack, bound)
        } else {
            let stack = self.fwd_stack(range.start_bound(), self.root.clone())?;
            let bound: Bound<K> = match range.end_bound() {
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(q) => Bound::Included(q.to_owned()),
                Bound::Excluded(q) => Bound::Excluded(q.to_owned()),
            };
            (stack, bound)
        };
        let mut iter = Iter::new(self, bound, stack, reverse, versions);

        while let Some(item) = iter.next() {
            match item {
                Ok(entry) if reverse => {
                    let key = entry.borrow_key();
                    match range.end_bound() {
                        Bound::Included(ekey) if key.gt(ekey) => (),
                        Bound::Excluded(ekey) if key.ge(ekey) => (),
                        _ => {
                            iter.push(entry);
                            break;
                        }
                    }
                }
                Ok(entry) => {
                    let key = entry.borrow_key();
                    match range.start_bound() {
                        Bound::Included(skey) if key.lt(skey) => (),
                        Bound::Excluded(skey) if key.le(skey) => (),
                        _ => {
                            iter.push(entry);
                            break;
                        }
                    }
                }
                Err(err) => return Err(err),
            }
        }

        Ok(iter)
    }

    fn fwd_stack<Q>(
        &mut self,
        sk: Bound<&Q>,
        block: Vec<Entry<K, V, D>>,
    ) -> Result<Vec<Vec<Entry<K, V, D>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
    {
        let (entry, rem) = match block.first().map(|e| e.is_zblock()) {
            Some(false) => match block.binary_search_by(|e| fcmp(e.borrow_key(), sk)) {
                Ok(off) => (block[off].clone(), block[off + 1..].to_vec()),
                Err(off) => {
                    let off = off.saturating_sub(1);
                    (block[off].clone(), block[off + 1..].to_vec())
                }
            },
            Some(true) => match block.binary_search_by(|e| fcmp(e.borrow_key(), sk)) {
                Ok(off) | Err(off) => {
                    return Ok(vec![block[off..].to_vec()]);
                }
            },
            None => return Ok(vec![]),
        };

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.m_blocksize;

        let block = match entry {
            Entry::MM { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), m_blocksize, "read mm-block")?
            }
            Entry::MZ { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };

        let block = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
        let mut stack = self.fwd_stack(sk, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    fn rwd_stack<Q>(
        &mut self,
        ek: Bound<&Q>,
        block: Vec<Entry<K, V, D>>,
    ) -> Result<Vec<Vec<Entry<K, V, D>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
    {
        let (entry, mut rem) = match block.first().map(|e| e.is_zblock()) {
            Some(false) => match block.binary_search_by(|e| rcmp(e.borrow_key(), ek)) {
                Ok(off) => (block[off].clone(), block[..off].to_vec()),
                Err(off) => {
                    let off = off.saturating_sub(1);
                    (block[off].clone(), block[..off].to_vec())
                }
            },
            Some(true) => match block.binary_search_by(|e| rcmp(e.borrow_key(), ek)) {
                Ok(off) | Err(off) => {
                    let off = cmp::min(off + 1, block.len());
                    let mut rem = block[..off].to_vec();
                    rem.reverse();
                    return Ok(vec![rem]);
                }
            },
            None => return Ok(vec![]),
        };
        rem.reverse();

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;

        let block = match entry {
            Entry::MM { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), m_blocksize, "read mm-block")?
            }
            Entry::MZ { fpos, .. } => {
                read_file!(fd, io::SeekFrom::Start(fpos), z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };

        let block = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
        let mut stack = self.rwd_stack(ek, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    pub fn print(&mut self) -> Result<()>
    where
        K: Clone + fmt::Debug + FromCbor,
        V: Clone + fmt::Debug + FromCbor,
        D: Clone + fmt::Debug + FromCbor,
    {
        for entry in self.root.clone().into_iter() {
            entry.print("", self)?;
        }
        Ok(())
    }
}

pub struct Iter<'a, K, V, D> {
    reader: &'a mut Reader<K, V, D>,
    stack: Vec<Vec<Entry<K, V, D>>>,
    reverse: bool,
    versions: bool,
    entry: Option<db::Entry<K, V, D>>,
    bound: Bound<K>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<'a, K, V, D> Iter<'a, K, V, D> {
    fn new(
        r: &'a mut Reader<K, V, D>,
        bound: Bound<K>,
        stack: Vec<Vec<Entry<K, V, D>>>,
        reverse: bool,
        versions: bool,
    ) -> Self {
        Iter {
            reader: r,
            stack,
            reverse,
            versions,
            entry: None,
            bound,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    fn push(&mut self, entry: db::Entry<K, V, D>) {
        self.entry = Some(entry);
    }

    fn till(&mut self, e: db::Entry<K, V, D>) -> Option<Result<db::Entry<K, V, D>>>
    where
        K: Ord,
    {
        let key = &e.key;

        if self.reverse {
            match &self.bound {
                Bound::Unbounded => Some(Ok(e)),
                Bound::Included(till) if key.ge(till) => Some(Ok(e)),
                Bound::Excluded(till) if key.gt(till) => Some(Ok(e)),
                _ => {
                    self.stack.drain(..);
                    None
                }
            }
        } else {
            match &self.bound {
                Bound::Unbounded => Some(Ok(e)),
                Bound::Included(till) if key.le(till) => Some(Ok(e)),
                Bound::Excluded(till) if key.lt(till) => Some(Ok(e)),
                _ => {
                    self.stack.drain(..);
                    None
                }
            }
        }
    }

    fn fetchzz(&mut self, mut entry: Entry<K, V, D>) -> Result<Entry<K, V, D>>
    where
        V: FromCbor,
        D: FromCbor,
    {
        match &mut self.reader.vlog {
            Some(fd) if self.versions => entry.into_native(fd, self.versions),
            Some(fd) => {
                entry.drain_deltas();
                entry.into_native(fd, self.versions)
            }
            None => {
                entry.drain_deltas();
                Ok(entry)
            }
        }
    }
}

impl<'a, K, V, D> Iterator for Iter<'a, K, V, D>
where
    K: Ord + FromCbor,
    V: FromCbor,
    D: FromCbor,
{
    type Item = Result<db::Entry<K, V, D>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entry.take() {
            Some(entry) => return Some(Ok(entry)),
            None => (),
        }

        let fd = &mut self.reader.index;
        let m_blocksize = self.reader.m_blocksize;

        match self.stack.pop() {
            Some(block) if block.len() == 0 => self.next(),
            Some(mut block) => match block.remove(0) {
                entry @ Entry::ZZ { .. } => {
                    self.stack.push(block);
                    let entry = iter_result!(self.fetchzz(entry));
                    self.till(entry.into())
                }
                Entry::MM { fpos, .. } | Entry::MZ { fpos, .. } => {
                    self.stack.push(block);

                    let mut entries = iter_result!(|| -> Result<Vec<Entry<K, V, D>>> {
                        let fpos = io::SeekFrom::Start(fpos);
                        let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                        Ok(util::from_cbor_bytes(&block)?.0)
                    }());
                    if self.reverse {
                        entries.reverse();
                    }
                    self.stack.push(entries);
                    self.next()
                }
            },
            None => None,
        }
    }
}

fn fcmp<Q>(key: &Q, skey: Bound<&Q>) -> cmp::Ordering
where
    Q: Ord,
{
    match skey {
        Bound::Unbounded => cmp::Ordering::Greater,
        Bound::Included(skey) | Bound::Excluded(skey) => key.cmp(skey),
    }
}

fn rcmp<Q>(key: &Q, ekey: Bound<&Q>) -> cmp::Ordering
where
    Q: Ord,
{
    match ekey {
        Bound::Unbounded => cmp::Ordering::Less,
        Bound::Included(ekey) | Bound::Excluded(ekey) => key.cmp(ekey),
    }
}

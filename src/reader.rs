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

    pub fn get<Q>(&mut self, ukey: &Q) -> Result<Entry<K, V, D>>
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
            es = match &es[off] {
                Entry::MM { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, m_blocksize, "read mm-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                Entry::MZ { fpos, .. } => {
                    let fpos = io::SeekFrom::Start(*fpos);
                    let block = read_file!(fd, fpos, z_blocksize, "read mz-block")?;
                    util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
                }
                e @ Entry::ZZ { .. } if e.borrow_key() == ukey => break Ok(e.clone()),
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
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let end_bound: Bound<K> = match range.end_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(q) => Bound::Included(q.to_owned()),
            Bound::Excluded(q) => Bound::Excluded(q.to_owned()),
        };
        let stack = if reverse {
            self.iter_rwd(range, self.root.clone())?
        } else {
            self.iter_fwd(range, self.root.clone())?
        };
        let iter = Iter::new(self, end_bound, stack, reverse, versions);
        Ok(iter)
    }

    fn iter_fwd<Q, R>(
        &mut self,
        range: R,
        mut block: Vec<Entry<K, V, D>>,
    ) -> Result<Vec<Vec<Entry<K, V, D>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
        R: RangeBounds<Q>,
    {
        let sk = range.start_bound();
        let z = block.first().map(|e| e.is_zblock()).unwrap_or(false);

        let fr = block.binary_search_by(|e| Self::scmp(e.borrow_key(), sk, z));
        let (entry, rem) = match fr {
            Ok(off) if z => return Ok(vec![block[off..].to_vec()]),
            Err(off) if z => return Ok(vec![block[off..].to_vec()]),
            Ok(off) => (block.remove(off), block[off..].to_vec()),
            Err(off) if off >= block.len() => return Ok(vec![]),
            Err(off) => {
                let off = off.saturating_sub(1);
                (block.remove(off), block[off..].to_vec())
            }
        };

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.m_blocksize;

        let block = match entry {
            Entry::MM { fpos, .. } => {
                let fpos = io::SeekFrom::Start(fpos);
                read_file!(fd, fpos, m_blocksize, "read mm-block")?
            }
            Entry::MZ { fpos, .. } => {
                let fpos = io::SeekFrom::Start(fpos);
                read_file!(fd, fpos, z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };

        let block = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
        let mut stack = self.iter_fwd(range, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    fn iter_rwd<Q, R>(
        &mut self,
        range: R,
        mut block: Vec<Entry<K, V, D>>,
    ) -> Result<Vec<Vec<Entry<K, V, D>>>>
    where
        K: Clone + Borrow<Q>,
        V: Clone,
        D: Clone,
        Q: Ord,
        R: RangeBounds<Q>,
    {
        block.reverse();

        let sk = range.end_bound();
        let z = block.first().map(|e| e.is_zblock()).unwrap_or(false);

        let fr = block.binary_search_by(|e| Self::scmp(e.borrow_key(), sk, z));
        let (entry, rem) = match fr {
            Ok(off) if z => return Ok(vec![block[off..].to_vec()]),
            Err(off) if z => return Ok(vec![block[off..].to_vec()]),
            Ok(off) => (block.remove(off), block[off..].to_vec()),
            Err(off) if off >= block.len() => return Ok(vec![]),
            Err(off) => {
                let off = off.saturating_sub(1);
                (block.remove(off), block[off..].to_vec())
            }
        };

        let fd = &mut self.index;
        let m_blocksize = self.m_blocksize;
        let z_blocksize = self.z_blocksize;

        let block = match entry {
            Entry::MM { fpos, .. } => {
                let fpos = io::SeekFrom::Start(fpos);
                read_file!(fd, fpos, m_blocksize, "read mm-block")?
            }
            Entry::MZ { fpos, .. } => {
                let fpos = io::SeekFrom::Start(fpos);
                read_file!(fd, fpos, z_blocksize, "read mz-block")?
            }
            _ => unreachable!(),
        };

        let block = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0;
        let mut stack = self.iter_fwd(range, block)?;
        stack.insert(0, rem);
        Ok(stack)
    }

    fn scmp<Q>(key: &Q, skey: Bound<&Q>, z: bool) -> cmp::Ordering
    where
        Q: Ord,
    {
        match skey {
            Bound::Unbounded => cmp::Ordering::Greater,
            Bound::Included(skey) => key.cmp(skey),
            Bound::Excluded(skey) if z => match key.cmp(skey) {
                cmp::Ordering::Equal => cmp::Ordering::Less,
                c => c,
            },
            Bound::Excluded(skey) => key.cmp(skey),
        }
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
    end_bound: Bound<K>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<'a, K, V, D> Iter<'a, K, V, D> {
    fn new(
        r: &'a mut Reader<K, V, D>,
        end_bound: Bound<K>,
        stack: Vec<Vec<Entry<K, V, D>>>,
        reverse: bool,
        versions: bool,
    ) -> Self {
        Iter {
            reader: r,
            stack,
            reverse,
            versions,
            end_bound,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        }
    }

    fn till(&mut self, e: db::Entry<K, V, D>) -> Option<Result<db::Entry<K, V, D>>>
    where
        K: Ord,
    {
        let key = &e.key;

        match &self.end_bound {
            Bound::Unbounded => Some(Ok(e)),
            Bound::Included(till) if self.reverse && key.ge(till) => Some(Ok(e)),
            Bound::Excluded(till) if self.reverse && key.gt(till) => Some(Ok(e)),
            Bound::Included(till) if key.le(till) => Some(Ok(e)),
            Bound::Excluded(till) if key.lt(till) => Some(Ok(e)),
            _ => {
                self.stack.drain(..);
                None
            }
        }
    }

    fn fetchzz(&mut self, entry: Entry<K, V, D>) -> Result<Entry<K, V, D>>
    where
        V: FromCbor,
        D: FromCbor,
    {
        match &mut self.reader.vlog {
            Some(vlog) => entry.into_native(vlog, self.versions),
            None => Ok(entry),
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
        let fd = &mut self.reader.index;
        let m_blocksize = self.reader.m_blocksize;

        match self.stack.pop() {
            Some(mut es) => match es.pop() {
                Some(entry @ Entry::ZZ { .. }) => {
                    let entry = iter_result!(self.fetchzz(entry));
                    self.stack.push(es);
                    self.till(entry.into())
                }
                Some(Entry::MM { fpos, .. }) | Some(Entry::MZ { fpos, .. }) => {
                    self.stack.push(es);

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
                None => self.next(),
            },
            None => None,
        }
    }
}

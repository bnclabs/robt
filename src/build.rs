use mkit::{
    cbor::{self, Cbor, IntoCbor},
    db,
};

use std::{cell::RefCell, convert::TryFrom, rc::Rc};

use crate::{
    config::Config, entry::Entry, flush::Flusher, scans::BuildScan, util, Result,
};

pub struct BuildMM<K, V, D, I> {
    m_blocksize: usize,
    iflush: Rc<RefCell<Flusher>>,
    iter: Box<BuildIter<K, V, D, I>>,
    entry: Option<(K, u64)>,
}

impl<K, V, D, I> BuildMM<K, V, D, I> {
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        iter: BuildIter<K, V, D, I>,
    ) -> Self {
        BuildMM {
            m_blocksize: config.m_blocksize,
            iflush,
            iter: Box::new(iter),
            entry: None,
        }
    }
}

impl<K, V, D, I> Iterator for BuildMM<K, V, D, I>
where
    K: Clone + IntoCbor,
    V: Clone + IntoCbor,
    D: Clone + IntoCbor,
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut mblock = Vec::with_capacity(self.m_blocksize);
        let block_size = self.m_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;
        let mut curr_fpos = None;
        let mut n = 0;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut mblock));

        loop {
            let entry = {
                let entry = self.entry.take().map(|e| Some(Ok(e)));
                entry.unwrap_or_else(|| self.iter.next())
            };
            match entry {
                Some(Ok((key, fpos))) => {
                    curr_fpos = Some(fpos);
                    n += 1;

                    first_key.get_or_insert_with(|| key.clone());
                    let ibytes = {
                        let e = Entry::<K, V, D>::new_mm(key.clone(), fpos);
                        iter_result!(util::into_cbor_bytes(e))
                    };
                    if (mblock.len() + ibytes.len()) > block_size {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&ibytes);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        mblock.extend_from_slice(&brk);
        mblock.resize(self.m_blocksize, 0);

        if n > 1 {
            curr_fpos = Some(self.iflush.borrow().to_fpos().unwrap_or(0));
            iter_result!(self.iflush.borrow_mut().flush(mblock));
        }

        Some(Ok((first_key.unwrap(), curr_fpos.unwrap())))
    }
}

pub struct BuildMZ<K, V, D, I> {
    m_blocksize: usize,
    iflush: Rc<RefCell<Flusher>>,
    iter: BuildZZ<K, V, D, I>,
    entry: Option<(K, u64)>,
}

impl<K, V, D, I> BuildMZ<K, V, D, I> {
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        iter: BuildZZ<K, V, D, I>,
    ) -> Self {
        BuildMZ {
            m_blocksize: config.m_blocksize,
            iflush,
            iter,
            entry: None,
        }
    }
}

impl<K, V, D, I> Iterator for BuildMZ<K, V, D, I>
where
    K: Clone + IntoCbor,
    V: Clone + IntoCbor,
    D: Clone + IntoCbor,
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut mblock = Vec::with_capacity(self.m_blocksize);
        let block_size = self.m_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut mblock));

        loop {
            let entry = {
                let entry = self.entry.take().map(|e| Some(Ok(e)));
                entry.unwrap_or_else(|| self.iter.next())
            };
            match entry {
                Some(Ok((key, fpos))) => {
                    first_key.get_or_insert_with(|| key.clone());
                    let ibytes = {
                        let e = Entry::<K, V, D>::new_mz(key.clone(), fpos);
                        iter_result!(util::into_cbor_bytes(e))
                    };
                    if (mblock.len() + ibytes.len()) > block_size {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&ibytes);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        mblock.extend_from_slice(&brk);
        mblock.resize(self.m_blocksize, 0);

        let fpos = self.iflush.borrow().to_fpos().unwrap_or(0);

        iter_result!(self.iflush.borrow_mut().flush(mblock));
        Some(Ok((first_key.unwrap(), fpos)))
    }
}

pub struct BuildZZ<K, V, D, I> {
    z_blocksize: usize,
    v_blocksize: usize,
    value_in_vlog: bool,
    delta_ok: bool,
    iflush: Rc<RefCell<Flusher>>,
    vflush: Rc<RefCell<Flusher>>,
    iter: Rc<RefCell<BuildScan<K, V, D, I>>>,
}

impl<K, V, D, I> BuildZZ<K, V, D, I> {
    pub fn new(
        config: &Config,
        iflush: Rc<RefCell<Flusher>>,
        vflush: Rc<RefCell<Flusher>>,
        iter: Rc<RefCell<BuildScan<K, V, D, I>>>,
    ) -> Self {
        BuildZZ {
            z_blocksize: config.z_blocksize,
            v_blocksize: config.v_blocksize,
            value_in_vlog: config.value_in_vlog,
            delta_ok: config.delta_ok,
            iflush,
            vflush,
            iter,
        }
    }
}

impl<K, V, D, I> Iterator for BuildZZ<K, V, D, I>
where
    K: Clone + IntoCbor,
    V: Clone + IntoCbor,
    D: Clone + IntoCbor,
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut zblock = Vec::with_capacity(self.z_blocksize);
        let mut vblock = Vec::with_capacity(self.v_blocksize);
        let block_size = self.z_blocksize.saturating_sub(1);

        let mut first_key: Option<K> = None;

        iter_result!(Cbor::Major4(cbor::Info::Indefinite, vec![]).encode(&mut zblock));

        let mut iter = self.iter.borrow_mut();
        let mut vfpos = self.vflush.borrow().to_fpos().unwrap_or(0);

        loop {
            match iter.next() {
                Some(mut entry) => {
                    if !self.delta_ok {
                        entry.drain_deltas()
                    }
                    first_key.get_or_insert_with(|| entry.key.clone());
                    let (e, vbytes) = {
                        let e = Entry::<K, V, D>::from(entry.clone());
                        iter_result!(e.into_reference(vfpos, self.value_in_vlog))
                    };
                    let ibytes = iter_result!(util::into_cbor_bytes(e));

                    if (zblock.len() + ibytes.len()) > block_size {
                        iter.push(entry);
                        break;
                    }
                    zblock.extend_from_slice(&ibytes);
                    vblock.extend_from_slice(&vbytes);
                    vfpos += u64::try_from(vbytes.len()).unwrap();
                }
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        let brk = iter_result!(util::into_cbor_bytes(cbor::SimpleValue::Break));
        zblock.extend_from_slice(&brk);
        zblock.resize(self.z_blocksize, 0);

        let fpos = self.iflush.borrow().to_fpos().unwrap_or(0);

        iter_result!(self.vflush.borrow_mut().flush(vblock));
        iter_result!(self.iflush.borrow_mut().flush(zblock));
        Some(Ok((first_key.unwrap(), fpos)))
    }
}

pub enum BuildIter<K, V, D, I> {
    MM(BuildMM<K, V, D, I>),
    MZ(BuildMZ<K, V, D, I>),
}

impl<K, V, D, I> From<BuildMZ<K, V, D, I>> for BuildIter<K, V, D, I> {
    fn from(val: BuildMZ<K, V, D, I>) -> Self {
        BuildIter::MZ(val)
    }
}

impl<K, V, D, I> From<BuildMM<K, V, D, I>> for BuildIter<K, V, D, I> {
    fn from(val: BuildMM<K, V, D, I>) -> Self {
        BuildIter::MM(val)
    }
}

impl<K, V, D, I> Iterator for BuildIter<K, V, D, I>
where
    K: Clone + IntoCbor,
    V: Clone + IntoCbor,
    D: Clone + IntoCbor,
    I: Iterator<Item = db::Entry<K, V, D>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BuildIter::MM(iter) => iter.next(),
            BuildIter::MZ(iter) => iter.next(),
        }
    }
}

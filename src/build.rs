use mkit::{
    db,
    {cbor::FromCbor, cbor::IntoCbor},
};

use std::{cell::RefCell, hash::Hash, rc::Rc};

use crate::{entry::Entry, flush::Flusher, robt::Config, scans::BuildScan, util, Result};

macro_rules! try_value {
    ($val:expr) => {{
        match $val {
            Ok(val) => val,
            Err(err) => return Some(Err(err)),
        }
    }};
}

macro_rules! next_item {
    ($name:ident) => {
        match $name.entry.take() {
            Some(item) => Some(Ok(item)),
            None => $name.iter.next(),
        }
    };
}

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
    K: Clone + Hash + FromCbor + IntoCbor,
    V: Clone + FromCbor + IntoCbor,
    D: Clone + FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V, D>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut iflush = self.iflush.borrow_mut();

        let mut mblock = Vec::with_capacity(self.m_blocksize);

        let fpos = iflush.to_fpos().unwrap_or(0);
        let mut first_key: Option<K> = None;
        let mut n = 0;
        loop {
            match next_item!(self) {
                Some(Ok((key, fpos))) => {
                    n += 1;

                    first_key.get_or_insert_with(|| key.clone());
                    let e = Entry::<K, V, D>::new_mm(key.clone(), fpos);
                    let data = try_value!(util::to_cbor_bytes(e));
                    if (mblock.len() + data.len()) > self.m_blocksize {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&data);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        if n > 1 {
            mblock.resize(self.m_blocksize, 0);
            try_value!(iflush.flush(mblock));
        }
        Some(Ok((first_key.unwrap(), fpos)))
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
    K: Clone + FromCbor + IntoCbor,
    V: Clone + FromCbor + IntoCbor,
    D: Clone + FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V, D>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut iflush = self.iflush.borrow_mut();

        let mut mblock = Vec::with_capacity(self.m_blocksize);

        let fpos = iflush.to_fpos().unwrap_or(0);
        let mut first_key: Option<K> = None;

        loop {
            let item = match self.entry.take() {
                Some((key, fpos)) => Some(Ok((key, fpos))),
                None => self.iter.next(),
            };
            match item {
                Some(Ok((key, fpos))) => {
                    first_key.get_or_insert_with(|| key.clone());
                    let e = Entry::<K, V, D>::new_mz(key.clone(), fpos);
                    let data = match util::to_cbor_bytes(e) {
                        Ok(data) => data,
                        Err(err) => return Some(Err(err)),
                    };
                    if (mblock.len() + data.len()) > self.m_blocksize {
                        self.entry = Some((key, fpos));
                        break;
                    }
                    mblock.extend_from_slice(&data);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        mblock.resize(self.m_blocksize, 0);
        try_value!(iflush.flush(mblock));
        Some(Ok((first_key.unwrap(), fpos)))
    }
}

pub struct BuildZZ<K, V, D, I> {
    z_blocksize: usize,
    v_blocksize: usize,
    value_in_vlog: bool,
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
            iflush,
            vflush,
            iter,
        }
    }
}

impl<K, V, D, I> Iterator for BuildZZ<K, V, D, I>
where
    K: Clone + FromCbor + IntoCbor,
    V: Clone + FromCbor + IntoCbor,
    D: Clone + FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V, D>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut iflush = self.iflush.borrow_mut();
        let mut vflush = self.vflush.borrow_mut();

        let mut zblock = Vec::with_capacity(self.z_blocksize);
        let mut vblock = Vec::with_capacity(self.v_blocksize);

        let fpos = iflush.to_fpos().unwrap_or(0);
        let mut first_key: Option<K> = None;

        let mut iter = self.iter.borrow_mut();
        loop {
            let vfpos = vflush.to_fpos().unwrap_or(0);
            match iter.next() {
                Some(Ok(entry)) => {
                    first_key.get_or_insert_with(|| entry.key.clone());
                    let e = Entry::<K, V, D>::from(entry.clone());
                    let (a, b) = match e.encode_zz(vfpos, self.value_in_vlog) {
                        Ok((a, b)) => (a, b),
                        Err(err) => return Some(Err(err)),
                    };

                    if (zblock.len() + a.len()) > self.z_blocksize {
                        iter.push(entry);
                        break;
                    }
                    zblock.extend_from_slice(&a);
                    vblock.extend_from_slice(&b);
                }
                Some(Err(err)) => return Some(Err(err)),
                None if first_key.is_some() => break,
                None => return None,
            }
        }

        zblock.resize(self.z_blocksize, 0);
        try_value!(vflush.flush(vblock));
        try_value!(iflush.flush(zblock));
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
    K: Clone + Hash + FromCbor + IntoCbor,
    V: Clone + FromCbor + IntoCbor,
    D: Clone + FromCbor + IntoCbor,
    I: Iterator<Item = Result<db::Entry<K, V, D>>>,
{
    type Item = Result<(K, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BuildIter::MM(iter) => iter.next(),
            BuildIter::MZ(iter) => iter.next(),
        }
    }
}

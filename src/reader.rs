use std::fs;

use crate::{entry::Entry, robt::Stats, Error, Result};

struct Reader<K, V, D> {
    m_blocksize: usize,
    z_blocksize: usize,
    v_blocksize: usize,

    index: fs::File,
    vlog: Option<fs::File>,
    entries: Vec<Entry<K, V, D>>,
}

impl<K, V, D> for Reader<K, V, D> {
    pub fn from_root(stats: &Stats, root: Vec<u8>, index: fs::File, vlog: Option<fs::File>) -> Result<Self> {
        let (entries, _) = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?;
        Ok(Reader {
            m_blocksize: stats.m_blocksize,
            z_blocksize: stats.z_blocksize,
            v_blocksize: stats.v_blocksize,
            index, vlog, entries,
        })
    }

    pub fn find(&mut self, key: &Q) -> Result<Entry<K, V, D>> {
        let mut entries = self.entries.clone();

        loop {
            let off = match entries.binary_search_by_key(key, |x| x.as_key().borrow()) {
                Ok(off) => off,
                Err(off) => off.saturating_sub(1),
            };
            match &entries[off] {
                Entry::MM { fpos, .. } => {
                    let block = read_file!(
                        &mut self.index, fpos, self.m_blocksize, "reading mm block"
                    )?;
                    let (es, _) = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?;
                    entries = es;
                }
                Entry::MZ { fpos, .. } => {
                    let block = read_file!(
                        &mut self.index, fpos, self.m_blocksize, "reading mm block"
                    )?;
                    let (entries, _) = util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?;
                    match entries.binary_search_by_key(key, |x| x.as_key().borrow()) {
                        Ok(off) => break Ok(entries[off].clone()),
                        Err(off) => break err_at!(KeyNotFound, msg: "missing key"),
                    }
                }
                Entry::ZZ { .. } => unreachable!(),
            }
        }
    }
}

pub struct IterTill<'a, K, V, D, R> {
    range: R,
    iter: Iter<'a, K, V, D>,
}

pub struct Iter<'a, K, V, D> {
    index: &'a mut fs::File,
    vlog: Option<&'a mut fs::File>,
    stack: Vec<Vec<Entry<K, V, D>>>,
    reverse: bool,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
    _dff: marker::PhantomData<D>,
}

impl<K, V, D> Iterator for Iter<K, V, D>
where
    K: FromCbor,
    V: Diff + FromCbor,
    D: FromCbor,
{
    type Item = Result<db::Entry<K, V, D>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}


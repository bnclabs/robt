use fs2::FileExt;
use log::debug;
use mkit::{
    self,
    cbor::{FromCbor, IntoCbor},
    db::Bloom,
    db::{self, BuildIndex},
    Cborize,
};

use std::{
    borrow::Borrow,
    cell::RefCell,
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io, marker, mem,
    ops::{Bound, RangeBounds},
    path,
    rc::Rc,
    sync::Arc,
};

use crate::{
    build,
    config::{to_index_file, to_vlog_file, Config, Stats},
    files::{IndexFileName, VlogFileName},
    flush::Flusher,
    marker::ROOT_MARKER,
    reader::{Iter, Reader},
    scans::{BitmappedScan, BuildScan, CompactScan},
    util, Error, Result,
};

/// Marker block size, not to be tampered with.
const MARKER_BLOCK_SIZE: usize = 1024 * 4;

/// Build an immutable read-only btree index from an iterator.
///
/// Refer to package documentation for typical work-flow.
pub struct Builder<K, V, D> {
    // configuration
    config: Config,
    // active values
    iflush: Rc<RefCell<Flusher>>,
    vflush: Rc<RefCell<Flusher>>,
    // final result to be persisted
    app_meta: Vec<u8>,
    stats: Stats,
    root: u64,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
    _dff: marker::PhantomData<D>,
}

impl<K, V, D> Builder<K, V, D> {
    /// Build a fresh index, using configuration and snapshot specific
    /// meta-data.
    pub fn initial(config: Config, meta: Vec<u8>) -> Result<Self> {
        let queue_size = config.flush_queue_size;
        let iflush = {
            let file_path = to_index_file(&config.dir, &config.name);
            Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?))
        };
        let (vflush, vlog_file) = if config.value_in_vlog || config.delta_ok {
            let file_path = to_vlog_file(&config.dir, &config.name);
            (
                Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?)),
                Some(file_path),
            )
        } else {
            (Rc::new(RefCell::new(Flusher::empty())), None)
        };

        let mut stats: Stats = config.clone().into();
        stats.vlog_file = vlog_file;

        let val = Builder {
            config,
            iflush,
            vflush,

            app_meta: meta,
            stats,
            root: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
        };

        Ok(val)
    }

    /// Build an incremental index on top of an existing index. Note
    /// that the entire btree along with root-node, intermediate-nodes
    /// and leaf-nodes shall be built fresh from the iterator, but entries
    /// form the iterator can hold reference, as `{fpos, length}` to values
    /// and deltas within a value-log file. Instead of creating a fresh
    /// value-log file, incremental build will serialize values and deltas
    /// into supplied `vlog` file in append only fashion.
    pub fn incremental(
        config: Config,
        vlog: Option<ffi::OsString>,
        meta: Vec<u8>,
    ) -> Result<Self> {
        let queue_size = config.flush_queue_size;
        let iflush = {
            let file_path = to_index_file(&config.dir, &config.name);
            Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?))
        };
        let (vflush, vlog_file) = match vlog {
            Some(vlog) if config.value_in_vlog || config.delta_ok => (
                Rc::new(RefCell::new(Flusher::new(&vlog, true, queue_size)?)),
                Some(vlog),
            ),
            Some(_) => err_at!(Invalid, msg: "vlog not required")?,
            None => (Rc::new(RefCell::new(Flusher::empty())), None),
        };

        let mut stats: Stats = config.clone().into();
        stats.vlog_file = vlog_file;

        let val = Builder {
            config,
            iflush,
            vflush,

            app_meta: meta,
            stats,
            root: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
            _dff: marker::PhantomData,
        };

        Ok(val)
    }
}

impl<K, V, D, B> BuildIndex<K, V, D, B> for Builder<K, V, D>
where
    K: Clone + Hash + IntoCbor,
    V: Clone + IntoCbor,
    D: Clone + IntoCbor,
    B: Bloom,
{
    type Err = Error;

    fn build_index<I>(&mut self, iter: I, bitmap: B, seqno: Option<u64>) -> Result<()>
    where
        I: Iterator<Item = db::Entry<K, V, D>>,
    {
        let iter = {
            let iter = BitmappedScan::<K, V, D, B, I>::new(iter, bitmap);
            BuildScan::new(iter, 0 /*seqno*/)
        };

        let iter = self.build_from_iter(iter)?;

        let (bitmap, _) = iter.unwrap()?;

        self.build_flush(err_at!(Fatal, bitmap.to_bytes())?, seqno)?;

        Ok(())
    }
}

impl<K, V, D> Builder<K, V, D>
where
    K: Clone + IntoCbor,
    V: Clone + IntoCbor,
{
    fn build_from_iter<I>(&mut self, iter: BuildScan<K, V, D, I>) -> Result<I>
    where
        I: Iterator<Item = db::Entry<K, V, D>>,
        D: Clone + IntoCbor,
    {
        self.stats.n_abytes = self.vflush.as_ref().borrow().to_fpos().unwrap_or(0);

        let (iter, root) = self.build_tree(iter)?;
        let (build_time, seqno, n_count, n_deleted, epoch, iter) = iter.unwrap()?;
        self.root = root;
        self.stats.n_count = n_count;
        self.stats.n_deleted = n_deleted.try_into().unwrap();
        self.stats.build_time = build_time;
        self.stats.epoch = epoch;
        self.stats.seqno = seqno;

        Ok(iter)
    }

    fn build_tree<I>(
        &self,
        iter: BuildScan<K, V, D, I>,
    ) -> Result<(BuildScan<K, V, D, I>, u64)>
    where
        I: Iterator<Item = db::Entry<K, V, D>>,
        D: Clone + IntoCbor,
    {
        let iter = Rc::new(RefCell::new(iter));

        let zz = build::BuildZZ::new(
            &self.config,
            Rc::clone(&self.iflush),
            Rc::clone(&self.vflush),
            Rc::clone(&iter),
        );
        let mz = build::BuildMZ::new(&self.config, Rc::clone(&self.iflush), zz);
        let mut build = (0..28).fold(build::BuildIter::from(mz), |build, _| {
            build::BuildMM::new(&self.config, Rc::clone(&self.iflush), build).into()
        });

        let root = match build.next() {
            Some(Ok((_, root))) => root,
            Some(Err(err)) => return Err(err),
            None => err_at!(Invalid, msg: "empty iterator")?,
        };
        mem::drop(build);

        Ok((Rc::try_unwrap(iter).ok().unwrap().into_inner(), root))
    }

    fn build_flush(&mut self, bitmap: Vec<u8>, seqno: Option<u64>) -> Result<(u64, u64)> {
        let block = self.meta_blocks(bitmap, seqno)?;

        self.iflush.borrow_mut().flush(block)?;

        let len1 = self.iflush.borrow_mut().close()?;
        let len2 = self.vflush.borrow_mut().close()?;

        Ok((len1, len2))
    }

    fn meta_blocks(&mut self, bitmap: Vec<u8>, seqno: Option<u64>) -> Result<Vec<u8>> {
        self.stats.seqno = seqno.unwrap_or(self.stats.seqno);
        let stats = util::into_cbor_bytes(self.stats.clone())?;

        let metas = vec![
            MetaItem::AppMetadata(self.app_meta.clone()),
            MetaItem::Stats(stats),
            MetaItem::Bitmap(bitmap),
            MetaItem::Root(self.root),
            MetaItem::Marker(ROOT_MARKER.clone()),
        ];

        let mut block = util::into_cbor_bytes(metas)?;
        let len = err_at!(Fatal, u64::try_from(block.len()))?;
        let m = Self::compute_root_block(block.len() + 16);
        block.resize(m, 0);
        let off = err_at!(Fatal, u64::try_from(m))?;

        // 8-byte length-prefixed-message, message is the meta-block.
        block[m - 16..m - 8].copy_from_slice(&off.to_be_bytes());
        block[m - 8..m].copy_from_slice(&len.to_be_bytes());

        Ok(block)
    }

    fn compute_root_block(n: usize) -> usize {
        match n % MARKER_BLOCK_SIZE {
            0 => n,
            _ => ((n / MARKER_BLOCK_SIZE) + 1) * MARKER_BLOCK_SIZE,
        }
    }
}

/// Enumeration of meta items stored in [Robt] index.
///
/// [Robt] index is a fully packed immutable [Btree] index. To interpret
/// the index a list of meta items are appended to the tip of index-file.
///
/// [Btree]: https://en.wikipedia.org/wiki/B-tree
#[derive(Clone, Debug, Cborize)]
pub enum MetaItem {
    /// Application supplied metadata, typically serialized and opaque to `robt`.
    AppMetadata(Vec<u8>),
    /// Contains index-statistics along with configuration values.
    Stats(Vec<u8>),
    /// Bloom-filter.
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts.
    Root(u64),
    /// Finger print for robt.
    Marker(Vec<u8>),
}

impl MetaItem {
    const ID: &'static str = "robt/metaitem/0.0.1";
}

/// Index type, immutable, durable, fully-packed and lockless reads.
pub struct Index<K, V, D, B> {
    dir: ffi::OsString,
    name: String,

    reader: Reader<K, V, D>,
    metas: Arc<Vec<MetaItem>>,
    stats: Stats,
    bitmap: Arc<B>,
}

impl<K, V, D, B> Index<K, V, D, B> {
    /// Open an existing index for read-only.
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, D, B>>
    where
        K: FromCbor,
        V: FromCbor,
        D: FromCbor,
        B: Bloom,
    {
        match find_index_file(dir, name) {
            Some(file) => Self::open_file(&file),
            None => err_at!(Invalid, msg: "no index file {:?}/{}", dir, name)?,
        }
    }

    /// Open an existing index for read-only, from index file. file must be
    /// supplied along with full-path.
    pub fn open_file(file: &ffi::OsStr) -> Result<Index<K, V, D, B>>
    where
        K: FromCbor,
        V: FromCbor,
        D: FromCbor,
        B: Bloom,
    {
        let dir = match path::Path::new(file).parent() {
            Some(dir) => dir.as_os_str().to_os_string(),
            None => err_at!(IOError, msg: "file {:?} does not have parent dir", file)?,
        };
        let name = String::try_from(IndexFileName(file.to_os_string()))?;

        let mut index = err_at!(IOError, fs::OpenOptions::new().read(true).open(&file))?;

        let metas: Vec<MetaItem> = {
            let off = {
                let seek = io::SeekFrom::End(-16);
                let data = read_file!(index, seek, 8, "reading meta-off from index")?;
                i64::from_be_bytes(data.try_into().unwrap())
            };
            let len = {
                let seek = io::SeekFrom::End(-8);
                let data = read_file!(index, seek, 8, "reading meta-len from index")?;
                u64::from_be_bytes(data.try_into().unwrap())
            };
            let seek = io::SeekFrom::End(-off);
            let block = read_file!(index, seek, len, "reading meta-data from index")?;
            util::from_cbor_bytes(&block)?.0
        };

        let stats: Stats = match &metas[1] {
            MetaItem::Stats(stats) => util::from_cbor_bytes(stats)?.0,
            _ => unreachable!(),
        };

        let bitmap = match &metas[2] {
            MetaItem::Bitmap(data) => err_at!(Fatal, B::from_bytes(&data))?.0,
            _ => unreachable!(),
        };

        let root = match &metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        };

        if let MetaItem::Marker(mrkr) = &metas[4] {
            if mrkr.ne(ROOT_MARKER.as_slice()) {
                err_at!(Invalid, msg: "invalid marker {:?}", mrkr)?
            }
        }

        let vlog = match stats.value_in_vlog || stats.delta_ok {
            true => {
                let vlog_file = stats.vlog_file.as_ref();
                let file_name = match vlog_file.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(file_name)) => file_name.to_os_string(),
                    _ => ffi::OsString::from(VlogFileName::from(name.to_string())),
                };
                let vp: path::PathBuf = [dir.clone(), file_name].iter().collect();
                let vlog = err_at!(IOError, fs::OpenOptions::new().read(true).open(&vp))?;
                Some(vlog)
            }
            false => None,
        };

        let reader = Reader::from_root(root, &stats, index, vlog)?;

        let val = Index {
            dir,
            name,

            reader,
            metas: Arc::new(metas),
            stats,
            bitmap: Arc::new(bitmap),
        };

        Ok(val)
    }

    /// Optionally set a different bitmap over this index. Know what you are
    /// doing before calling this API.
    pub fn set_bitmap(&mut self, bitmap: B) {
        self.bitmap = Arc::new(bitmap)
    }

    /// Clone this index instance, with its underlying meta-data `shared`
    /// across index instances. Note that file-descriptors are not `shared`.
    pub fn try_clone(&self) -> Result<Self>
    where
        K: FromCbor,
        V: FromCbor,
        D: FromCbor,
    {
        let index = match find_index_file(&self.dir, &self.name) {
            Some(ip) => err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?,
            None => err_at!(Invalid, msg: "bad file {:?}/{}", &self.dir, &self.name)?,
        };

        let vlog = match self.stats.value_in_vlog || self.stats.delta_ok {
            true => {
                let vlog_file = self.stats.vlog_file.as_ref();
                let fnm = match vlog_file.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(fnm)) => fnm.to_os_string(),
                    _ => ffi::OsString::from(VlogFileName::from(self.name.to_string())),
                };
                let vp: path::PathBuf = [self.dir.to_os_string(), fnm].iter().collect();
                Some(err_at!(IOError, fs::OpenOptions::new().read(true).open(&vp))?)
            }
            false => None,
        };

        let root = match &self.metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        };

        let reader = Reader::from_root(root, &self.stats, index, vlog)?;

        let val = Index {
            dir: self.dir.clone(),
            name: self.name.clone(),

            reader,
            metas: Arc::clone(&self.metas),
            stats: self.stats.clone(),
            bitmap: Arc::clone(&self.bitmap),
        };

        Ok(val)
    }

    /// Compact this index into a new index specified by [Config].
    /// The `bitmap` argument carry same meaning as that of `build_index`
    /// method. Refer to package documentation to know more about `Cutoff`.
    pub fn compact(
        mut self,
        config: Config,
        bitmap: B,
        cutoff: db::Cutoff,
    ) -> Result<Self>
    where
        K: Clone + Ord + Hash + FromCbor + IntoCbor,
        V: Clone + FromCbor + IntoCbor,
        D: Clone + FromCbor + IntoCbor,
        B: Bloom,
    {
        let mut builder = {
            let app_meta = self.to_app_metadata();
            Builder::<K, V, D>::initial(config.clone(), app_meta)?
        };
        let r = (Bound::<K>::Unbounded, Bound::<K>::Unbounded);
        let iter = CompactScan::new(self.iter(r)?.map(|e| e.unwrap()), cutoff);

        builder.build_index(iter, bitmap, None)?;

        Index::open(&config.dir, &config.name)
    }

    /// Close this index, releasing OS resources. To purge, call `purge()`
    /// method.
    pub fn close(self) -> Result<()> {
        Ok(())
    }

    /// Purge this index from disk.
    pub fn purge(self) -> Result<()> {
        purge_file(to_index_file(&self.dir, &self.name))?;

        if self.stats.value_in_vlog || self.stats.delta_ok {
            purge_file(to_vlog_file(&self.dir, &self.name))?;
        }

        Ok(())
    }
}

impl<K, V, D, B> Index<K, V, D, B> {
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_app_metadata(&self) -> Vec<u8> {
        match &self.metas[0] {
            MetaItem::AppMetadata(data) => data.clone(),
            _ => unreachable!(),
        }
    }

    pub fn to_stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn as_bitmap(&self) -> &B {
        self.bitmap.as_ref()
    }

    pub fn to_bitmap(&self) -> B
    where
        B: Clone,
    {
        self.bitmap.as_ref().clone()
    }

    pub fn to_root(&self) -> u64 {
        match &self.metas[3] {
            MetaItem::Root(root) => *root,
            _ => unreachable!(),
        }
    }

    pub fn to_seqno(&self) -> u64 {
        self.stats.seqno
    }

    pub fn is_compacted(&self) -> bool {
        self.stats.n_abytes == 0
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.stats.n_count).unwrap()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_index_file_location(&self) -> ffi::OsString {
        let config: Config = self.stats.clone().into();
        config.to_index_file_location()
    }

    pub fn to_vlog_file_location(&self) -> Option<ffi::OsString> {
        match &self.stats.vlog_file {
            Some(vlog_file) => {
                let loc: path::PathBuf =
                    [self.dir.clone(), path::Path::new(vlog_file).file_name()?.into()]
                        .iter()
                        .collect();
                Some(loc.into())
            }
            None => None,
        }
    }

    pub fn get<Q>(&mut self, key: &Q) -> Result<db::Entry<K, V, D>>
    where
        K: Clone + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord,
    {
        let versions = false;
        Ok(self.reader.get(key, versions)?.into())
    }

    pub fn get_versions<Q>(&mut self, key: &Q) -> Result<db::Entry<K, V, D>>
    where
        K: Clone + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord,
    {
        let versions = true;
        Ok(self.reader.get(key, versions)?.into())
    }

    pub fn iter<Q, R>(&mut self, range: R) -> Result<Iter<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (false, false);
        self.reader.iter(range, reverse, versions)
    }

    pub fn reverse<Q, R>(&mut self, range: R) -> Result<Iter<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (true, false);
        self.reader.iter(range, reverse, versions)
    }

    pub fn iter_versions<Q, R>(&mut self, range: R) -> Result<Iter<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (false, true);
        self.reader.iter(range, reverse, versions)
    }

    pub fn reverse_versions<Q, R>(&mut self, range: R) -> Result<Iter<K, V, D>>
    where
        K: Clone + Ord + Borrow<Q> + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
        Q: Ord + ToOwned<Owned = K>,
        R: RangeBounds<Q>,
    {
        let (reverse, versions) = (true, true);
        self.reader.iter(range, reverse, versions)
    }

    pub fn validate(&mut self) -> Result<Stats>
    where
        K: Clone + PartialOrd + Ord + fmt::Debug + FromCbor,
        V: Clone + FromCbor,
        D: Clone + FromCbor,
    {
        let iter = self.iter((Bound::<K>::Unbounded, Bound::<K>::Unbounded))?;

        let mut prev_key: Option<K> = None;
        let (mut n_count, mut n_deleted, mut seqno) = (0, 0, 0);

        for entry in iter {
            let entry = entry?;
            n_count += 1;

            if entry.is_deleted() {
                n_deleted += 1;
            }

            seqno = cmp::max(seqno, entry.to_seqno());

            match prev_key.as_ref().map(|pk| pk.lt(&entry.key)) {
                Some(true) | None => (),
                Some(false) => err_at!(Fatal, msg: "{:?} >= {:?}", prev_key, entry.key)?,
            }

            for d in entry.deltas.iter() {
                if d.to_seqno() >= seqno {
                    err_at!(Fatal, msg: "delta is newer {} {}", d.to_seqno(), seqno)?;
                }
            }

            prev_key = Some(entry.key.clone());
        }

        let s = self.to_stats();
        if n_count != s.n_count {
            err_at!(Fatal, msg: "validate, n_count {} > {}", n_count, s.n_count)
        } else if n_deleted != s.n_deleted {
            err_at!(Fatal, msg: "validate, n_deleted {} > {}", n_deleted, s.n_deleted)
        } else if seqno > 0 && seqno > s.seqno {
            err_at!(Fatal, msg: "validate, seqno {} > {}", seqno, s.seqno)
        } else {
            Ok(s)
        }
    }

    pub fn print(&mut self) -> Result<()>
    where
        K: Clone + FromCbor + fmt::Debug,
        V: Clone + FromCbor + fmt::Debug,
        D: Clone + FromCbor + fmt::Debug,
        B: Bloom,
    {
        println!("name              : {}", self.to_name());
        println!("app_meta_data     : {}", self.to_app_metadata().len());
        println!("root block at     : {}", self.to_root());
        println!("sequence num. at  : {}", self.to_seqno());
        let stats = self.to_stats();
        println!("stats         :");
        println!("  z_blocksize  : {}", stats.z_blocksize);
        println!("  m_blocksize  : {}", stats.m_blocksize);
        println!("  v_blocksize  : {}", stats.v_blocksize);
        println!("  delta_ok     : {}", stats.delta_ok);
        println!("  vlog_file    : {:?}", stats.vlog_file);
        println!("  value_in_vlog: {}", stats.value_in_vlog);
        println!("  n_count      : {}", stats.n_count);
        println!("  n_deleted    : {}", stats.n_deleted);
        println!("  seqno        : {}", stats.seqno);
        println!("  n_abytes     : {}", stats.n_abytes);
        println!("  build_time   : {}", stats.build_time);
        println!("  epoch        : {}", stats.epoch);
        println!();
        self.reader.print()
    }
}

fn find_index_file(dir: &ffi::OsStr, name: &str) -> Option<ffi::OsString> {
    let iter = fs::read_dir(dir).ok()?;
    let entry = iter.filter_map(|entry| entry.ok()).find(|entry| {
        let filen = IndexFileName(entry.file_name());
        matches!(String::try_from(filen), Ok(nm) if nm == name)
    });

    entry.map(|entry| {
        let file_path: path::PathBuf =
            [dir.to_os_string(), IndexFileName(entry.file_name()).into()]
                .iter()
                .collect();
        file_path.as_os_str().to_os_string()
    })
}

fn purge_file(file: ffi::OsString) -> Result<()> {
    let fd = open_file_r(&file)?;
    match fd.try_lock_exclusive() {
        Ok(_) => {
            err_at!(IOError, fs::remove_file(&file), "remove file {:?}", file)?;
            debug!(target: "robt", "purged file {:?}", file);
            err_at!(IOError, fd.unlock(), "fail unlock for exclusive lock {:?}", file)
        }
        Err(_) => {
            debug!(target: "robt", "unable to get exclusive lock for {:?}", file);
            err_at!(Retry, msg: "file {:?} locked", file)
        }
    }
}

fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(IOError, fs::OpenOptions::new().read(true).open(os_file))?)
}

#[cfg(test)]
#[path = "robt_test.rs"]
mod robt_test;

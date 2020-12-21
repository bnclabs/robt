use fs2::FileExt;
use log::debug;
use mkit::{
    self,
    cbor::{Cbor, FromCbor, IntoCbor},
    db,
    traits::{Bloom, Diff},
    Cborize,
};

use std::{
    borrow::Borrow,
    cell::RefCell,
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    hash::Hash,
    io, marker,
    ops::RangeBounds,
    path,
    rc::Rc,
    sync::Arc,
};

use crate::{
    build,
    entry::Entry,
    files::{IndexFileName, VlogFileName},
    flush::Flusher,
    marker::ROOT_MARKER,
    nobitmap::NoBitmap,
    reader::Reader,
    scans::{BitmappedScan, BuildScan, CompactScan},
    util, Error, Result,
};

/// Marker block size, not to be tampered with.
const MARKER_BLOCK_SIZE: usize = 1024 * 4;

pub struct Builder<K, V, B = NoBitmap>
where
    K: Clone + Hash + IntoCbor,
    V: Clone + Diff + IntoCbor,
    <V as Diff>::D: IntoCbor,
    B: Bloom,
{
    // configuration
    config: Config,
    // active values
    iflush: Rc<RefCell<Flusher>>,
    vflush: Rc<RefCell<Flusher>>,
    // final result to be persisted
    app_meta: Vec<u8>,
    stats: Stats,
    bitmap: B,
    root: u64,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B> Builder<K, V, B>
where
    K: Clone + Hash + IntoCbor,
    V: Clone + Diff + IntoCbor,
    <V as Diff>::D: IntoCbor,
    B: Bloom,
{
    pub fn initial(c: Config, app_meta: Vec<u8>) -> Result<Builder<K, V, B>> {
        let queue_size = c.flush_queue_size;
        let iflush = {
            let file_path = to_index_file(&c.dir, &c.name);
            Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?))
        };
        let vflush = if c.value_in_vlog || c.delta_ok {
            let file_path = to_vlog_file(&c.dir, &c.name);
            Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?))
        } else {
            Rc::new(RefCell::new(Flusher::empty()))
        };

        let val = Builder {
            config: c,
            iflush,
            vflush,

            app_meta,
            stats: Stats::default(),
            bitmap: B::create(),
            root: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    pub fn incremental(
        c: Config,
        vlog_file: ffi::OsString,
        app_meta: Vec<u8>,
    ) -> Result<Builder<K, V, B>> {
        let queue_size = c.flush_queue_size;
        let iflush = {
            let file_path = to_index_file(&c.dir, &c.name);
            Rc::new(RefCell::new(Flusher::new(&file_path, true, queue_size)?))
        };
        let vflush = if c.value_in_vlog || c.delta_ok {
            Rc::new(RefCell::new(Flusher::new(&vlog_file, true, queue_size)?))
        } else {
            Rc::new(RefCell::new(Flusher::empty()))
        };

        let val = Builder {
            config: c,
            iflush,
            vflush,

            app_meta,
            stats: Stats::default(),
            bitmap: B::create(),
            root: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }
}

impl<K, V, B> Builder<K, V, B>
where
    K: Clone + Hash + IntoCbor,
    V: Clone + Diff + IntoCbor,
    <V as Diff>::D: IntoCbor,
    B: Bloom,
{
    pub fn build_from_iter<I>(mut self, iter: I) -> Result<Stats>
    where
        I: Iterator<Item = Result<db::Entry<K, V, <V as Diff>::D>>>,
    {
        let iter = {
            let seqno = 0_64;
            BuildScan::new(
                BitmappedScan::<K, V, <V as Diff>::D, B, I>::new(iter),
                seqno,
            )
        };
        let (iter, root) = self.build_tree(iter)?;
        let (build_time, seqno, n_count, n_deleted, epoch, iter) = iter.unwrap()?;
        let (bitmap, _) = iter.unwrap()?;
        self.stats.n_count = n_count;
        self.stats.n_deleted = n_deleted.try_into().unwrap();
        self.stats.build_time = build_time;
        self.stats.epoch = epoch;
        self.stats.seqno = seqno;

        let stats = {
            self.stats.n_bitmap = bitmap.len()?;
            self.stats.n_abytes = self.vflush.as_ref().borrow().to_fpos().unwrap_or(0);
            self.stats.clone()
        };

        self.root = root;
        self.bitmap = bitmap;

        self.build_flush()?;

        Ok(stats)
    }

    fn build_tree<I>(
        &self,
        iter: BuildScan<K, V, <V as Diff>::D, I>,
    ) -> Result<(BuildScan<K, V, <V as Diff>::D, I>, u64)>
    where
        I: Iterator<Item = Result<db::Entry<K, V, <V as Diff>::D>>>,
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
            Some(Err(err)) => Err(err)?,
            None => err_at!(Invalid, msg: "empty iterator")?,
        };

        Ok((Rc::try_unwrap(iter).ok().unwrap().into_inner(), root))
    }

    fn build_flush(self) -> Result<(u64, u64)> {
        let mut block = self.to_meta_blocks()?;
        block.extend_from_slice(&(u64::try_from(block.len()).unwrap().to_be_bytes()));

        self.iflush.borrow_mut().flush(block)?;

        let len1 = {
            let iflush = Rc::try_unwrap(self.iflush).ok().unwrap();
            let iflush = iflush.into_inner();
            iflush.close()?
        };
        let len2 = {
            let vflush = Rc::try_unwrap(self.vflush).ok().unwrap();
            let vflush = vflush.into_inner();
            vflush.close()?
        };

        Ok((len1, len2))
    }

    fn to_meta_blocks(&self) -> Result<Vec<u8>> {
        let stats = util::to_cbor_bytes(self.stats.clone())?;

        debug!(
            target: "robt",
            "{:?}, metablocks root:{} bitmap:{} meta:{}  stats:{}",
            self.iflush.as_ref().borrow().to_file_path(), self.root, self.bitmap.len()?,
            self.app_meta.len(), stats.len(),
        );

        let metas = vec![
            MetaItem::AppMetadata(self.app_meta.clone()),
            MetaItem::Stats(stats),
            MetaItem::Bitmap(self.bitmap.to_vec()),
            MetaItem::Root(self.root),
            MetaItem::Marker(ROOT_MARKER.clone()),
        ];

        let mut block = util::to_cbor_bytes(metas)?;
        block.resize(Self::compute_root_block(block.len()), 0);
        Ok(block)
    }

    fn compute_root_block(n: usize) -> usize {
        if (n % MARKER_BLOCK_SIZE) == 0 {
            n
        } else {
            ((n / MARKER_BLOCK_SIZE) + 1) * MARKER_BLOCK_SIZE
        }
    }
}

/// Enumeration of meta items stored in [Robt] index.
///
/// [Robt] index is a fully packed immutable [Btree] index. To interpret
/// the index a list of meta items are appended to the tip of index-file.
///
/// [Btree]: https://en.wikipedia.org/wiki/B-tree
#[derive(Clone, Cborize)]
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
    const ID: u32 = 0x0;
}

/// Index type, immutable, durable, fully-packed and lockless reads.
pub struct Index<K, V, B, D = db::NoDiff> {
    dir: ffi::OsString,
    name: String,

    metas: Arc<Vec<MetaItem>>,
    stats: Stats,
    bitmap: Arc<B>,

    reader: Reader<K, V, D>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B, D> Index<K, V, B, D> {
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, B>>
    where
        B: Bloom,
    {
        let mut ifd = match find_index_file(dir, name) {
            Some(ip) => err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?,
            None => err_at!(Invalid, msg: "bad file {:?}/{}", dir, name)?,
        };
        err_at!(IOError, ifd.lock_shared())?;

        let metas = {
            let n = {
                let seek = io::SeekFrom::End(-8);
                let data = read_file!(ifd, seek, 8, "reading meta-len from index")?;
                u64::from_be_bytes(data.try_into().unwrap())
            };
            let seek = io::SeekFrom::End(-8 - i64::try_from(n).unwrap());
            let block = read_file!(ifd, seek, n, "reading meta-data from index")?;
            Vec::<MetaItem>::from_cbor(Cbor::decode(&mut block.as_slice())?.0)?
        };

        let stats = match &metas[1] {
            MetaItem::Stats(stats) => {
                let (val, _) = Cbor::decode(&mut stats.as_slice())?;
                Stats::from_cbor(val)?
            }
            _ => unreachable!(),
        };

        let bitmap = match &metas[2] {
            MetaItem::Bitmap(data) => B::from_vec(&data)?,
            _ => unreachable!(),
        };

        let root = match &metas[3] {
            MetaItem::Root(root) => {
                let root = io::SeekFrom::Start(*root);
                let block = read_file!(&mut ifd, root, stats.m_blocksize, "read block")?;
                util::from_cbor_bytes::<Vec<Entry<K, V, D>>>(&block)?.0
            }
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
                let vp: path::PathBuf = [dir.to_os_string(), file_name].iter().collect();
                let vlog = err_at!(IOError, fs::OpenOptions::new().read(true).open(&vp))?;
                err_at!(IOError, vlog.lock_shared())?;
                Some(vlog)
            }
            false => None,
        };

        let reader = Reader::from_root(&stats, root, ifd, vlog)?;

        let val = Index {
            dir: dir.to_os_string(),
            name: name.to_string(),

            metas: Arc::new(metas),
            stats,
            bitmap: Arc::new(bitmap),

            reader,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    pub fn clone(&self) -> Result<Self> {
        //let index = match find_index_file(&self.dir, &self.name) {
        //    Some(ip) => err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?,
        //    None => err_at!(Invalid, msg: "bad file {:?}/{}", &self.dir, &self.name)?,
        //};

        //let vlog = match self.stats.value_in_vlog || self.stats.delta_ok {
        //    true => {
        //        let vlog_file = self.stats.vlog_file.as_ref();
        //        let fnm = match vlog_file.map(|f| path::Path::new(f).file_name()) {
        //            Some(Some(fnm)) => fnm.to_os_string(),
        //            _ => ffi::OsString::from(VlogFileName::from(self.name.to_string())),
        //        };
        //        let vp: path::PathBuf = [self.dir.to_os_string(), fnm].iter().collect();
        //        Some(err_at!(
        //            IOError,
        //            fs::OpenOptions::new().read(true).open(&vp)
        //        )?)
        //    }
        //    false => None,
        //};

        //let val = Index {
        //    dir: self.dir.clone(),
        //    name: self.name.clone(),

        //    metas: Arc::clone(&self.metas),
        //    stats: self.stats.clone(),
        //    bitmap: Arc::clone(&self.bitmap),

        //    index,
        //    vlog,

        //    _key: marker::PhantomData,
        //    _val: marker::PhantomData,
        //};

        //Ok(val)

        todo!()
    }

    pub fn compact(
        mut self,
        dir: &ffi::OsStr,
        name: &str,
        cutoff: db::Cutoff,
    ) -> Result<Self>
    where
        K: Clone + Hash + FromCbor + IntoCbor,
        V: Clone + Diff + FromCbor + IntoCbor,
        <V as Diff>::D: FromCbor + IntoCbor,
        B: Bloom,
    {
        let config = {
            let mut config: Config = self.stats.clone().into();
            config.dir = dir.to_os_string();
            config.name = name.to_string();
            config
        };

        let builder = {
            let app_meta = self.to_app_metadata();
            Builder::<K, V, B>::initial(config, app_meta)?
        };
        //let iter = CompactScan::new(self.iter::<<V as Diff>::D>()?, cutoff);
        //builder.build_from_iter(iter)?;
        let x = 10;

        Index::open(dir, name)
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn purge(self) -> Result<()> {
        let index_file = to_index_file(&self.dir, &self.name);
        purge_file(index_file)?;

        if self.stats.value_in_vlog || self.stats.delta_ok {
            let vlog_file = to_vlog_file(&self.dir, &self.name);
            purge_file(vlog_file)?;
        }

        Ok(())
    }

    pub fn set_bitmap(&mut self, bitmap: B) {
        self.bitmap = Arc::new(bitmap)
    }
}

impl<K, V, B, D> Index<K, V, B, D> {
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
        if self.stats.n_abytes == 0 {
            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.stats.n_count).unwrap()
    }

    pub fn get<Q>(&mut self, _key: &Q) -> Result<db::Entry<K, V, D>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + Hash,
    {
        todo!()
    }

    //pub fn range<R, Q, D>(&mut self, _range: R) -> Result<Iter<K, V, D>>
    //where
    //    K: Borrow<Q>,
    //    R: RangeBounds<Q>,
    //    Q: Ord + ?Sized,
    //{
    //    todo!()
    //}

    //pub fn reverse<R, Q, D>(&mut self, _range: R) -> Result<Iter<K, V, D>>
    //where
    //    K: Borrow<Q>,
    //    R: RangeBounds<Q>,
    //    Q: Ord + ?Sized,
    //{
    //    todo!()
    //}

    //pub fn iter<D>(&mut self) -> Result<Iter<K, V, D>>
    //where
    //    V: Diff,
    //{
    //    todo!()
    //}

    pub fn validate(&mut self) -> Result<Stats>
    where
        K: Clone + fmt::Debug + PartialOrd + FromCbor,
        V: Diff + FromCbor,
        <V as Diff>::D: FromCbor,
    {
        //let iter = self.iter::<<V as Diff>::D>()?;

        //let mut prev_key: Option<K> = None;
        //let (mut n_count, mut n_deleted, mut seqno) = (0, 0, 0);

        //for entry in iter {
        //    let entry = entry?;
        //    n_count += 1;

        //    if entry.is_deleted() {
        //        n_deleted += 1;
        //    }

        //    seqno = cmp::max(seqno, entry.to_seqno());

        //    match prev_key.as_ref().map(|pk| pk.lt(&entry.key)) {
        //        Some(true) | None => (),
        //        Some(false) => err_at!(Fatal, msg: "{:?} >= {:?}", prev_key, entry.key)?,
        //    }

        //    for d in entry.deltas.iter() {
        //        if d.to_seqno() >= seqno {
        //            err_at!(Fatal, msg: "delta is newer {} {}", d.to_seqno(), seqno)?;
        //        }
        //    }

        //    prev_key.get_or_insert_with(|| entry.key.clone());
        //}

        //let s = self.to_stats();
        //if n_count != s.n_count {
        //    err_at!(Fatal, msg: "validate, n_count {} > {}", n_count, s.n_count)
        //} else if n_deleted != s.n_deleted {
        //    err_at!(Fatal, msg: "validate, n_deleted {} > {}", n_deleted, s.n_deleted)
        //} else if seqno > 0 && seqno > s.seqno {
        //    err_at!(Fatal, msg: "validate, seqno {} > {}", seqno, s.seqno)
        //} else {
        //    Ok(s)
        //}
        todo!()
    }
}

fn find_index_file(dir: &ffi::OsStr, name: &str) -> Option<ffi::OsString> {
    let iter = fs::read_dir(dir).ok()?;
    let entry = iter
        .filter_map(|entry| entry.ok())
        .filter_map(
            |entry| match String::try_from(IndexFileName(entry.file_name())) {
                Ok(nm) if nm == name => Some(entry),
                _ => None,
            },
        )
        .next();

    entry.map(|entry| entry.file_name())
}

fn to_index_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
    let file_path: path::PathBuf = [
        dir.to_os_string(),
        IndexFileName::from(name.to_string()).into(),
    ]
    .iter()
    .collect();
    file_path.into_os_string()
}

fn to_vlog_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
    let file_path: path::PathBuf = [
        dir.to_os_string(),
        VlogFileName::from(name.to_string()).into(),
    ]
    .iter()
    .collect();
    file_path.into_os_string()
}

fn purge_file(file: ffi::OsString) -> Result<&'static str> {
    let fd = open_file_r(&file)?;
    match fd.try_lock_exclusive() {
        Ok(_) => {
            err_at!(IOError, fs::remove_file(&file), "remove file {:?}", file)?;
            debug!(target: "robt", "purged file {:?}", file);
            fd.unlock().ok();
            Ok("ok")
        }
        Err(_) => {
            debug!(target: "robt", "locked file {:?}", file);
            Ok("locked")
        }
    }
}

fn open_file_r(file: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file);
    Ok(err_at!(
        IOError,
        fs::OpenOptions::new().read(true).open(os_file)
    )?)
}

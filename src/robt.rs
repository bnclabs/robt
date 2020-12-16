use fs2::FileExt;
use log::debug;
use mkit::{
    self,
    cbor::{Cbor, FromCbor},
    traits::{Bloom, Footprint},
    Cborize, Entry,
};

use std::{
    convert::{TryFrom, TryInto},
    ffi, fs,
    hash::Hash,
    io, marker, path,
    sync::Arc,
};

use crate::{
    files::{IndexFileName, VlogFileName},
    flush::Flusher,
    marker::ROOT_MARKER,
    nobitmap::NoBitmap,
    scans::{BitmappedScan, BuildScan, CompactScan},
    util, Error, Result,
};

/// Default value for z-block-size, 4 * 1024 bytes.
pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
/// Default value for m-block-size, 4 * 1024 bytes.
pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
/// Default value for v-block-size, 4 * 1024 bytes.
pub const VBLOCKSIZE: usize = 4 * 1024; // 4KB of blobs.
/// Default value for Flush queue size, channel queue size, holding
/// index blocks.
pub const FLUSH_QUEUE_SIZE: usize = 64;

/// Marker block size, not to be tampered with.
const MARKER_BLOCK_SIZE: usize = 1024 * 4;

/// Configuration for Read Only BTree index.
#[derive(Clone)]
pub struct Config {
    /// location path where index files are created.
    pub dir: ffi::OsString,
    /// name of the index.
    pub name: String,
    /// Leaf block size in btree index.
    /// Default: Config::ZBLOCKSIZE
    pub z_blocksize: usize,
    /// Intermediate block size in btree index.
    /// Default: Config::MBLOCKSIZE
    pub m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log
    /// file.
    /// Default: Config::VBLOCKSIZE
    pub v_blocksize: usize,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    /// Default: true
    pub delta_ok: bool,
    /// If true, then value shall be persisted in a separate file called
    /// value log file. Otherwise value shall be saved in the index's
    /// leaf node. Default: false
    pub value_in_vlog: bool,
    /// Flush queue size. Default: Config::FLUSH_QUEUE_SIZE
    pub flush_queue_size: usize,
}

impl Config {
    fn new(dir: &ffi::OsStr, name: &str) -> Config {
        Config {
            dir: dir.to_os_string(),
            name: name.to_string(),
            z_blocksize: ZBLOCKSIZE,
            m_blocksize: MBLOCKSIZE,
            v_blocksize: VBLOCKSIZE,
            delta_ok: true,
            value_in_vlog: false,
            flush_queue_size: FLUSH_QUEUE_SIZE,
        }
    }

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> &mut Self {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        self
    }

    /// Enable delta persistence, and configure value-log-file.
    pub fn set_delta(&mut self, ok: bool) -> &mut Self {
        self.delta_ok = true;
        self
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `ok` as false.
    pub fn set_value_log(&mut self, ok: bool) -> &mut Self {
        self.value_in_vlog = true;
        self
    }

    /// Set flush queue size, increasing the queue size will improve batch
    /// flushing.
    pub fn set_flush_queue_size(&mut self, size: usize) -> &mut Self {
        self.flush_queue_size = size;
        self
    }
}

/// Statistic for Read Only BTree index.
#[derive(Clone, Default, Cborize)]
pub struct Stats {
    /// Comes from [Config] type.
    pub name: String,
    /// Comes from [Config] type.
    pub z_blocksize: usize,
    /// Comes from [Config] type.
    pub m_blocksize: usize,
    /// Comes from [Config] type.
    pub v_blocksize: usize,
    /// Comes from [Config] type.
    pub delta_ok: bool,
    /// Comes from [Config] type.
    pub vlog_file: Option<ffi::OsString>,
    /// Comes from [Config] type.
    pub value_in_vlog: bool,

    /// Number of entries indexed.
    pub n_count: u64,
    /// Number of entries that are marked as deleted.
    pub n_deleted: usize,
    /// Sequence number for the latest entry.
    pub seqno: u64,
    /// Total disk footprint for all keys.
    pub key_mem: usize,
    /// Total disk footprint for all deltas.
    pub diff_mem: usize,
    /// Total disk footprint for all values.
    pub val_mem: usize,
    /// Total disk footprint for all leaf-nodes.
    pub z_bytes: usize,
    /// Total disk footprint for all intermediate-nodes.
    pub m_bytes: usize,
    /// Total disk footprint for values and deltas.
    pub v_bytes: usize,
    /// Total disk size wasted in padding leaf-nodes and intermediate-nodes.
    pub padding: usize,
    /// Older size of value-log file, applicable only in compaction.
    pub n_abytes: u64,
    /// Number of entries in bitmap.
    pub n_bitmap: usize,

    /// Time take to build this btree.
    pub build_time: u64,
    /// Timestamp when this index was build, from UNIX EPOCH, in secs.
    pub epoch: u64,
}

impl Stats {
    const ID: u32 = 0x0;
}

impl From<Stats> for Config {
    fn from(val: Stats) -> Config {
        Config {
            dir: ffi::OsString::default(),
            name: val.name,
            z_blocksize: val.z_blocksize,
            m_blocksize: val.m_blocksize,
            v_blocksize: val.v_blocksize,
            delta_ok: val.delta_ok,
            value_in_vlog: val.value_in_vlog,
            flush_queue_size: FLUSH_QUEUE_SIZE,
        }
    }
}

pub struct Builder<K, V, B = NoBitmap> {
    // configuration
    config: Config,
    // active values
    iflush: Flusher,
    vflush: Flusher,
    initial: bool,
    // final result to be persisted
    app_meta: Vec<u8>,
    stats: Stats,
    bitmap: B,
    root: u64,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B> Builder<K, V, B> {
    pub fn initial(config: Config, app_meta: Vec<u8>) -> Result<Builder<K, V, B>>
    where
        B: Bloom,
    {
        let queue_size = config.flush_queue_size;
        let iflush =
            Flusher::new(&to_index_file(&config.dir, &config.name), true, queue_size)?;
        let vflush = if config.value_in_vlog || config.delta_ok {
            Flusher::new(&to_vlog_file(&config.dir, &config.name), true, queue_size)?
        } else {
            Flusher::empty()
        };

        let val = Builder {
            config,
            iflush,
            vflush,
            initial: true,

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
        config: Config,
        vlog_file: ffi::OsString,
        app_meta: Vec<u8>,
    ) -> Result<Builder<K, V, B>>
    where
        B: Bloom,
    {
        let queue_size = config.flush_queue_size;
        let iflush =
            Flusher::new(&to_index_file(&config.dir, &config.name), true, queue_size)?;
        let vflush = if config.value_in_vlog || config.delta_ok {
            Flusher::new(&to_vlog_file(&config.dir, &config.name), false, queue_size)?
        } else {
            Flusher::empty()
        };

        let val = Builder {
            config,
            iflush,
            vflush,
            initial: false,

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

impl<K, V, B> Builder<K, V, B> {
    pub fn build_from_iter<I, E>(mut self, iter: I) -> Result<Stats>
    where
        K: Hash,
        I: Iterator<Item = Result<E>>,
        E: Entry<K, V>,
        B: Bloom,
    {
        let mut iter = {
            let seqno = 0_64;
            BuildScan::new(BitmappedScan::<K, V, B, I, E>::new(iter), seqno)
        };
        let root = self.build_tree(&mut iter)?;
        let (build_time, seqno, n_count, n_deleted, epoch, iter) = iter.unwrap()?;
        let (bitmap, _) = iter.unwrap()?;
        self.stats.n_count = n_count;
        self.stats.n_deleted = n_deleted.try_into().unwrap();
        self.stats.build_time = build_time;
        self.stats.epoch = epoch;
        self.stats.seqno = seqno;

        let stats = {
            self.stats.n_bitmap = bitmap.len()?;
            self.stats.n_abytes = self.vflush.to_start_fpos().unwrap_or(0);
            self.stats.clone()
        };

        self.root = root;
        self.bitmap = bitmap;

        self.build_flush();

        Ok(stats)
    }

    fn build_tree<I, E>(&self, iter: &mut BuildScan<K, V, I, E>) -> Result<u64>
    where
        K: Hash,
        I: Iterator<Item = Result<E>>,
        E: Entry<K, V>,
    {
        // return root
        todo!()
    }

    fn build_flush(self) -> Result<(u64, u64)>
    where
        B: Bloom,
    {
        let mut block = self.to_meta_blocks()?;
        block.extend_from_slice(&(u64::try_from(block.len()).unwrap().to_be_bytes()));

        self.iflush.post(block)?;

        let len1 = self.iflush.close()?;
        let len2 = self.vflush.close()?;

        Ok((len1, len2))
    }

    fn to_meta_blocks(&self) -> Result<Vec<u8>>
    where
        B: Bloom,
    {
        let stats = util::encode_to_cbor(self.stats.clone())?;

        debug!(
            target: "robt",
            "{:?}, metablocks root:{} bitmap:{} meta:{}  stats:{}",
            self.iflush.to_file_path(), self.root, self.bitmap.len()?,
            self.app_meta.len(), stats.len(),
        );

        let metas = vec![
            MetaItem::AppMetadata(self.app_meta.clone()),
            MetaItem::Stats(stats),
            MetaItem::Bitmap(self.bitmap.to_vec()),
            MetaItem::Root(self.root),
            MetaItem::Marker(ROOT_MARKER.clone()),
        ];

        util::encode_to_cbor(metas)
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
pub struct Index<K, V, B> {
    dir: ffi::OsString,
    name: String,
    footprint: usize,

    metas: Arc<Vec<MetaItem>>,
    stats: Stats,
    bitmap: Arc<B>,

    index: fs::File,
    vlog: Option<fs::File>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B> Footprint for Index<K, V, B> {
    fn footprint(&self) -> mkit::Result<usize> {
        Ok(self.footprint)
    }
}

impl<K, V, B> Index<K, V, B> {
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, B>>
    where
        B: Bloom,
    {
        let mut ifd = match Self::find_index_file(dir, name) {
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

        let val = Index {
            dir: dir.to_os_string(),
            name: name.to_string(),
            footprint: usize::try_from(err_at!(IOError, ifd.metadata())?.len()).unwrap(),

            metas: Arc::new(metas),
            stats,
            bitmap: Arc::new(bitmap),

            index: ifd,
            vlog,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    pub fn clone(&self) -> Result<Self> {
        let mut index = match Self::find_index_file(&self.dir, &self.name) {
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
                Some(err_at!(
                    IOError,
                    fs::OpenOptions::new().read(true).open(&vp)
                )?)
            }
            false => None,
        };

        let val = Index {
            dir: self.dir.clone(),
            name: self.name.clone(),
            footprint: self.footprint,

            metas: Arc::clone(&self.metas),
            stats: self.stats.clone(),
            bitmap: Arc::clone(&self.bitmap),

            index,
            vlog,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    pub fn compact(
        self,
        dir: &ffi::OsStr,
        name: &str,
        cutoff: mkit::Cutoff,
    ) -> Result<Self>
    where
        K: Hash,
        V: mkit::Diff,
        B: Bloom,
    {
        let config = {
            let mut config: Config = self.stats.clone().into();
            config.dir = dir.to_os_string();
            config.name = name.to_string();
            config
        };

        let builder = {
            let app_meta = self.to_app_meta();
            Builder::<K, V, B>::initial(config, app_meta)?
        };
        let iter = CompactScan::new(self.iter_with_versions()?, cutoff);
        builder.build_from_iter(iter)?;

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

    fn find_index_file(dir: &ffi::OsStr, name: &str) -> Option<ffi::OsString> {
        let iter = fs::read_dir(dir).ok()?;
        let entry = iter
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                match String::try_from(IndexFileName(entry.file_name())) {
                    Ok(nm) if nm == name => Some(entry),
                    _ => None,
                }
            })
            .next();

        entry.map(|entry| entry.file_name())
    }
}

impl<K, V, B> Index<K, V, B> {
    pub fn to_name(&self) -> String {
        self.name.clone()
    }

    pub fn to_app_meta(&self) -> Vec<u8> {
        match &self.metas[0] {
            MetaItem::AppMetadata(data) => data.clone(),
            _ => unreachable!(),
        }
    }

    pub fn to_seqno(&self) -> u64 {
        self.stats.seqno
    }

    pub fn to_stats(&self) -> Stats {
        self.stats.clone()
    }

    pub fn is_compacted(&self) -> bool {
        if self.stats.n_abytes == 0 {
            true
        } else if self.stats.delta_ok {
            true
        } else {
            false
        }
    }

    pub fn iter(&self) -> Result<Iter<K, V>>
    where
        V: mkit::Diff,
    {
        todo!()
    }

    pub fn iter_with_versions(&self) -> Result<Iter<K, V>>
    where
        V: mkit::Diff,
    {
        todo!()
    }
}

pub struct Iter<K, V> {
    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V> Iterator for Iter<K, V>
where
    V: mkit::Diff,
{
    type Item = Result<crate::Item<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

fn validate<K, V, B>(index: &Index<K, V, B>) -> Result<()> {
    todo!()
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

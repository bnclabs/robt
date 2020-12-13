use log::debug;
use mkit::{
    cbor::{Cbor, FromCbor},
    traits::Bloom,
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
    scans::{BitmappedScan, BuildScan},
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
    /// Optional value log file name. If not supplied, but `delta_ok` or
    /// `value_in_vlog` is true, then value log file name will be computed
    /// based on configuration `name` and `dir`. Default: None
    pub vlog_file: Option<ffi::OsString>,
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
            vlog_file: None,
            flush_queue_size: FLUSH_QUEUE_SIZE,
        }
    }

    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> Result<&mut Self> {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        Ok(self)
    }

    /// Enable delta persistence, and configure value-log-file. To disable
    /// delta persistance, pass `vlog_file` as None.
    pub fn set_delta(
        &mut self,
        ok: bool,
        vlog_file: Option<ffi::OsString>,
    ) -> Result<&mut Self> {
        match vlog_file {
            Some(vlog_file) => {
                self.delta_ok = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.delta_ok = true,
            None => self.delta_ok = false,
        }
        Ok(self)
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `ok` as false.
    pub fn set_value_log(
        &mut self,
        ok: bool,
        file: Option<ffi::OsString>,
    ) -> Result<&mut Self> {
        match file {
            Some(vlog_file) => {
                self.value_in_vlog = true;
                self.vlog_file = Some(vlog_file);
            }
            None if ok => self.value_in_vlog = true,
            None => self.value_in_vlog = false,
        }
        Ok(self)
    }

    /// Set flush queue size, increasing the queue size will improve batch
    /// flushing.
    pub fn set_flush_queue_size(&mut self, size: usize) -> Result<&mut Self> {
        self.flush_queue_size = size;
        Ok(self)
    }
}

impl Config {
    fn to_index_file(&self) -> ffi::OsString {
        let file_path: path::PathBuf = [
            self.dir.to_os_string(),
            IndexFileName::from(self.name.clone()).into(),
        ]
        .iter()
        .collect();
        file_path.into_os_string()
    }

    fn to_vlog_file(&self) -> ffi::OsString {
        let file_path: path::PathBuf = [
            self.dir.to_os_string(),
            VlogFileName::from(self.name.clone()).into(),
        ]
        .iter()
        .collect();
        file_path.into_os_string()
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
        let iflush = Flusher::new(&config.to_index_file(), true, queue_size)?;
        let vflush = Flusher::new(&config.to_vlog_file(), true, queue_size)?;

        let val = Builder {
            config,
            iflush,
            vflush,
            stats: Stats::default(),

            initial: true,
            app_meta,
            bitmap: B::create(),
            root: u64::default(),

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
    }

    pub fn incremental(config: Config, app_meta: Vec<u8>) -> Result<Builder<K, V, B>>
    where
        B: Bloom,
    {
        let queue_size = config.flush_queue_size;
        let iflush = Flusher::new(&config.to_index_file(), true, queue_size)?;
        let vflush = Flusher::new(&config.to_vlog_file(), true, queue_size)?;

        let val = Builder {
            config,
            iflush,
            vflush,
            stats: Stats::default(),

            initial: false,
            app_meta,
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
    footprint: isize,
    meta: Arc<Vec<MetaItem>>,
    stats: Stats,
    bitmap: Arc<B>,

    index: fs::File,
    vlog: Option<fs::File>,

    _key: marker::PhantomData<K>,
    _val: marker::PhantomData<V>,
}

impl<K, V, B> Index<K, V, B> {
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, B>>
    where
        B: Bloom,
    {
        let ip = match Self::find_index_file(dir, name) {
            Some(ip) => ip,
            None => err_at!(Invalid, msg: "bad file {:?}/{}", dir, name)?,
        };

        let mut fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?;

        let n = {
            let seek = io::SeekFrom::End(-8);
            let data = read_file!(fd, seek, 8, "reading meta-len from index")?;
            u64::from_be_bytes(data.try_into().unwrap())
        };
        let seek = io::SeekFrom::End(-8 - i64::try_from(n).unwrap());
        let block = read_file!(fd, seek, n, "reading meta-data from index")?;
        let metas = Vec::<MetaItem>::from_cbor(Cbor::decode(&mut block.as_slice())?.0)?;

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

        let vlog = match stats.value_in_vlog {
            true => {
                let vlog_file = stats.vlog_file.as_ref();
                let file_name = match vlog_file.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(file_name)) => file_name.to_os_string(),
                    _ => ffi::OsString::from(VlogFileName::from(name.to_string())),
                };
                let vp: path::PathBuf = [dir.to_os_string(), file_name].iter().collect();
                Some(err_at!(
                    IOError,
                    fs::OpenOptions::new().read(true).open(&vp)
                )?)
            }
            false => None,
        };

        let val = Index {
            dir: dir.to_os_string(),
            name: name.to_string(),
            footprint: isize::try_from(err_at!(IOError, fd.metadata())?.len()).unwrap(),
            meta: Arc::new(metas),
            stats,
            bitmap: Arc::new(bitmap),

            index: fd,
            vlog,

            _key: marker::PhantomData,
            _val: marker::PhantomData,
        };

        Ok(val)
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

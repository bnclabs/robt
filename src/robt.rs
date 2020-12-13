use log::debug;
use mkit::{cbor::Cbor, Cborize, Entry};

use std::{ffi, fs, hash::Hash, io, marker, path, sync::Arc};

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
        let file_path: path::PathBuf =
            [self.dir, IndexFileName::from(self.name.clone()).into()]
                .iter()
                .collect();
        file_path.into_os_string()
    }

    fn to_vlog_file(&self) -> ffi::OsString {
        let file_path: path::PathBuf =
            [self.dir, VlogFileName::from(self.name.clone()).into()]
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
    pub n_abytes: usize,
    /// Number of entries in bitmap.
    pub n_bitmap: usize,

    /// Time take to build this btree.
    pub build_time: u64,
    /// Timestamp when this index was build, from UNIX EPOCH.
    pub epoch: i128,
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
    _value: marker::PhantomData<V>,
}

impl<K, V, B> Builder<K, V, B> {
    pub fn initial(config: Config, app_meta: Vec<u8>) -> Builder<K, V, B> {
        let iflush = Flusher::new(config.to_index_file(), true, config.flush_queue_size);
        let vflush = Flusher::new(config.to_vlog_file(), true, config.flush_queue_size);

        Builder {
            config,
            iflush,
            vflush,
            stats: Stats::default(),

            initial: true,
            app_meta,
            bitmap: B::create(),
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        }
    }

    pub fn incremental(config: Config, app_meta: Vec<u8>) -> Builder<K, V, B> {
        let iflush = Flusher::new(config.to_index_file(), true, config.flush_queue_size);
        let vflush = Flusher::new(config.to_vlog_file(), true, config.flush_queue_size);

        Builder {
            config,
            iflush,
            vflush,
            stats: Stats::default(),

            initial: false,
            app_meta,
            bitmap: B::create(),
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        }
    }
}

impl<K, V, B> Builder<K, V, B> {
    pub fn build_from_iter<I, E>(self, iter: I) -> Result<Stats>
    where
        K: Hash,
        I: Iterator<Item = Result<E>>,
        E: Entry<K, V>,
    {
        let mut iter = {
            let seqno = 0_64;
            BuildScan::new(BitmappedScan::new(iter), seqno)
        };
        let root = self.build_tree(&mut iter)?;
        let (_, bitmap) = iter.unwrap_with(&mut self.stats)?.unwrap()?;

        let stats = {
            self.stats.n_bitmap = bitmap.len();
            self.stats.n_abytes = self.vflusher.map(|f| f.to_start_fpos()).unwrap_or(0);
            self.stats.clone()
        };

        self.root = root;
        self.bitmap = bitmap;

        self.build_flush();

        Ok(stats)
    }

    fn build_tree<I, E>(iter: &mut BuildScan<K, V, I, E>) -> Result<u64>
    where
        K: Hash,
        I: Iterator<Item = Result<E>>,
        E: Entry<K, V>,
    {
        // return root
        todo!()
    }

    fn build_flush(mut self) -> Result<(u64, u64)> {
        let mut block = self.to_meta_blocks()?;
        block.extend_from_slice(&(u64::try_from(block.len()).unwrap().to_be_bytes()));

        self.iflush.post(block)?;

        let len1 = self.iflush.close()?;
        let len2 = self.vflush.close()?;

        Ok((len1, len2))
    }

    fn to_meta_blocks(&self, root: u64, bitmap: Vec<u8>) -> Result<Vec<u8>> {
        let stats = util::encode_to_cbor(self.stats.clone())?;

        debug!(
            target: "robt",
            "{:?}, metablocks root:{} bitmap:{} meta:{}  stats:{}",
            self.iflush.to_file_path(), self.root, self.bitmap.len(),
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
        if (n % Config::MARKER_BLOCK_SIZE) == 0 {
            n
        } else {
            ((n / Config::MARKER_BLOCK_SIZE) + 1) * Config::MARKER_BLOCK_SIZE
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
    Stats(String),
    /// Bloom-filter.
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts.
    Root(u64),
    /// Finger print for robt.
    Marker(Vec<u8>),
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
    _value: marker::PhantomData<V>,
}

impl<K, V, B> Clone for Index<K, V, B> {
    fn clone(&self) -> Self {
        Index {
            dir: self.dir.clone(),
            name: self.name.clone(),
            footprint: self.footprint,
            meta: Arc::clone(&self.meta),
            stats: self.stats.clone(),
            bitmap: Arc::clone(&self.bitmap),
        }
    }
}

impl<K, V, B> Index<K, V, B> {
    pub fn open(dir: &ffi::OsStr, name: &str) -> Result<Index<K, V, B>> {
        let ip = Self::find_index_file(dir, name)
            .ok_or(err_at!(Invalid, msg: "bad file {:?}/{}", dir, name).unwrap_err())?;

        let fd = err_at!(IOError, fs::OpenOptions::new().read(true).open(&ip))?;

        let n = {
            let seek = io::SeekFrom::End(-8);
            u64::from_be_bytes(&read_file!(fd, seek, 8, "reading meta-len from index")?)
        };
        let seek = io::SeekFrom::End(-8 - n);
        let block = read_file!(fd, seek, n, "reading meta-data from index")?;
        let metas = Vec::<MetaItem>::from_cbor(Cbor::decode(&mut block)?)?;

        let stats = match &metas[1] {
            MetaItem::Stats(stats) => Stats::from_cbor(Cbor::decode(&mut stats)?)?,
            _ => unreachable!(),
        };

        let bitmap = match &metas[2] {
            MetaItem::Bitmap(data) => B::from_vec(data)?,
            _ => unreachable!(),
        };

        let vlog = match stats.value_in_vlog {
            true => {
                let vlog_file = stats.vlog_file.as_ref();
                let file_name = match vlog_file.map(|f| path::Path::new(f).file_name()) {
                    Some(Some(file_name)) => file_name,
                    None => ffi::OsString::from(VlogFileName::from(name.to_string())),
                };
                let vp: path::PathBuf = [dir.to_os_string(), vlog_file].iter().collect();
                err_at!(IOError, fs::OpenOptions::new().read(true).open(&vp))?
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
        };

        Ok(val)
    }

    fn find_index_file(dir: &ffi::OsStr, name: &str) -> Option<ffi::OsString> {
        let iter = err_at!(IOError, fs::read_dir(dir))?;
        let entry = iter
            .filter(|entry| entry.ok())
            .filter_map(|entry| {
                match String::try_from(IndexFileName(entry.file_name())) {
                    Ok(nm) if nm == name => Some(entry),
                    Err(_) => None,
                }
            })
            .next();

        entry.map(|entry| entry.file_name())
    }
}

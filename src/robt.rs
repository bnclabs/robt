use mkit::Entry;

use std::marker;

use crate::marker::ROOT_MARKER;

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
    /// Configure differt set of block size for leaf-node, intermediate-node.
    pub fn set_blocksize(
        &mut self,
        z: usize,
        v: usize,
        m: usize,
    ) -> Result<&mut Self> {
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
        file_path.to_os_string()
    }

    fn to_vlog_file(&self) -> ffi::OsString {
        let file_path: path::PathBuf =
            [self.dir, VlogFileName::from(self.name.clone()).into()]
                .iter()
                .collect();
        file_path.to_os_string()
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

pub struct Builder<K, V, B = NoBitmap> {
    config: Config,

    iflush: Flusher,
    vflush: Flusher,

    initial: bool,
    root: u64,
    app_meta: Vec<u8>,
    bitmap: B,
    stats: Stats,
    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
}

impl Builder<K, V, B> {
    pub fn initial(config: Config, app_meta: Vec<u8>) -> Builder {
        let iflush =
            Flusher::new(config.to_index_file(), true, config.flush_queue_size);
        let vflush =
            Flusher::new(config.to_vlog_file(), true, config.flush_queue_size);

        Builder {
            config,
            iflush,
            ivlush,
            stats: Stats::default(),

            initial: true,
            app_meta,
            bitmap: B::create(),
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        }
    }

    pub fn incremental(config: Config, app_meta: Vec<u8>) -> Builder {
        let iflush =
            Flusher::new(config.to_index_file(), true, config.flush_queue_size);
        let vflush =
            Flusher::new(config.to_vlog_file(), true, config.flush_queue_size);

        Builder {
            config,
            iflush,
            ivlush,
            stats: Stats::default(),

            initial: false,
            app_meta,
            bitmap: B::create(),
            _key: marker::PhantomData,
            _value: marker::PhantomData,
        }
    }
}

impl Builder<K, V, B> {
    pub fn build_from_iter<I, E>(iter: I) -> Result<Stats>
    where
        K: Hash,
        I: Iterator<Item = Result<Entry<K, V>>>,
        E: Entry,
    {
        let mut iter = {
            let seqno = 0_64;
            BuildScan::new(scans::BitmappedScan::new(iter), seqno)
        };
        let root = 0_u64; // self.build_tree(&mut iter)?;
        let (_, bitmap) = iter.unwrap_with(&mut self.stats)?.unwrap()?;

        let stats = {
            self.stats.n_bitmap = bitmap.len();
            self.stats.n_abytes =
                self.vflusher.map(|f| f.to_start_fpos()).unwrap_or(0);
            self.stats.clone()
        };

        self.root = root;
        self.bitmap = bitmap;

        self.build_flush();

        Ok(stats)
    }

    fn build_tree<I>(iter: &mut BuildScan<K, V, I>) -> Result<u64>
    where
        K: Hash,
        I: Iterator<Item = Result<Entry<K, V>>>,
        E: Entry,
    {
        // return root
        todo!()
    }

    fn build_flush(mut self) -> Result<(u64, u64)> {
        let block = self.to_meta_blocks()?;
        block.extend_from_slice(&block.len().to_be_bytes());
        block.extend_from_slice(&ROOT_MARKER);

        self.iflush.post(block)?;

        let len1 = self.iflush.close()?;
        let len2 = self.vflush.close()?;

        Ok((len1, len2))
    }

    fn to_meta_blocks(&self, root: u64, bitmap: Vec<u8>) -> Result<Vec<u8>> {
        let stats = {
            let mut buf = vec![];
            let val = self.stats.into_cbor();
            val.encode(&mut buf)?;
            buf
        };

        debug!(
            target: "robt",
            "{:?}, metablocks root:{} bitmap:{} meta:{}  stats:{}",
            file, self.root, self.bitmap.len(), self.app_meta.len(), stats.len(),
        );

        let metas = vec![
            MetaItem::Root(self.root),
            MetaItem::Bitmap(self.bitmap.to_vec()),
            MetaItem::AppMetadata(self.app_meta.clone()),
            MetaItem::Stats(stats),
        ];

        let mut block = vec![];
        let val = self.stats.into_cbor();
        val.encode(&mut block)?;
        block
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
    /// Contains index-statistics along with configuration values.
    Stats(String),
    /// Application supplied metadata, typically serialized and opaque
    /// to [Rdms].
    AppMetadata(Vec<u8>),
    /// Probability data structure, only valid from read_meta_items().
    Bitmap(Vec<u8>),
    /// File-position where the root block for the Btree starts.
    Root(u64),
}

/// Index type, immutable, durable, fully-packed and lockless reads.
pub struct Index<K, V, B> {
    dir: ffi::OsString,
    name: Name,
    footprint: isize,

    stats: Stats,
    meta: Vec<MetaItem>,
    bitmap: Arc<B>,
}

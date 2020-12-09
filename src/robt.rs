use mkit::Entry;

use std::marker;

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
    /// Size of serialized bitmap bytes.
    pub mem_bitmap: usize,
    /// Number of entries in bitmap.
    pub n_bitmap: usize,

    /// Time take to build this btree.
    pub build_time: u64,
    /// Timestamp when this index was build, from UNIX EPOCH.
    pub epoch: i128,
}

pub struct Builder<K, V, B> {
    config: Config,
    iflush: Flusher,
    vflush: Flusher,
    stats: Stats,

    _key: marker::PhantomData<K>,
    _value: marker::PhantomData<V>,
    _bitmap: marker::PhantomData<B>,
}

impl Builder<K, V, B> {
    fn new(config: Config) -> Builder {
        Builder {
            config,
            stats: Stats::default(),
            iflusher: Flusher,
            _key: marker::PhantomData,
            _value: marker::PhantomData,
            _bitmap: marker::PhantomData,
        }
    }

    fn build_from_iter<E>(iter: impl Iterator<Item = E>) -> Result<Stats>
    where
        E: Entry,
    {
        todo!()
    }
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

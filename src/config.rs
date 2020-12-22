use std::ffi;

use mkit::Cborize;

/// Default value for z-block-size, 4 * 1024 bytes.
pub const ZBLOCKSIZE: usize = 4 * 1024; // 4KB leaf node
/// Default value for m-block-size, 4 * 1024 bytes.
pub const MBLOCKSIZE: usize = 4 * 1024; // 4KB intermediate node
/// Default value for v-block-size, 4 * 1024 bytes.
pub const VBLOCKSIZE: usize = 4 * 1024; // 4KB of blobs.

/// Default value for Flush queue size, channel queue size, holding
/// index blocks.
pub const FLUSH_QUEUE_SIZE: usize = 64;

const STATS_VER1: u32 = 0x0001;

/// Configuration for Read Only BTree index, configuration type is used
/// only build an index. Subsequently configuration parameters are
/// persisted along with the index.
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
    /// Create a new configuration value, use the `set_*` methods to
    /// add more configuration.
    pub fn new(dir: &ffi::OsStr, name: &str) -> Config {
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
    pub fn set_delta(&mut self, delta_ok: bool) -> &mut Self {
        self.delta_ok = delta_ok;
        self
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `ok` as false.
    pub fn set_value_log(&mut self, value_log: bool) -> &mut Self {
        self.value_in_vlog = value_log;
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
    const ID: u32 = STATS_VER1;
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

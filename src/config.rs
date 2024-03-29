use mkit::Cborize;

use std::{ffi, path};

use crate::files::{IndexFileName, VlogFileName};

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

pub fn to_index_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
    let file_path: path::PathBuf =
        [dir.to_os_string(), IndexFileName::from(name.to_string()).into()]
            .iter()
            .collect();
    file_path.into_os_string()
}

pub fn to_vlog_file(dir: &ffi::OsStr, name: &str) -> ffi::OsString {
    let file_path: path::PathBuf =
        [dir.to_os_string(), VlogFileName::from(name.to_string()).into()]
            .iter()
            .collect();
    file_path.into_os_string()
}

/// Configuration for Read Only BTree index.
///
/// Configuration type is used only for building an index. Subsequently,
/// configuration parameters are persisted along with the index.
#[derive(Clone, Debug)]
pub struct Config {
    /// location path where index files are created.
    pub dir: ffi::OsString,
    /// name of the index.
    pub name: String,
    /// Leaf block size in btree index.
    /// Default: [ZBLOCKSIZE]
    pub z_blocksize: usize,
    /// Intermediate block size in btree index.
    /// Default: [MBLOCKSIZE]
    pub m_blocksize: usize,
    /// If deltas are indexed and/or value to be stored in separate log file.
    /// Default: [VBLOCKSIZE]
    pub v_blocksize: usize,
    /// Include delta as part of entry. Note that delta values are always
    /// stored in separate value-log file.
    /// Default: true
    pub delta_ok: bool,
    /// If true, then value shall be persisted in a separate file called
    /// value log file. Otherwise value shall be saved in the index's
    /// leaf node. Default: false
    pub value_in_vlog: bool,
    /// Flush queue size. Default: [FLUSH_QUEUE_SIZE]
    pub flush_queue_size: usize,
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

    /// Configure block size for leaf-node, intermediate-node, and value-log
    pub fn set_blocksize(&mut self, z: usize, v: usize, m: usize) -> &mut Self {
        self.z_blocksize = z;
        self.v_blocksize = v;
        self.m_blocksize = m;
        self
    }

    /// Enable delta persistence. If `delta_ok` is false, older versions of value
    /// shall be ignored.
    pub fn set_delta(&mut self, delta_ok: bool) -> &mut Self {
        self.delta_ok = delta_ok;
        self
    }

    /// Persist values in a separate file, called value-log file. To persist
    /// values along with leaf node, pass `value_log` as false.
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

impl Config {
    pub fn to_index_file_location(&self) -> ffi::OsString {
        to_index_file(&self.dir, &self.name)
    }

    pub fn to_vlog_file_location(&self) -> ffi::OsString {
        to_vlog_file(&self.dir, &self.name)
    }
}

/// Statistic for Read Only BTree index.
#[derive(Clone, Default, Debug, Cborize)]
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
    pub value_in_vlog: bool,

    /// Optional value log file if either `value_in_log` or `delta_ok` is true.
    pub vlog_file: Option<ffi::OsString>,

    /// Number of entries indexed.
    pub n_count: u64,
    /// Number of entries that are marked as deleted.
    pub n_deleted: usize,
    /// Sequence number for the latest entry.
    pub seqno: u64,
    /// Older size of value-log file, applicable only in incremental build.
    pub n_abytes: u64,

    /// Time taken to build this btree.
    pub build_time: u64,
    /// Timestamp when this index was built, from UNIX EPOCH, in secs.
    pub epoch: u64,
}

impl Stats {
    const ID: u32 = STATS_VER1;
}

impl From<Config> for Stats {
    fn from(config: Config) -> Stats {
        Stats {
            name: config.name.clone(),
            z_blocksize: config.z_blocksize,
            m_blocksize: config.m_blocksize,
            v_blocksize: config.v_blocksize,
            delta_ok: config.delta_ok,
            vlog_file: Option::default(),
            value_in_vlog: config.value_in_vlog,
            n_count: u64::default(),
            n_deleted: usize::default(),
            seqno: u64::default(),
            n_abytes: u64::default(),
            build_time: u64::default(),
            epoch: u64::default(),
        }
    }
}

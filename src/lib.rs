//! Package implement an immutable read-only BTree index.
//!
//! Use [Builder] type to build a new index. And subsequently load the
//! index using the [Index] type. Index can be concurrently accessed by
//! cloning the `Index` type. Note that a single Index instance cannot be
//! shared among threads. Once an index is built using the `Builder` type
//! it is not possible to modify them. While strict immutablility might
//! seem like an inconvinience, they have certain advantages,
//!
//! * They are fully packed and hence less overhead and lesser tree depth.
//! * Easy and efficient caching of btree-blocks.
//!
//! **Inventory of features**
//!
//! * Index can be parametrized over Key-type and Value-type.
//! * Uses [Cbor][cbor] for serialization and deserialization.
//! * Key and Value types can be made `robt` compliant by `derive(Cborize)`.
//! * Value can either be stored in leaf-node or in a separate log-file.
//! * Additionally, incoming iterator, to build index, can supply older
//!   versions for value using the [Diff] mechanics.
//! * Bloom filter can help optimize false lookups.
//! * API `get()` operation, with bloom-filter support.
//! * API `iter()` and `reverse()` operation for forward and reverse iteration.
//! * API `iter_version()` and `reverse_version()` operation similar to
//!   iter/reverse but also fetches older versions for a entry. Note that
//!   iter/reverse do not fetch the older versions.
//!
//! **Value-log file**
//!
//! Values and its deltas (older versions) can be stored in a separate log
//! file. This has following advantage,
//!
//! * Keep the leaf-node extremely compact and help better caching.
//! * Efficient when building multi-level index.
//! * Applications typically deal with older-versions as archives.
//!
//! While storing value in the value-log file is optional, deltas are always
//! stored in separate value-log file.
//!
//! **Building an index**
//!
//! Unlike mutable data-structure, that support `set()`, `write()`,
//! `update()` etc.. `robt` indexes are built from pre-sorted iterators.
//! In a way each btree index can be seen as an immutable snapshot of
//! sorted `{key,value}` dataset.
//! Typical workflow is,
//!
//! ```ignore
//! use mkit::traits::BuildIndex;
//!
//! let config = Config::new("/opt/data/", "movies");
//! // use one or more set_ method to configure the btree parameters.
//! let builder = Build::initial(config, app_meta);
//! builder.from_iter(iter, NoBitmap);
//! ```
//!
//! Let us look at the steps one by one:
//!
//! * First create a configuration. More configurations available via the
//!  `set_` methods.
//! * By supplying `app_meta`, caller can also persist snapshot specific
//!   meta-data.
//! * After creating a builder, use `BuildIndex` trait's `from_iter()` to
//!   build a btree-index from an iterator. It is expected that iterated
//!   entries are pre-sorted.
//! * Caller can optionally pass a bitmap instance that shall be used
//!   for implementing a [bloom filter][bloom-filter].
//! * Bitmap type is parametrized via the `BuildIndex` trait. If
//!   probablistic bitmap table is not required, pass `NoBitmap` value
//!   to `from_iter()` method.
//!
//! In the example above, we are using `initial()` constructor to create
//! a builder instance, it is also possible to incrementally build an
//! index via `incremental()` constructor. To understand the difference
//! we shall dig deeper into how data-set is indexed with `robt`.
//!
//! `robt` is a simple btree-index, made up of `root-node`,
//! `intermediate-node` (called m-block) and `leaf-node` (called z-block).
//! The entire dataset is maintained in the leaf node and the intermediate
//! nodes are constructed in bottoms-up fashion using the first-key in the
//! leaf-node, all the way up to the root-node. The shape and behaviour of
//! root-node is exactly same as the `intermediate-node`.
//!
//! The dataset is made up of entries and each entires is made up of key,
//! value, seqno, a flag to denoted wether the node was deleted or upserted.
//! Reason for maintaining seqno, and deleted-flag is to support database
//! features like vector-timestamping, log-structured-merge etc..
//!
//! **Version control your values**, an additional feature with `robt`
//! index is that applications can version control their values. That is,
//! each entry, along with key, value, seqno, etc.. also maintains previous
//! version of the value along with its modification seqno. And instead of
//! persisting the entire value (older versions), their deltas as computed
//! in relation to its new-versions and persisted as deltas. This is
//! achieved using the [Diff] mechanics. Also note that `robt` itself
//! doesn't compute the version deltas, but it is treated as part of an
//! entry and persisted.
//!
//! Each entry in the index is defined as Entry<K, V, D> type and defined
//! in a common crate. Note that an index entry is parametrised over
//! key-type, value-type, and delta-type. Here delta-type `D` can be
//! `NoDiff` if application is not interested in preserving older-versions
//! or should be same as `<V as Diff>::D`. Refer to [Diff] mechanics for
//! more detail.
//!
//! Now coming back to the leaf-node, all entries are stored in the
//! leaf-node. And to facilitate archival of older versions `deltas`
//! are persisted in a seperate value-log file. And optionally, to
//! facilitate incremental build, value can also be persisted in the
//! value-log file. When both values and deltas are persisted in a
//! separate value-log file, leaf nodes become very compact and ends
//! up suitable for caching, compaction, incremental-build, optimized
//! IOPS and delta-archival.
//!
//! **Reading from index**
//!
//! **Simple Key-Value index**
//!
//! **Index Entry**
//!
//! **Compaction**
//!
//! Compaction is the process of de-duplicating/removing entries
//! and/or entry-versions from an index instance. In `robt` there
//! are three types of compaction.
//!
//! _deduplication_
//!
//! When same value-log file is used to incrementally build newer
//! batch of mutations older values gets duplicated. This requires
//! a periodic clean up of garbage values to reduce disk foot-print.
//!
//! _mono-compaction_
//!
//! This is applicable for index instances that do not need distributed
//! LSM. In such cases, the oldest-level's snapshot can compact away older
//! versions of each entry and purge entries that are marked deleted.
//!
//! _lsm-compaction_
//!
//! Robt, unlike other lsm-based-storage, can have the entire index
//! as LSM for distributed database designs. To be more precise, in lsm
//! mode, even the root level that holds the entire dataset can retain
//! older versions. With this feature it is possible to design secondary
//! indexes, network distribution and other features like `backup` and
//! `archival` ensuring consistency. This also means the index footprint
//! will indefinitely accumulate older versions. With limited disk space,
//! it is upto the application logic to issue `lsm-compaction` when
//! it is safe to purge entries/versions that are older than certain seqno.
//!
//! _tombstone-compaction_
//!
//! Tombstone compaction is similar to `lsm-compaction` with one main
//! difference. When application logic issue `tombstone-compaction` only
//! deleted entries that are older than specified seqno will be purged.
//!
//! [bloom-filter]: https://en.wikipedia.org/wiki/Bloom_filter
//! [cbor]: https://en.wikipedia.org/wiki/CBOR

#[allow(unused_imports)]
use mkit::traits::Diff;

use std::{error, fmt, result};

macro_rules! read_file {
    ($fd:expr, $seek:expr, $n:expr, $msg:expr) => {{
        use std::{
            convert::TryFrom,
            io::{Read, Seek},
        };

        match $fd.seek($seek) {
            Ok(_) => {
                let mut buf = vec![0; usize::try_from($n).unwrap()];
                match $fd.read(&mut buf) {
                    Ok(n) if buf.len() == n => Ok(buf),
                    Ok(n) => {
                        let m = buf.len();
                        err_at!(Fatal, msg: concat!($msg, " {}/{} at {:?}"), m, n, $seek)
                    }
                    Err(err) => err_at!(IOError, Err(err)),
                }
            }
            Err(err) => err_at!(IOError, Err(err)),
        }
    }};
}

macro_rules! write_file {
    ($fd:expr, $buffer:expr, $file:expr, $msg:expr) => {{
        use std::io::Write;

        match err_at!(IOError, $fd.write($buffer))? {
            n if $buffer.len() == n => Ok(n),
            n => err_at!(
                Fatal, msg: "partial-wr {}, {:?}, {}/{}", $msg, $file, $buffer.len(), n
            ),
        }
    }};
}

macro_rules! iter_result {
    ($res:expr) => {{
        match $res {
            Ok(res) => res,
            Err(err) => return Some(Err(err.into())),
        }
    }};
}

/// Short form to compose Error values.
///
/// Here are few possible ways:
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, msg: format!("bad argument"));
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::io::read(buf));
/// ```
///
/// ```ignore
/// use crate::Error;
/// err_at!(ParseError, std::fs::read(file_path), format!("read failed"));
/// ```
///
macro_rules! err_at {
    ($v:ident, msg: $($arg:expr),+) => {{
        let prefix = format!("{}:{}", file!(), line!());
        Err(Error::$v(prefix, format!($($arg),+)))
    }};
    ($v:ident, $e:expr) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                Err(Error::$v(prefix, format!("{}", err)))
            }
        }
    }};
    ($v:ident, $e:expr, $($arg:expr),+) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                let msg = format!($($arg),+);
                Err(Error::$v(prefix, format!("{} {}", err, msg)))
            }
        }
    }};
}

mod build;
mod config;
mod entry;
mod files;
mod flush;
mod marker;
mod reader;
mod robt;
mod scans;
mod util;
mod vlog;

/// Place holder type to skip bloom filter for robt index.
#[derive(Clone)]
pub struct NoBitmap;

pub use config::{Config, Stats, FLUSH_QUEUE_SIZE, MBLOCKSIZE, VBLOCKSIZE, ZBLOCKSIZE};
pub use robt::{Builder, Index};

/// Type alias for Result return type, used by this package.
pub type Result<T> = result::Result<T, Error>;

/// Error variants that can be returned by this package's API.
///
/// Each variant carries a prefix, typically identifying the
/// error location.
pub enum Error {
    FailConvert(String, String),
    FailCbor(String, String),
    IOError(String, String),
    Fatal(String, String),
    Invalid(String, String),
    IPCFail(String, String),
    ThreadFail(String, String),
    InvalidFile(String, String),
    KeyNotFound(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use Error::*;

        match self {
            FailConvert(p, msg) => write!(f, "{} FailConvert: {}", p, msg),
            FailCbor(p, msg) => write!(f, "{} FailCbor: {}", p, msg),
            IOError(p, msg) => write!(f, "{} IOError: {}", p, msg),
            Fatal(p, msg) => write!(f, "{} Fatal: {}", p, msg),
            Invalid(p, msg) => write!(f, "{} Invalid: {}", p, msg),
            IPCFail(p, msg) => write!(f, "{} IPCFail: {}", p, msg),
            ThreadFail(p, msg) => write!(f, "{} ThreadFail: {}", p, msg),
            InvalidFile(p, msg) => write!(f, "{} InvalidFile: {}", p, msg),
            KeyNotFound(p, msg) => write!(f, "{} KeyNotFound: {}", p, msg),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

impl error::Error for Error {}

impl From<mkit::Error> for Error {
    fn from(err: mkit::Error) -> Error {
        match err {
            mkit::Error::Fatal(p, m) => Error::Fatal(p, m),
            mkit::Error::FailConvert(p, m) => Error::FailConvert(p, m),
            mkit::Error::IOError(p, m) => Error::IOError(p, m),
            mkit::Error::FailCbor(p, m) => Error::FailCbor(p, m),
            mkit::Error::IPCFail(p, m) => Error::IPCFail(p, m),
            mkit::Error::ThreadFail(p, m) => Error::ThreadFail(p, m),
        }
    }
}

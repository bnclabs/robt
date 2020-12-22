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
#[macro_export]
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

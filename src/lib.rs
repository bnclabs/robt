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

use std::{error, fmt, ops::Bound, result};

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

        let n = err_at!(IOError, $fd.write($buffer))?;
        if $buffer.len() == n {
            Ok(n)
        } else {
            err_at!(
                Fatal,
                msg: "{}, {:?}, {}/{}", $msg, $file, $buffer.len(), n
            )
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

mod entry;
mod files;
mod flush;
mod marker;
mod nobitmap;
mod robt;
mod scans;
mod util;
mod vlog;

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

pub struct Item<K, V>
where
    V: mkit::Diff,
{
    key: K,
    value: Value<V>,
    deltas: Vec<Delta<<V as mkit::Diff>::D>>,
}

pub enum Value<V> {
    U { value: vlog::Value<V>, seqno: u64 },
    D { seqno: u64 },
}

pub enum Delta<D> {
    U { delta: vlog::Delta<D>, seqno: u64 },
    D { seqno: u64 },
}

impl<D> Delta<D> {
    fn to_seqno(&self) -> u64 {
        match self {
            Delta::U { seqno, .. } => *seqno,
            Delta::D { seqno } => *seqno,
        }
    }
}

impl<K, V> mkit::Entry<K, V> for Item<K, V>
where
    V: mkit::Diff,
{
    fn as_key(&self) -> &K {
        &self.key
    }

    fn is_deleted(&self) -> bool {
        match &self.value {
            Value::U { .. } => false,
            Value::D { .. } => true,
        }
    }

    fn to_seqno(&self) -> u64 {
        match &self.value {
            Value::U { seqno, .. } => *seqno,
            Value::D { seqno } => *seqno,
        }
    }

    fn purge(mut self, cutoff: mkit::Cutoff) -> Option<Self>
    where
        Self: Sized,
    {
        let n = self.to_seqno();

        let cutoff = match cutoff {
            mkit::Cutoff::Mono if self.is_deleted() => return None,
            mkit::Cutoff::Mono => {
                self.deltas = vec![];
                return Some(self);
            }
            mkit::Cutoff::Lsm(cutoff) => cutoff,
            mkit::Cutoff::Tombstone(cutoff) if self.is_deleted() => match cutoff {
                Bound::Included(cutoff) if n <= cutoff => return None,
                Bound::Excluded(cutoff) if n < cutoff => return None,
                Bound::Unbounded => return None,
                _ => return Some(self),
            },
            mkit::Cutoff::Tombstone(_) => return Some(self),
        };

        // If all versions of this entry are before cutoff, then purge entry
        match cutoff {
            Bound::Included(std::u64::MIN) => return Some(self),
            Bound::Excluded(std::u64::MIN) => return Some(self),
            Bound::Included(cutoff) if n <= cutoff => return None,
            Bound::Excluded(cutoff) if n < cutoff => return None,
            Bound::Unbounded => return None,
            _ => (),
        }
        // Otherwise, purge only those versions that are before cutoff
        self.deltas = self
            .deltas
            .drain(..)
            .take_while(|d| {
                let seqno = d.to_seqno();
                match cutoff {
                    Bound::Included(cutoff) if seqno > cutoff => true,
                    Bound::Excluded(cutoff) if seqno >= cutoff => true,
                    _ => false,
                }
            })
            .collect();
        Some(self)
    }
}

use std::{error, fmt, result};

macro_rules! read_file {
    ($fd:expr, $fpos:expr, $n:expr, $msg:expr) => {{
        use std::io::{Read, Seek};
        match $fd.seek(io::SeekFrom::Start($fpos)) {
            Ok(_) => {
                let mut buf = {
                    let mut buf = Vec::with_capacity($n as usize);
                    buf.resize(buf.capacity(), 0);
                    buf
                };
                match $fd.read(&mut buf) {
                    Ok(n) if buf.len() == n => Ok(buf),
                    Ok(n) => {
                        let m = buf.len();
                        err_at!(Fatal, msg: concat!($msg, " {}/{} at {}"), m, n, $fpos)
                    }
                    Err(err) => err_at!(IOError, Err(err)),
                }
            }
            Err(err) => err_at!(IOError, Err(err)),
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

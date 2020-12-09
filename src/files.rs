use std::{convert::TryFrom, ffi, fmt, path, result};

use crate::{Error, Result};

#[derive(Clone)]
struct IndexFileName(ffi::OsString);

impl From<String> for IndexFileName {
    fn from(name: String) -> IndexFileName {
        let file_name = format!("{}-robt.indx", name);
        let name: &ffi::OsStr = file_name.as_ref();
        IndexFileName(name.to_os_string())
    }
}

impl TryFrom<IndexFileName> for String {
    type Error = Error;

    fn try_from(fname: IndexFileName) -> Result<String> {
        let ffpp = path::Path::new(&fname.0);
        let fname = || -> Option<String> {
            let fname = ffpp.file_name()?;
            if fname.to_str()?.ends_with("-robt.indx") {
                Some(path::Path::new(fname).file_stem()?.to_str()?.to_string())
            } else {
                None
            }
        }();

        match fname {
            Some(fname) => Ok(fname),
            None => err_at!(InvalidFile, msg: "{:?}", ffpp),
        }
    }
}

impl From<IndexFileName> for ffi::OsString {
    fn from(name: IndexFileName) -> ffi::OsString {
        name.0
    }
}

impl fmt::Display for IndexFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

#[derive(Clone)]
struct VlogFileName(ffi::OsString);

impl From<String> for VlogFileName {
    fn from(name: String) -> VlogFileName {
        let file_name = format!("{}-robt.vlog", name);
        let name: &ffi::OsStr = file_name.as_ref();
        VlogFileName(name.to_os_string())
    }
}

impl TryFrom<VlogFileName> for String {
    type Error = Error;

    fn try_from(fname: VlogFileName) -> Result<String> {
        let ffpp = path::Path::new(&fname.0);

        let fname = || -> Option<String> {
            let fname = ffpp.file_name()?;
            if fname.to_str()?.ends_with("-robt.vlog") {
                Some(path::Path::new(fname).file_stem()?.to_str()?.to_string())
            } else {
                None
            }
        }();

        match fname {
            Some(fname) => Ok(fname),
            None => err_at!(InvalidFile, msg: "{:?}", ffpp),
        }
    }
}

impl fmt::Display for VlogFileName {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        match self.0.to_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, "{:?}", self.0),
        }
    }
}

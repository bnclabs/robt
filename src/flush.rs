use fs2::FileExt;
use log::{info, trace};
use mkit::thread;

use std::{convert::TryFrom, ffi, fs, mem, path};

use crate::{Error, Result};

pub enum Flusher {
    File {
        file_path: ffi::OsString,
        fpos: u64,
        th: Option<thread::Thread<Vec<u8>, u64, Result<u64>>>,
        tx: Option<thread::Tx<Vec<u8>, u64>>,
    },
    None,
}

impl Drop for Flusher {
    fn drop(&mut self) {
        match self {
            Flusher::File { file_path, .. } => {
                info!( target: "robt", "dropping flusher for {:?}", file_path);
            }
            Flusher::None => (),
        }
    }
}

impl Flusher {
    pub fn new(
        file_path: &ffi::OsStr,
        create: bool,
        flush_queue_size: usize,
    ) -> Result<Flusher> {
        let (fd, fpos) = if create {
            (create_file_a(file_path)?, 0)
        } else {
            let fpos = err_at!(IOError, fs::metadata(file_path))?.len();
            (open_file_a(file_path)?, fpos)
        };

        let ffpp = file_path.to_os_string();
        let (th, tx) = thread::Thread::new_sync(
            "flusher",
            flush_queue_size,
            move |rx: thread::Rx<Vec<u8>, u64>| move || thread_flush(ffpp, fd, rx, fpos),
        );

        let val = Flusher::File {
            file_path: file_path.to_os_string(),
            fpos,
            th: Some(th),
            tx: Some(tx),
        };

        Ok(val)
    }

    pub fn empty() -> Flusher {
        Flusher::None
    }

    pub fn to_file_path(&self) -> ffi::OsString {
        match self {
            Flusher::File { file_path, .. } => file_path.clone(),
            Flusher::None => unreachable!(),
        }
    }

    pub fn to_fpos(&self) -> Option<u64> {
        match self {
            Flusher::File { fpos, .. } => Some(*fpos),
            Flusher::None => None,
        }
    }

    pub fn flush(&mut self, data: Vec<u8>) -> Result<()> {
        match self {
            Flusher::File { fpos, tx, .. } => {
                *fpos = tx.as_ref().unwrap().request(data)?
            }
            Flusher::None => unreachable!(),
        };
        Ok(())
    }

    pub fn close(mut self) -> Result<u64> {
        match &mut self {
            Flusher::File { tx, th, .. } => {
                mem::drop(tx.take());
                th.take().unwrap().join()?
            }
            Flusher::None => Ok(0),
        }
    }
}

fn thread_flush(
    file_path: ffi::OsString,
    mut fd: fs::File,
    rx: thread::Rx<Vec<u8>, u64>,
    mut fpos: u64,
) -> Result<u64> {
    info!(target: "robt", "starting flusher for {:?} @ fpos {}", file_path, fpos);

    err_at!(
        IOError,
        fd.lock_shared(),
        "fail read lock for {:?}",
        file_path
    )?;

    for (data, res_tx) in rx {
        let n = write_file!(fd, &data, &file_path, "flushing file")?;
        if n != data.len() {
            err_at!(IOError, fd.unlock(), "fail read unlock {:?}", file_path)?;
            err_at!(IOError, msg: "partial flush for {:?}, {} != {}", file_path, n, data.len())?;
        }

        fpos += u64::try_from(data.len()).unwrap();
        trace!(
            target: "robt",
            "flusher {:?} {} {}",
            file_path,
            data.len(),
            fpos
        );
        res_tx.map(|tx| tx.send(fpos).ok());
    }

    err_at!(IOError, fd.sync_all(), "fail sync_all {:?}", file_path)?;
    err_at!(IOError, fd.unlock(), "fail read unlock {:?}", file_path)?;

    Ok(fpos)
}

// create a file in append mode for writing.
fn create_file_a(file_path: &ffi::OsStr) -> Result<fs::File> {
    let os_file = {
        let os_file = path::Path::new(file_path);
        fs::remove_file(os_file).ok(); // NOTE: ignore remove errors.
        os_file
    };

    match os_file.parent() {
        Some(parent) => err_at!(IOError, fs::create_dir_all(parent))?,
        None => err_at!(InvalidFile, msg: "{:?}", file_path)?,
    };

    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(
        IOError,
        opts.append(true).create_new(true).open(os_file)
    )?)
}

// open existing file in append mode for writing.
fn open_file_a(file_path: &ffi::OsStr) -> Result<fs::File> {
    let os_file = path::Path::new(file_path);
    let mut opts = fs::OpenOptions::new();
    Ok(err_at!(IOError, opts.append(true).open(os_file))?)
}

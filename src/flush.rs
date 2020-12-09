use fs2::FileExt;
use log::{info, trace};
use mkit::thread;

use std::{convert::TryFrom, ffi, fs, path};

use crate::{Error, Result};

struct Flusher {
    file_path: ffi::OsString,
    th: Option<thread::Thread<Vec<u8>, (), Result<u64>>>,
    tx: thread::Tx<Vec<u8>, ()>,
}

impl Drop for Flusher {
    fn drop(&mut self) {
        info!(target: "robt", "dropping flusher for {:?}", self.file_path);
    }
}

impl Flusher {
    fn new(
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
            move |rx: thread::Rx<Vec<u8>, ()>| {
                move || thread_flush(ffpp, fd, rx, fpos)
            },
        );

        let val = Flusher {
            file_path: file_path.to_os_string(),
            th: Some(th),
            tx,
        };
        Ok(val)
    }

    fn close(mut self) -> Result<u64> {
        self.th.take().unwrap().join()?
    }
}

fn thread_flush(
    file_path: ffi::OsString,
    mut fd: fs::File,
    rx: thread::Rx<Vec<u8>, ()>,
    mut fpos: u64,
) -> Result<u64> {
    info!(target: "robt", "starting flusher for {:?}", file_path);

    err_at!(
        IOError,
        fd.lock_shared(),
        "fail read lock for {:?}",
        file_path
    )?;

    for (data, _) in rx {
        fpos += u64::try_from(data.len()).unwrap();

        trace!(
            target: "robt",
            "flusher {:?} {} {}",
            file_path,
            data.len(),
            fpos
        );

        let n = write_file!(fd, &data, &file_path, "flushing file")?;

        if n != data.len() {
            err_at!(IOError, fd.unlock(), "fail read unlock {:?}", file_path)?;
            err_at!(IOError, msg: "partial flush for {:?}, {} != {}", file_path, n, data.len())?;
        }
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
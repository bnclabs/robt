use mkit::{cbor::FromCbor, db::Bloom, NoBitmap};
use structopt::StructOpt;

use std::{ffi, fmt};

use robt::db::Index;

/// Command line options.
#[derive(Clone, StructOpt)]
pub struct Opt {
    index_file: ffi::OsString,
}

fn main() {
    let opts = Opt::from_args();
    run::<u64, u64, u64, NoBitmap>(opts)
}

fn run<K, V, D, B>(opts: Opt)
where
    K: Clone + FromCbor + fmt::Debug,
    V: Clone + FromCbor + fmt::Debug,
    D: Clone + FromCbor + fmt::Debug,
    B: Bloom,
{
    let mut index = Index::<K, V, D, B>::open_file(&opts.index_file).unwrap();
    index.print().unwrap()
}

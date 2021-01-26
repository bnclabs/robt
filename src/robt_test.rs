use arbitrary::{self, unstructured::Unstructured, Arbitrary};
use mkit::nobitmap::NoBitmap;
use ppom::Mdb;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};
use xorfilter::Xor8;

use std::{cmp::Ordering, thread};

use super::*;

#[test]
fn test_robt_read() {
    let seed: u128 = random();
    let seed: u128 = 113769300070107559875811256583873123132;
    println!("test_robt_read {}", seed);

    // initial build

    let dir = std::env::temp_dir().join("test_robt_read");
    let name = "test_robt_read";
    let config = Config {
        dir: dir.as_os_str().to_os_string(),
        name: name.to_string(),
        z_blocksize: 4096,
        m_blocksize: 4096,
        v_blocksize: 4096,
        delta_ok: false,
        value_in_vlog: false,
        flush_queue_size: 32,
    };
    println!(
        "test_robt_read index file {:?}",
        config.to_index_file_location()
    );

    let testcases = [
        ("nodiff", "nobitmap"),
        ("nodiff", "xor"),
        ("diff", "nobitmap"),
        ("diff", "xor"),
    ];

    let n = 1_000_000;
    for (diff, bitmap) in testcases.iter() {
        let handles = match (*diff, *bitmap) {
            ("nodiff", "nobitmap") => {
                let mdb = util::load_index(seed, 100_000, 0, 100, 0);

                let appmd = "test_robt_read-metadata".as_bytes().to_vec();
                let mut build = Builder::initial(config.clone(), appmd.clone()).unwrap();
                build.build_index(mdb.iter().unwrap(), NoBitmap).unwrap();

                let mut handles = vec![];
                for _i in 0..16 {
                    let (dir, name) = (dir.clone().into_os_string(), name.to_string());
                    let (config, mdb) = (config.clone(), mdb.clone());
                    handles.push(thread::spawn(move || {
                        run_test_robt::<NoBitmap>(seed, dir, name, config, mdb)
                    }));
                }
                handles
            }
            ("nodiff", "xor") => {
                let mdb = util::load_index(seed, 100_000, 0, 100, 0);

                let appmd = "test_robt_read-metadata".as_bytes().to_vec();
                let mut build = Builder::initial(config.clone(), appmd.clone()).unwrap();
                build.build_index(mdb.iter().unwrap(), Xor8::new()).unwrap();

                let mut handles = vec![];
                for _i in 0..16 {
                    let (dir, name) = (dir.clone().into_os_string(), name.to_string());
                    let (config, mdb) = (config.clone(), mdb.clone());
                    handles.push(thread::spawn(move || {
                        run_test_robt::<Xor8>(seed, dir, name, config, mdb)
                    }));
                }
                handles
            }
            ("diff", "nobitmap") => {
                let mdb = util::load_index(seed, 100_000, 0, 100, 0);

                let appmd = "test_robt_read-metadata".as_bytes().to_vec();
                let mut build = Builder::initial(config.clone(), appmd.clone()).unwrap();
                build.build_index(mdb.iter().unwrap(), NoBitmap).unwrap();

                let mut handles = vec![];
                for _i in 0..16 {
                    let (dir, name) = (dir.clone().into_os_string(), name.to_string());
                    let (config, mdb) = (config.clone(), mdb.clone());
                    handles.push(thread::spawn(move || {
                        run_test_robt::<NoBitmap>(seed, dir, name, config, mdb)
                    }));
                }
                handles
            }
            ("diff", "xor") => {
                let mdb = util::load_index(seed, 100_000, 0, 100, 0);

                let appmd = "test_robt_read-metadata".as_bytes().to_vec();
                let mut build = Builder::initial(config.clone(), appmd.clone()).unwrap();
                build.build_index(mdb.iter().unwrap(), Xor8::new()).unwrap();

                let mut handles = vec![];
                for _i in 0..16 {
                    let (dir, name) = (dir.clone().into_os_string(), name.to_string());
                    let (config, mdb) = (config.clone(), mdb.clone());
                    handles.push(thread::spawn(move || {
                        run_test_robt::<Xor8>(seed, dir, name, config, mdb)
                    }));
                }
                handles
            }
            (_, _) => unreachable!(),
        };
    }
}

fn run_test_robt<B>(
    seed: u128,
    dir: ffi::OsString,
    name: String,
    config: Config,
    mdb: Mdb<u16, u64, u64>,
) where
    B: Bloom,
{
    use Error::KeyNotFound;

    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut index = {
        let dir = dir.as_os_str();
        let file = config.to_index_file_location();
        open_index::<B>(dir, &name, &file, seed)
    };

    let bytes = rng.gen::<[u8; 32]>();
    let mut uns = Unstructured::new(&bytes);

    let mut counts = [0_usize; 14];
    let n = 1_000

    for _i in 0..n {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);

        let op: Op<u64> = uns.arbitrary().unwrap();
        match op.clone() {
            Op::Mo(meta_op) => match meta_op {
                Name => assert_eq!(index.to_name(), name.to_string()),
                Stats => {
                    let stats = index.to_stats();
                    validate_stats(&stats, &config, None, None, None, 100100, 0);
                }
                AppMetadata => assert_eq!(index.to_app_metadata(), app_meta_data),
                Seqno => assert_eq!(index.to_seqno(), mdb.to_seqno()),
                IsCompacted => assert_eq!(index.is_compacted(), true),
                Len => assert_eq!(index.len(), mdb.len()),
                IsEmpty => assert_eq!(index.is_empty(), false),
            },
            Op::Get(key) => {
                match (index.get(&key), mdb.get(&key)) {
                    (Ok(e1), Ok(e2)) => assert_eq!(e1, e2),
                    (Err(KeyNotFound(_, _)), Err(ppom::Error::KeyNotFound(_, _))) => (),
                    (Err(err1), Err(err2)) => panic!("{} != {}", err1, err2),
                    (Ok(e), Err(err)) => panic!("{:?} != {}", e, err),
                    (Err(err), Ok(e)) => panic!("{} != {:?}", err, e),
                }
                counts[8] += 1;
            }
            Op::Iter((l, h)) => {
                counts[9] += 1;
                let _: Vec<Entry> = index.iter().unwrap().collect();
                (0, 0)
            }
            Op::Reverse((l, h)) => {
                counts[11] += 1;
                let r = (Bound::from(l), Bound::from(h));
                let _: Vec<Entry> = index.reverse(r).unwrap().collect();
                (0, 0)
            }
            Op::IterVersions((l, h)) => {
                counts[9] += 1;
                let _: Vec<Entry> = index.iter().unwrap().collect();
                (0, 0)
            }
            Op::ReverseVersions((l, h)) if asc_range(&l, &h) => {
                counts[11] += 1;
                let r = (Bound::from(l), Bound::from(h));
                let _: Vec<Entry> = index.reverse(r).unwrap().collect();
                (0, 0)
            }
            Op::Validate => {
                counts[13] += 1;
                index.validate().unwrap();
                (0, 0)
            }
        };
        // println!("{}-op -- {:?} seqno:{} cas:{}", id, op, _seqno, _cas);
    }
}

#[test]
fn test_compact_mono() {
    let seed: u128 = random();
    println!("test_compact_mono {}", seed);
}

#[test]
fn test_compact_lsm() {
    let seed: u128 = random();
    println!("test_compact {}", seed);
}

#[test]
fn test_compact_tombstone() {
    let seed: u128 = random();
    println!("test_compact {}", seed);
}

fn validate_stats(
    stats: &Stats,
    config: &Config,
    vlog_file: Option<ffi::OsString>,
    n_count: Option<u64>,
    n_deleted: Option<usize>,
    seqno: u64,
    n_abytes: u64,
) {
    assert_eq!(stats.name, config.name);
    assert_eq!(stats.z_blocksize, config.z_blocksize);
    assert_eq!(stats.m_blocksize, config.m_blocksize);
    assert_eq!(stats.v_blocksize, config.v_blocksize);
    assert_eq!(stats.delta_ok, config.delta_ok);
    assert_eq!(stats.vlog_file, vlog_file);
    assert_eq!(stats.value_in_vlog, config.value_in_vlog);

    if let Some(n) = n_count {
        assert_eq!(stats.n_count, n)
    }
    if let Some(n) = n_deleted {
        assert_eq!(stats.n_deleted, n)
    }
    assert_eq!(stats.seqno, seqno);
    assert_eq!(stats.n_abytes, n_abytes);
}

fn open_index<B>(
    dir: &ffi::OsStr,
    name: &str,
    file: &ffi::OsStr,
    seed: u128,
) -> Index<u16, u64, u64, B>
where
    B: Bloom,
{
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let index = match rng.gen::<u8>() % 2 {
        0 => Index::open(dir, name).unwrap(),
        1 => Index::open_file(file).unwrap(),
        _ => unreachable!(),
    };

    match rng.gen::<bool>() {
        true => index.try_clone().unwrap(),
        false => index,
    }
}

#[derive(Clone, Debug, Arbitrary)]
enum MetaOp {
    Name,
    Bitmap,
    Stats,
    AppMetadata,
    Root,
    Seqno,
    IsCompacted,
    Len,
    IsEmpty,
}

#[derive(Clone, Debug, Arbitrary)]
enum Op<K> {
    Mo(MetaOp),
    Get(K),
    Iter((Limit<K>, Limit<K>)),
    IterVersions((Limit<K>, Limit<K>)),
    Reverse((Limit<K>, Limit<K>)),
    ReverseVersions((Limit<K>, Limit<K>)),
    Validate,
}

#[derive(Clone, Debug, Arbitrary, Eq, PartialEq)]
enum Limit<T> {
    Unbounded,
    Included(T),
    Excluded(T),
}

impl<T> From<Limit<T>> for Bound<T> {
    fn from(limit: Limit<T>) -> Self {
        match limit {
            Limit::Unbounded => Bound::Unbounded,
            Limit::Included(v) => Bound::Included(v),
            Limit::Excluded(v) => Bound::Excluded(v),
        }
    }
}

//TODO
//fn compare_iter<'a>(
//    id: usize,
//    mut index: impl Iterator<Item = Entry>,
//    btmap: impl Iterator<Item = (&'a Ky, &'a Entry)>,
//    frwrd: bool,
//) {
//    for (_key, val) in btmap {
//        loop {
//            let e = index.next();
//            match e {
//                Some(e) => match e.as_key().cmp(val.as_key()) {
//                    Ordering::Equal => {
//                        assert!(e.contains(&val));
//                        break;
//                    }
//                    Ordering::Less if frwrd => (),
//                    Ordering::Greater if !frwrd => (),
//                    Ordering::Less | Ordering::Greater => {
//                        panic!("{} error miss entry {} {}", id, e.as_key(), val.as_key())
//                    }
//                },
//                None => panic!("{} error missing entry", id),
//            }
//        }
//    }
//}
//
//TODO
//fn compare_old_entry(index: Option<Entry>, btmap: Option<Entry>) {
//    match (index, btmap) {
//        (None, None) | (Some(_), None) => (),
//        (None, Some(btmap)) => panic!("{:?}", btmap),
//        (Some(e), Some(x)) => assert!(e.contains(&x)),
//    }
//}

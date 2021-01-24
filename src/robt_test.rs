use mkit::nobitmap::NoBitmap;
use ppom::Mdb;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_build_nodiff_nobit() {
    let seed: u128 = random();
    println!("test_build_init_nodiff_nobit {}", seed);
    let _rng = SmallRng::from_seed(seed.to_le_bytes());

    // initial build

    let dir = std::env::temp_dir().join("test_build_init_nodiff_nobit");
    let name = "test_build_init_nodiff_nobit";
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
        "test_build_init_nodiff_nobit index file {:?}",
        config.to_index_file_name()
    );

    let mdb: Mdb<u16, u64, u64> = util::load_index(seed, false, 1_000_000, 100);

    let app_meta_data = "test_build_init_nodiff_nobit-metadata".as_bytes().to_vec();
    let mut build = Builder::initial(config.clone(), app_meta_data.clone()).unwrap();
    build.build_index(mdb.iter().unwrap(), NoBitmap).unwrap();

    let index = {
        let dir = dir.as_os_str();
        open_index::<NoBitmap>(dir, name, &config.to_index_file_name(), seed)
    };
    assert_eq!(index.to_name(), name.to_string());
    assert_eq!(index.to_app_metadata(), app_meta_data);
    let stats = index.to_stats();
    validate_stats(&stats, &config, None, None, None, 1000100, 0);
    // as_bitmap, to_bitmap,
    // to_root, to_seqno, is_compacted, len, is_empty,
    // get, iter, reverse
    // iter_versions, reverse_versions, validate, purge

    // incremental build
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

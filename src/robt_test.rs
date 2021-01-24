use mkit::nobitmap::NoBitmap;
use rand::{prelude::random, rngs::SmallRng, SeedableRng};

use super::*;

#[test]
fn test_build1() {
    let seed: u128 = random();
    println!("test_build1 {}", seed);
    let _rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = std::env::temp_dir().join("test_build1");
    let cfg = Config {
        dir: dir.into_os_string(),
        name: "test_build".to_string(),
        z_blocksize: 4096,
        m_blocksize: 4096,
        v_blocksize: 4096,
        delta_ok: false,
        value_in_vlog: false,
        flush_queue_size: 32,
    };
    println!("test_build1 index file {:?}", cfg.to_index_file_name());

    let mdb = util::load_index(seed, false, 1_000_000, 0);

    let app_meta_data = "test_build1".as_bytes().to_vec();
    let mut build = Builder::initial(cfg, app_meta_data).unwrap();
    build.build_index(mdb.iter().unwrap(), NoBitmap).unwrap();
}

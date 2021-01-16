use mkit::NoBitmap;
use ppom::Mdb;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_build1() {
    let seed: u128 = random();
    println!("test_build1 {}", seed);
    // TODO let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = std::env::temp_dir();
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

    let mdb = load_index(seed, false, 1_000_000);

    let app_meta_data = "test_build1".as_bytes().to_vec();
    let mut build = Builder::initial(cfg, app_meta_data).unwrap();
    build.build_index(mdb.iter().unwrap(), NoBitmap).unwrap();
}

fn load_index(seed: u128, diff: bool, count: usize) -> Mdb<u16, u64, u64> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    let index = Mdb::new("testing");

    for _i in 0..count {
        let key: u16 = rng.gen();
        let value: u64 = rng.gen();
        match diff {
            true => index.insert(key, value).ok().map(|_| ()),
            false => index.set(key, value).ok().map(|_| ()),
        };
    }

    index
}

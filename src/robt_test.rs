use fs2::FileExt;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_name() {
    let name = Name("somename-0-robt-000".to_string());
    assert_eq!(name.to_string(), "somename-0-robt-000".to_string());

    let (s, n): (String, usize) = TryFrom::try_from(name.clone()).unwrap();
    assert_eq!(s, "somename-0".to_string());
    assert_eq!(n, 0);

    let name1: Name = (s, n).into();
    assert_eq!(name.0, name1.0);

    assert_eq!(name1.next().0, "somename-0-robt-001".to_string());
}

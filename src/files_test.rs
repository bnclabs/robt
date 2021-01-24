use super::*;

#[test]
fn test_index_file() {
    let name = "test-index".to_string();
    let index_file = IndexFileName::from(name.clone());
    assert_eq!(
        index_file.0,
        AsRef::<ffi::OsStr>::as_ref("test-index-robt.indx").to_os_string()
    );
    assert_eq!(String::try_from(index_file).unwrap(), name);
}

#[test]
fn test_vlog_file() {
    let name = "test-index".to_string();
    let vlog_file = VlogFileName::from(name.clone());
    assert_eq!(
        vlog_file.0,
        AsRef::<ffi::OsStr>::as_ref("test-index-robt.vlog").to_os_string()
    );
    assert_eq!(String::try_from(vlog_file).unwrap(), name);
}

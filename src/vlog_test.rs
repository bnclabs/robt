use super::*;

#[test]
fn test_value() {
    let dbval = {
        let (value, seqno) = (10, 1);
        db::Value::U { value, seqno }
    };

    assert_eq!(dbval, db::Value::from(Value::from(dbval.clone())));

    let value = Value::from(dbval.clone());
    let (value, data) = value.into_reference(1023).unwrap();
    let mut buf = vec![0; 1023];
    buf.extend(&data);
    assert_eq!(
        value,
        Value::R {
            fpos: 1023,
            length: data.len() as u64,
        }
    );

    let mut buf = io::Cursor::new(buf);
    assert_eq!(
        value.into_native(&mut buf).unwrap(),
        Value::from(dbval.clone())
    );
}

#[test]
fn test_delta() {
    let dbdelta = {
        let (delta, seqno) = (10, 1);
        db::Delta::U { delta, seqno }
    };

    assert_eq!(dbdelta, db::Delta::from(Delta::from(dbdelta.clone())));

    let delta = Delta::from(dbdelta.clone());
    let (delta, data) = delta.into_reference(1023).unwrap();
    let mut buf = vec![0; 1023];
    buf.extend(&data);
    assert_eq!(
        delta,
        Delta::R {
            fpos: 1023,
            length: data.len() as u64,
        }
    );

    let mut buf = io::Cursor::new(buf);
    assert_eq!(
        delta.into_native(&mut buf).unwrap(),
        Delta::from(dbdelta.clone())
    );
}

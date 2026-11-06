use super::*;
#[test]
fn malformed_route_table_is_rejected_without_opening_a_link() {
    assert!(routes(&[]).unwrap().is_empty());
    assert!(matches!(routes(&[255; 4]), Err(Status::InvalidArgument)));
    assert!(matches!(routes(&[1, 0, 0, 0, 255]), Err(Status::InvalidArgument)));
    let encoded = [1, 0, 0, 0, b'a', 1, 0, 0, 0, b'b'];
    assert_eq!(routes(&encoded).unwrap(), vec![("a".into(), "b".into())]);
    assert_eq!(size_of::<InvokeOptions>(), 40);
    assert_eq!(size_of::<InvokeInfo>(), 16);
}

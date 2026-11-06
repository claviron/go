use super::*;

#[test]
fn stale_and_wrong_kind_handles_are_rejected() {
    assert!(matches!(application(u64::MAX), Err(Status::InvalidHandle)));
    assert!(matches!(exchange(u64::MAX), Err(Status::InvalidHandle)));
    assert!(matches!(remove_application(u64::MAX), Err(Status::InvalidHandle)));
}

#[test]
fn identities_never_wrap() {
    let mut registry = Registry { next: u64::MAX, ..Registry::default() };
    assert_eq!(registry.identity(), Err(Status::Capacity));
}

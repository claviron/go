use super::*;
#[test]
fn typed_remote_results_preserve_rejection_and_uncertainty() {
    assert_eq!(outcome(Err(E::OutcomeUnknown)), (6, vec![]));
    assert_eq!(
        outcome(Err(E::Remote(Failure::Rejected(vec![0, 255])))),
        (14, vec![0, 255])
    );
    assert_eq!(reply(14, vec![7]), Ok(Err(Failure::Rejected(vec![7]))));
    assert_eq!(reply(6, vec![]), Err(Status::InvalidArgument));
    assert_eq!(reply(10, vec![1]), Err(Status::InvalidArgument));
}

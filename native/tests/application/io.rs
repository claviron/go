use super::*;
#[test]
fn output_gate_reserves_one_result_until_consumed() {
    let mut state = OutputState::default();
    assert_eq!(state.reserve(Action::Write), Err(Status::Closed));
    assert_eq!(state.reserve(Action::Head), Ok(()));
    assert_eq!(state.reserve(Action::Write), Err(Status::Busy));
    state.busy = false;
    assert_eq!(state.reserve(Action::Write), Ok(()));
    state.busy = false;
    assert_eq!(state.reserve(Action::Finish), Ok(()));
    state.busy = false;
    assert_eq!(state.reserve(Action::Write), Err(Status::Closed));
}

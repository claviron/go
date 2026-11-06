use super::*;
#[test]
fn transitions_retire_only_the_previous_generation() {
    let lifecycle = Lifecycle::new(CancellationToken::new());
    let first = lifecycle.capture().unwrap().unwrap();
    assert!(!lifecycle.apply(Action::Start).unwrap());
    assert!(lifecycle.apply(Action::Stop).unwrap());
    assert!(first.cancelled.is_cancelled());
    assert!(lifecycle.capture().unwrap().is_none());
    assert_eq!(
        lifecycle.snapshot().unwrap(),
        Snapshot { generation: 1, revision: 2, running: 0 }
    );
    assert!(!lifecycle.apply(Action::Stop).unwrap());
    assert_eq!(lifecycle.consume(), Err(Status::Busy));
    lifecycle.delivered().unwrap();
    lifecycle.consume().unwrap();
    assert!(lifecycle.apply(Action::Start).unwrap());
    let second = lifecycle.capture().unwrap().unwrap();
    assert_eq!(second.id, 2);
    assert!(!second.cancelled.is_cancelled());
    // Coalescing preserves the newest state and still retires intermediate work.
    assert!(!lifecycle.apply(Action::Restart).unwrap());
    assert!(second.cancelled.is_cancelled());
    lifecycle.delivered().unwrap();
    assert_eq!(
        lifecycle.consume().unwrap(),
        Snapshot { generation: 3, revision: 4, running: 1 }
    );
}
#[test]
fn exhausted_generation_does_not_cancel_or_mutate_current_state() {
    let lifecycle = Lifecycle::new(CancellationToken::new());
    lifecycle.data.lock().unwrap().snapshot.generation = u64::MAX;
    let generation = lifecycle.capture().unwrap().unwrap();
    let before = lifecycle.snapshot().unwrap();
    assert_eq!(lifecycle.apply(Action::Restart), Err(Status::Capacity));
    assert_eq!(lifecycle.snapshot().unwrap(), before);
    assert!(!generation.cancelled.is_cancelled());
}

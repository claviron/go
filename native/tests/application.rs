use super::*;

#[test]
fn options_require_declared_budgets_and_identity() {
    let mut options = Options {
        fleet: "example".into(),
        name: "app".into(),
        incarnation: 1,
        native_threads: 1,
        max_exchanges: 2,
        max_request_bytes: 128,
        max_response_bytes: 128,
        io_buffer_bytes: 16,
    };
    assert_eq!(options.validate(), Ok(()));
    options.max_exchanges = usize::MAX;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
    options.max_exchanges = 2;
    options.incarnation = 0;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
}

#[test]
fn streaming_requires_a_window_but_not_a_total_body_limit() {
    let mut options = Options {
        fleet: "example".into(),
        name: "app".into(),
        incarnation: 1,
        native_threads: 1,
        max_exchanges: 2,
        max_request_bytes: 0,
        max_response_bytes: 0,
        io_buffer_bytes: 4096,
    };
    assert_eq!(options.validate(), Ok(()));
    options.io_buffer_bytes = 0;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
}

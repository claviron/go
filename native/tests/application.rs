use super::*;

#[test]
fn options_require_declared_budgets_and_identity() {
    let mut options = Options {
        fleet: "example".into(),
        name: "app".into(),
        incarnation: 1,
        native_threads: 1,
        max_in_flight: 2,
        max_queued: 0,
        max_request_bytes: 128,
        max_response_bytes: 128,
        io_buffer_bytes: 16,
    };
    assert_eq!(options.validate(), Ok(2));
    let inbox = claviron::orbit::link::application::LINK_INBOX_CAPACITY;
    options.max_queued = inbox - 1 - options.max_in_flight;
    assert_eq!(options.validate(), Ok(inbox - 1));
    options.max_queued += 1;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
    options.max_queued = usize::MAX;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
    options.max_queued = 0;
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
        max_in_flight: 2,
        max_queued: 0,
        max_request_bytes: 0,
        max_response_bytes: 0,
        io_buffer_bytes: 4096,
    };
    assert_eq!(options.validate(), Ok(2));
    options.io_buffer_bytes = 0;
    assert_eq!(options.validate(), Err(Status::InvalidArgument));
}

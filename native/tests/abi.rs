use super::*;

#[test]
fn abi_layout_and_invalid_arguments_are_explicit() {
    let mut abi = Abi::default();
    assert_eq!(unsafe { og_abi(&mut abi) }, 0);
    assert_eq!(abi.version, 3);
    assert_eq!(abi.event_size as usize, size_of::<Event>());
    assert_eq!(
        unsafe { og_abi(ptr::null_mut()) },
        Status::InvalidArgument as u32
    );
    assert_eq!(og_cancel(u64::MAX), Status::InvalidHandle as u32);
}

#[test]
fn copy_out_never_overruns_and_reports_required_size() {
    let mut output = [0; 2];
    let mut size = 0;
    assert_eq!(
        unsafe { copy_out(b"abc", output.as_mut_ptr(), 2, &mut size) },
        Err(Status::Capacity)
    );
    assert_eq!(output, [0, 0]);
    assert_eq!(size, 3);
}

#[test]
fn incompatible_abi_is_rejected_before_join() {
    let options = OpenOptions {
        abi_version: u32::MAX,
        native_threads: 1,
        incarnation: 1,
        max_exchanges: 1,
        max_request_bytes: 1,
        max_response_bytes: 1,
        io_buffer_bytes: 1,
    };
    let mut handle = 0;
    assert_eq!(
        unsafe {
            og_open(ptr::null(), 0, ptr::null(), 0, &options, &mut handle)
        },
        Status::Unsupported as u32
    );
    assert_eq!(handle, 0);
}

#[test]
fn recoverable_unwind_becomes_explicit_status() {
    assert_eq!(boundary(|| panic!("ABI test")), Status::Panic as u32);
}

#[test]
fn abi_query_keeps_the_original_record_extent_for_older_callers() {
    // Older bindings must be able to query the version before they decide
    // whether to call any of the new record-taking functions.
    assert_eq!(size_of::<Abi>(), 56);
    #[repr(align(8))]
    struct Storage([u8; 64]);
    let mut storage = Storage([0xa5; 64]);
    assert_eq!(unsafe { og_abi(storage.0.as_mut_ptr().cast::<Abi>()) }, 0);
    assert_eq!(&storage.0[56..], &[0xa5; 8]);
}

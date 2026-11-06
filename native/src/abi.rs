//! C ABI version four: checked handles, borrowed call inputs and copy-out data.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use claviron::orbit::link::application::{self as link, HttpVersion};

use crate::{application::Application, registry};

pub type Result<T> = std::result::Result<T, Status>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Status {
    Ok = 0,
    InvalidArgument = 1,
    InvalidHandle = 2,
    Closed = 3,
    Capacity = 4,
    Unsupported = 5,
    Transport = 6,
    Panic = 7,
    Busy = 8,
    Cancelled = 9,
    Limit = 10,
}

#[derive(Clone, Copy)]
#[repr(u32)]
pub enum EventKind {
    Request = 1,
    Cancelled = 2,
    Complete = 3,
    Closed = 4,
    Read = 5,
    Output = 6,
    Lifecycle = 7,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Event {
    pub kind: u32,
    pub status: u32,
    pub exchange: u64,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Abi {
    pub version: u32,
    pub event_size: u32,
    pub options_size: u32,
    pub info_size: u32,
    pub fleet_capacity: u64,
    pub inbox_capacity: u64,
    pub inbox_payload_capacity: u64,
    pub lane_capacity: u64,
    pub buffer_bytes: u64,
}

#[repr(C)]
pub struct OpenOptions {
    pub abi_version: u32,
    pub native_threads: u32,
    pub incarnation: u64,
    pub max_exchanges: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub io_buffer_bytes: u64,
}

pub struct Options {
    pub fleet: Box<str>,
    pub name: Box<str>,
    pub native_threads: usize,
    pub incarnation: u64,
    pub max_exchanges: usize,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub io_buffer_bytes: usize,
}

impl Options {
    pub fn validate(&self) -> Result<()> {
        if self.native_threads == 0
            || self.incarnation == 0
            || self.max_exchanges == 0
            || self.io_buffer_bytes == 0
            || self.io_buffer_bytes > isize::MAX as usize
            || self
                .max_exchanges
                .checked_mul(5)
                .and_then(|n| n.checked_add(2))
                .is_none()
        {
            return Err(Status::InvalidArgument);
        }
        claviron::orbitive::FleetName::try_new(&self.fleet)
            .map_err(|_| Status::InvalidArgument)?;
        link::segment::check_application_name(&self.name)
            .map_err(|_| Status::InvalidArgument)
    }
}

#[derive(Default)]
#[repr(C)]
pub struct RequestInfo {
    pub version_major: u32,
    pub version_minor: u32,
    pub header_count: u32,
    pub body_stream: u32,
    pub generation: u64,
    pub cancelled: u32,
    pub reserved: u32,
}

pub(crate) fn boundary(action: impl FnOnce() -> Result<()>) -> u32 {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(())) => Status::Ok as u32,
        Ok(Err(status)) => status as u32,
        Err(_) => Status::Panic as u32,
    }
}

pub(crate) unsafe fn bytes<'a>(
    data: *const u8,
    length: u64,
) -> Result<&'a [u8]> {
    let length =
        usize::try_from(length).map_err(|_| Status::InvalidArgument)?;
    if length > isize::MAX as usize || (length != 0 && data.is_null()) {
        return Err(Status::InvalidArgument);
    }
    if length == 0 {
        return Ok(&[]);
    }
    // SAFETY: The ABI requires a readable, non-overlapping call-scoped input.
    Ok(unsafe { std::slice::from_raw_parts(data, length) })
}

pub(crate) unsafe fn output<T>(
    out: *mut T,
    value: T,
) -> Result<()> {
    if out.is_null() {
        return Err(Status::InvalidArgument);
    }
    // SAFETY: The ABI requires an aligned, writable output for one T.
    unsafe { out.write(value) };
    Ok(())
}

pub(crate) unsafe fn copy_out(
    value: &[u8],
    out: *mut u8,
    capacity: u64,
    length: *mut u64,
) -> Result<()> {
    unsafe {
        output(length, value.len() as u64)?;
    }
    if capacity == 0 && out.is_null() {
        return Ok(());
    }
    if capacity < value.len() as u64 {
        return Err(Status::Capacity);
    }
    if !value.is_empty() {
        if out.is_null() {
            return Err(Status::InvalidArgument);
        }
        // SAFETY: The ABI requires capacity writable bytes, disjoint from native storage.
        unsafe {
            ptr::copy_nonoverlapping(value.as_ptr(), out, value.len());
        }
    }
    Ok(())
}

/// # Safety
/// All pointers obey internal/native/claviron_go.h: aligned outputs, call-scoped
/// readable inputs and exclusive writable copy-out buffers. No input is retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_abi(out: *mut Abi) -> u32 {
    boundary(|| unsafe {
        output(
            out,
            Abi {
                version: 4,
                event_size: size_of::<Event>() as u32,
                options_size: size_of::<OpenOptions>() as u32,
                info_size: size_of::<RequestInfo>() as u32,
                fleet_capacity: link::LINK_FLEET_CAPACITY as u64,
                inbox_capacity: link::LINK_INBOX_CAPACITY as u64,
                inbox_payload_capacity: link::LINK_INBOX_PAYLOAD_CAPACITY
                    as u64,
                lane_capacity: link::LINK_LANE_CAPACITY as u64,
                buffer_bytes: link::LINK_BUFFER_BYTES as u64,
            },
        )
    })
}

/// # Safety
/// See og_abi. options points to one OpenOptions; out receives a new owned handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_open(
    fleet: *const u8,
    fleet_len: u64,
    name: *const u8,
    name_len: u64,
    options: *const OpenOptions,
    out: *mut u64,
) -> u32 {
    boundary(|| {
        if options.is_null() || out.is_null() {
            return Err(Status::InvalidArgument);
        }
        unsafe {
            out.write(0);
        }
        let options = unsafe { &*options };
        if options.abi_version != 4 {
            return Err(Status::Unsupported);
        }
        let fleet = std::str::from_utf8(unsafe { bytes(fleet, fleet_len)? })
            .map_err(|_| Status::InvalidArgument)?;
        let name = std::str::from_utf8(unsafe { bytes(name, name_len)? })
            .map_err(|_| Status::InvalidArgument)?;
        let application = Application::open(Options {
            fleet: fleet.into(),
            name: name.into(),
            incarnation: options.incarnation,
            native_threads: options.native_threads as usize,
            max_exchanges: usize::try_from(options.max_exchanges)
                .map_err(|_| Status::InvalidArgument)?,
            max_request_bytes: usize::try_from(options.max_request_bytes)
                .map_err(|_| Status::InvalidArgument)?,
            max_response_bytes: usize::try_from(options.max_response_bytes)
                .map_err(|_| Status::InvalidArgument)?,
            io_buffer_bytes: usize::try_from(options.io_buffer_bytes)
                .map_err(|_| Status::InvalidArgument)?,
        })?;
        unsafe { output(out, registry::insert_application(application)?) }
    })
}

/// # Safety
/// out is writable for one event. One consumer waits per application.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_wait(
    app: u64,
    out: *mut Event,
) -> u32 {
    boundary(|| {
        if out.is_null() {
            return Err(Status::InvalidArgument);
        }
        unsafe { output(out, registry::application(app)?.wait()?) }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_withdraw(app: u64) -> u32 {
    boundary(|| {
        registry::application(app)?.withdraw.cancel();
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_cancel(app: u64) -> u32 {
    boundary(|| {
        registry::application(app)?.stop.cancel();
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_release(app: u64) -> u32 {
    boundary(|| registry::remove_application(app))
}

/// # Safety
/// out receives the owned application's lane number.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_lane(
    app: u64,
    out: *mut u64,
) -> u32 {
    boundary(|| unsafe {
        output(out, registry::application(app)?.lane() as u64)
    })
}

/// # Safety
/// out points to one RequestInfo. Copy operations pin the exchange for each call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_request_info(
    exchange: u64,
    out: *mut RequestInfo,
) -> u32 {
    boundary(|| {
        let exchange = registry::exchange(exchange)?;
        let (major, minor) = match exchange.frame.version {
            HttpVersion::Http09 => (0, 9),
            HttpVersion::Http10 => (1, 0),
            HttpVersion::Http11 => (1, 1),
            HttpVersion::Http2 => (2, 0),
            HttpVersion::Http3 => (3, 0),
        };
        let info = RequestInfo {
            version_major: major,
            version_minor: minor,
            header_count: exchange.frame.headers.len() as u32,
            body_stream: u32::from(exchange.frame.body.is_some()),
            generation: exchange.generation.id,
            cancelled: u32::from(exchange.cancel.is_cancelled()),
            reserved: 0,
        };
        unsafe { output(out, info) }
    })
}

/// # Safety
/// See copy-out contract in the C header. Fields: method, scheme, raw path,
/// query, peer, local address, header name and header value (7 is reserved).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_request_copy(
    exchange: u64,
    field: u32,
    index: u32,
    out: *mut u8,
    capacity: u64,
    length: *mut u64,
) -> u32 {
    boundary(|| {
        let exchange = registry::exchange(exchange)?;
        let frame = &exchange.frame;
        let address;
        let value = match field {
            1 => frame.method.as_bytes(),
            2 => frame.scheme.as_bytes(),
            3 => &frame.raw_path,
            4 => &frame.query,
            5 | 6 => {
                address = if field == 5 { frame.client } else { frame.server }
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                address.as_bytes()
            }
            8 | 9 => {
                let pair = frame
                    .headers
                    .get(index as usize)
                    .ok_or(Status::InvalidArgument)?;
                if field == 8 { &pair.0 } else { &pair.1 }
            }
            _ => return Err(Status::InvalidArgument),
        };
        unsafe { copy_out(value, out, capacity, length) }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_reply_begin(
    exchange: u64,
    status: u32,
) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?
            .begin(u16::try_from(status).map_err(|_| Status::InvalidArgument)?)
    })
}

/// # Safety
/// name/value are call-scoped input byte slices; native copies retained headers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_reply_header(
    exchange: u64,
    name: *const u8,
    name_len: u64,
    value: *const u8,
    value_len: u64,
) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?
            .header(unsafe { bytes(name, name_len)? }, unsafe {
                bytes(value, value_len)?
            })
    })
}

/// # Safety
/// body is a call-scoped chunk no larger than io_buffer_bytes. Native copies it
/// before returning and completes through the output-result event.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_output_write(
    exchange: u64,
    body: *const u8,
    length: u64,
) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?.write(unsafe { bytes(body, length)? })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_exchange_cancel(exchange: u64) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?.cancel.cancel();
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_exchange_release(
    app: u64,
    exchange: u64,
) -> u32 {
    boundary(|| registry::remove_exchange(app, exchange))
}

#[cfg(test)]
#[path = "../tests/abi.rs"]
mod tests;

/// # Safety
/// out is writable for one u64. Counts unsupported frames without applying them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_unsupported(
    app: u64,
    out: *mut u64,
) -> u32 {
    boundary(|| unsafe {
        output(
            out,
            registry::application(app)?
                .unsupported
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn og_head_submit(exchange: u64) -> u32 {
    boundary(|| registry::exchange(exchange)?.head())
}
#[unsafe(no_mangle)]
pub extern "C" fn og_output_finish(exchange: u64) -> u32 {
    boundary(|| registry::exchange(exchange)?.finish_output())
}
#[unsafe(no_mangle)]
pub extern "C" fn og_read_start(
    exchange: u64,
    capacity: u64,
) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?.read(
            usize::try_from(capacity).map_err(|_| Status::InvalidArgument)?,
        )
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn og_read_close(exchange: u64) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?.close_input();
        Ok(())
    })
}
/// # Safety
/// out is writable for capacity bytes and length for one u64. Requires the read event.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_read_result(
    exchange: u64,
    out: *mut u8,
    capacity: u64,
    length: *mut u64,
) -> u32 {
    boundary(|| {
        registry::exchange(exchange)?.read_result(|data| unsafe {
            copy_out(data, out, capacity, length)
        })
    })
}
/// # Safety
/// written is writable for one u64. Requires the output event.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_output_result(
    exchange: u64,
    written: *mut u64,
) -> u32 {
    boundary(|| {
        if written.is_null() {
            return Err(Status::InvalidArgument);
        }
        let (count, status) = registry::exchange(exchange)?.output_result()?;
        unsafe {
            output(written, count as u64)?;
        }
        if status == Status::Ok { Ok(()) } else { Err(status) }
    })
}

/// # Safety
/// out points to one lifecycle Snapshot. This does not consume a notification.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_snapshot(
    app: u64,
    out: *mut crate::application::lifecycle::Snapshot,
) -> u32 {
    boundary(|| unsafe {
        output(out, registry::application(app)?.lifecycle.snapshot()?)
    })
}
/// # Safety
/// out points to one lifecycle Snapshot. Requires the lifecycle event and
/// consumes its coalesced notification credit, returning the newest snapshot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_lifecycle_result(
    app: u64,
    out: *mut crate::application::lifecycle::Snapshot,
) -> u32 {
    boundary(|| {
        if out.is_null() {
            return Err(Status::InvalidArgument);
        }
        unsafe { output(out, registry::application(app)?.lifecycle.consume()?) }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn og_lifecycle(
    app: u64,
    action: u32,
) -> u32 {
    boundary(|| {
        registry::application(app)?
            .action(crate::application::lifecycle::Action::from_code(action)?)
    })
}

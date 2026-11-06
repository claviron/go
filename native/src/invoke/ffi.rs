//! Checked invoke downcalls. No input pointer survives its call.
use super::{Invocations, Settings, get};
use crate::{
    abi::{Event, Result, Status, boundary, bytes, copy_out, output},
    registry,
};
use claviron::orbit::link::invoke::Limits;

#[repr(C)]
pub struct InvokeOptions {
    pub pending_calls: u64,
    pub accepted_calls: u64,
    pub operation_bytes: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
}
#[derive(Default)]
#[repr(C)]
pub struct InvokeInfo {
    pub generation: u64,
    pub cancelled: u64,
}
fn text(value: &[u8]) -> Result<&str> {
    std::str::from_utf8(value).map_err(|_| Status::InvalidArgument)
}
fn size(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| Status::InvalidArgument)
}
// Routes are length-prefixed UTF-8 pairs. No borrowed pointers are nested in a
// Go-allocated array, and no JSON or text round trip enters the call path.
fn routes(mut value: &[u8]) -> Result<Vec<(Box<str>, Box<str>)>> {
    let mut result = Vec::new();
    while !value.is_empty() {
        let mut pair = Vec::with_capacity(2);
        for _ in 0..2 {
            let prefix = value.get(..4).ok_or(Status::InvalidArgument)?;
            let n = u32::from_le_bytes(
                prefix.try_into().map_err(|_| Status::InvalidArgument)?,
            ) as usize;
            value = &value[4..];
            pair.push(Box::<str>::from(text(
                value.get(..n).ok_or(Status::InvalidArgument)?,
            )?));
            value = &value[n..];
        }
        let service = pair.pop().ok_or(Status::InvalidArgument)?;
        let target = pair.pop().ok_or(Status::InvalidArgument)?;
        result.push((target, service));
    }
    Ok(result)
}
/// # Safety
/// See orbit_go.h: aligned records, valid call-scoped inputs and writable outputs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_invoke_open(
    app: u64,
    fleet: *const u8,
    fleet_len: u64,
    service: *const u8,
    service_len: u64,
    route_data: *const u8,
    route_len: u64,
    options: *const InvokeOptions,
    out: *mut u64,
) -> u32 {
    boundary(|| unsafe {
        if options.is_null() || out.is_null() {
            return Err(Status::InvalidArgument);
        }
        output(out, 0)?;
        let o = &*options;
        let settings = Settings {
            fleet: text(bytes(fleet, fleet_len)?)?.into(),
            service: text(bytes(service, service_len)?)?.into(),
            routes: routes(bytes(route_data, route_len)?)?,
            limits: Limits {
                pending_calls: size(o.pending_calls)?,
                accepted_calls: size(o.accepted_calls)?,
                operation_bytes: size(o.operation_bytes)?,
                request_bytes: size(o.request_bytes)?,
                response_bytes: size(o.response_bytes)?,
            },
        };
        let reservation = registry::reserve_invoke(app)?;
        let owner = Invocations::open(
            std::sync::Arc::clone(&reservation.application),
            settings,
        )?;
        reservation.commit();
        output(out, owner)
    })
}
/// # Safety
/// out is an aligned writable Event. One waiter is allowed per invoke owner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_invoke_wait(
    owner: u64,
    out: *mut Event,
) -> u32 {
    boundary(|| {
        if out.is_null() {
            return Err(Status::InvalidArgument);
        }
        unsafe { output(out, get(owner)?.wait()?) }
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn og_invoke_withdraw(owner: u64) -> u32 {
    boundary(|| get(owner)?.withdraw())
}
#[unsafe(no_mangle)]
pub extern "C" fn og_invoke_cancel(owner: u64) -> u32 {
    boundary(|| get(owner)?.cancel())
}
#[unsafe(no_mangle)]
pub extern "C" fn og_invoke_finish(owner: u64) -> u32 {
    boundary(|| get(owner)?.finish())
}
#[unsafe(no_mangle)]
pub extern "C" fn og_invoke_release(owner: u64) -> u32 {
    boundary(|| super::release(owner))
}
/// # Safety
/// out is aligned and writable. The handle belongs to owner and is retained.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_invocation_info(
    owner: u64,
    id: u64,
    out: *mut InvokeInfo,
) -> u32 {
    boundary(|| {
        let owner = get(owner)?;
        let request = owner.incoming(id)?;
        unsafe {
            output(
                out,
                InvokeInfo {
                    generation: request.generation.id,
                    cancelled: u64::from(
                        request.generation.cancelled.is_cancelled()
                            || owner.cancel.is_cancelled(),
                    ),
                },
            )
        }
    })
}
/// # Safety
/// Output follows copy-out rules; field 1 is operation, 2 is payload.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_invocation_copy(
    owner: u64,
    id: u64,
    field: u32,
    out: *mut u8,
    capacity: u64,
    length: *mut u64,
) -> u32 {
    boundary(|| {
        let request = get(owner)?.incoming(id)?;
        let value = match field {
            1 => request.operation.as_bytes(),
            2 => &request.payload,
            _ => return Err(Status::InvalidArgument),
        };
        unsafe { copy_out(value, out, capacity, length) }
    })
}
/// # Safety
/// payload is call-scoped and copied before returning.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_invocation_reply(
    owner: u64,
    id: u64,
    code: u32,
    payload: *const u8,
    length: u64,
) -> u32 {
    boundary(|| {
        let owner = get(owner)?;
        if size(length)? > owner.limits.response_bytes {
            return Err(Status::Limit);
        }
        owner.respond(id, code, unsafe { bytes(payload, length)? }.to_vec())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn og_invocation_release(
    owner: u64,
    id: u64,
) -> u32 {
    boundary(|| get(owner)?.release_incoming(id))
}
/// # Safety
/// Inputs are call-scoped. out receives a retained call; info its generation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_call_start(
    owner: u64,
    expected_generation: u64,
    target: *const u8,
    target_len: u64,
    operation: *const u8,
    operation_len: u64,
    payload: *const u8,
    payload_len: u64,
    out: *mut u64,
    info: *mut InvokeInfo,
) -> u32 {
    boundary(|| unsafe {
        if out.is_null() || info.is_null() {
            return Err(Status::InvalidArgument);
        }
        output(out, 0)?;
        let owner = get(owner)?;
        if size(payload_len)? > owner.limits.request_bytes
            || size(operation_len)? > owner.limits.operation_bytes
        {
            return Err(Status::Limit);
        }
        let (id, generation) = owner.call(
            expected_generation,
            text(bytes(target, target_len)?)?.into(),
            text(bytes(operation, operation_len)?)?.into(),
            bytes(payload, payload_len)?.to_vec(),
        )?;
        output(info, InvokeInfo { generation, cancelled: 0 })?;
        output(out, id)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn og_call_cancel(
    owner: u64,
    id: u64,
) -> u32 {
    boundary(|| {
        get(owner)?.outgoing(id)?.cancel.cancel();
        Ok(())
    })
}
/// # Safety
/// All outputs follow copy-out rules. Query does not consume; a successful
/// non-query copy consumes the retained result, including empty payloads.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn og_call_result(
    owner: u64,
    id: u64,
    code: *mut u32,
    status: *mut u32,
    out: *mut u8,
    capacity: u64,
    length: *mut u64,
    consume: u32,
) -> u32 {
    boundary(|| unsafe {
        if code.is_null() || status.is_null() || length.is_null() {
            return Err(Status::InvalidArgument);
        }
        let owner = get(owner)?;
        owner.with_result(
            id,
            consume != 0,
            |result_status, result_code, payload| {
                output(code, result_code)?;
                output(status, result_status as u32)?;
                copy_out(payload, out, capacity, length)
            },
        )
    })
}
#[cfg(test)]
#[path = "../../tests/invoke/ffi.rs"]
mod tests;

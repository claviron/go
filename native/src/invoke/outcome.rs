use crate::abi::{Result, Status};
use claviron::orbit::invoke::{CallError as E, CallResult, Failure};

pub fn outcome(value: CallResult<Vec<u8>>) -> (u32, Vec<u8>) {
    match value {
        Ok(bytes) => (0, bytes),
        Err(E::Remote(Failure::Rejected(bytes))) => (14, bytes),
        Err(error) => (
            match error {
                E::UnknownTarget => 1,
                E::AmbiguousTarget => 2,
                E::Unavailable => 3,
                E::Busy => 4,
                E::Closed => 5,
                E::OutcomeUnknown => 6,
                E::PayloadTooLarge => 7,
                E::Protocol(_) => 8,
                E::Transport(_) => 9,
                E::Remote(Failure::Busy) => 10,
                E::Remote(Failure::Closing) => 11,
                E::Remote(Failure::Unsupported) => 12,
                E::Remote(Failure::ResponseTooLarge) => 13,
                E::InvalidConfig(_) => 15,
                E::Remote(Failure::Rejected(_)) => unreachable!(),
            },
            Vec::new(),
        ),
    }
}
pub fn reply(
    code: u32,
    payload: Vec<u8>,
) -> Result<std::result::Result<Vec<u8>, Failure>> {
    Ok(match code {
        0 => Ok(payload),
        10 if payload.is_empty() => Err(Failure::Busy),
        11 if payload.is_empty() => Err(Failure::Closing),
        12 if payload.is_empty() => Err(Failure::Unsupported),
        13 if payload.is_empty() => Err(Failure::ResponseTooLarge),
        14 => Err(Failure::Rejected(payload)),
        _ => return Err(Status::InvalidArgument),
    })
}
#[cfg(test)]
#[path = "../../tests/invoke/outcome.rs"]
mod tests;

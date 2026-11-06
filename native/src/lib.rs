//! Native ownership and signalled completion delivery for the Go library.

mod abi;
mod application;
mod invoke;
mod registry;
pub use invoke::ffi::*;

pub use abi::*;

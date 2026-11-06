# Native binding

The application library's [architecture](../ARCHITECTURE.md) defines ABI,
completion reservation, runtime ownership and shutdown contracts.

This crate is an implementation detail of the Go library. `lib.rs` exposes the
C ABI implemented by `abi.rs`; `registry.rs` pins typed owners for each call;
`application.rs` owns Link membership and application drain.
`application/io.rs` owns independent read/write actors, reserved completion
slots and terminal exchange completion.
`application/lifecycle.rs` owns checked generation state and notification credit.
`application/events.rs` owns the private notified event-ring cursor and fan-out.
`application/status.rs` uses the existing Claviron application status schema.

Dependencies enter through the public `claviron` facade. No internal Claviron
crate path, embedded Go runtime or second SHM protocol belongs here.

`invoke/mod.rs` owns application-child reservations, separate Link drivers,
bounded incoming/outgoing records, generation capture and signalled drain.
`invoke/outcome.rs` maps the existing typed protocol outcomes.
`invoke/ffi.rs` implements ABI v4 invoke records and checked copy-out calls.
The original 56-byte ABI query record remains unchanged.

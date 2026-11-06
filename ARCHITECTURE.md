# Application library architecture

## Ownership boundary

orbit-go is imported by a normal Go application. The caller supplies an
`http.Handler`, application identity and explicit resource budgets. The library
owns one application Link membership. The caller owns `main`, signals, process
lifetime and handler code.

The Rust native library consumes the Claviron facade with `orbit` and
`application`. It uses `orbit::link::application` for the existing wire schema,
body streams and status rows. It never implements SHM layout or wake primitives
in Go. The separate direct-integration Go FFI project is not a dependency.

## Sources

- `application.go`: Go application ownership, completion pump and shutdown.
- `http.go`: server request mapping, raw headers and handler dispatch.
- `internal/native/native.go`: private cgo calls and copy-out metadata.
- `internal/httpadapter/response.go`: bounded first-stage response buffering.
- `native/include/orbit_go.h`: ABI version one and ownership rules.
- `native/src/abi.rs`: checked downcalls and unwind containment.
- `native/src/registry.rs`: never-reused, kind-checked application/exchange handles.
- `native/src/application.rs`: Link reader, native I/O and terminal delivery.
- `native/src/application/status.rs`: existing application status schema, with
  Go identity and `net/http` request mode. No custom runner is reported.
- `examples/plain/main.go`: an ordinary Go application using the library.
- `harness/`: Go fixture and real Claviron caller, outside routine unit tests.

## Execution and completions

An application owns a Tokio I/O runtime with the caller's `NativeThreads` count.
The runtime stays in the external handle registry; its own tasks hold only the
application state, so the runtime is never dropped on its own worker thread.
There is no runtime per HTTP request.

One Go goroutine blocks in `og_wait`. This occupies one native-wait thread for
the application, not one per request. It dispatches requests to Go goroutines;
it never executes user handlers. Native read/write work returns by terminal
events, rather than by blocking a handler in cgo for the duration of I/O.

`MaxExchanges` bounds retained exchange handles. Each credit reserves space for
at most Request, Cancelled and Complete; one extra queue slot belongs to Closed.
A native completion therefore never waits for data-queue space and is never
discarded. The credit remains reserved until Go has observed native completion
and the handler has returned. Release before terminal completion is rejected.
This is one exchange ownership budget, not a second permit over the same queue.
Full exchange admission sends an empty 503 and does not evict an older exchange.

Every accepted exchange has one terminal completion, including body refusal,
peer cancellation and failed head publication. Native failure also cancels the
Go request context. No Go pointer is retained across a downcall. Copy-out calls
pin their native owner for the call and expose no borrowed native pointer.
Response headers/body are copied into native ownership before submission returns.
Submission success means accepted work; it does not mean delivery succeeded.

The ABI queries fixed record sizes and version before opening. Native Link
geometry is reported separately, and mapping uses Claviron's existing geometry
validation. A native ABI match is not proof of peer wire compatibility.

## First-stage HTTP behavior

Request bodies are buffered up to `MaxRequestBytes`; larger requests receive
413 before Go dispatch. Responses are buffered up to `MaxResponseBytes` and
published after the handler returns. Buffers have bounded payload length;
allocator capacity, metadata and the Go/native copies are additional memory.
This stage is not a streaming or full-duplex handler adapter.

Server requests preserve the original request target, method, HTTP version,
Host, remote/local addresses, repeated headers and buffered body. `RawHeaders`
retains order/casing that `http.Header` cannot represent. `Scheme` exposes the
caller's scheme; Request.TLS stays nil because the application did not perform
the public handshake. Trusted-forwarding policy is not reapplied.

Response headers freeze at the first final status or write. An implicit status
is 200. HEAD omits response bytes; 204/304 reject writes. Repeated response
headers are preserved. A panic or response-budget error produces an empty 500;
`http.ErrAbortHandler` resets the exchange. Header/publication failure resets
the existing stream, so a caller waiting for the head wakes with an error.
No optional streaming, hijacking or push interface is advertised. Informational
responses, trailers, upgrades and general streaming are not implemented.

## Lifecycle scope

`Shutdown(ctx)` withdraws this lane from admission and drains accepted work.
Its context controls the caller's wait only; expiry does not secretly cancel
handlers. `Close` explicitly cancels owned contexts/I/O and joins handlers.
A handler that ignores cancellation can delay Close; no force termination or
implicit deadline is invented. Both operations are safe to call repeatedly.
Native waits use real wakeups, cancellation tokens and completion signals.

The reader and lane ownership hold are released in order after native work
finishes. Closing never unlinks a fleet or closes a sibling lane. Status queries
use Claviron's application schema, reporting this connected application; they
are not an assertion that every handler is healthy.

Start/stop/restart, application events and edge-shutdown callbacks belong to a
later stage. Unsupported frames increment `UnsupportedFrames` and perform no
operation or success acknowledgement. In particular, a shutdown notice from
one edge cannot cancel work admitted by another. Malformed protocol input or
an actual reader failure instead terminates the connection with an error.

## Validation and cleanup

Rust test bodies live in `native/tests/`, mirroring modules; Go tests live under
`tests/`. Pure tests do not boot Claviron. The explicit acceptance fixture is a
Go program; the Rust program represents the real Claviron caller.

The acceptance driver waits for child readiness and lifecycle signals without
sleeps. Its Go fixture uses the race detector; it can also run with
`GOEXPERIMENT=cgocheck2`. It checks real HTTP exchanges, concurrent instances,
peer cancellation, handler failures, reply failures and graceful drain.

The external Python parent clears only the successful test's unique fleet,
after the Rust caller and all Go children have exited. Failed runs retain their
fleet for inspection; stop and verify all owners before any manual cleanup.

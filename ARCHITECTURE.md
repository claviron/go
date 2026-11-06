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
- `body.go`: lazy, serialized reads and concurrent input closure.
- `internal/native/native.go`: private cgo calls and copy-out metadata.
- `internal/httpadapter/response.go`: streaming response ownership and net/http behavior.
- `internal/native/orbit_go.h`: ABI version two and ownership rules.
- `native/src/abi.rs`: checked downcalls and unwind containment.
- `native/src/registry.rs`: never-reused, kind-checked application/exchange handles.
- `native/src/application.rs`: Link membership, frame dispatch and application drain.
- `native/src/application/io.rs`: independent input/output actors, operation slots
  and terminal exchange completion.
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

`MaxExchanges` bounds retained exchange handles. Each credit reserves Request,
Cancelled, Complete and at most one unconsumed Read result and Output result;
one additional queue slot belongs to Closed. Each direction admits one operation
at a time. A result becomes reusable only after its event has been delivered AND
its data/status consumed. Terminal delivery is also checked before release.
This prevents early result consumption from recycling credit while old events
remain queued. No terminal completion is discarded or waits for data-queue space.

Go pins an exchange for each operation through native result consumption. Release
requires native completion, handler return and zero Go I/O leases. Application
closure joins handlers and I/O consumers before releasing the native runtime.
Full exchange admission sends an empty 503 without evicting an older exchange.

Read and output use separate native actors and command slots. The input owner
reads only on Go demand, at most IOBufferBytes, and reuses its buffer. Its result
is copied directly into the caller's Go buffer. The output owner accepts one
copied chunk of at most IOBufferBytes and acknowledges actual written bytes,
including partial failure. No Go pointer survives an asynchronous downcall.

The output actor alone owns response readiness, peer-departure observation and
FIN/reset. It joins the input owner before reporting final exchange completion,
including panic and cancellation paths. Input Close interrupts its own wait but
never cancels output. Peer departure and explicit application Close cancel owned
I/O and Request.Context. Both directions retain exactly one terminal result for
a submission that races cancellation; unsubmitted work returns an immediate error.

The ABI queries fixed record sizes and version before opening. Native Link
geometry is reported separately, and mapping uses Claviron's existing geometry
validation. A native ABI match is not proof of peer wire compatibility.

## HTTP streaming behavior

ABI v2 dispatches after the request head and stream ownership are ready. It does
not pre-read the complete upload. Request.Body serializes reads; Close signals
the native input owner without waiting behind a blocked Read. Input EOF and
read closure are independent of response FIN. A present stream without declared
length has unknown ContentLength; no stream maps to http.NoBody. Declared length
is used when supplied. Body presence is limited to what the Link envelope carries.

`IOBufferBytes` bounds each operation. Zero MaxRequestBytes/MaxResponseBytes
means no total payload limit; configured nonzero totals are enforced separately.
Memory is bounded by operation buffers, metadata and the shared stream windows,
not by complete body size. A read crossing its limit returns the allowed prefix
and a MaxBytesError; handlers can answer before consuming the rest of an upload.

ResponseWriter publishes head/write/FIN operations immediately and waits in Go
for each native acknowledgement. It implements Flusher, FlushError and
EnableFullDuplex. There is no hidden output buffer to flush; Flush publishes the
head when necessary. Content-Length is enforced when declared; buffering is not
used to infer one. HEAD omits body bytes; 204/304 reject writes.

Header names/values and repeated fields retain their existing ownership rules.
RawHeaders preserves request order and casing. The TLS connection belongs to
Claviron, so only Scheme exposes the supplied scheme and Request.TLS stays nil.
The adapter does not reapply trusted-forwarding policy.

An unhandled request-limit error before commit becomes 413. A handler panic or
output-limit error before commit becomes an empty 500. Once a final head is
committed, failures reset the stream instead of inventing a replacement status.
The caller's existing response reset signal wakes head/body waits on failure.
The Link API supplies byte streams without a trailer envelope; trailers,
informational responses, upgrades, hijacking and push are not advertised.

The canonical ABI header is in internal/native so the Go package cache includes
it. The binding checks v2 and fixed record sizes before opening; v1 is incompatible.

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
sleeps. Its Go fixture and actual Go example use the race detector; it can also run with
`GOEXPERIMENT=cgocheck2`. It checks real HTTP exchanges, concurrent instances,
peer cancellation, handler failures, reply failures and graceful drain. It proves
a full upload window makes a further write pending, transfers more than a window,
closes pending reads, and drains the actual example after SIGTERM.

The external Python parent clears only the successful test's unique fleet,
after the Rust caller and all Go children have exited. Failed runs retain their
fleet for inspection; stop and verify all owners before any manual cleanup.

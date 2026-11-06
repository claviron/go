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
- `invoke.go`: application-owned invoke admission, dispatch and drain.
- `internal/native/invoke.go`: private invoke downcalls and retained-result copy-out.
- `native/src/invoke/`: core Link driver, typed outcomes and checked invoke ABI.
- `lifecycle.go`: authoritative snapshots, generation contexts and coalesced state wakes.
- `http.go`: server request mapping, raw headers and handler dispatch.
- `body.go`: lazy, serialized reads and concurrent input closure.
- `internal/native/native.go`: private cgo calls and copy-out metadata.
- `internal/httpadapter/response.go`: streaming response ownership and net/http behavior.
- `internal/native/orbit_go.h`: ABI version four and ownership rules.
- `native/src/abi.rs`: checked downcalls and unwind containment.
- `native/src/registry.rs`: never-reused, kind-checked application/exchange handles.
- `native/src/application.rs`: Link membership, frame dispatch and application drain.
- `native/src/application/io.rs`: independent input/output actors, operation slots
  and terminal exchange completion.
- `native/src/application/lifecycle.rs`: checked generation transitions and state-notification credit.
- `native/src/application/events.rs`: private notified lifecycle fan-out among live Go instances.
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
two additional queue slots belong to Closed and a coalesced lifecycle wake. Each direction admits one operation
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

The streaming binding dispatches after the request head and stream ownership are ready. It does
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
it. The binding checks v4 and fixed record sizes before opening; older bindings are incompatible.
The ABI query record retains its original 56-byte extent so old callers can
inspect the version before invoking any new record-taking function.

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

## Application generations

The initial application is running at generation 1, revision 1. A running Start
and a stopped Stop are idempotent. Stop retains the generation number and
cancels its token. Start after stop and every Restart create a new generation.
Generation and revision exhaustion are checked before cancellation or publication.

A native exchange captures one Arc-owned generation before dispatch. Its I/O
cancellation token is a child of that generation, itself below the application's
connection-stop token. Requests retain that immutable ID through the ABI and
Generation(r). Stopped applications refuse with 503 without running a handler.
Native operations check their old owner's cancellation; a delayed handler never
acquires the current generation or writes to a new request's ticket.

Go request contexts are driven by native exchange cancellation, not parented to
a potentially older Go snapshot cache. GenerationContext scopes separate
caller-owned background work. Native snapshots carry a monotonic revision;
older observations cannot undo a stop or resurrect a retired Go context. The
caller owns creation and joining of background goroutines.

Each native transition reserves at most one coalesced state wake. Its credit is
recycled only after delivery and lifecycle_result consumption. The newest state
is returned, not a historical callback queue. Go Changes uses the same latest-
state semantics and never executes user callbacks on the completion pump. These
state watches cannot block lifecycle progress when the user does not read them.

Lifecycle controls require the application name and a runtime-qualified `go`
target. Private ApplicationEvents carry the action to other live instances with
that same runtime/name through the existing notified fleet ring. Each reader
owns a bounded cursor, ignores its locally applied records, and reports ring
lag as failure. There is no periodic scan, invented timeout or replay at boot.
The event cursor/readiness is prepared before Open returns. Its reader is owned
by the application driver and is dropped before the lane ownership hold.

A per-instance transition lock orders local actions and received decisions;
there is no cross-node total order or synchronous sibling acknowledgement.
New connections start at generation 1, matching the Java/JS initial-state model.
Status reports use the existing application schema with actual running state
and generation. An action or status publication error cannot roll back a token
that has already been retired.

Stop/restart is cooperative generation cancellation, not connection withdrawal
or process termination. Shutdown keeps its existing drain semantics; Close
explicitly cancels owned work. Public application-event subscriptions and
edge-shutdown callbacks remain future work. Unsupported frames are counted and
perform no action; one edge's shutdown does not cancel unrelated callers.

## Validation and cleanup

Rust test bodies live in `native/tests/`, mirroring modules; Go tests live under
`tests/`. Pure tests do not boot Claviron. The explicit acceptance fixture is a
Go program; the Rust program represents the real Claviron caller.

The acceptance driver waits for child readiness and lifecycle signals without
sleeps. Its Go fixture and actual Go example use the race detector; it can also run with
`GOEXPERIMENT=cgocheck2`. It checks real HTTP exchanges, concurrent instances,
peer cancellation, handler failures, reply failures and graceful drain. It proves
a full upload window makes a further write pending, transfers more than a window,
closes pending reads, and drains the actual example after SIGTERM. Lifecycle fixtures use authoritative
status rows as transition barriers, cover multiple Go processes and wrong targets,
and keep old delayed handlers alive while new generations serve. The actual Go
example is also stopped/restarted without changing its PID or lane.

The external Python parent clears only the successful test's unique fleet,
after the Rust caller and all Go children have exited. Failed runs retain their
fleet for inspection; stop and verify all owners before any manual cleanup.

## Application-owned invoke

An invoke endpoint opens an independent Link fleet using the existing compiled
Link geometry and Claviron invoke protocol. It never reads the HTTP inbox or
adds HTTP frame variants. Parent admission serializes opening against shutdown;
a native child reservation pins the application's runtime before setup begins.
Failed setup returns the reservation. External handle release refuses a parent
with retained invoke children. Each endpoint shares the native runtime's Handle;
there is no additional Tokio runtime or Go process.

One Go waiter owns the endpoint's completion queue. It dispatches incoming Go
handlers and wakes outgoing callers, never executing user code in the pump.
The queue reserves `3 * AcceptedCalls + PendingCalls + 2` slots: Request,
Cancelled and Complete per retained incoming handle, Result per retained
outgoing call, and Withdrawal/Closed barriers. Incoming credit is held through
native completion delivery AND Go handler return; outgoing credit through result
delivery and successful copy-out. No terminal event is dropped or blocked behind
a full data queue. Monotonic process-local IDs are shared with the other ABI
handle kinds; each endpoint checks its own incoming/outgoing maps.

Opaque input bytes are copied before return, native outputs copied directly
into Go-owned memory. Result queries borrow the retained native payload rather
than cloning it. A too-small copy-out buffer does not consume result credit.
Pending/accepted bounds and operation/request/response byte bounds are explicit,
independent of HTTP. Zero payload bounds allow empty payloads only. Routes are
immutable after startup. Core result codes preserve rejection bytes and
OutcomeUnknown; no discovery policy, retry or timeout is added.

Incoming work retains the native generation captured at acceptance. Cancelling
that generation or explicitly closing the endpoint races its reply receiver,
emits context cancellation and terminal completion, and prevents late replies.
Go handler return alone does not release its handle before native completion.
Outgoing work races the caller cancellation token, captured generation, endpoint
cancel and core call. Cancelling interest drops only the local core future;
remote execution is not rolled back. One terminal result is always delivered.
HTTP, invoke and GenerationContext values carry an application/generation stamp
through nested calls; native validation rejects a retired stamp even when Go
cancellation was detached or its notification has not yet been consumed.

Graceful application shutdown joins HTTP handlers before withdrawing invoke
intake, preserving their nested calls. Invoke withdrawal sets an admission flag
and enqueues its barrier under the same lock as incoming request publication.
The Go pump reaches that barrier only after adding every accepted handler to
its join set. It joins those handlers while the core driver stays running for
their nested calls. Once handlers return, outgoing admission closes; all
retained results/completions are consumed before the driver shutdown signal.
The core driver then drains accepted result-stream writes and withdraws its own
lane. Go joins callers/handlers before native endpoint release, then the parent
can release its runtime. Closed endpoints leave the parent's ownership map.

Explicit Close cancels endpoint tokens as well as withdrawing intake, but still
joins cooperative handlers. Shutdown context expiry limits only the caller's
wait. Neither operation unlinks a fleet, kills Go goroutines or supplies an
implicit termination duration. Broken driver completion cancels/settles owned
work and surfaces an error. Public application events remain a separate stage.

The explicit acceptance harness drives an independent Rust invoke peer and
HTTP caller against a normal Go fixture and the shipped Go invoke example.
It checks typed/binary outcomes, panic/oversize results, pending capacity with
HTTP progress, caller cancellation without rollback, generation cancellation in
both directions, stopped refusal, late reply isolation, generation-stamp checks
through WithoutCancel, HTTP/invoke nested drain, explicit Close and SIGTERM.
All Go executables use the race detector; strict cgo checking is supported.
Only the Python parent clears both exact test fleets, after every process exits.

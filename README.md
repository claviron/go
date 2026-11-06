# orbit-go

A Go library for handling Claviron application requests over same-host Orbit
Link. Your normal Go executable imports the library and supplies a standard
`net/http.Handler`. Claviron owns public connections and host routing; your Go
application owns its handlers, goroutines and process lifetime.

## Current stage

Application membership, incremental request reads and streaming responses are
implemented through a versioned C ABI and private cgo adapter. A normal Go
`net/http.Handler` reads `Request.Body` and writes or flushes the response while
the request is still arriving. Read and output operations have independent
native owners and bounded buffers.

Application status uses Claviron's existing schema. Application start/stop/restart and generation cancellation are supported.
Public event subscriptions and invoke remain later stages. Unsupported management/event
frames outside the supported lifecycle/status contract are counted by
`UnsupportedFrames`; they perform no operation or success acknowledgement. An edge-shutdown notice does not cancel another caller's work.

This is unreleased source. The intended module path is
`github.com/claviron/orbit-go`; no public Go release or native prebuild is
available. The first implementation uses the current, unreleased Claviron Link
application API. The manifest's Claviron version alone does not establish that
the published package contains that API; source validation requires a matching
checkout override until its release.

## Use from a Go application

```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "os/signal"

    orbitgo "github.com/claviron/orbit-go"
)

func main() {
    app, err := orbitgo.Open(orbitgo.Options{
        Fleet: "example", Name: "hello", Incarnation: 1,
        NativeThreads: 1, MaxExchanges: 32, IOBufferBytes: 4096,
        MaxRequestBytes: 0, MaxResponseBytes: 0,
    }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/plain; charset=utf-8")
        fmt.Fprintln(w, "Hello from Go")
    }))
    if err != nil { panic(err) }
    defer app.Close()

    // Your application's lifecycle decides when to withdraw and drain.
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()
    <-ctx.Done()
    if err := app.Shutdown(context.Background()); err != nil { panic(err) }
}
```

See the complete [Go example](examples/plain/main.go). Signal handling belongs
to that application. The library installs no signals and does not own `main`.
The fleet namespace contains 1–8 ASCII letters, digits, hyphens or underscores;
the application name is separate. Incarnation and every native resource budget
are supplied by the caller.

`Shutdown(ctx)` withdraws this lane and drains accepted requests. Expiring ctx
ends the wait; use `Close` explicitly to cancel owned work. Close waits for
handlers to return, so handlers must respect Request.Context. Neither operation
unlinks shared resources or stops sibling applications.

## Streaming and limits

`IOBufferBytes` is required and bounds each native read/write operation. The
binding retains at most one pending read and one pending output per exchange;
its memory does not grow with the complete body. Go goroutines await signalled
completions, rather than blocking one native thread per request. Writes stop
when the SHM window fills and resume when the peer consumes data.

`MaxRequestBytes` and `MaxResponseBytes` are optional total payload limits:
zero disables the corresponding total limit. They are independent of the I/O
buffer/window bound. A request-limit error is an `*http.MaxBytesError`; handlers
can choose an appropriate response. If unhandled before any head is sent, the
adapter sends 413. Once a head has been committed, an unhandled input or output
failure resets the stream instead of replacing its status. An intentional error
response after a read-limit error is preserved.

Responses stream from `Write`; `http.Flusher` and `http.ResponseController.Flush`
publish a head even without body bytes. ResponseController.EnableFullDuplex is
supported. Header changes after the final head do not affect the response.
HEAD suppresses body bytes; Content-Length is enforced when supplied, and is
not inferred by buffering the complete response. A panic before commit produces
an empty 500; panic after commit or `http.ErrAbortHandler` resets the stream.

Use `RawHeaders(r)` for original ordered headers and `Scheme(r)` for the caller's
scheme. Request.TLS remains nil because the public handshake happens in
Claviron. Request ContentLength comes from declared metadata, or is unknown for
a present stream without a length; an absent stream maps to `http.NoBody`.
Request.Body.Close interrupts a pending read without cancelling the response.

The Link body API currently carries byte streams, so trailers are not exposed
by this adapter. Declared request trailers are rejected; response trailers,
informational responses, hijacking and upgrades are unsupported.

The complete Go example has three routes:

- `/`: a small text response.
- `POST /echo`: copies upload chunks directly to the response, with backpressure.
- `GET /stream?bytes=262144`: flushes a head and generates the requested byte count.

Its `--io-buffer-bytes`, `--max-request-bytes` and `--max-response-bytes` flags
configure the budgets. Signal handling belongs to the example, not the library.

## Application lifecycle

Application lifecycle is separate from connection shutdown. `Start`, `Stop` and
`Restart` leave the Link lane, native I/O host and Go process alive:

| Action | State and generation |
| --- | --- |
| Start while running | No change. |
| Start while stopped | Open a new running generation. |
| Stop | Mark stopped and cancel the current generation; retain its number. |
| Restart | Cancel the previous generation and open a new running one. |

A stopped application returns 503 without calling the Go handler. New requests
capture an immutable generation, available through `Generation(r)`.
Stop/restart cancels their `Request.Context`, including blocked body reads and
response writes. Delayed handlers cannot publish a reply using a retired
native owner, even while the next generation is serving. Cancellation does not
kill goroutines or undo application side effects.

```go
snapshot, err := app.Snapshot() // Generation, Revision, Running
if err != nil { panic(err) }
_ = snapshot

ctx, err := app.GenerationContext() // scope caller-owned background work
if err != nil { panic(err) }
_ = ctx // observe ctx.Done(); the caller still owns starting/joining its jobs

if err := app.Stop(); err != nil { panic(err) }
if err := app.Start(); err != nil { panic(err) }
if err := app.Restart(); err != nil { panic(err) }
```

`Changes()` is a bounded, coalescing state notification: after a signal, read
Snapshot or GenerationContext for current local state. It is not an action log.
Revision orders state observations so a delayed notification cannot resurrect
an old generation context. A new generation receives a fresh context. Close
also cancels the generation context; Shutdown preserves accepted work through
its normal drain before final connection closure.

Claviron management uses the existing application ControlCommand with both a
`go` runtime target and matching application name. Wrong-runtime/name or
unqualified lifecycle commands do not execute. A decision received by one
instance is spread to currently connected same-name Go instances through the
existing notified application-event ring. This private fan-out is implemented;
it does not expose the public application-events API yet.

Fan-out is asynchronous, not an atomic fleet transaction or durable command log.
A local API result is not a sibling acknowledgement. Serialize management
changes and observe each instance's status when a coordinated transition is
required. A newly opened connection starts at generation 1; retained ring
history is not replayed as its initial application policy. Reader lag is surfaced
as a connection error rather than silently losing lifecycle decisions.

The Go example also exposes `GET /generation` and `GET /wait-generation`:
the latter flushes a head and waits for its request generation to be cancelled.
No lifecycle operation restarts the example's Go process.

## ABI migration

ABI v3 adds generation metadata, lifecycle actions, snapshots and a coalesced
lifecycle notification to v2 streaming operations. Rebuild the Go package and
its matching native library together. Older bindings are rejected before joining;
the version-query record keeps its original extent so they can query safely. Go
callers must now supply `IOBufferBytes`; zero total body limits mean unlimited
payload length. The canonical C header lives in the cgo package so Go's build
cache tracks header changes as well as Go source changes.

## Build and validate

Requirements: Go with cgo and a C compiler, Rust 1.98 or newer, Python 3 and a
matching Claviron source checkout. The Go language baseline is 1.23; initial
validation used Go 1.27.3 and Rust 1.99.1 on Linux x86-64. Other platforms have
not been validated. The explicit acceptance driver also needs the `orbit` CLI
for cleanup of its isolated test fleet.

```sh
python3 scripts/build.py --claviron-source /path/to/claviron -- test -race ./...
python3 scripts/acceptance.py --claviron-source /path/to/claviron
python3 scripts/build.py --claviron-source /path/to/claviron -- run ./examples/plain --fleet example --name hello
```

Use `--go /path/to/go` or `GO` to select the Go executable, and `--release` on
build.py for a release native library. The script preserves PATH and sets the
native library search path for the Go command. A local source override may also
live in ignored Cargo configuration. No local paths belong in the manifest.
For downstream builds, link the matching native library with `CGO_LDFLAGS` and
make it available to the platform loader; automated native distribution follows
in a later stage.

The acceptance driver builds both a normal Go fixture and the actual plain Go
example, then exchanges requests with Claviron's real ApplicationClient. It covers repeated headers, encoded URI,
addresses, bounded bodies, HEAD, concurrent application instances, cancellation,
handler panic before/after commit, failed reply publication and graceful drain.
It fills the upload window and proves the next write is pending, transfers bodies
larger than the stream window in both directions, cancels reads/writes, and
sends SIGTERM to the actual Go example during a streaming response. Lifecycle
acceptance verifies two-instance fan-out, wrong-target isolation, generation
context cancellation, delayed old handlers, Go lifecycle APIs and unchanged
process/lane ownership. The actual Go example is stopped, started and restarted
through real Claviron control frames. It stops its own
processes before clearing its unique fleet and preserves failed fleets for
inspection. No production listener or load benchmark is involved.

See [ARCHITECTURE.md](ARCHITECTURE.md), [the ABI header](internal/native/orbit_go.h)
and [PLAN.md](PLAN.md) for ownership contracts and remaining stages.

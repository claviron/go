# claviron/go

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
Application-owned invoke is implemented on a separate Link, sharing the same
Go process and native I/O runtime. Public event subscriptions remain a later stage. Unsupported management/event
frames outside the supported lifecycle/status contract are counted by
`UnsupportedFrames`; they perform no operation or success acknowledgement. An edge-shutdown notice does not cancel another caller's work.

The source targets Claviron 0.2.1. Rebuild the native library with the
matching Edge version; a Go/native ABI match alone does not establish Link wire
compatibility.

Go owns a dedicated multi-thread Orbit I/O runtime sized by `NativeThreads`.
`inherit`, running Orbit I/O on the host's own loop, is reserved and unavailable.

`MaxInFlight` bounds running exchanges and must be positive. `MaxQueued`
bounds waiting exchanges and may be zero. Their sum must be below the Link
inbox capacity. Waiting work uses the native Link queue.

## Install

Requires Go 1.23 or later with cgo and a C compiler.

```sh
go get github.com/claviron/go/orbit@latest
```

cgo links a native library that `go get` does not fetch. Each
[GitHub release](https://github.com/claviron/go/releases) carries one for
Linux and FreeBSD on x86-64 and arm64, and macOS on arm64. Download the one for
your platform and version, and build with it:

```sh
CGO_LDFLAGS="-L/path/to/lib -Wl,-rpath,/path/to/lib" go build
```

Alternatively, make it available to the platform loader at run time.

End-to-end validation covers Linux x86-64; the other platforms are built and
checked but have not been run end to end.

## Use from a Go application

```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "os/signal"

    "github.com/claviron/go/orbit"
)

func main() {
    app, err := orbit.Open(orbit.Options{
        Fleet: "example", Name: "hello", Incarnation: 1,
        NativeThreads: 1, MaxInFlight: 32, IOBufferBytes: 4096,
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
informational responses and hijacking are unsupported. WebSocket and WebTransport
requests go to `Options.Sessions`, described below, never to the HTTP handler.

The complete Go example has three routes:

- `/`: a small text response.
- `POST /echo`: copies upload chunks directly to the response, with backpressure.
- `GET /stream?bytes=262144`: flushes a head and generates the requested byte count.

Its `--io-buffer-bytes`, `--max-request-bytes` and `--max-response-bytes` flags
configure the budgets. Signal handling belongs to the example, not the library.

## WebSocket and WebTransport

Claviron terminates the protocol and the client's connection. Your application
decides whether to accept, and then exchanges whole messages; it never sees a
socket, a frame or a handshake. Both protocols use the same API:

```go
app, err := orbit.Open(orbit.Options{
	// identity and budgets as above
	Sessions: func(offer *orbit.SessionRequest) {
		if offer.Request.Header.Get("Authorization") == "" {
			_ = offer.Reject(http.StatusUnauthorized, nil)
			return
		}
		session, err := offer.Accept(64*1024, nil)
		if err != nil {
			return
		}
		for {
			message, err := session.Receive()
			if err != nil || message.Kind == orbit.MessageClose {
				return
			}
			if message.Kind == orbit.MessageText {
				err = session.SendText(string(message.Data))
			} else {
				err = session.SendBinary(message.Data)
			}
			if err != nil {
				return
			}
		}
	},
}, handler)
```

The client has not been answered while the handler decides, so authentication
belongs here and `Reject` reaches the client as a real HTTP status. Without
`Sessions` these requests are refused with 501.

`Receive` and one send may run at the same time from different goroutines. A
received close is answered for you. Return after `Close` to end cleanly; ending
without one is reported to the client as 1011. An accepted session holds one
`MaxInFlight` slot for its whole life and keeps `Shutdown` waiting until it ends,
so end long-lived sessions when you begin shutting down. Stopping or restarting the
application cancels them.

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

## Invoke on a separate Link

`app.OpenInvocations` attaches a service to the application's lifecycle. It uses
an independent fleet from HTTP, with Claviron's existing invoke protocol and
caller-reserved result streams. There is no second Go executable, embedded Go
runtime or framework dependency. HTTP uses `http.Handler`; invoke uses its own
`InvokeHandler` and opaque operation/payload values.

```go
calls, err := app.OpenInvocations(orbit.InvokeOptions{
    Fleet: "ex-rpc", Service: "go-service",
    PendingCalls: 32, AcceptedCalls: 32,
    OperationBytes: 64, RequestBytes: 4096, ResponseBytes: 65536,
    Routes: map[string]string{"peer": "peer-service"},
}, func(ctx context.Context, request orbit.Invocation) orbit.InvokeResult {
    if ctx.Err() != nil {
        return orbit.InvokeResult{Code: orbit.InvokeRemoteClosing}
    }
    if request.Operation != "echo" {
        return orbit.InvokeResult{Code: orbit.InvokeUnsupported}
    }
    return orbit.InvokeResult{Payload: request.Payload}
})
if err != nil { panic(err) }

// Use the HTTP request context inside an HTTP handler, or an owned generation
// context for background work. The target is a registered logical name.
ctx, err := app.GenerationContext()
if err != nil { panic(err) }
result, err := calls.Call(ctx, "peer", "echo", []byte("hello"))
if err != nil { /* local cancellation, capacity or binding failure */ }
if err == nil && result.Code == orbit.InvokeSuccess {
    _ = result.Payload
}
```

Prepare captured handler state before opening: dispatch can begin before the
open call returns. Routes are copied at open and then immutable. A missing or
ambiguous service returns its typed result; the binding does not select a peer
or retry. `InvokeOutcomeUnknown` means execution may have happened and is never
permission to retry automatically. Rejected domain bytes stay separate from
transport errors. Handlers may return Success, RemoteBusy, RemoteClosing,
Unsupported, ResponseTooLarge or Rejected. A panic becomes an empty Rejected;
an oversized response becomes ResponseTooLarge.

PendingCalls bounds retained outgoing calls; AcceptedCalls bounds retained
incoming handlers, including cancelled handlers that have not returned.
OperationBytes is positive; zero request/response byte bounds allow only empty
payloads. Bounds must fit the compiled Link geometry. These are independent of
HTTP admission/body limits. One native completion waiter serves each invoke
endpoint; individual calls park Go goroutines rather than one cgo thread per
call. Inputs are copied before asynchronous retention; no Go pointer is retained.

Caller context cancellation withdraws local interest and consumes its native
completion before returning. It does not cancel or roll back accepted remote
work. An invocation captures one immutable application generation; stop/restart
cancels its context, pending outbound calls and native reply eligibility. Late
handler results cannot answer another generation. HTTP, invoke and generation
contexts also carry their native owner and generation into nested calls; even
`context.WithoutCancel` cannot grant retired work a newer generation. A plain
independent context captures the current generation at submission.

Application Shutdown first withdraws HTTP and joins accepted HTTP handlers while
invoke remains usable for their nested calls. It then withdraws invoke intake,
joins accepted invoke handlers and their nested calls, and drains the core
result streams before releasing the application runtime. New invoke requests
receive RemoteClosing during this drain. Closing an endpoint explicitly cancels
its owned calls/handlers without closing HTTP. Application Close cancels both.
A handler must return cooperatively; no implicit deadline or forced termination
is installed. Do not synchronously wait for your own endpoint/application to
shut down from inside its handler: the owner joins that handler.

`calls.Shutdown(ctx)` starts endpoint drain; expiring ctx ends only the wait.
`calls.Done()` closes after native release and handler/caller joins. Closed
endpoints release their application ownership and can be replaced without
retaining old handlers for the application's lifetime.

See [the complete invoke example](examples/invoke/main.go). Its `/invoke` HTTP
route calls a peer service, while its invoke handler serves `echo` concurrently:

The operator supplies the separate peer service. Invocations are a trusted
same-host Link capability; domain authorization belongs in the handler.

## ABI migration

ABI v6 adds `MaxInFlight` and `MaxQueued` for receiver admission. ABI v5 added message sessions. ABI v4 added separate invoke owners, retained call/handler handles and signalled
completion delivery to v3 lifecycle/streaming operations. Use the Go package and
the native library of one version together. Older bindings are rejected before joining;
the version-query record keeps its original extent so they can query safely. Go
callers must now supply `IOBufferBytes`; zero total body limits mean unlimited
payload length. The canonical C header lives in the cgo package so Go's build
cache tracks header changes as well as Go source changes.

See [ARCHITECTURE.md](ARCHITECTURE.md) and
[the ABI header](internal/native/claviron_go.h) for ownership contracts.

# orbit-go

A Go library for handling Claviron application requests over same-host Orbit
Link. Your normal Go executable imports the library and supplies a standard
`net/http.Handler`. Claviron owns public connections and host routing; your Go
application owns its handlers, goroutines and process lifetime.

## Current stage

The first application binding is implemented: a versioned C ABI, named `go`
application membership, asynchronous native completion delivery, bounded
request/response bodies, peer cancellation and graceful withdrawal. A Rust
native library sits behind the private cgo adapter.

Application status uses Claviron's existing schema. Streaming, application
start/stop/restart, event subscriptions and invoke are later stages. Unsupported
management/event frames are counted by `UnsupportedFrames`; they are not
executed or acknowledged as successful. An edge-shutdown notice does not cancel
another caller's work.

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
        NativeThreads: 1, MaxExchanges: 32,
        MaxRequestBytes: 65536, MaxResponseBytes: 65536,
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

The initial HTTP adapter buffers request/response payloads within the declared
limits. Request overflow returns 413; handler panic or response overflow returns
an empty 500. `http.ErrAbortHandler` abandons the exchange. Use `RawHeaders(r)`
for ordered original headers and `Scheme(r)` for the authoritative caller scheme.
Request.TLS is nil: TLS belongs to the public connection. Streaming, trailers,
informational responses and upgrades are not supported yet.

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

The acceptance driver builds a normal Go fixture and exchanges requests with
Claviron's real ApplicationClient. It covers repeated headers, encoded URI,
addresses, bounded bodies, HEAD, concurrent application instances, cancellation,
handler panic, failed reply publication and graceful drain. It stops its own
processes before clearing its unique fleet and preserves failed fleets for
inspection. No production listener or load benchmark is involved.

See [ARCHITECTURE.md](ARCHITECTURE.md), [the ABI header](native/include/orbit_go.h)
and [PLAN.md](PLAN.md) for ownership contracts and remaining stages.

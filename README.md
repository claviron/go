# orbit-go

Go application integration for Claviron over Orbit Link, following the separate
application-process model used by orbit-js and orbit-java.

The Go process owns its runtime and application handlers. Claviron routes
application requests to it over shared memory. HTTP exchanges and invocations
are intended to use separate Link connections within the same Go process,
with cancellation, generation ownership and graceful drain.

The intended Go module path is `github.com/claviron/orbit-go`. This repository
is a scaffold: no binding, handler API or release is available yet. Framework
integrations can build on the Go API once it exists.

package orbit

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/claviron/go/internal/native"
)

// inputBody serializes reads but Close never waits behind a blocked Read.
// A read leases one bounded native buffer until the Go caller consumes it.
type inputBody struct {
	app    *Application
	id     native.Handle
	mu     sync.Mutex
	closed atomic.Bool
	eof    bool
	err    error
}

func (b *inputBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return 0, http.ErrBodyReadAfterClose
	}
	if len(p) == 0 {
		return 0, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	if b.eof {
		return 0, io.EOF
	}
	size := min(len(p), int(b.app.options.IOBufferBytes))
	n, err := b.app.ioOperation(b.id, true, func() error { return native.ReadStart(b.id, size) }, func() (int, error) { return native.ReadResult(b.id, p[:size]) })
	if err == native.Status(10) {
		err = &http.MaxBytesError{Limit: int64(b.app.options.MaxRequestBytes)}
	}
	if b.closed.Load() {
		err = http.ErrBodyReadAfterClose
	}
	if err != nil {
		b.err = err
		return n, err
	}
	if n == 0 {
		b.eof = true
		return 0, io.EOF
	}
	return n, nil
}
func (b *inputBody) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	return b.app.closeRead(b.id)
}
func (b *inputBody) failure() error { b.mu.Lock(); defer b.mu.Unlock(); return b.err }

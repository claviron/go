package httpadapter

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

var ErrResponseClosed = errors.New("orbit-go: response writer is closed")
var ErrResponseTooLarge = errors.New("orbit-go: response exceeds MaxResponseBytes")
var ErrUnsupportedResponse = errors.New("orbit-go: informational responses and trailers are not supported")

// Sink acknowledges transport operations after the bounded native writer accepts
// them. Finish sends FIN; Abort resets an already committed response.
type Sink interface {
	Head(int, http.Header) error
	Write([]byte) (int, error)
	Finish() error
	Abort()
}

// ResponseWriter sends each write immediately and implements http.Flusher.
// Header mutation follows net/http's usual handler-owned concurrency contract.
type ResponseWriter struct {
	mu             sync.Mutex
	sink           Sink
	header         http.Header
	committed      bool
	status         int
	total          uint64
	limit          uint64
	length         int64
	head, finished bool
	err            error
}

func NewResponseWriter(sink Sink, maxBytes uint64, head bool) *ResponseWriter {
	return &ResponseWriter{sink: sink, header: make(http.Header), limit: maxBytes, length: -1, head: head}
}
func (w *ResponseWriter) Header() http.Header { return w.header }
func (w *ResponseWriter) commit(status int) error {
	if status < 200 || status > 599 {
		return ErrUnsupportedResponse
	}
	for key := range w.header {
		if strings.EqualFold(key, "Trailer") || strings.EqualFold(key, "Transfer-Encoding") || strings.HasPrefix(key, http.TrailerPrefix) {
			return ErrUnsupportedResponse
		}
	}
	if values := w.header.Values("Content-Length"); len(values) > 0 {
		if len(values) != 1 {
			return fmt.Errorf("orbit-go: multiple Content-Length values")
		}
		n, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("orbit-go: invalid Content-Length")
		}
		w.length = n
	}
	w.status = status
	w.committed = true
	return w.sink.Head(status, w.header.Clone())
}
func (w *ResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if status < 100 || status > 999 {
		panic("invalid HTTP response status")
	}
	if w.committed || w.finished || w.err != nil {
		return
	}
	w.err = w.commit(status)
}
func (w *ResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return 0, ErrResponseClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.limit != 0 && uint64(len(p)) > w.limit-w.total {
		w.err = ErrResponseTooLarge
		return 0, w.err
	}
	if w.length >= 0 && uint64(len(p)) > uint64(w.length)-w.total {
		return 0, http.ErrContentLength
	}
	if !w.committed {
		if w.header.Get("Content-Type") == "" && w.header.Get("Content-Encoding") == "" && len(p) > 0 {
			w.header.Set("Content-Type", http.DetectContentType(p))
		}
		w.err = w.commit(http.StatusOK)
		if w.err != nil {
			return 0, w.err
		}
		// commit may have obtained a caller-declared length.
		if w.length >= 0 && uint64(len(p)) > uint64(w.length)-w.total {
			w.err = http.ErrContentLength
			return 0, w.err
		}
	}
	if w.status == http.StatusNoContent || w.status == http.StatusNotModified {
		return 0, http.ErrBodyNotAllowed
	}
	if w.head {
		w.total += uint64(len(p))
		return len(p), nil
	}
	n, err := w.sink.Write(p)
	w.total += uint64(n)
	w.err = err
	return n, err
}
func (w *ResponseWriter) Flush() { _ = w.FlushError() }
func (w *ResponseWriter) FlushError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return ErrResponseClosed
	}
	if w.err != nil {
		return w.err
	}
	if !w.committed {
		w.err = w.commit(http.StatusOK)
	}
	return w.err
}

// Reads and writes already have independent native owners.
func (w *ResponseWriter) EnableFullDuplex() error { return nil }

// Finish is called after the handler returns. A pre-head failure can become a
// final error response; after a committed head, failure must reset the stream.
func (w *ResponseWriter) Finish(failureStatus int, abort bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return w.err
	}
	w.finished = true
	if abort {
		w.sink.Abort()
		return w.err
	}
	if failureStatus == 0 && w.err != nil {
		failureStatus = 500
	}
	if failureStatus != 0 {
		if w.committed {
			w.sink.Abort()
			return w.err
		}
		w.header = make(http.Header)
		w.length = -1
		w.err = w.commit(failureStatus)
	}
	if !w.committed && w.err == nil {
		w.err = w.commit(http.StatusOK)
	}
	if w.length >= 0 && !w.head && w.total != uint64(w.length) {
		w.err = http.ErrContentLength
	}
	if w.err != nil {
		w.sink.Abort()
		return w.err
	}
	w.err = w.sink.Finish()
	if w.err != nil {
		w.sink.Abort()
	}
	return w.err
}

func (w *ResponseWriter) CommittedStatus() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.committed {
		return 0
	}
	return w.status
}

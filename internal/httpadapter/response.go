package httpadapter

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var ErrResponseTooLarge = errors.New("orbit-go: response exceeds MaxResponseBytes")
var ErrUnsupportedResponse = errors.New("orbit-go: streaming, informational responses and trailers are not supported in this stage")

// ResponseWriter is the first-stage bounded response adapter. It implements
// only http.ResponseWriter, with no streaming, hijacking or push interfaces.
type ResponseWriter struct {
	header    http.Header
	committed http.Header
	status    int
	body      []byte
	limit     uint64
	head      bool
	err       error
}

func NewResponseWriter(maxBytes uint64, head bool) *ResponseWriter {
	return &ResponseWriter{header: make(http.Header), limit: maxBytes, head: head}
}
func (w *ResponseWriter) Header() http.Header { return w.header }
func (w *ResponseWriter) WriteHeader(status int) {
	if status < 100 || status > 999 {
		panic("invalid HTTP response status")
	}
	if w.status != 0 {
		return
	}
	if status < 200 || status > 599 {
		w.err = ErrUnsupportedResponse
		return
	}
	w.status = status
	w.committed = w.header.Clone()
	if len(w.committed.Values("Trailer")) != 0 {
		w.err = ErrUnsupportedResponse
	}
	for key := range w.committed {
		if strings.HasPrefix(key, http.TrailerPrefix) {
			w.err = ErrUnsupportedResponse
		}
	}
}
func (w *ResponseWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.status == 0 {
		if w.header.Get("Content-Type") == "" && w.header.Get("Content-Encoding") == "" && len(p) > 0 {
			w.header.Set("Content-Type", http.DetectContentType(p))
		}
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.status == http.StatusNoContent || w.status == http.StatusNotModified {
		return 0, http.ErrBodyNotAllowed
	}
	if uint64(len(p)) > w.limit-uint64(len(w.body)) {
		w.err = ErrResponseTooLarge
		return 0, w.err
	}
	w.body = append(w.body, p...)
	return len(p), nil
}

// Response returns the committed buffered reply. It is valid after the handler
// returns; do not mutate the returned body or writer concurrently with serving.
func (w *ResponseWriter) Response() (int, http.Header, []byte, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, nil, nil, w.err
	}
	if length := w.committed.Get("Content-Length"); length != "" {
		n, err := strconv.ParseUint(length, 10, 64)
		if err != nil || (!w.head && n != uint64(len(w.body))) {
			return 0, nil, nil, fmt.Errorf("orbit-go: invalid response Content-Length")
		}
	} else if w.status != http.StatusNoContent && w.status != http.StatusNotModified {
		w.committed.Set("Content-Length", strconv.Itoa(len(w.body)))
	}
	if w.head {
		return w.status, w.committed, nil, nil
	}
	return w.status, w.committed, w.body, nil
}

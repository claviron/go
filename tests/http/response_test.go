package http_test

import (
	"bytes"
	"errors"
	"net/http"
	"testing"

	adapter "github.com/claviron/orbit-go/internal/httpadapter"
)

type sink struct {
	status            int
	headers           http.Header
	data              bytes.Buffer
	finished, aborted bool
}

func (s *sink) Head(status int, headers http.Header) error {
	s.status, s.headers = status, headers
	return nil
}
func (s *sink) Write(p []byte) (int, error) { return s.data.Write(p) }
func (s *sink) Finish() error               { s.finished = true; return nil }
func (s *sink) Abort()                      { s.aborted = true }

func TestStreamingCommitAndRepeatedHeaders(t *testing.T) {
	out := &sink{}
	w := adapter.NewResponseWriter(out, 16, false)
	w.Header().Add("Set-Cookie", "a=1")
	w.Header().Add("Set-Cookie", "b=2")
	w.WriteHeader(201)
	if out.status != 201 {
		t.Fatal("head was buffered")
	}
	w.Header().Set("X-Late", "ignored")
	w.WriteHeader(202)
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if out.data.String() != "hello" || out.finished {
		t.Fatal("write did not stream before handler completion")
	}
	if err := w.Finish(0, false); err != nil {
		t.Fatal(err)
	}
	if out.status != 201 || len(out.headers.Values("Set-Cookie")) != 2 || out.headers.Get("X-Late") != "" || !out.finished {
		t.Fatal(out)
	}
}
func TestFlushAndHead(t *testing.T) {
	out := &sink{}
	w := adapter.NewResponseWriter(out, 0, false)
	if err := http.NewResponseController(w).Flush(); err != nil {
		t.Fatal(err)
	}
	if out.status != 200 || out.finished {
		t.Fatal("flush did not publish a head")
	}
	if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
		t.Fatal(err)
	}
	out = &sink{}
	w = adapter.NewResponseWriter(out, 4, true)
	w.Header().Set("Content-Length", "3")
	if n, err := w.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	if err := w.Finish(0, false); err != nil || out.data.Len() != 0 || out.headers.Get("Content-Length") != "3" {
		t.Fatal(out, err)
	}
}
func TestFailuresBeforeAndAfterHead(t *testing.T) {
	out := &sink{}
	w := adapter.NewResponseWriter(out, 2, false)
	if _, err := w.Write([]byte("abc")); !errors.Is(err, adapter.ErrResponseTooLarge) {
		t.Fatal(err)
	}
	if err := w.Finish(0, false); err != nil || out.status != 500 || !out.finished || out.aborted {
		t.Fatal(out, err)
	}
	out = &sink{}
	w = adapter.NewResponseWriter(out, 2, false)
	w.WriteHeader(200)
	_, _ = w.Write([]byte("abc"))
	_ = w.Finish(0, false)
	if !out.aborted || out.status != 200 || out.finished {
		t.Fatal("committed failure was rewritten", out)
	}
	out = &sink{}
	w = adapter.NewResponseWriter(out, 0, false)
	w.WriteHeader(204)
	if _, err := w.Write([]byte("x")); !errors.Is(err, http.ErrBodyNotAllowed) {
		t.Fatal(err)
	}
	out = &sink{}
	w = adapter.NewResponseWriter(out, 0, false)
	w.Header().Set("Trailer", "X-End")
	w.WriteHeader(200)
	if err := w.Finish(0, false); err != nil || out.status != 500 {
		t.Fatal(out, err)
	}
}

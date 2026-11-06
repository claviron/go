package http_test

import (
	"errors"
	adapter "github.com/claviron/orbit-go/internal/httpadapter"
	"net/http"
	"testing"
)

func TestResponseCommitAndRepeatedHeaders(t *testing.T) {
	w := adapter.NewResponseWriter(16, false)
	w.Header().Add("Set-Cookie", "a=1")
	w.Header().Add("Set-Cookie", "b=2")
	w.WriteHeader(201)
	w.Header().Set("X-Late", "ignored")
	w.WriteHeader(202)
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	status, headers, body, err := w.Response()
	if err != nil || status != 201 || string(body) != "hello" || len(headers.Values("Set-Cookie")) != 2 || headers.Get("X-Late") != "" {
		t.Fatalf("unexpected response: %d %v %q %v", status, headers, body, err)
	}
}
func TestResponseBudgetAndHead(t *testing.T) {
	w := adapter.NewResponseWriter(2, false)
	if _, err := w.Write([]byte("abc")); !errors.Is(err, adapter.ErrResponseTooLarge) {
		t.Fatal(err)
	}
	w = adapter.NewResponseWriter(4, true)
	w.Write([]byte("abc"))
	status, headers, body, err := w.Response()
	if err != nil || status != 200 || body != nil || headers.Get("Content-Length") != "3" {
		t.Fatal(status, headers, body, err)
	}
}
func TestUnsupportedResponsesAndForbiddenBodies(t *testing.T) {
	for _, status := range []int{103, 600} {
		w := adapter.NewResponseWriter(16, false)
		w.WriteHeader(status)
		if _, _, _, err := w.Response(); !errors.Is(err, adapter.ErrUnsupportedResponse) {
			t.Fatal(status, err)
		}
	}
	w := adapter.NewResponseWriter(16, false)
	w.WriteHeader(204)
	if _, err := w.Write([]byte("x")); !errors.Is(err, http.ErrBodyNotAllowed) {
		t.Fatal(err)
	}
	w = adapter.NewResponseWriter(16, false)
	w.Header().Set("Trailer", "X-End")
	w.WriteHeader(200)
	if _, _, _, err := w.Response(); !errors.Is(err, adapter.ErrUnsupportedResponse) {
		t.Fatal(err)
	}
}

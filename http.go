package orbitgo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/claviron/orbit-go/internal/httpadapter"
	"github.com/claviron/orbit-go/internal/native"
)

// Header retains the ordered, original Link header name and value.
type Header struct{ Name, Value string }
type requestMetadata struct {
	headers []Header
	scheme  string
}
type metadataKey struct{}

// RawHeaders returns a copy of the original ordered header list, including Host.
func RawHeaders(r *http.Request) []Header {
	data, _ := r.Context().Value(metadataKey{}).(requestMetadata)
	return append([]Header(nil), data.headers...)
}

// Scheme reports the authoritative scheme supplied by the Claviron caller.
// Request.TLS is nil: this Go process did not perform that TLS handshake.
func Scheme(r *http.Request) string {
	data, _ := r.Context().Value(metadataKey{}).(requestMetadata)
	return data.scheme
}

func requestFromNative(input native.Request, body io.ReadCloser, ctx context.Context) (*http.Request, error) {
	target := input.Path
	if input.Query != "" {
		target += "?" + input.Query
	}
	parsed, err := url.ParseRequestURI(target)
	if err != nil {
		return nil, err
	}
	if input.Scheme != "http" && input.Scheme != "https" {
		return nil, fmt.Errorf("unsupported request scheme")
	}
	if input.Method == "" || strings.ContainsAny(input.Method, " \t\r\n") {
		return nil, fmt.Errorf("invalid request method")
	}
	headers := make(http.Header)
	raw := make([]Header, 0, len(input.Headers))
	host := ""
	for _, pair := range input.Headers {
		raw = append(raw, Header{pair.Name, pair.Value})
		if strings.EqualFold(pair.Name, "host") {
			if host != "" {
				return nil, fmt.Errorf("multiple Host headers")
			}
			host = pair.Value
		} else {
			headers.Add(pair.Name, pair.Value)
		}
	}
	if host == "" {
		host = input.Local
	}
	metadata := requestMetadata{headers: raw, scheme: input.Scheme}
	ctx = context.WithValue(ctx, metadataKey{}, metadata)
	if address, err := netip.ParseAddrPort(input.Local); err == nil {
		ctx = context.WithValue(ctx, http.LocalAddrContextKey, net.TCPAddrFromAddrPort(address))
	}
	length := int64(-1)
	if !input.HasBodyStream {
		body = http.NoBody
		length = 0
	}
	if values := headers.Values("Content-Length"); len(values) > 0 {
		if len(values) != 1 {
			return nil, fmt.Errorf("multiple Content-Length values")
		}
		n, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid Content-Length")
		}
		length = n
		if length == 0 {
			body = http.NoBody
		}
	}
	if headers.Get("Trailer") != "" {
		return nil, httpadapter.ErrUnsupportedResponse
	}
	r := &http.Request{Method: input.Method, URL: parsed, Proto: fmt.Sprintf("HTTP/%d.%d", input.Major, input.Minor), ProtoMajor: input.Major, ProtoMinor: input.Minor,
		Header: headers, Host: host, RemoteAddr: input.Peer, RequestURI: target,
		Body: body, ContentLength: length}
	return r.WithContext(ctx), nil
}

func (a *Application) dispatch(id native.Handle, input native.Request, ctx context.Context, mappingError error) {
	defer a.workers.Done()
	defer a.complete(id, true)
	body := &inputBody{app: a, id: id}
	defer body.Close()
	sink := &nativeSink{app: a, id: id}
	writer := httpadapter.NewResponseWriter(sink, a.options.MaxResponseBytes, input.Method == http.MethodHead)
	request, err := requestFromNative(input, body, ctx)
	if mappingError != nil || err != nil {
		status := 400
		if errors.Is(err, httpadapter.ErrUnsupportedResponse) {
			status = 501
		}
		_ = writer.Finish(status, false)
		return
	}
	if a.options.MaxRequestBytes != 0 && request.ContentLength > int64(a.options.MaxRequestBytes) {
		_ = writer.Finish(413, false)
		return
	}
	aborted, panicked := false, false
	func() {
		completed := false
		defer func() {
			if !completed {
				value := recover()
				aborted = value == http.ErrAbortHandler
				panicked = true
			}
		}()
		a.handler.ServeHTTP(writer, request)
		completed = true
	}()
	// Close input independently: a response may deliberately finish before an
	// upload. Existing pending reads receive their own completion before release.
	_ = body.Close()
	failure := 0
	if panicked {
		failure = 500
	} else {
		var limit *http.MaxBytesError
		if errors.As(body.failure(), &limit) && writer.CommittedStatus() < 400 {
			failure = 413
		}
	}
	_ = writer.Finish(failure, aborted || ctx.Err() != nil)
}

type nativeSink struct {
	app *Application
	id  native.Handle
}

func (s *nativeSink) Head(status int, headers http.Header) error {
	_, err := s.app.ioOperation(s.id, false, func() error { return native.Head(s.id, status, headers) }, func() (int, error) { return native.OutputResult(s.id) })
	return err
}
func (s *nativeSink) Write(data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		chunk := data[:min(len(data), int(s.app.options.IOBufferBytes))]
		n, err := s.app.ioOperation(s.id, false, func() error { return native.Write(s.id, chunk) }, func() (int, error) { return native.OutputResult(s.id) })
		total += n
		if err != nil {
			return total, err
		}
		if n != len(chunk) {
			return total, io.ErrShortWrite
		}
		data = data[n:]
	}
	return total, nil
}
func (s *nativeSink) Finish() error {
	_, err := s.app.ioOperation(s.id, false, func() error { return native.Finish(s.id) }, func() (int, error) { return native.OutputResult(s.id) })
	return err
}
func (s *nativeSink) Abort() {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if s.app.active[s.id] != nil {
		_ = native.Abort(s.id)
	}
}

package orbitgo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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

func requestFromNative(input native.Request, ctx context.Context) (*http.Request, error) {
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
	if address, err := net.ResolveTCPAddr("tcp", input.Local); input.Local != "" && err == nil {
		ctx = context.WithValue(ctx, http.LocalAddrContextKey, address)
	}
	var body io.ReadCloser = http.NoBody
	if len(input.Body) > 0 {
		body = io.NopCloser(bytes.NewReader(input.Body))
	}
	r := &http.Request{Method: input.Method, URL: parsed, Proto: fmt.Sprintf("HTTP/%d.%d", input.Major, input.Minor), ProtoMajor: input.Major, ProtoMinor: input.Minor,
		Header: headers, Host: host, RemoteAddr: input.Peer, RequestURI: target,
		Body: body, ContentLength: int64(len(input.Body))}
	return r.WithContext(ctx), nil
}

func (a *Application) dispatch(id native.Handle, input native.Request, ctx context.Context, mappingError error) {
	defer a.workers.Done()
	defer a.complete(id, true)
	request, err := requestFromNative(input, ctx)
	if mappingError != nil || err != nil {
		_ = native.Reply(id, 400, nil, nil)
		return
	}
	defer request.Body.Close()
	writer := httpadapter.NewResponseWriter(a.options.MaxResponseBytes, request.Method == http.MethodHead)
	aborted, panicked := false, false
	func() {
		defer func() {
			if value := recover(); value != nil {
				aborted = value == http.ErrAbortHandler
				panicked = true
			}
		}()
		a.handler.ServeHTTP(writer, request)
	}()
	if aborted || ctx.Err() != nil {
		_ = native.Abort(id)
		return
	}
	status, headers, body, err := writer.Response()
	if panicked || err != nil {
		status, headers, body = 500, nil, nil
	}
	_ = native.Reply(id, status, headers, body)
}

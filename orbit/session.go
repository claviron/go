package orbit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/claviron/go/internal/native"
)

// ErrMessageTooLarge reports a message over the session's maxMessageBytes. A
// received one ends the session; a refused send leaves it open.
var ErrMessageTooLarge = errors.New("orbit: message exceeds maxMessageBytes")

// SessionKind names the protocol the client spoke to Claviron. The application
// sees the same messages for both: Claviron owns the wire protocol.
type SessionKind string

const (
	SessionWebSocket    SessionKind = "websocket"
	SessionWebTransport SessionKind = "webtransport"
)

// MessageKind is one record's type on a session.
type MessageKind uint32

const (
	MessageText   = MessageKind(native.MessageText)
	MessageBinary = MessageKind(native.MessageBinary)
	MessageClose  = MessageKind(native.MessageClose)
)

// Message is one whole message. For MessageClose, Code is the close code and
// Data the reason. 1005 means no code was given and 1006 that the client left
// without a close; both are observations and are never sent.
type Message struct {
	Kind MessageKind
	Data []byte
	Code uint16
}

// SessionHandler decides one session request and then owns the accepted
// session. It runs on its own goroutine. Returning without answering refuses
// with 501, and returning without Close ends the session as an application end
// without a close, which clients see as 1011.
type SessionHandler func(*SessionRequest)

// SessionRequest is a client asking for a message session. Claviron has not
// answered that client yet, so Reject reaches it as a real HTTP status.
// Authentication and authorization belong here, from Request's headers.
type SessionRequest struct {
	Request *http.Request
	Kind    SessionKind

	app     *Application
	id      native.Handle
	mu      sync.Mutex
	replied bool
	session *Session
}

// Accept starts the session with an explicit per-message byte limit. header
// adds response headers to the handshake Claviron completes for the client.
func (r *SessionRequest) Accept(maxMessageBytes uint32, header http.Header) (*Session, error) {
	if maxMessageBytes == 0 {
		return nil, fmt.Errorf("orbit: a positive maxMessageBytes is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replied {
		return nil, fmt.Errorf("orbit: this session request was already answered")
	}
	r.replied = true
	_, err := r.app.ioOperation(r.id, false, func() error { return native.AcceptSession(r.id, maxMessageBytes, header) }, func() (int, error) { return native.OutputResult(r.id) })
	if err != nil {
		return nil, err
	}
	r.session = &Session{app: r.app, id: r.id, limit: maxMessageBytes}
	return r.session, nil
}

// Reject answers the client with a redirect or an error status.
func (r *SessionRequest) Reject(status int, header http.Header) error {
	if status < 300 || status > 599 {
		return fmt.Errorf("orbit: session rejection requires a 3xx-5xx status")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.replied {
		return fmt.Errorf("orbit: this session request was already answered")
	}
	r.replied = true
	return r.refuse(status, header)
}

func (r *SessionRequest) refuse(status int, header http.Header) error {
	sink := &nativeSink{app: r.app, id: r.id}
	if err := sink.Head(status, header); err != nil {
		return err
	}
	return sink.Finish()
}

// settle closes what the handler left open once it has returned.
func (r *SessionRequest) settle(failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case !r.replied && failed:
		_ = r.refuse(500, nil)
	case !r.replied:
		_ = r.refuse(501, nil)
	case r.session != nil && failed:
		r.session.Abort()
	case r.session != nil:
		r.session.finish()
	}
}

// Session is one accepted message stream. One Receive and one send may run at
// a time, each from its own goroutine; further calls wait for them.
type Session struct {
	app       *Application
	id        native.Handle
	limit     uint32
	receiving sync.Mutex
	sending   sync.Mutex
	ended     bool
}

// Receive returns the next message and io.EOF after the last one. A MessageClose
// was already answered, so the handler only has to return.
func (s *Session) Receive() (Message, error) {
	s.receiving.Lock()
	defer s.receiving.Unlock()
	var message Message
	var ended bool
	_, err := s.app.ioOperation(s.id, true, func() error { return native.MessageReadStart(s.id) }, func() (int, error) {
		kind, code, data, err := native.MessageReadResult(s.id)
		ended = kind == native.MessageEnd
		message = Message{Kind: MessageKind(kind), Data: data, Code: uint16(code)}
		return len(data), err
	})
	switch {
	case err == native.Status(10):
		return Message{}, ErrMessageTooLarge
	case err == ErrClosed || err == native.Status(3):
		return Message{}, io.EOF
	case err != nil:
		return Message{}, err
	case ended:
		return Message{}, io.EOF
	}
	return message, nil
}

// SendText sends one text message; text must be valid UTF-8.
func (s *Session) SendText(text string) error {
	return s.send(native.MessageText, 0, []byte(text))
}

// SendBinary sends one binary message.
func (s *Session) SendBinary(data []byte) error {
	return s.send(native.MessageBinary, 0, data)
}

// Close sends a close message and ends the session. Later sends fail.
func (s *Session) Close(code uint16, reason string) error {
	s.sending.Lock()
	defer s.sending.Unlock()
	if s.ended {
		return nil
	}
	s.ended = true
	return s.write(native.MessageClose, uint32(code), []byte(reason))
}

// Abort ends the session at once, without a close message.
func (s *Session) Abort() {
	s.app.mu.Lock()
	defer s.app.mu.Unlock()
	if s.app.active[s.id] != nil {
		_ = native.Abort(s.id)
	}
}

func (s *Session) send(kind, code uint32, data []byte) error {
	if uint64(len(data)) > uint64(s.limit) {
		return ErrMessageTooLarge
	}
	s.sending.Lock()
	defer s.sending.Unlock()
	if s.ended {
		return ErrClosed
	}
	return s.write(kind, code, data)
}

func (s *Session) write(kind, code uint32, data []byte) error {
	_, err := s.app.ioOperation(s.id, false, func() error { return native.MessageWrite(s.id, kind, code, data) }, func() (int, error) { return native.OutputResult(s.id) })
	return err
}

func (s *Session) finish() {
	s.sending.Lock()
	defer s.sending.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	_, _ = s.app.ioOperation(s.id, false, func() error { return native.Finish(s.id) }, func() (int, error) { return native.OutputResult(s.id) })
}

func (a *Application) dispatchSession(id native.Handle, input native.Request, ctx context.Context, mappingError error) {
	defer a.workers.Done()
	defer a.complete(id, true)
	if ctx.Err() != nil {
		_ = native.Abort(id)
		return
	}
	offer := &SessionRequest{app: a, id: id, Kind: SessionWebSocket}
	if input.Major == 3 {
		offer.Kind = SessionWebTransport
	}
	request, err := requestFromNative(input, http.NoBody, ctx)
	if mappingError != nil || err != nil {
		_ = offer.Reject(400, nil)
		return
	}
	if a.options.Sessions == nil {
		_ = offer.Reject(501, nil)
		return
	}
	offer.Request = request
	failed := true
	defer func() {
		// A handler's panic ends only its own session, as it does for HTTP.
		_ = recover()
		offer.settle(failed || ctx.Err() != nil)
	}()
	a.options.Sessions(offer)
	failed = false
}

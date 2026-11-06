package orbit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/claviron/go/internal/native"
)

// InvokeCode preserves the core protocol's typed results. OutcomeUnknown never
// means safe to retry: the remote operation may have executed.
type InvokeCode uint32

const (
	InvokeSuccess InvokeCode = iota
	InvokeUnknownTarget
	InvokeAmbiguousTarget
	InvokeUnavailable
	InvokeBusy
	InvokeClosed
	InvokeOutcomeUnknown
	InvokePayloadTooLarge
	InvokeProtocol
	InvokeTransport
	InvokeRemoteBusy
	InvokeRemoteClosing
	InvokeUnsupported
	InvokeResponseTooLarge
	InvokeRejected
	InvokeInvalidConfig
)

// InvokeResult contains opaque bytes. Only Success and Rejected carry payloads.
// Handlers may return Success, RemoteBusy, RemoteClosing, Unsupported,
// ResponseTooLarge or Rejected; other codes belong to the caller/transport.
type InvokeResult struct {
	Code    InvokeCode
	Payload []byte
}

// Invocation is independent of HTTP, with an immutable application generation.
// Payload is owned Go memory and may be retained by the handler.
type Invocation struct {
	Operation  string
	Payload    []byte
	Generation uint64
}
type InvokeHandler func(context.Context, Invocation) InvokeResult

// InvokeOptions declares a separate Link fleet, this endpoint's service name,
// caller-owned capacity/byte bounds, and immutable logical-target routes.
// Zero request/response bytes permits empty payloads only. No defaults, implicit
// timeout, discovery policy or retry is installed. Fleet must differ from HTTP.
type InvokeOptions struct {
	Fleet, Service                                                           string
	PendingCalls, AcceptedCalls, OperationBytes, RequestBytes, ResponseBytes uint64
	Routes                                                                   map[string]string
}
type invocationState struct {
	cancel                  context.CancelFunc
	handlerDone, nativeDone bool
}
type callState struct{ ready chan struct{} }

// Invocations owns a separate Link, sharing its Application's native runtime.
// One completion waiter dispatches ordinary Go handler goroutines.
type Invocations struct {
	parent             *Application
	mu                 sync.Mutex
	handle             native.Handle
	application        native.Handle
	handler            InvokeHandler
	incoming           map[native.Handle]*invocationState
	calls              map[native.Handle]*callState
	handlers, callers  sync.WaitGroup
	withdrawn, done    chan struct{}
	shutdown           sync.Once
	finishing          bool
	finalizing, closed bool
	err                error
}

// OpenInvocations attaches an invoke endpoint to this application's lifecycle.
// HTTP and invoke handlers run in the same Go process. Handler dispatch may
// start before this call returns; prepare captured state before opening.
func (a *Application) OpenInvocations(o InvokeOptions, handler InvokeHandler) (*Invocations, error) {
	maxInt := uint64(int(^uint(0) >> 1))
	if handler == nil || o.Fleet == "" || o.Service == "" || o.PendingCalls == 0 || o.AcceptedCalls == 0 || o.OperationBytes == 0 || o.PendingCalls > maxInt || o.AcceptedCalls > maxInt || o.RequestBytes > maxInt || o.ResponseBytes > maxInt {
		return nil, fmt.Errorf("orbit: invoke identity, handler and positive capacities are required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal || a.shutting {
		return nil, ErrClosed
	}
	handle, err := native.InvokeOpen(a.handle, native.InvokeOptions{Fleet: o.Fleet, Service: o.Service, PendingCalls: o.PendingCalls, AcceptedCalls: o.AcceptedCalls, OperationBytes: o.OperationBytes, RequestBytes: o.RequestBytes, ResponseBytes: o.ResponseBytes, Routes: o.Routes})
	if err != nil {
		return nil, err
	}
	owner := &Invocations{parent: a, application: a.handle, handle: handle, handler: handler, incoming: make(map[native.Handle]*invocationState), calls: make(map[native.Handle]*callState), withdrawn: make(chan struct{}), done: make(chan struct{})}
	if a.invocations == nil {
		a.invocations = make(map[*Invocations]struct{})
	}
	a.invocations[owner] = struct{}{}
	go owner.pump()
	return owner, nil
}
func (i *Invocations) Done() <-chan struct{} { return i.done }
func (i *Invocations) Err() error            { i.mu.Lock(); defer i.mu.Unlock(); return i.err }
func (i *Invocations) setError(err error) {
	if err != nil && i.err == nil {
		i.err = err
	}
}

// Call withdraws local interest on context cancellation, never remote work.
// Native completion is consumed before returning; errors are local failures,
// whereas remote/protocol outcomes are preserved in InvokeResult.Code.
func (i *Invocations) Call(ctx context.Context, target, operation string, payload []byte) (InvokeResult, error) {
	if ctx == nil {
		return InvokeResult{}, fmt.Errorf("orbit: nil invoke context")
	}
	if err := ctx.Err(); err != nil {
		return InvokeResult{}, err
	}
	i.mu.Lock()
	if i.finalizing || i.closed {
		i.mu.Unlock()
		return InvokeResult{}, ErrClosed
	}
	expected := uint64(0)
	if owner, ok := ctx.Value(generationContextKey{}).(generationOwner); ok {
		if owner.application != i.application {
			i.mu.Unlock()
			return InvokeResult{}, fmt.Errorf("orbit: invoke context belongs to another application")
		}
		expected = owner.generation
	}
	id, _, err := native.CallStart(i.handle, expected, target, operation, payload)
	if err != nil {
		i.mu.Unlock()
		return InvokeResult{}, invokeError(err)
	}
	call := &callState{ready: make(chan struct{})}
	i.calls[id] = call
	i.callers.Add(1)
	i.mu.Unlock()
	defer i.callers.Done()
	var cancelled error
	select {
	case <-call.ready:
	case <-ctx.Done():
		cancelled = ctx.Err()
		i.mu.Lock()
		_ = native.CallCancel(i.handle, id)
		i.mu.Unlock()
		<-call.ready
	}
	i.mu.Lock()
	code, bytes, err := native.CallResult(i.handle, id)
	delete(i.calls, id)
	i.tryFinish()
	i.mu.Unlock()
	if cancelled != nil {
		return InvokeResult{}, cancelled
	}
	return InvokeResult{InvokeCode(code), bytes}, invokeError(err)
}
func invokeError(err error) error {
	if errors.Is(err, native.Status(9)) {
		return context.Canceled
	}
	return err
}

// Shutdown closes inbound admission, joins accepted handlers (including their
// nested outbound calls), then drains the core driver. ctx limits only waiting.
func (i *Invocations) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("orbit: nil invoke shutdown context")
	}
	i.beginShutdown(false)
	select {
	case <-i.done:
		return i.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close explicitly cancels this endpoint's work, without stopping the Go process
// or its HTTP application. Cooperative handlers are joined without a deadline.
func (i *Invocations) Close() error { i.beginShutdown(true); <-i.done; return i.Err() }
func (i *Invocations) beginShutdown(cancel bool) {
	i.mu.Lock()
	if !i.closed {
		if cancel {
			_ = native.InvokeCancel(i.handle)
		} else {
			_ = native.InvokeWithdraw(i.handle)
		}
	}
	i.mu.Unlock()
	i.shutdown.Do(func() {
		go func() {
			<-i.withdrawn
			// The barrier is enqueued under the same native admission lock as requests.
			// All Add calls for accepted handlers precede this Wait.
			i.handlers.Wait()
			i.mu.Lock()
			i.finalizing = true
			i.tryFinish()
			i.mu.Unlock()
		}()
	})
}
func (i *Invocations) complete(id native.Handle, handler bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	state := i.incoming[id]
	if state == nil {
		return
	}
	if handler {
		state.handlerDone = true
		state.cancel()
	} else {
		state.nativeDone = true
	}
	if state.handlerDone && state.nativeDone {
		i.setError(native.InvocationRelease(i.handle, id))
		delete(i.incoming, id)
		i.tryFinish()
	}
}
func (i *Invocations) dispatch(id native.Handle, request native.Invocation, ctx context.Context, readErr error) {
	defer i.handlers.Done()
	defer i.complete(id, true)
	result := InvokeResult{Code: InvokeUnsupported}
	// User panics are a rejected handler result, never a panic across cgo.
	func() {
		defer func() {
			if recover() != nil {
				result = InvokeResult{Code: InvokeRejected}
			}
		}()
		if readErr == nil && ctx.Err() == nil {
			result = i.handler(ctx, Invocation{request.Operation, request.Payload, request.Generation})
		}
	}()
	err := native.InvocationReply(i.handle, id, uint32(result.Code), result.Payload)
	if errors.Is(err, native.Status(10)) {
		_ = native.InvocationReply(i.handle, id, uint32(InvokeResponseTooLarge), nil)
	} else if errors.Is(err, native.Status(1)) {
		_ = native.InvocationReply(i.handle, id, uint32(InvokeRejected), nil)
	}
}
func (i *Invocations) pump() {
	for {
		event, err := native.InvokeWait(i.handle)
		if err != nil {
			i.mu.Lock()
			i.setError(err)
			i.mu.Unlock()
			close(i.done)
			return
		}
		switch event.Kind {
		case 1:
			request, err := native.ReadInvocation(i.handle, event.Exchange)
			ctx, cancel := context.WithCancel(generationContext(i.application, request.Generation))
			if request.Cancelled {
				cancel()
			}
			i.mu.Lock()
			i.incoming[event.Exchange] = &invocationState{cancel: cancel}
			i.handlers.Add(1)
			i.mu.Unlock()
			go i.dispatch(event.Exchange, request, ctx, err)
		case 2:
			i.mu.Lock()
			if state := i.incoming[event.Exchange]; state != nil {
				state.cancel()
			}
			i.mu.Unlock()
		case 3:
			i.complete(event.Exchange, false)
		case 4:
			close(i.withdrawn)
		case 5:
			i.mu.Lock()
			if call := i.calls[event.Exchange]; call != nil {
				close(call.ready)
			} else {
				i.setError(fmt.Errorf("orbit: invoke result without owner"))
			}
			i.mu.Unlock()
		case 6:
			i.mu.Lock()
			i.closed = true
			if event.Status != 0 {
				i.setError(native.Status(event.Status))
			}
			i.mu.Unlock()
			i.handlers.Wait()
			i.callers.Wait()
			i.mu.Lock()
			i.setError(native.InvokeRelease(i.handle))
			i.mu.Unlock()
			close(i.done)
			i.parent.mu.Lock()
			delete(i.parent.invocations, i)
			i.parent.mu.Unlock()
			i.parent = nil
			return
		}
	}
}

// Called under mu. Native completion may follow the Go handler's return; both
// owners must release before closing the core driver. No polling is needed.
func (i *Invocations) tryFinish() {
	if i.finalizing && !i.finishing && !i.closed && len(i.incoming) == 0 && len(i.calls) == 0 {
		i.finishing = true
		i.setError(native.InvokeFinish(i.handle))
	}
}

// Package orbitgo connects a Go application's handlers to Claviron over Orbit Link.
// The application owns main, its Go runtime and its process lifecycle.
package orbitgo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/claviron/orbit-go/internal/native"
)

var ErrClosed = errors.New("orbit-go: application is closed")

// Options declares application identity and native resource budgets. Identity,
// NativeThreads, MaxExchanges and IOBufferBytes are required. The library invents
// no timeout, retry policy or worker count.
// IOBufferBytes bounds each pending read/write. Zero total body limits disable
// that limit; they do not change the bounded stream window or operation size.
type Options struct {
	Fleet, Name                                     string
	Incarnation                                     uint64
	NativeThreads                                   uint32
	MaxExchanges, MaxRequestBytes, MaxResponseBytes uint64
	IOBufferBytes                                   uint64
}

type exchangeState struct {
	cancel                  context.CancelFunc
	handlerDone, nativeDone bool
	io                      int
	readDone, outputDone    chan struct{}
}

// Application is one application Link membership, not an HTTP server or runner.
// One native completion waiter and NativeThreads native I/O threads serve it.
// User handlers run on Go goroutines and never on the native I/O owner.
type Application struct {
	mu        sync.Mutex
	handle    native.Handle
	options   Options
	handler   http.Handler
	lane      uint64
	active    map[native.Handle]*exchangeState
	workers   sync.WaitGroup
	ioWorkers sync.WaitGroup
	done      chan struct{}
	err       error
	terminal  bool
}

// Open joins the Link and begins serving the supplied handler. It opens no
// public listener, installs no signal handler and does not own the Go process.
// Dispatch may begin before Open returns; handler state must already be ready.
func Open(options Options, handler http.Handler) (*Application, error) {
	maxInt := uint64(int(^uint(0) >> 1))
	if handler == nil || options.Fleet == "" || options.Name == "" || options.Incarnation == 0 || options.NativeThreads == 0 || options.MaxExchanges == 0 || options.IOBufferBytes == 0 || options.IOBufferBytes > maxInt || options.MaxRequestBytes > maxInt || options.MaxResponseBytes > maxInt {
		return nil, fmt.Errorf("orbit-go: a handler, identity and positive resource budgets are required")
	}
	handle, lane, err := native.Open(native.Options{Fleet: options.Fleet, Name: options.Name, Incarnation: options.Incarnation, NativeThreads: options.NativeThreads, MaxExchanges: options.MaxExchanges, MaxRequestBytes: options.MaxRequestBytes, MaxResponseBytes: options.MaxResponseBytes, IOBufferBytes: options.IOBufferBytes})
	if err != nil {
		return nil, err
	}
	app := &Application{handle: handle, lane: lane, options: options, handler: handler, active: make(map[native.Handle]*exchangeState), done: make(chan struct{})}
	go app.pump()
	return app, nil
}

func (a *Application) Lane() uint64          { return a.lane }
func (a *Application) Done() <-chan struct{} { return a.done }

// Err reports a terminal connection/control error after Done closes.
func (a *Application) Err() error { a.mu.Lock(); defer a.mu.Unlock(); return a.err }

// Shutdown withdraws admission and drains already admitted handlers and replies.
// Expiring ctx ends only this wait; call Close explicitly to cancel owned work.
func (a *Application) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("orbit-go: nil shutdown context")
	}
	a.mu.Lock()
	if !a.terminal {
		if err := native.Withdraw(a.handle); err != nil {
			a.mu.Unlock()
			return err
		}
	}
	a.mu.Unlock()
	select {
	case <-a.done:
		return a.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close withdraws intake, cancels owned request contexts and joins native I/O
// and handler goroutines. Handlers must cooperate with their context; Go code
// cannot be forcibly terminated. It never exits the process or unlinks a fleet.
func (a *Application) Close() error {
	a.mu.Lock()
	if !a.terminal {
		_ = native.Cancel(a.handle)
		for _, exchange := range a.active {
			exchange.cancel()
		}
	}
	a.mu.Unlock()
	<-a.done
	return a.Err()
}

func (a *Application) setError(err error) {
	if err != nil && a.err == nil {
		a.err = err
	}
}

func (a *Application) complete(id native.Handle, handler bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.active[id]
	if state == nil {
		// Native refusals (e.g. an over-limit body) never dispatch a Go handler.
		if !handler {
			a.setError(native.ReleaseExchange(a.handle, id))
		}
		return
	}
	if handler {
		state.handlerDone = true
		state.cancel()
	} else {
		state.nativeDone = true
	}
	if state.handlerDone && state.nativeDone && state.io == 0 {
		a.setError(native.ReleaseExchange(a.handle, id))
		delete(a.active, id)
	}
}

func (a *Application) pump() {
	for {
		event, err := native.Wait(a.handle)
		if err != nil {
			a.mu.Lock()
			a.setError(err)
			a.mu.Unlock()
			break
		}
		switch event.Kind {
		case 1:
			request, err := native.ReadRequest(event.Exchange)
			ctx, cancel := context.WithCancel(context.Background())
			a.mu.Lock()
			a.active[event.Exchange] = &exchangeState{cancel: cancel, readDone: make(chan struct{}, 1), outputDone: make(chan struct{}, 1)}
			a.workers.Add(1)
			a.mu.Unlock()
			go a.dispatch(event.Exchange, request, ctx, err)
		case 2:
			a.mu.Lock()
			if state := a.active[event.Exchange]; state != nil {
				state.cancel()
			}
			a.mu.Unlock()
		case 3:
			if event.Status != 0 {
				a.mu.Lock()
				if state := a.active[event.Exchange]; state != nil {
					state.cancel()
				}
				a.mu.Unlock()
			}
			a.complete(event.Exchange, false)
		case 4:
			a.mu.Lock()
			a.terminal = true
			if event.Status != 0 {
				a.setError(native.Status(event.Status))
			}
			for _, state := range a.active {
				state.cancel()
			}
			a.mu.Unlock()
			a.workers.Wait()
			a.ioWorkers.Wait()
			a.mu.Lock()
			a.setError(native.Release(a.handle))
			a.mu.Unlock()
			close(a.done)
			return
		case 5, 6:
			a.mu.Lock()
			state := a.active[event.Exchange]
			if state != nil {
				ready := state.outputDone
				if event.Kind == 5 {
					ready = state.readDone
				}
				select {
				case ready <- struct{}{}:
				default:
					a.setError(fmt.Errorf("orbit-go: duplicate I/O completion"))
					_ = native.Cancel(a.handle)
				}
			} else {
				a.setError(fmt.Errorf("orbit-go: completion without owner"))
				_ = native.Cancel(a.handle)
			}
			a.mu.Unlock()
		default:
			a.mu.Lock()
			a.setError(fmt.Errorf("orbit-go: invalid completion kind %d", event.Kind))
			_ = native.Cancel(a.handle)
			a.mu.Unlock()
		}
	}
	// A broken ABI wait is not a normal application close. Surface the error;
	// do not claim native resources were successfully released.
	close(a.done)
}

// UnsupportedFrames reports management/event frames this stage could not handle.
// They are not executed or acknowledged as successful operations. In particular,
// an edge shutdown notice never cancels work owned by other callers.
func (a *Application) UnsupportedFrames() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return 0, ErrClosed
	}
	return native.Unsupported(a.handle)
}

// ioOperation pins the exchange until its delivered result has been consumed.
// Blocking here parks a Go goroutine, never a native thread per request.
func (a *Application) ioOperation(id native.Handle, read bool, submit func() error, result func() (int, error)) (int, error) {
	a.mu.Lock()
	state := a.active[id]
	if a.terminal || state == nil || state.nativeDone || state.handlerDone {
		a.mu.Unlock()
		return 0, ErrClosed
	}
	state.io++
	a.ioWorkers.Add(1)
	ready := state.outputDone
	if read {
		ready = state.readDone
	}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		state.io--
		if state.io == 0 && state.nativeDone && state.handlerDone {
			a.setError(native.ReleaseExchange(a.handle, id))
			delete(a.active, id)
		}
		a.mu.Unlock()
		a.ioWorkers.Done()
	}()
	if err := submit(); err != nil {
		return 0, err
	}
	<-ready
	return result()
}

func (a *Application) closeRead(id native.Handle) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[id] == nil || a.terminal {
		return nil
	}
	return native.CloseRead(id)
}

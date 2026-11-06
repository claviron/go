package orbit

import (
	"context"
	"github.com/claviron/go/internal/native"
)

// A handler context carries its native owner as well as its generation. This
// survives context.WithoutCancel: detaching a Go deadline must not grant an old
// handler admission to a newer native generation.
type generationContextKey struct{}
type generationOwner struct {
	application native.Handle
	generation  uint64
}

func generationContext(app native.Handle, generation uint64) context.Context {
	return context.WithValue(context.Background(), generationContextKey{}, generationOwner{app, generation})
}

// Snapshot describes the local application, independently of its connection.
// Revision orders observed state changes; Generation identifies accepted work.
type Snapshot struct {
	Generation, Revision uint64
	Running              bool
}

// syncLifecycle runs before publication at Open, or under a.mu afterward.
// Older snapshots cannot resurrect a retired generation or stopped context.
func (a *Application) syncLifecycle(value native.LifecycleState) {
	if value.Revision <= a.lifecycle.Revision {
		return
	}
	if a.generationCancel != nil && (value.Generation != a.lifecycle.Generation || !value.Running) {
		a.generationCancel()
	}
	if value.Generation != a.lifecycle.Generation {
		a.generationContext, a.generationCancel = context.WithCancel(generationContext(a.handle, value.Generation))
	}
	if !value.Running {
		a.generationCancel()
	}
	a.lifecycle = Snapshot{Generation: value.Generation, Revision: value.Revision, Running: value.Running}
	select {
	case a.changes <- struct{}{}:
	default:
	}
}

// Snapshot returns authoritative local state. Fan-out to sibling instances is
// asynchronous; a successful local action is not an acknowledgement from them.
func (a *Application) Snapshot() (Snapshot, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return Snapshot{}, ErrClosed
	}
	value, err := native.Snapshot(a.handle, false)
	if err != nil {
		return Snapshot{}, err
	}
	a.syncLifecycle(value)
	return a.lifecycle, nil
}

// GenerationContext is cancelled on stop/restart or connection Close. It can
// scope background work that the application owns. It does not terminate Go
// goroutines or roll back work. A running new generation receives a fresh context.
func (a *Application) GenerationContext() (context.Context, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return nil, ErrClosed
	}
	value, err := native.Snapshot(a.handle, false)
	if err != nil {
		return nil, err
	}
	a.syncLifecycle(value)
	return a.generationContext, nil
}

// Changes signals that a newer snapshot is available. Signals coalesce; this
// is not an action log. Read Snapshot after a signal. The channel closes with
// the connection; never execute user callbacks on the native completion pump.
func (a *Application) Changes() <-chan struct{} { return a.changes }

func (a *Application) action(code uint32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return ErrClosed
	}
	err := native.Lifecycle(a.handle, code)
	// A publication error cannot undo a transition or restore its old token.
	if snapshot, snapshotErr := native.Snapshot(a.handle, false); snapshotErr == nil {
		a.syncLifecycle(snapshot)
	}
	return err
}

// Start resumes a stopped application in a new generation; running Start is a no-op.
func (a *Application) Start() error { return a.action(1) }

// Stop retires the current generation but retains the Link, I/O host and process.
func (a *Application) Stop() error { return a.action(2) }

// Restart retires current work and immediately opens a new running generation.
func (a *Application) Restart() error { return a.action(3) }

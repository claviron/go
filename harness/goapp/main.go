// Isolated acceptance fixture; this is an ordinary Go application.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/claviron/go/orbit"
)

func main() {
	processID := fmt.Sprint(os.Getpid())
	if len(os.Args) < 3 {
		panic("usage: goapp <fleet> <name>")
	}
	gate := make(chan struct{})
	lateGate := make(chan struct{})
	var lateOnce sync.Once
	panicGate := make(chan struct{})
	var panicOnce sync.Once
	var release sync.Once
	closeBody := make(chan struct{})
	var closeBodyOnce sync.Once
	limit := uint64(1024)
	if len(os.Args) > 3 && os.Args[3] == "stream" {
		limit = 0
	}
	var owner atomic.Pointer[orbit.Application]
	app, err := orbit.Open(orbit.Options{Fleet: os.Args[1], Name: os.Args[2], Incarnation: 1,
		NativeThreads: 1, MaxInFlight: 32, IOBufferBytes: 4096, MaxRequestBytes: limit, MaxResponseBytes: limit, Sessions: serveSession},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/generation":
				fmt.Fprint(w, orbit.Generation(r))
				return
			case "/late":
				generation := orbit.Generation(r)
				fmt.Printf("LATE %d\n", generation)
				<-lateGate
				if _, err := w.Write([]byte("late")); err == nil {
					panic("retired handler published a response")
				}
				fmt.Printf("LATE_REJECTED %d\n", orbit.Generation(r))
				return
			case "/consume":
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					w.WriteHeader(413)
					return
				}
				w.WriteHeader(204)
				return
			case "/read-gate":
				fmt.Println("READ_GATE")
				<-gate
				_, err := io.CopyBuffer(w, r.Body, make([]byte, 4096))
				if err != nil {
					panic(http.ErrAbortHandler)
				}
				return
			case "/read-cancel":
				fmt.Println("READ_WAIT")
				_, err := r.Body.Read(make([]byte, 1))
				if err == nil {
					panic("read unexpectedly succeeded")
				}
				<-r.Context().Done()
				fmt.Println("READ_CANCELLED")
				return
			case "/read-close":
				go func() { <-closeBody; _ = r.Body.Close() }()
				fmt.Println("READ_CLOSE_WAIT")
				_, err := r.Body.Read(make([]byte, 1))
				if err != http.ErrBodyReadAfterClose {
					panic(err)
				}
				fmt.Println("READ_CLOSED")
				w.WriteHeader(204)
				return
			case "/write-cancel", "/write-drain":
				// The caller chooses the physical stream-window size.
				size, err := strconv.Atoi(r.URL.Query().Get("window"))
				if err != nil || size <= 0 {
					panic("invalid stream window")
				}
				if _, err = w.Write(bytes.Repeat([]byte("w"), size)); err != nil {
					panic(err)
				}
				fmt.Println("WRITE_WINDOW")
				_, err = w.Write([]byte("tail"))
				if r.URL.Path == "/write-cancel" {
					if err == nil {
						panic("cancel test unexpectedly drained")
					}
					<-r.Context().Done()
					fmt.Println("WRITE_CANCELLED")
				} else if err != nil {
					panic(err)
				}
				return
			case "/diagnostics":
				count, err := owner.Load().UnsupportedFrames()
				if err != nil {
					panic(err)
				}
				fmt.Fprint(w, count)
				return
			case "/panic-stream":
				w.WriteHeader(201)
				w.(http.Flusher).Flush()
				fmt.Println("PANIC_HEAD")
				<-panicGate
				panic("failure after head")
			case "/panic":
				panic("handler failed")
			case "/abort":
				panic(http.ErrAbortHandler)
			case "/large":
				_, _ = w.Write(make([]byte, 1025))
				return
			case "/wait":
				fmt.Println("WAIT")
				<-r.Context().Done()
				fmt.Println("CANCELLED")
				return
			case "/drain":
				fmt.Println("DRAIN")
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
				_, _ = io.WriteString(w, "drained")
				return
			case "/inspect/name":
				if r.URL.Scheme != "" || r.TLS != nil {
					panic("not a server request")
				}
				if r.RemoteAddr != "192.0.2.10:1234" {
					panic("peer lost")
				}
				if r.Context().Value(http.LocalAddrContextKey) == nil {
					panic("local address lost")
				}
				raw := orbit.RawHeaders(r)
				if len(raw) != 3 || raw[1].Name != "x-Test" || raw[2].Value != "second" {
					panic("raw headers lost")
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(413)
					return
				}
				w.Header().Add("Set-Cookie", "a=1")
				w.Header().Add("Set-Cookie", "b=2")
				w.WriteHeader(201)
				fmt.Fprintf(w, "%s|%s|%s|%s|%s|%s", r.Method, r.RequestURI, r.Host, orbit.Scheme(r), r.Header.Values("X-Test"), body)
				return
			}
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", "5")
			}
			w.Header().Set("X-Instance", processID)
			_, _ = io.WriteString(w, "hello")
		}))
	if err != nil {
		panic(err)
	}
	owner.Store(app)
	fmt.Printf("READY %d\n", app.Lane())
	finished := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text() {
			case "watch":
				ctx, err := app.GenerationContext()
				if err != nil {
					panic(err)
				}
				snapshot, err := app.Snapshot()
				if err != nil {
					panic(err)
				}
				fmt.Printf("WATCH %d\n", snapshot.Generation)
				go func(generation uint64) { <-ctx.Done(); fmt.Printf("GEN_CANCELLED %d\n", generation) }(snapshot.Generation)
			case "release-late":
				lateOnce.Do(func() { close(lateGate) })
			case "start-api", "stop-api", "restart-api":
				var err error
				switch scanner.Text() {
				case "start-api":
					err = app.Start()
				case "stop-api":
					err = app.Stop()
				case "restart-api":
					err = app.Restart()
				}
				if err != nil {
					panic(err)
				}
				snapshot, err := app.Snapshot()
				if err != nil {
					panic(err)
				}
				fmt.Printf("STATE %d %d %t\n", snapshot.Generation, snapshot.Revision, snapshot.Running)
			case "panic":
				panicOnce.Do(func() { close(panicGate) })
			case "close-body":
				closeBodyOnce.Do(func() { close(closeBody) })
			case "release":
				release.Do(func() { close(gate) })
			case "shutdown":
				go func() {
					if err := app.Shutdown(context.Background()); err != nil {
						panic(err)
					}
					fmt.Println("CLOSED")
					close(finished)
				}()
			case "close":
				if err := app.Close(); err != nil {
					panic(err)
				}
				if err := app.Close(); err != nil {
					panic(err)
				}
				fmt.Println("CLOSED")
				close(finished)
				return
			}
		}
	}()
	select {
	case <-finished:
	case <-app.Done():
		if err := app.Err(); err != nil {
			fmt.Printf("FAULT %v\n", err)
			return
		}
		<-finished
	}
}

// serveSession is the acceptance fixture for message sessions.
func serveSession(offer *orbit.SessionRequest) {
	switch offer.Request.URL.Path {
	case "/session-reject":
		_ = offer.Reject(403, http.Header{"X-Refused": {offer.Request.Header.Get("X-Token")}})
	case "/session-silent":
	case "/session-panic":
		panic("session handler failed")
	case "/session-end":
		if _, err := offer.Accept(16, nil); err != nil {
			panic(err)
		}
	case "/session-close":
		session, err := offer.Accept(16, nil)
		if err != nil {
			panic(err)
		}
		if err = session.Close(4000, "bye"); err != nil {
			panic(err)
		}
		if err = session.SendText("late"); err != orbit.ErrClosed {
			panic(fmt.Sprintf("send after close: %v", err))
		}
	default:
		session, err := offer.Accept(16, http.Header{"X-Accepted": {string(offer.Kind)}})
		if err != nil {
			panic(err)
		}
		if err = session.SendBinary(make([]byte, 17)); err != orbit.ErrMessageTooLarge {
			panic(fmt.Sprintf("oversized send: %v", err))
		}
		for {
			message, err := session.Receive()
			if err == orbit.ErrMessageTooLarge {
				fmt.Println("SESSION_TOO_LARGE")
				return
			}
			if err == io.EOF {
				fmt.Println("SESSION_END")
				return
			}
			if err != nil {
				panic(err)
			}
			switch message.Kind {
			case orbit.MessageText:
				err = session.SendText(string(message.Data))
			case orbit.MessageBinary:
				err = session.SendBinary(message.Data)
			case orbit.MessageClose:
				fmt.Printf("SESSION_CLOSE %d %s\n", message.Code, message.Data)
				return
			}
			if err != nil {
				panic(err)
			}
		}
	}
}

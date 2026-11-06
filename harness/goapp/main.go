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

	orbitgo "github.com/claviron/orbit-go"
)

func main() {
	if len(os.Args) < 3 {
		panic("usage: goapp <fleet> <name>")
	}
	gate := make(chan struct{})
	panicGate := make(chan struct{})
	var panicOnce sync.Once
	var release sync.Once
	closeBody := make(chan struct{})
	var closeBodyOnce sync.Once
	limit := uint64(1024)
	if len(os.Args) > 3 && os.Args[3] == "stream" {
		limit = 0
	}
	var owner atomic.Pointer[orbitgo.Application]
	app, err := orbitgo.Open(orbitgo.Options{Fleet: os.Args[1], Name: os.Args[2], Incarnation: 1,
		NativeThreads: 1, MaxExchanges: 32, IOBufferBytes: 4096, MaxRequestBytes: limit, MaxResponseBytes: limit},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
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
				raw := orbitgo.RawHeaders(r)
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
				fmt.Fprintf(w, "%s|%s|%s|%s|%s|%s", r.Method, r.RequestURI, r.Host, orbitgo.Scheme(r), r.Header.Values("X-Test"), body)
				return
			}
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", "5")
			}
			w.Header().Set("X-Instance", fmt.Sprint(os.Getpid()))
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

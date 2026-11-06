// Isolated acceptance fixture; this is an ordinary Go application.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	orbitgo "github.com/claviron/orbit-go"
)

func main() {
	if len(os.Args) != 3 {
		panic("usage: goapp <fleet> <name>")
	}
	gate := make(chan struct{})
	var release sync.Once
	var owner atomic.Pointer[orbitgo.Application]
	app, err := orbitgo.Open(orbitgo.Options{Fleet: os.Args[1], Name: os.Args[2], Incarnation: 1,
		NativeThreads: 1, MaxExchanges: 32, MaxRequestBytes: 1024, MaxResponseBytes: 1024},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/diagnostics":
				count, err := owner.Load().UnsupportedFrames()
				if err != nil {
					panic(err)
				}
				fmt.Fprint(w, count)
				return
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
					panic(err)
				}
				w.Header().Add("Set-Cookie", "a=1")
				w.Header().Add("Set-Cookie", "b=2")
				w.WriteHeader(201)
				fmt.Fprintf(w, "%s|%s|%s|%s|%s|%s", r.Method, r.RequestURI, r.Host, orbitgo.Scheme(r), r.Header.Values("X-Test"), body)
				return
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

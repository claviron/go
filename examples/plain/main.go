// A normal Go application using claviron-go. The caller owns its process lifecycle.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/claviron/go/orbit"
)

func main() {
	fleet := flag.String("fleet", "example", "Orbit Link fleet")
	name := flag.String("name", "hello", "application name")
	buffer := flag.Uint64("io-buffer-bytes", 4096, "maximum bytes per native I/O operation")
	maxRequest := flag.Uint64("max-request-bytes", 0, "total upload limit; zero disables")
	maxResponse := flag.Uint64("max-response-bytes", 0, "total response limit; zero disables")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /generation", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, orbit.Generation(r))
	})
	mux.HandleFunc("GET /wait-generation", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Generation", strconv.FormatUint(orbit.Generation(r), 10))
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		<-r.Context().Done()
	})
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if _, err := io.CopyBuffer(w, r.Body, make([]byte, int(*buffer))); err != nil {
			panic(http.ErrAbortHandler)
		}
	})
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, r *http.Request) {
		size, err := strconv.ParseUint(r.URL.Query().Get("bytes"), 10, 64)
		if err != nil {
			http.Error(w, "a nonnegative bytes parameter is required", 400)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		chunk := bytes.Repeat([]byte("g"), int(*buffer))
		for size > 0 {
			n := min(size, uint64(len(chunk)))
			if _, err := w.Write(chunk[:int(n)]); err != nil {
				return
			}
			size -= n
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "Hello from Go")
	})
	app, err := orbit.Open(orbit.Options{Fleet: *fleet, Name: *name, Incarnation: 1,
		NativeThreads: 1, MaxExchanges: 32, IOBufferBytes: *buffer,
		MaxRequestBytes: *maxRequest, MaxResponseBytes: *maxResponse}, mux)
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()
	fmt.Printf("Application %s joined lane %d\n", *name, app.Lane())
	select {
	case <-ctx.Done():
		err = app.Shutdown(context.Background())
	case <-app.Done():
		err = app.Err()
	}
	if err != nil {
		log.Print(err)
	}
}

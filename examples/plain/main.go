// A normal Go application using orbit-go; no HTTP listener or custom runner.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	orbitgo "github.com/claviron/orbit-go"
)

func main() {
	fleet := flag.String("fleet", "example", "Orbit Link fleet")
	name := flag.String("name", "hello", "application name")
	flag.Parse()
	app, err := orbitgo.Open(orbitgo.Options{Fleet: *fleet, Name: *name, Incarnation: 1,
		NativeThreads: 1, MaxExchanges: 32, MaxRequestBytes: 65536, MaxResponseBytes: 65536},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprintln(w, "Hello from Go")
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()
	fmt.Printf("Application %s joined lane %d\n", *name, app.Lane())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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

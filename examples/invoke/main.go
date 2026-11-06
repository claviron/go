// A normal Go application serving HTTP and invoke on separate Orbit Links.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/claviron/go/orbit"
)

func main() {
	fleet := flag.String("fleet", "", "HTTP application fleet")
	invokeFleet := flag.String("invoke-fleet", "", "separate invoke fleet")
	service := flag.String("service", "", "local invoke service name")
	peer := flag.String("peer", "", "peer invoke service name")
	flag.Parse()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	var endpoint atomic.Pointer[orbit.Invocations]
	app, err := orbit.Open(orbit.Options{Fleet: *fleet, Name: "app", Incarnation: uint64(os.Getpid()), NativeThreads: 1, MaxExchanges: 32, IOBufferBytes: 4096}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/invoke" {
			client := endpoint.Load()
			if client == nil {
				http.Error(w, "invoke is not ready", 503)
				return
			}
			result, err := client.Call(r.Context(), "peer", "echo", []byte("hello from HTTP"))
			if err != nil || result.Code != orbit.InvokeSuccess {
				http.Error(w, "invoke failed", 502)
				return
			}
			_, _ = w.Write(result.Payload)
			return
		}
		_, _ = w.Write([]byte("hello"))
	}))
	if err != nil {
		panic(err)
	}
	client, err := app.OpenInvocations(orbit.InvokeOptions{Fleet: *invokeFleet, Service: *service, PendingCalls: 32, AcceptedCalls: 32, OperationBytes: 64, RequestBytes: 4096, ResponseBytes: 65536, Routes: map[string]string{"peer": *peer}}, func(ctx context.Context, request orbit.Invocation) orbit.InvokeResult {
		if ctx.Err() != nil {
			return orbit.InvokeResult{Code: orbit.InvokeRemoteClosing}
		}
		if request.Operation != "echo" {
			return orbit.InvokeResult{Code: orbit.InvokeUnsupported}
		}
		return orbit.InvokeResult{Payload: request.Payload}
	})
	if err != nil {
		_ = app.Close()
		panic(err)
	}
	endpoint.Store(client)
	fmt.Printf("READY %d\n", app.Lane())
	select {
	case <-signals:
		if err := app.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	case <-app.Done():
		if err := app.Err(); err != nil {
			panic(err)
		}
	}
}

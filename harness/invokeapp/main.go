// Real-Link acceptance fixture, running HTTP and invoke in one normal Go process.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	"github.com/claviron/go/orbit"
)

func main() {
	if len(os.Args) != 3 {
		panic("usage: invokeapp <HTTP fleet> <invoke fleet>")
	}
	var endpoint atomic.Pointer[orbit.Invocations]
	late := make(chan struct{})
	drain := make(chan struct{})
	httpGate := make(chan struct{})
	var lateOnce, drainOnce, httpOnce sync.Once
	app, err := orbit.Open(orbit.Options{Fleet: os.Args[1], Name: "app", Incarnation: 1, NativeThreads: 1, MaxInFlight: 8, IOBufferBytes: 4096}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-PID", fmt.Sprint(os.Getpid()))
		if r.URL.Path == "/hold-invoke" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			fmt.Println("HTTP_HELD")
			<-httpGate
			result, err := endpoint.Load().Call(r.Context(), "rust", "echo", []byte("from-http"))
			if err != nil || result.Code != orbit.InvokeSuccess {
				panic("HTTP nested invoke failed")
			}
			_, _ = w.Write(result.Payload)
			return
		}
		fmt.Fprintf(w, "%d", orbit.Generation(r))
	}))
	if err != nil {
		panic(err)
	}
	// Failed attachment must return its native parent reservation and leave
	// HTTP usable. Opening the valid endpoint below must not mask a leaked pin.
	if _, err := app.OpenInvocations(orbit.InvokeOptions{Fleet: os.Args[1], Service: "invalid", PendingCalls: 1, AcceptedCalls: 1, OperationBytes: 1}, func(context.Context, orbit.Invocation) orbit.InvokeResult { return orbit.InvokeResult{} }); err == nil {
		panic("HTTP and invoke fleets were not isolated")
	}
	client, err := app.OpenInvocations(orbit.InvokeOptions{Fleet: os.Args[2], Service: "go-calls", PendingCalls: 2, AcceptedCalls: 2, OperationBytes: 64, RequestBytes: 1024, ResponseBytes: 1024, Routes: map[string]string{"rust": "rust-peer"}}, func(ctx context.Context, request orbit.Invocation) orbit.InvokeResult {
		switch request.Operation {
		case "echo":
			return orbit.InvokeResult{Payload: request.Payload}
		case "generation":
			return orbit.InvokeResult{Payload: []byte(fmt.Sprint(request.Generation))}
		case "reject":
			return orbit.InvokeResult{Code: orbit.InvokeRejected, Payload: request.Payload}
		case "oversize":
			return orbit.InvokeResult{Payload: make([]byte, 1025)}
		case "panic":
			panic("handler fixture")
		case "cancel-close":
			fmt.Println("CLOSE_HELD")
			<-ctx.Done()
			fmt.Println("CLOSE_CANCELLED")
			return orbit.InvokeResult{Code: orbit.InvokeRemoteClosing}
		case "hold":
			fmt.Printf("INCOMING %d\n", request.Generation)
			<-ctx.Done()
			fmt.Printf("IN_CANCELLED %d\n", request.Generation)
			<-late
			if _, err := endpoint.Load().Call(context.WithoutCancel(ctx), "rust", "echo", nil); !errors.Is(err, context.Canceled) {
				panic("retired context entered a new native generation")
			}
			fmt.Printf("LATE_RETURN %d\n", request.Generation)
			return orbit.InvokeResult{Payload: []byte("retired")}
		case "drain":
			fmt.Println("DRAIN_HELD")
			<-drain
			result, err := endpoint.Load().Call(ctx, "rust", "echo", request.Payload)
			if err != nil || result.Code != orbit.InvokeSuccess {
				panic(fmt.Sprintf("nested invoke: %v / %v", result, err))
			}
			return result
		default:
			return orbit.InvokeResult{Code: orbit.InvokeUnsupported}
		}
	})
	if err != nil {
		panic(err)
	}
	endpoint.Store(client)
	fmt.Printf("READY %d\n", app.Lane())
	var cancel context.CancelFunc
	finished := make(chan struct{})
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch scanner.Text() {
		case "call-cancel", "call-generation":
			ctx, stop := context.WithCancel(context.Background())
			cancel = stop
			go func() {
				_, err := client.Call(ctx, "rust", "hold", nil)
				if !errors.Is(err, context.Canceled) {
					panic(fmt.Sprintf("expected call cancellation: %v", err))
				}
				fmt.Println("CALL_CANCELLED")
			}()
		case "capacity-call":
			go func() {
				result, err := client.Call(context.Background(), "rust", "hold", nil)
				if err != nil || result.Code != orbit.InvokeSuccess {
					panic("capacity call failed")
				}
				fmt.Println("CAPACITY_DONE")
			}()
		case "capacity-probe":
			if _, err := client.Call(context.Background(), "rust", "echo", nil); err == nil {
				panic("pending capacity was not enforced")
			}
			fmt.Println("CALL_BUSY")
		case "cancel-call":
			cancel()
		case "release-late":
			lateOnce.Do(func() { close(late) })
		case "release-http":
			httpOnce.Do(func() { close(httpGate) })
		case "release-drain":
			drainOnce.Do(func() { close(drain) })
		case "restart":
			if err := app.Restart(); err != nil {
				panic(err)
			}
			fmt.Println("RESTARTED")
		case "stop":
			if err := app.Stop(); err != nil {
				panic(err)
			}
			fmt.Println("STOPPED")
		case "start":
			if err := app.Start(); err != nil {
				panic(err)
			}
			fmt.Println("STARTED")
		case "withdraw-invoke":
			ctx, stop := context.WithCancel(context.Background())
			stop()
			if !errors.Is(client.Shutdown(ctx), context.Canceled) {
				panic("shutdown did not preserve wait cancellation")
			}
			fmt.Println("INVOKE_WITHDRAWN")
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
			fmt.Println("CLOSED")
			return
		case "finish":
			<-finished
			return
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

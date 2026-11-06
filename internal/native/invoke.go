package native

/*
#include "orbit_go.h"
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"unsafe"
)

type InvokeOptions struct {
	Fleet, Service                                                           string
	PendingCalls, AcceptedCalls, OperationBytes, RequestBytes, ResponseBytes uint64
	Routes                                                                   map[string]string
}
type Invocation struct {
	Operation  string
	Payload    []byte
	Generation uint64
	Cancelled  bool
}

func InvokeOpen(app Handle, o InvokeOptions) (Handle, error) {
	var abi C.og_abi_info
	if err := check(C.og_abi(&abi)); err != nil {
		return 0, err
	}
	if abi.version != 4 || unsafe.Sizeof(C.og_invoke_options{}) != 40 || unsafe.Sizeof(C.og_invoke_info{}) != 16 {
		return 0, fmt.Errorf("orbit-go: incompatible invoke ABI")
	}
	var routes []byte
	keys := make([]string, 0, len(o.Routes))
	for target := range o.Routes {
		keys = append(keys, target)
	}
	sort.Strings(keys)
	for _, target := range keys {
		for _, value := range []string{target, o.Routes[target]} {
			if uint64(len(value)) > uint64(^uint32(0)) {
				return 0, Status(1)
			}
			routes = binary.LittleEndian.AppendUint32(routes, uint32(len(value)))
			routes = append(routes, value...)
		}
	}
	options := C.og_invoke_options{pending_calls: C.uint64_t(o.PendingCalls), accepted_calls: C.uint64_t(o.AcceptedCalls), operation_bytes: C.uint64_t(o.OperationBytes), request_bytes: C.uint64_t(o.RequestBytes), response_bytes: C.uint64_t(o.ResponseBytes)}
	var handle C.uint64_t
	err := check(C.og_invoke_open(C.uint64_t(app), stringData(o.Fleet), C.uint64_t(len(o.Fleet)), stringData(o.Service), C.uint64_t(len(o.Service)), byteData(routes), C.uint64_t(len(routes)), &options, &handle))
	runtime.KeepAlive(o)
	runtime.KeepAlive(routes)
	return Handle(handle), err
}
func InvokeWait(owner Handle) (Event, error) {
	var event C.og_event
	err := check(C.og_invoke_wait(C.uint64_t(owner), &event))
	return Event{uint32(event.kind), uint32(event.status), Handle(event.exchange)}, err
}
func InvokeWithdraw(owner Handle) error { return check(C.og_invoke_withdraw(C.uint64_t(owner))) }
func InvokeCancel(owner Handle) error   { return check(C.og_invoke_cancel(C.uint64_t(owner))) }
func InvokeFinish(owner Handle) error   { return check(C.og_invoke_finish(C.uint64_t(owner))) }
func InvokeRelease(owner Handle) error  { return check(C.og_invoke_release(C.uint64_t(owner))) }
func invocationField(owner, id Handle, field uint32) ([]byte, error) {
	var length C.uint64_t
	if err := check(C.og_invocation_copy(C.uint64_t(owner), C.uint64_t(id), C.uint32_t(field), nil, 0, &length)); err != nil {
		return nil, err
	}
	if uint64(length) > uint64(int(^uint(0)>>1)) {
		return nil, Status(4)
	}
	value := make([]byte, int(length))
	err := check(C.og_invocation_copy(C.uint64_t(owner), C.uint64_t(id), C.uint32_t(field), byteData(value), C.uint64_t(len(value)), &length))
	runtime.KeepAlive(value)
	return value, err
}
func ReadInvocation(owner, id Handle) (Invocation, error) {
	var info C.og_invoke_info
	if err := check(C.og_invocation_info(C.uint64_t(owner), C.uint64_t(id), &info)); err != nil {
		return Invocation{}, err
	}
	operation, err := invocationField(owner, id, 1)
	if err != nil {
		return Invocation{}, err
	}
	payload, err := invocationField(owner, id, 2)
	return Invocation{string(operation), payload, uint64(info.generation), info.cancelled != 0}, err
}
func InvocationReply(owner, id Handle, code uint32, payload []byte) error {
	err := check(C.og_invocation_reply(C.uint64_t(owner), C.uint64_t(id), C.uint32_t(code), byteData(payload), C.uint64_t(len(payload))))
	runtime.KeepAlive(payload)
	return err
}
func InvocationRelease(owner, id Handle) error {
	return check(C.og_invocation_release(C.uint64_t(owner), C.uint64_t(id)))
}
func CallStart(owner Handle, generation uint64, target, operation string, payload []byte) (Handle, uint64, error) {
	var handle C.uint64_t
	var info C.og_invoke_info
	err := check(C.og_call_start(C.uint64_t(owner), C.uint64_t(generation), stringData(target), C.uint64_t(len(target)), stringData(operation), C.uint64_t(len(operation)), byteData(payload), C.uint64_t(len(payload)), &handle, &info))
	runtime.KeepAlive(target)
	runtime.KeepAlive(operation)
	runtime.KeepAlive(payload)
	return Handle(handle), uint64(info.generation), err
}
func CallCancel(owner, id Handle) error {
	return check(C.og_call_cancel(C.uint64_t(owner), C.uint64_t(id)))
}
func CallResult(owner, id Handle) (uint32, []byte, error) {
	var code, status C.uint32_t
	var length C.uint64_t
	if err := check(C.og_call_result(C.uint64_t(owner), C.uint64_t(id), &code, &status, nil, 0, &length, 0)); err != nil {
		return 0, nil, err
	}
	if uint64(length) > uint64(int(^uint(0)>>1)) {
		return 0, nil, Status(4)
	}
	value := make([]byte, int(length))
	err := check(C.og_call_result(C.uint64_t(owner), C.uint64_t(id), &code, &status, byteData(value), C.uint64_t(len(value)), &length, 1))
	runtime.KeepAlive(value)
	if err == nil && status != 0 {
		err = Status(status)
	}
	return uint32(code), value, err
}

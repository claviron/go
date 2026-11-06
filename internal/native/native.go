// Package native is the private cgo boundary. It owns no Go application code.
package native

/*
#cgo LDFLAGS: -lorbit_go
#include "orbit_go.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sort"
	"unsafe"
)

type Status uint32

func (s Status) Error() string {
	names := [...]string{"ok", "invalid argument", "invalid handle", "closed", "capacity exhausted", "unsupported operation", "transport failure", "native panic", "busy", "cancelled", "body limit exceeded"}
	if int(s) < len(names) {
		return "orbit-go: " + names[s]
	}
	return fmt.Sprintf("orbit-go: native status %d", s)
}
func check(s C.uint32_t) error {
	if s != 0 {
		return Status(s)
	}
	return nil
}

type Options struct {
	Fleet, Name                                     string
	Incarnation                                     uint64
	NativeThreads                                   uint32
	MaxExchanges, MaxRequestBytes, MaxResponseBytes uint64
	IOBufferBytes                                   uint64
}
type Handle uint64
type Event struct {
	Kind, Status uint32
	Exchange     Handle
}
type Header struct{ Name, Value string }
type Request struct {
	Method, Scheme, Path, Query, Peer, Local string
	Major, Minor                             int
	Headers                                  []Header
	HasBodyStream                            bool
}

func stringData(value string) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(unsafe.StringData(value)))
}
func byteData(value []byte) *C.uint8_t { return (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(value))) }

func Open(o Options) (Handle, uint64, error) {
	var abi C.og_abi_info
	if err := check(C.og_abi(&abi)); err != nil {
		return 0, 0, err
	}
	if abi.version != 2 || uintptr(abi.event_size) != unsafe.Sizeof(C.og_event{}) || uintptr(abi.options_size) != unsafe.Sizeof(C.og_options{}) || uintptr(abi.info_size) != unsafe.Sizeof(C.og_request_info_record{}) {
		return 0, 0, fmt.Errorf("orbit-go: incompatible native ABI")
	}
	opts := C.og_options{abi_version: 2, native_threads: C.uint32_t(o.NativeThreads), incarnation: C.uint64_t(o.Incarnation), max_exchanges: C.uint64_t(o.MaxExchanges), max_request_bytes: C.uint64_t(o.MaxRequestBytes), max_response_bytes: C.uint64_t(o.MaxResponseBytes), io_buffer_bytes: C.uint64_t(o.IOBufferBytes)}
	var handle, lane C.uint64_t
	err := check(C.og_open(stringData(o.Fleet), C.uint64_t(len(o.Fleet)), stringData(o.Name), C.uint64_t(len(o.Name)), &opts, &handle))
	runtime.KeepAlive(o)
	if err != nil {
		return 0, 0, err
	}
	if err = check(C.og_lane(handle, &lane)); err != nil {
		C.og_cancel(handle)
		return 0, 0, err
	}
	return Handle(handle), uint64(lane), nil
}
func Wait(app Handle) (Event, error) {
	var event C.og_event
	err := check(C.og_wait(C.uint64_t(app), &event))
	return Event{uint32(event.kind), uint32(event.status), Handle(event.exchange)}, err
}
func Withdraw(app Handle) error   { return check(C.og_withdraw(C.uint64_t(app))) }
func Cancel(app Handle) error     { return check(C.og_cancel(C.uint64_t(app))) }
func Release(app Handle) error    { return check(C.og_release(C.uint64_t(app))) }
func Abort(exchange Handle) error { return check(C.og_exchange_cancel(C.uint64_t(exchange))) }
func ReleaseExchange(app, exchange Handle) error {
	return check(C.og_exchange_release(C.uint64_t(app), C.uint64_t(exchange)))
}

func field(exchange Handle, field, index uint32) ([]byte, error) {
	var length C.uint64_t
	if err := check(C.og_request_copy(C.uint64_t(exchange), C.uint32_t(field), C.uint32_t(index), nil, 0, &length)); err != nil {
		return nil, err
	}
	if uint64(length) > uint64(int(^uint(0)>>1)) {
		return nil, Status(4)
	}
	data := make([]byte, int(length))
	err := check(C.og_request_copy(C.uint64_t(exchange), C.uint32_t(field), C.uint32_t(index), byteData(data), length, &length))
	runtime.KeepAlive(data)
	return data, err
}
func ReadRequest(exchange Handle) (Request, error) {
	var info C.og_request_info_record
	if err := check(C.og_request_info(C.uint64_t(exchange), &info)); err != nil {
		return Request{}, err
	}
	r := Request{Major: int(info.version_major), Minor: int(info.version_minor), HasBodyStream: info.body_stream != 0}
	fields := []*string{&r.Method, &r.Scheme, &r.Path, &r.Query, &r.Peer, &r.Local}
	for i, out := range fields {
		data, err := field(exchange, uint32(i+1), 0)
		if err != nil {
			return r, err
		}
		*out = string(data)
	}
	for i := uint32(0); i < uint32(info.header_count); i++ {
		name, err := field(exchange, 8, i)
		if err != nil {
			return r, err
		}
		value, err := field(exchange, 9, i)
		if err != nil {
			return r, err
		}
		r.Headers = append(r.Headers, Header{string(name), string(value)})
	}
	return r, nil
}
func Head(exchange Handle, status int, headers map[string][]string) error {
	if err := check(C.og_reply_begin(C.uint64_t(exchange), C.uint32_t(status))); err != nil {
		_ = Abort(exchange)
		return err
	}
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range headers[key] {
			err := check(C.og_reply_header(C.uint64_t(exchange), stringData(key), C.uint64_t(len(key)), stringData(value), C.uint64_t(len(value))))
			runtime.KeepAlive(key)
			runtime.KeepAlive(value)
			if err != nil {
				_ = Abort(exchange)
				return err
			}
		}
	}
	err := check(C.og_head_submit(C.uint64_t(exchange)))
	if err != nil {
		_ = Abort(exchange)
	}
	return err
}

func Write(exchange Handle, data []byte) error {
	err := check(C.og_output_write(C.uint64_t(exchange), byteData(data), C.uint64_t(len(data))))
	runtime.KeepAlive(data)
	return err
}
func Finish(exchange Handle) error { return check(C.og_output_finish(C.uint64_t(exchange))) }
func OutputResult(exchange Handle) (int, error) {
	var written C.uint64_t
	err := check(C.og_output_result(C.uint64_t(exchange), &written))
	return int(written), err
}
func ReadStart(exchange Handle, size int) error {
	return check(C.og_read_start(C.uint64_t(exchange), C.uint64_t(size)))
}
func ReadResult(exchange Handle, data []byte) (int, error) {
	var n C.uint64_t
	err := check(C.og_read_result(C.uint64_t(exchange), byteData(data), C.uint64_t(len(data)), &n))
	runtime.KeepAlive(data)
	return int(n), err
}
func CloseRead(exchange Handle) error { return check(C.og_read_close(C.uint64_t(exchange))) }

func Unsupported(app Handle) (uint64, error) {
	var count C.uint64_t
	err := check(C.og_unsupported(C.uint64_t(app), &count))
	return uint64(count), err
}

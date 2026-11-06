#ifndef ORBIT_GO_H
#define ORBIT_GO_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* ABI 1. Inputs live only for the call; native never retains Go pointers.
 * Outputs must be aligned and writable. Copy-out buffers may not overlap inputs.
 * Status: 0 OK, 1 invalid argument, 2 stale/wrong-kind handle, 3 closed,
 * 4 capacity, 5 unsupported, 6 transport, 7 contained panic, 8 busy, 9 cancelled.
 * Handles are process-local, never reused. Only release after terminal delivery.
 */
typedef struct {
    uint32_t version, event_size, options_size, info_size;
    uint64_t fleet_capacity, inbox_capacity, inbox_payload_capacity, lane_capacity, buffer_bytes;
} og_abi_info;
typedef struct {
    uint32_t abi_version, native_threads;
    uint64_t incarnation, max_exchanges, max_request_bytes, max_response_bytes;
} og_options;
typedef struct { uint32_t kind, status; uint64_t exchange; } og_event;
typedef struct {
    uint32_t version_major, version_minor, header_count, reserved;
    uint64_t body_bytes;
} og_request_info_record;
uint32_t og_abi(og_abi_info *out);
uint32_t og_open(const uint8_t *fleet, uint64_t fleet_len, const uint8_t *name, uint64_t name_len, const og_options *options, uint64_t *out);
/* Only og_wait blocks: one waiter per application, explicitly woken by close.
 * Events: 1 request, 2 cancelled, 3 complete (exactly once), 4 application closed.
 * Each exchange retains admission/completion credit until exchange_release.
 * Submit success means accepted native work, not response delivery.
 */
uint32_t og_wait(uint64_t app, og_event *out);
uint32_t og_withdraw(uint64_t app);
uint32_t og_cancel(uint64_t app);
uint32_t og_release(uint64_t app);
uint32_t og_lane(uint64_t app, uint64_t *out);
uint32_t og_unsupported(uint64_t app, uint64_t *out);
uint32_t og_request_info(uint64_t exchange, og_request_info_record *out);
/* field: 1 method, 2 scheme, 3 raw path, 4 query, 5 peer, 6 local, 7 body,
 * 8 header name, 9 header value. index is used for headers only.
 * NULL/0 queries required length; otherwise copy exactly that many bytes.
 * No borrowed native pointer escapes a call. */
uint32_t og_request_copy(uint64_t exchange, uint32_t field, uint32_t index, uint8_t *out, uint64_t capacity, uint64_t *length);
uint32_t og_reply_begin(uint64_t exchange, uint32_t status);
uint32_t og_reply_header(uint64_t exchange, const uint8_t *name, uint64_t name_len, const uint8_t *value, uint64_t value_len);
uint32_t og_reply_submit(uint64_t exchange, const uint8_t *body, uint64_t length);
uint32_t og_exchange_cancel(uint64_t exchange);
uint32_t og_exchange_release(uint64_t app, uint64_t exchange);
#ifdef __cplusplus
}
#endif
#endif

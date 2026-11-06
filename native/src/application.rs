mod status;

use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use claviron::orbit::link::application::{
    ApplicationBodies, ApplicationSegment, ControlCommand, Exchange as Frame,
    ExchangeReply, FrameKind, Headers, Received, ShutdownNotice, frame_kind,
};
use claviron::orbitive::link::InboxReceiver;
use claviron::orbitive::shm::LaneHold;
use claviron::orbitive::stream::WriteHalf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

use crate::abi::{Event, EventKind, Options, Result, Status};
use crate::registry;

pub struct Application {
    pub state: Arc<State>,
    // Only the external registry owns the runtime, never one of its tasks.
    _runtime: tokio::runtime::Runtime,
}

pub struct State {
    options: Options,
    segment: Arc<ApplicationSegment>,
    bodies: ApplicationBodies,
    lane: usize,
    hold: Mutex<Option<LaneHold>>,
    send: mpsc::SyncSender<Event>,
    receive: Mutex<mpsc::Receiver<Event>>,
    pub stop: CancellationToken,
    pub withdraw: CancellationToken,
    retained: AtomicUsize,
    running: AtomicUsize,
    changed: Notify,
    closed: AtomicBool,
    delivered: AtomicBool,
    pub unsupported: AtomicU64,
}

pub struct Reply {
    pub status: u16,
    pub headers: Headers,
    pub body: Vec<u8>,
    pub header_bytes: usize,
}

pub struct Exchange {
    pub application: Arc<State>,
    pub frame: Frame,
    pub body: Mutex<Vec<u8>>,
    pub response: Mutex<Option<Reply>>,
    pub reply: Mutex<Option<oneshot::Sender<Reply>>>,
    pub cancel: CancellationToken,
    finished: AtomicBool,
}

impl Exchange {
    pub fn finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    pub fn max_response_bytes(&self) -> usize {
        self.application.options.max_response_bytes
    }

    pub fn begin(
        &self,
        status: u16,
    ) -> Result<()> {
        if self.finished() || self.cancel.is_cancelled() {
            return Err(Status::Cancelled);
        }
        if !(200..=599).contains(&status) {
            return Err(Status::Unsupported);
        }
        let mut response = self.response.lock().map_err(|_| Status::Panic)?;
        if response.is_some() {
            return Err(Status::Busy);
        }
        *response = Some(Reply {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            header_bytes: 0,
        });
        Ok(())
    }

    pub fn header(
        &self,
        name: &[u8],
        value: &[u8],
    ) -> Result<()> {
        http::HeaderName::from_bytes(name)
            .map_err(|_| Status::InvalidArgument)?;
        http::HeaderValue::from_bytes(value)
            .map_err(|_| Status::InvalidArgument)?;
        let mut response = self.response.lock().map_err(|_| Status::Panic)?;
        let reply = response.as_mut().ok_or(Status::Busy)?;
        let bytes = reply
            .header_bytes
            .checked_add(name.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(Status::Capacity)?;
        if bytes
            > claviron::orbit::link::application::LINK_INBOX_PAYLOAD_CAPACITY
        {
            return Err(Status::Capacity);
        }
        reply.header_bytes = bytes;
        reply.headers.push((name.to_vec(), value.to_vec()));
        Ok(())
    }

    pub fn submit(
        &self,
        body: &[u8],
    ) -> Result<()> {
        if self.finished() || self.cancel.is_cancelled() {
            return Err(Status::Cancelled);
        }
        if body.len() > self.max_response_bytes() {
            return Err(Status::Capacity);
        }
        let mut slot = self.reply.lock().map_err(|_| Status::Panic)?;
        let mut response = self.response.lock().map_err(|_| Status::Panic)?;
        let mut reply = response.take().ok_or(Status::Busy)?;
        reply.body = body.to_vec();
        slot.take()
            .ok_or(Status::Busy)?
            .send(reply)
            .map_err(|_| Status::Cancelled)
    }
}

impl Application {
    pub fn open(options: Options) -> Result<Self> {
        options.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.native_threads)
            .enable_all()
            .build()
            .map_err(|_| Status::Transport)?;
        let (state, receiver) = runtime.block_on(async {
            let segment = Arc::new(
                ApplicationSegment::for_fleet(&options.fleet)
                    .map_err(|_| Status::Transport)?,
            );
            let joined = segment
                .join(&options.name, "go", options.incarnation)
                .map_err(|_| Status::Transport)?;
            // Own the lane immediately, including fallible body/reader setup.
            let hold = joined.hold;
            let setup = (|| {
                let bodies = ApplicationBodies::for_segment(
                    &segment,
                    joined.lane,
                    options.incarnation,
                )
                .map_err(|_| Status::Transport)?;
                bodies.reclaimed(&joined.reclaimed);
                let receiver = segment
                    .receiver(joined.lane)
                    .map_err(|_| Status::Transport)?;
                Ok((bodies, receiver))
            })();
            let (bodies, receiver) = match setup {
                Ok(value) => value,
                Err(error) => {
                    let _ = segment.inbox().release_lane(joined.lane);
                    return Err(error);
                }
            };
            // Each retained exchange can emit Request, Cancelled and Complete
            // once. One extra slot belongs to the application's Closed event.
            // Admission credits are retained until explicit exchange release.
            let (send, receive) =
                mpsc::sync_channel(options.max_exchanges * 3 + 1);
            Ok((
                Arc::new(State {
                    options,
                    segment,
                    bodies,
                    lane: joined.lane,
                    hold: Mutex::new(Some(hold)),
                    send,
                    receive: Mutex::new(receive),
                    stop: CancellationToken::new(),
                    withdraw: CancellationToken::new(),
                    retained: AtomicUsize::new(0),
                    running: AtomicUsize::new(0),
                    changed: Notify::new(),
                    closed: AtomicBool::new(false),
                    delivered: AtomicBool::new(false),
                    unsupported: AtomicU64::new(0),
                }),
                receiver,
            ))
        })?;
        let driving = Arc::clone(&state);
        runtime.spawn(async move {
            let outcome = AssertUnwindSafe(driving.drive(receiver))
                .catch_unwind()
                .await
                .unwrap_or(Status::Panic);
            driving.finish(outcome).await;
        });
        Ok(Self { state, _runtime: runtime })
    }
}

impl State {
    pub fn lane(&self) -> usize {
        self.lane
    }

    pub fn wait(&self) -> Result<Event> {
        let receiver = self.receive.lock().map_err(|_| Status::Panic)?;
        if self.delivered.load(Ordering::Acquire) {
            return Err(Status::Closed);
        }
        let event = receiver.recv().map_err(|_| Status::Closed)?;
        if event.kind == EventKind::Closed as u32 {
            self.delivered.store(true, Ordering::Release);
        }
        Ok(event)
    }

    pub fn releasable(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            && self.delivered.load(Ordering::Acquire)
            && self.retained.load(Ordering::Acquire) == 0
    }

    pub fn release_credit(&self) {
        self.retained.fetch_sub(1, Ordering::AcqRel);
    }

    fn event(
        &self,
        kind: EventKind,
        exchange: u64,
        status: Status,
    ) {
        // The reserved-space invariant above makes Full impossible. Do not
        // silently discard a terminal completion if that invariant is broken.
        self.send
            .try_send(Event {
                kind: kind as u32,
                status: status as u32,
                exchange,
            })
            .expect("reserved completion space");
    }

    async fn drive(
        self: &Arc<Self>,
        mut receiver: InboxReceiver,
    ) -> Status {
        let mut bytes = Vec::new();
        let outcome = loop {
            let received = tokio::select! {
                biased;
                () = self.stop.cancelled() => break Status::Ok,
                () = self.withdraw.cancelled() => break Status::Ok,
                received = receiver.next(&mut bytes) => received,
            };
            match received {
                Ok(Received::Directory(_)) => continue,
                // Stage one has no registered application-event codec. Do not
                // consume events or management commands as successful HTTP work.
                Ok(Received::Event) => {
                    self.unsupported.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(_) => break Status::Transport,
                Ok(Received::Frame) => {}
            }
            match frame_kind(&bytes) {
                Ok(FrameKind::Request) => {}
                Ok(FrameKind::Control) => {
                    let command = match ControlCommand::decode(&bytes) {
                        Ok(command) => command,
                        Err(_) => break Status::InvalidArgument,
                    };
                    if command.application == *self.options.name
                        && command.name == "status"
                        && command.target.is_none()
                    {
                        if let Err(error) = self.publish_status() {
                            break error;
                        }
                        continue;
                    }
                    self.unsupported.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(FrameKind::ShutdownInitiated) => {
                    if ShutdownNotice::decode(&bytes).is_err() {
                        break Status::InvalidArgument;
                    }
                    // One caller's shutdown is never authority over other callers'
                    // accepted work. Lifecycle delivery follows in a later stage.
                    self.unsupported.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                _ => {
                    self.unsupported.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            let frame = match Frame::decode(&bytes) {
                Ok(frame) => frame,
                Err(_) => break Status::InvalidArgument,
            };
            if frame.upgrade {
                self.refuse(frame, 501).await;
                continue;
            }
            if self.retained.fetch_add(1, Ordering::AcqRel)
                >= self.options.max_exchanges
            {
                self.release_credit();
                self.refuse(frame, 503).await;
                continue;
            }
            let (send, reply) = oneshot::channel();
            let exchange = Arc::new(Exchange {
                application: Arc::clone(self),
                frame,
                body: Mutex::new(Vec::new()),
                response: Mutex::new(None),
                reply: Mutex::new(Some(send)),
                cancel: self.stop.child_token(),
                finished: AtomicBool::new(false),
            });
            let id = match registry::insert_exchange(Arc::clone(&exchange)) {
                Ok(id) => id,
                Err(error) => {
                    self.release_credit();
                    break error;
                }
            };
            self.running.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(async move {
                exchange.drive(id, reply).await;
            });
        };
        drop(receiver);
        outcome
    }

    async fn finish(
        &self,
        outcome: Status,
    ) {
        if outcome != Status::Ok {
            self.stop.cancel();
        }
        // Withdraw only this lane; hold its ownership until accepted I/O ends.
        let released = self.segment.inbox().release_lane(self.lane);
        loop {
            let changed = self.changed.notified();
            if self.running.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
        self.hold.lock().expect("lane hold").take();
        self.closed.store(true, Ordering::Release);
        self.event(
            EventKind::Closed,
            0,
            if released.is_err() { Status::Transport } else { outcome },
        );
    }

    async fn refuse(
        &self,
        frame: Frame,
        status: u16,
    ) {
        let endpoint =
            frame.body.and_then(|ticket| self.bodies.accept(ticket).ok());
        let output = endpoint.map(|endpoint| endpoint.split().1);
        let reply = Reply {
            status,
            headers: Vec::new(),
            body: Vec::new(),
            header_bytes: 0,
        };
        let _ = self.send_reply(&frame, output, reply, &self.stop).await;
    }

    async fn send_reply(
        &self,
        frame: &Frame,
        mut output: Option<WriteHalf>,
        reply: Reply,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let head = ExchangeReply {
            correlation: frame.correlation,
            status: reply.status,
            headers: reply.headers,
            body: frame.body,
        };
        let bytes = head.encode().map_err(|_| Status::Capacity)?;
        self.segment
            .inbox()
            .write(frame.origin as usize, &bytes)
            .map_err(|_| Status::Transport)?;
        match output.as_mut() {
            Some(output) => tokio::select! {
                biased;
                () = cancellation.cancelled() => { output.reset(); Err(Status::Cancelled) }
                result = async {
                    output.write_all(&reply.body).await?;
                    output.shutdown().await
                } => result.map_err(|_| Status::Cancelled),
            },
            None if reply.body.is_empty() => Ok(()),
            None => Err(Status::Unsupported),
        }
    }
}

impl Exchange {
    async fn drive(
        self: Arc<Self>,
        id: u64,
        reply: oneshot::Receiver<Reply>,
    ) {
        let outcome = AssertUnwindSafe(self.perform(id, reply))
            .catch_unwind()
            .await
            .unwrap_or(Err(Status::Panic));
        if outcome == Err(Status::Cancelled) {
            self.cancel.cancel();
            self.application.event(EventKind::Cancelled, id, Status::Cancelled);
        }
        self.finished.store(true, Ordering::Release);
        self.application.event(
            EventKind::Complete,
            id,
            outcome.err().unwrap_or(Status::Ok),
        );
        self.application.running.fetch_sub(1, Ordering::AcqRel);
        self.application.changed.notify_one();
    }

    async fn perform(
        &self,
        id: u64,
        reply: oneshot::Receiver<Reply>,
    ) -> Result<()> {
        let (input, mut output) = match self.frame.body {
            Some(ticket) => {
                let (read, write) = self
                    .application
                    .bodies
                    .accept(ticket)
                    .map_err(|_| Status::Transport)?
                    .split();
                (Some(read), Some(write))
            }
            None => (None, None),
        };
        let mut body = Vec::new();
        let read = async {
            if let Some(input) = input {
                input
                    .take(self.application.options.max_request_bytes as u64 + 1)
                    .read_to_end(&mut body)
                    .await
                    .map_err(|_| Status::Cancelled)?;
            }
            Ok(())
        };
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => return Err(Status::Cancelled),
            _ = peer_closed(&mut output) => return Err(Status::Cancelled),
            result = read => result?,
        }
        if body.len() > self.application.options.max_request_bytes {
            return self
                .application
                .send_reply(
                    &self.frame,
                    output,
                    Reply {
                        status: 413,
                        headers: Vec::new(),
                        body: Vec::new(),
                        header_bytes: 0,
                    },
                    &self.cancel,
                )
                .await;
        }
        *self.body.lock().map_err(|_| Status::Panic)? = body;
        self.application.event(EventKind::Request, id, Status::Ok);
        let response = tokio::select! {
            biased;
            () = self.cancel.cancelled() => return Err(Status::Cancelled),
            _ = peer_closed(&mut output) => return Err(Status::Cancelled),
            response = reply => response.map_err(|_| Status::Cancelled)?,
        };
        self.application
            .send_reply(&self.frame, output, response, &self.cancel)
            .await
    }
}

async fn peer_closed(output: &mut Option<WriteHalf>) {
    match output {
        Some(output) => {
            let _ = output.peer_closed().await;
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[path = "../tests/application.rs"]
mod tests;

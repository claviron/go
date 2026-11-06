mod events;
mod io;
pub(crate) mod lifecycle;
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
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::abi::{Event, EventKind, Options, Result, Status};
use crate::registry;

pub struct Application {
    pub state: Arc<State>,
    // Only the external registry owns the runtime, never one of its tasks.
    _runtime: tokio::runtime::Runtime,
}

pub struct State {
    pub(crate) options: Options,
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
    pub lifecycle: lifecycle::Lifecycle,
    events: Arc<events::Events>,
    transition: Mutex<()>,
    pub(crate) host: tokio::runtime::Handle,
    pub(crate) invoke_children: AtomicUsize,
}

pub use io::Exchange;

impl Application {
    pub fn open(options: Options) -> Result<Self> {
        options.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.native_threads)
            .enable_all()
            .build()
            .map_err(|_| Status::Transport)?;
        let (state, receiver, events) = runtime.block_on(async {
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
                let (events, event_reader) = events::Events::open(
                    &options.fleet,
                    &options.name,
                    joined.lane,
                )?;
                Ok((bodies, receiver, events, event_reader))
            })();
            let (bodies, receiver, events, event_reader) = match setup {
                Ok(value) => value,
                Err(error) => {
                    let _ = segment.inbox().release_lane(joined.lane);
                    return Err(error);
                }
            };
            // Each retained exchange reserves Request, Cancelled, Complete,
            // one read and one output result. The extra slot belongs to Closed.
            // Admission credits are retained until explicit exchange release.
            let (send, receive) =
                mpsc::sync_channel(options.max_exchanges * 5 + 2);
            let stop = CancellationToken::new();
            let lifecycle = lifecycle::Lifecycle::new(stop.clone());
            Ok((
                Arc::new(State {
                    host: runtime.handle().clone(),
                    invoke_children: AtomicUsize::new(0),
                    options,
                    segment,
                    bodies,
                    lane: joined.lane,
                    hold: Mutex::new(Some(hold)),
                    send,
                    receive: Mutex::new(receive),
                    stop,
                    lifecycle,
                    events,
                    transition: Mutex::new(()),
                    withdraw: CancellationToken::new(),
                    retained: AtomicUsize::new(0),
                    running: AtomicUsize::new(0),
                    changed: Notify::new(),
                    closed: AtomicBool::new(false),
                    delivered: AtomicBool::new(false),
                    unsupported: AtomicU64::new(0),
                }),
                receiver,
                event_reader,
            ))
        })?;
        let driving = Arc::clone(&state);
        runtime.spawn(async move {
            let outcome = AssertUnwindSafe(driving.drive(receiver, events))
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
        if event.kind == EventKind::Complete as u32
            || event.kind == EventKind::Read as u32
            || event.kind == EventKind::Output as u32
        {
            registry::exchange(event.exchange)?.delivered(event.kind)?;
        }
        if event.kind == EventKind::Lifecycle as u32 {
            self.lifecycle.delivered()?;
        }
        if event.kind == EventKind::Closed as u32 {
            self.delivered.store(true, Ordering::Release);
        }
        Ok(event)
    }

    pub fn releasable(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            && self.delivered.load(Ordering::Acquire)
            && self.retained.load(Ordering::Acquire) == 0
            && self.invoke_children.load(Ordering::Acquire) == 0
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
        mut events: events::Reader,
    ) -> Status {
        let mut bytes = Vec::new();
        let outcome = loop {
            let received = tokio::select! {
                biased;
                () = self.stop.cancelled() => break Status::Ok,
                () = self.withdraw.cancelled() => break Status::Ok,
                action = events.next() => {
                    match action.and_then(|action| self.apply_from_ring(action)) {
                        Ok(()) => continue,
                        Err(error) => break error,
                    }
                }
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
                    if command.application == *self.options.name
                        && command.target.as_ref().is_some_and(|target| {
                            target.runtime == "go"
                                && target.name == *self.options.name
                        })
                        && let Some(action) =
                            lifecycle::Action::from_name(&command.name)
                    {
                        if let Err(error) = self.action(action) {
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
            let generation = match self.lifecycle.capture() {
                Ok(Some(generation)) => generation,
                Ok(None) => {
                    self.refuse(frame, 503).await;
                    continue;
                }
                Err(error) => break error,
            };
            if self.retained.fetch_add(1, Ordering::AcqRel)
                >= self.options.max_exchanges
            {
                self.release_credit();
                self.refuse(frame, 503).await;
                continue;
            }
            let (exchange, input, output) =
                Exchange::new(Arc::clone(self), frame, generation);
            let id = match registry::insert_exchange(Arc::clone(&exchange)) {
                Ok(id) => id,
                Err(error) => {
                    self.release_credit();
                    break error;
                }
            };
            self.running.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(async move {
                exchange.drive(id, input, output).await;
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
        let _ = self.publish_head(&frame, status, Vec::new());
        if let Some(mut output) = output {
            let _ = output.shutdown().await;
        }
    }

    fn publish_head(
        &self,
        frame: &Frame,
        status: u16,
        headers: Headers,
    ) -> Result<()> {
        let head = ExchangeReply {
            correlation: frame.correlation,
            status,
            headers,
            body: frame.body,
        };
        let bytes = head.encode().map_err(|_| Status::Capacity)?;
        self.segment
            .inbox()
            .write(frame.origin as usize, &bytes)
            .map(|_| ())
            .map_err(|_| Status::Transport)
    }
}

#[cfg(test)]
#[path = "../tests/application.rs"]
mod tests;

impl State {
    pub fn action(
        &self,
        action: lifecycle::Action,
    ) -> Result<()> {
        let _transition = self.transition.lock().map_err(|_| Status::Panic)?;
        if self.stop.is_cancelled() || self.withdraw.is_cancelled() {
            return Err(Status::Closed);
        }
        // The transition lock also covers remote application, so capacity is
        // checked before publishing a decision this instance cannot apply.
        self.lifecycle.validate(action)?;
        self.events.publish(action)?;
        self.apply(action)
    }
    fn apply_from_ring(
        &self,
        action: lifecycle::Action,
    ) -> Result<()> {
        let _transition = self.transition.lock().map_err(|_| Status::Panic)?;
        self.apply(action)
    }
    fn apply(
        &self,
        action: lifecycle::Action,
    ) -> Result<()> {
        if self.lifecycle.apply(action)? {
            self.event(EventKind::Lifecycle, 0, Status::Ok);
        }
        self.publish_status()
    }
}

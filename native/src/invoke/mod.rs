//! Application-owned calls over a separate, protocol-compatible Link.
pub mod ffi;
mod outcome;
use crate::{
    abi::{Event, Result, Status},
    application::{State, lifecycle::Generation},
    registry,
};
use claviron::orbit::{
    invoke::{Client, Failure},
    link::{
        Connection,
        application::LINK_SPEC,
        invoke::{Call, Driver, Limits},
    },
};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

// Each retained incoming item owns Request, Cancelled and Complete credits;
// each outgoing item owns one result credit. Withdrawal and Closed each own one.
pub const REQUEST: u32 = 1;
pub const CANCELLED: u32 = 2;
pub const COMPLETE: u32 = 3;
pub const WITHDRAWN: u32 = 4;
pub const RESULT: u32 = 5;
pub const CLOSED: u32 = 6;
type Reply = std::result::Result<Vec<u8>, Failure>;

pub struct Settings {
    pub fleet: Box<str>,
    pub service: Box<str>,
    pub limits: Limits,
    pub routes: Vec<(Box<str>, Box<str>)>,
}
pub struct Incoming {
    pub operation: Box<str>,
    pub payload: Vec<u8>,
    pub generation: Arc<Generation>,
    reply: Mutex<Option<oneshot::Sender<Reply>>>,
    terminal: AtomicBool,
    delivered: AtomicBool,
}
pub struct Outgoing {
    cancel: CancellationToken,
    result: Mutex<Option<(Status, u32, Vec<u8>)>>,
    delivered: AtomicBool,
}
struct Data {
    closing: bool,
    finalizing: bool,
    closed: bool,
    incoming: HashMap<u64, Arc<Incoming>>,
    outgoing: HashMap<u64, Arc<Outgoing>>,
}
pub struct Invocations {
    pub application: Arc<State>,
    pub limits: Limits,
    client: Client,
    data: Mutex<Data>,
    send: std::sync::mpsc::SyncSender<Event>,
    receive: Mutex<std::sync::mpsc::Receiver<Event>>,
    cancel: CancellationToken,
    finish: CancellationToken,
    outgoing: TaskTracker,
}
fn owners() -> &'static Mutex<HashMap<u64, Arc<Invocations>>> {
    static OWNERS: OnceLock<Mutex<HashMap<u64, Arc<Invocations>>>> =
        OnceLock::new();
    OWNERS.get_or_init(Mutex::default)
}
pub fn get(id: u64) -> Result<Arc<Invocations>> {
    owners()
        .lock()
        .map_err(|_| Status::Panic)?
        .get(&id)
        .cloned()
        .ok_or(Status::InvalidHandle)
}
pub fn release(id: u64) -> Result<()> {
    let mut owners = owners().lock().map_err(|_| Status::Panic)?;
    let owner = owners.get(&id).ok_or(Status::InvalidHandle)?;
    let data = owner.data.lock().map_err(|_| Status::Panic)?;
    if !data.closed || !data.incoming.is_empty() || !data.outgoing.is_empty() {
        return Err(Status::Busy);
    }
    drop(data);
    let owner = owners.remove(&id).ok_or(Status::InvalidHandle)?;
    owner.application.invoke_children.fetch_sub(1, Ordering::AcqRel);
    Ok(())
}
impl Invocations {
    pub fn open(
        application: Arc<State>,
        options: Settings,
    ) -> Result<u64> {
        if options.fleet == application.options.fleet {
            return Err(Status::InvalidArgument);
        }
        claviron::orbitive::FleetName::try_new(&options.fleet)
            .map_err(|_| Status::InvalidArgument)?;
        options
            .limits
            .validate(LINK_SPEC.inbox.payload_capacity)
            .map_err(|_| Status::InvalidArgument)?;
        let capacity = options
            .limits
            .accepted_calls
            .checked_mul(3)
            .and_then(|n| n.checked_add(options.limits.pending_calls))
            .and_then(|n| n.checked_add(2))
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or(Status::InvalidArgument)?;
        if application.stop.is_cancelled()
            || application.withdraw.is_cancelled()
        {
            return Err(Status::Closed);
        }
        let id = registry::identity()?;
        let host = application.host.clone();
        let (owner, driver) = host.block_on(async {
            let connection = Connection::open(
                &options.fleet,
                LINK_SPEC,
                &options.service,
                "invoke",
                application.options.incarnation,
            )
            .map_err(|_| Status::Transport)?;
            let (handle, driver) = connection
                .invocations(options.limits)
                .map_err(|_| Status::InvalidArgument)?;
            let mut client = Client::new();
            for (target, service) in options.routes {
                client
                    .register(
                        target.into_string(),
                        handle
                            .route(service.into_string())
                            .map_err(|_| Status::InvalidArgument)?,
                    )
                    .map_err(|_| Status::InvalidArgument)?;
            }
            let (send, receive) = std::sync::mpsc::sync_channel(capacity);
            Ok((
                Arc::new(Self {
                    application,
                    limits: options.limits,
                    client,
                    data: Mutex::new(Data {
                        closing: false,
                        finalizing: false,
                        closed: false,
                        incoming: HashMap::new(),
                        outgoing: HashMap::new(),
                    }),
                    send,
                    receive: Mutex::new(receive),
                    cancel: CancellationToken::new(),
                    finish: CancellationToken::new(),
                    outgoing: TaskTracker::new(),
                }),
                driver,
            ))
        })?;
        // Establish startup before returning the handle; an immediately submitted
        // call must never race the core driver's initial running flag.
        let (send, ready) = oneshot::channel();
        let driving = owner.clone();
        host.spawn(async move {
            driving.run(driver, send).await;
        });
        host.block_on(ready).map_err(|_| Status::Transport)??;
        owners().lock().map_err(|_| Status::Panic)?.insert(id, owner.clone());
        Ok(id)
    }
    fn event(
        &self,
        kind: u32,
        id: u64,
        status: Status,
    ) {
        self.send
            .try_send(Event { kind, exchange: id, status: status as u32 })
            .expect("reserved invoke completion credit");
    }
    async fn run(
        self: Arc<Self>,
        driver: Driver,
        started: oneshot::Sender<Result<()>>,
    ) {
        let service = self.clone();
        let driving = driver.run_until(
            self.finish.cancelled(),
            move |call| service.clone().accept(call),
            |_| async { Ok(()) },
        );
        tokio::pin!(driving);
        let mut started = Some(started);
        let driving = std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(driving.as_mut(), cx);
            if let Some(started) = started.take() {
                let _ = started.send(if result.is_pending() {
                    Ok(())
                } else {
                    Err(Status::Transport)
                });
            }
            result
        });
        let status = match AssertUnwindSafe(driving).catch_unwind().await {
            Ok(Ok(())) => Status::Ok,
            Ok(Err(_)) => Status::Transport,
            Err(_) => Status::Panic,
        };
        let _ = self.withdraw();
        self.cancel.cancel();
        // A failed driver has dropped its accepted futures. Deliver their
        // terminal credits too, so Go can cancel and join every retained owner.
        if status != Status::Ok
            && let Ok(data) = self.data.lock()
        {
            for (&id, request) in &data.incoming {
                if !request.terminal.swap(true, Ordering::AcqRel) {
                    self.event(CANCELLED, id, Status::Cancelled);
                    self.event(COMPLETE, id, status);
                }
            }
        }
        self.outgoing.close();
        self.outgoing.wait().await;
        self.event(CLOSED, 0, status);
    }
    async fn accept(
        self: Arc<Self>,
        call: Call,
    ) -> Reply {
        let generation = self
            .application
            .lifecycle
            .capture()
            .map_err(|_| Failure::Closing)?
            .ok_or(Failure::Closing)?;
        let (sender, receiver) = oneshot::channel();
        let request = Arc::new(Incoming {
            operation: call.request.operation.into_boxed_str(),
            payload: call.request.payload,
            generation,
            reply: Mutex::new(Some(sender)),
            terminal: false.into(),
            delivered: false.into(),
        });
        let id;
        {
            let mut data = self.data.lock().map_err(|_| Failure::Closing)?;
            if data.closing || self.cancel.is_cancelled() {
                return Err(Failure::Closing);
            }
            if data.incoming.len() >= self.limits.accepted_calls {
                return Err(Failure::Busy);
            }
            id = registry::identity().map_err(|_| Failure::Busy)?;
            data.incoming.insert(id, request.clone());
            self.event(REQUEST, id, Status::Ok);
        }
        let result = tokio::select! { biased;
            () = self.cancel.cancelled() => Err(Failure::Closing),
            () = request.generation.cancelled.cancelled() => Err(Failure::Closing),
            value = receiver => value.unwrap_or(Err(Failure::Closing)),
        };
        request.terminal.store(true, Ordering::Release);
        if self.cancel.is_cancelled()
            || request.generation.cancelled.is_cancelled()
        {
            self.event(CANCELLED, id, Status::Cancelled);
        }
        self.event(COMPLETE, id, Status::Ok);
        result
    }
    pub fn incoming(
        &self,
        id: u64,
    ) -> Result<Arc<Incoming>> {
        self.data
            .lock()
            .map_err(|_| Status::Panic)?
            .incoming
            .get(&id)
            .cloned()
            .ok_or(Status::InvalidHandle)
    }
    pub fn outgoing(
        &self,
        id: u64,
    ) -> Result<Arc<Outgoing>> {
        self.data
            .lock()
            .map_err(|_| Status::Panic)?
            .outgoing
            .get(&id)
            .cloned()
            .ok_or(Status::InvalidHandle)
    }
    pub fn respond(
        &self,
        id: u64,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        if payload.len() > self.limits.response_bytes {
            return Err(Status::Limit);
        }
        let value = outcome::reply(code, payload)?;
        let request = self.incoming(id)?;
        if self.cancel.is_cancelled()
            || request.generation.cancelled.is_cancelled()
            || request.terminal.load(Ordering::Acquire)
        {
            return Err(Status::Cancelled);
        }
        request
            .reply
            .lock()
            .map_err(|_| Status::Panic)?
            .take()
            .ok_or(Status::Closed)?
            .send(value)
            .map_err(|_| Status::Cancelled)
    }
    pub fn call(
        self: &Arc<Self>,
        expected_generation: u64,
        target: Box<str>,
        operation: String,
        payload: Vec<u8>,
    ) -> Result<(u64, u64)> {
        if operation.len() > self.limits.operation_bytes
            || payload.len() > self.limits.request_bytes
        {
            return Err(Status::Limit);
        }
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        // During withdrawal accepted handlers may still make nested calls.
        if data.finalizing || self.cancel.is_cancelled() {
            return Err(Status::Closed);
        }
        if data.outgoing.len() >= self.limits.pending_calls {
            return Err(Status::Busy);
        }
        let generation =
            self.application.lifecycle.capture()?.ok_or(Status::Cancelled)?;
        if generation.cancelled.is_cancelled()
            || (expected_generation != 0
                && expected_generation != generation.id)
        {
            return Err(Status::Cancelled);
        }
        let generation_id = generation.id;
        let caller = Arc::new(Outgoing {
            cancel: CancellationToken::new(),
            result: Mutex::new(None),
            delivered: false.into(),
        });
        let id = registry::identity()?;
        data.outgoing.insert(id, caller.clone());
        let owner = self.clone();
        self.outgoing.spawn_on(async move {
            let result = tokio::select! { biased;
                () = caller.cancel.cancelled() => (Status::Cancelled, 0, Vec::new()),
                () = generation.cancelled.cancelled() => (Status::Cancelled, 0, Vec::new()),
                () = owner.cancel.cancelled() => (Status::Cancelled, 0, Vec::new()),
                value = owner.client.call(&target, operation, payload) => { let (code, bytes) = outcome::outcome(value); (Status::Ok, code, bytes) },
            };
            *caller.result.lock().expect("invoke result") = Some(result);
            owner.event(RESULT, id, Status::Ok);
        }, &self.application.host);
        Ok((id, generation_id))
    }
    pub fn wait(&self) -> Result<Event> {
        let receiver = self.receive.lock().map_err(|_| Status::Panic)?;
        if self.data.lock().map_err(|_| Status::Panic)?.closed {
            return Err(Status::Closed);
        }
        let event = receiver.recv().map_err(|_| Status::Closed)?;
        match event.kind {
            COMPLETE => self
                .incoming(event.exchange)?
                .delivered
                .store(true, Ordering::Release),
            RESULT => self
                .outgoing(event.exchange)?
                .delivered
                .store(true, Ordering::Release),
            CLOSED => {
                self.data.lock().map_err(|_| Status::Panic)?.closed = true
            }
            _ => (),
        }
        Ok(event)
    }
    pub fn withdraw(&self) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data.closing {
            data.closing = true;
            self.event(WITHDRAWN, 0, Status::Ok);
        }
        Ok(())
    }
    pub fn cancel(&self) -> Result<()> {
        self.withdraw()?;
        self.cancel.cancel();
        Ok(())
    }
    pub fn finish(&self) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data.closing
            || !data.incoming.is_empty()
            || !data.outgoing.is_empty()
        {
            return Err(Status::Busy);
        }
        data.finalizing = true;
        self.finish.cancel();
        Ok(())
    }
    pub fn release_incoming(
        &self,
        id: u64,
    ) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data
            .incoming
            .get(&id)
            .ok_or(Status::InvalidHandle)?
            .delivered
            .load(Ordering::Acquire)
        {
            return Err(Status::Busy);
        }
        data.incoming.remove(&id);
        Ok(())
    }
    pub fn with_result<T>(
        &self,
        id: u64,
        consume: bool,
        action: impl FnOnce(Status, u32, &[u8]) -> Result<T>,
    ) -> Result<T> {
        let call = self.outgoing(id)?;
        if !call.delivered.load(Ordering::Acquire) {
            return Err(Status::Busy);
        }
        let result = call.result.lock().map_err(|_| Status::Panic)?;
        let (status, code, payload) = result.as_ref().ok_or(Status::Closed)?;
        let value = action(*status, *code, payload)?;
        drop(result);
        if consume {
            self.consume(id)?;
        }
        Ok(value)
    }
    pub fn consume(
        &self,
        id: u64,
    ) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data
            .outgoing
            .get(&id)
            .ok_or(Status::InvalidHandle)?
            .delivered
            .load(Ordering::Acquire)
        {
            return Err(Status::Busy);
        }
        data.outgoing.remove(&id);
        Ok(())
    }
}

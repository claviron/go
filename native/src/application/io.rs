//! One input and one output owner per exchange; completion space is reserved
//! until both delivery and result consumption, including cancellation races.
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use claviron::orbit::link::application::{
    Exchange as Frame, Headers, LINK_INBOX_PAYLOAD_CAPACITY,
};
use claviron::orbitive::stream::{ReadHalf, WriteHalf};
use futures_util::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::State;
use crate::abi::{EventKind, Result, Status};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    New,
    Open,
    Finished,
    Aborted,
}
#[derive(Clone, Copy)]
enum Action {
    Head,
    Write,
    Finish,
}

struct OutputState {
    phase: Phase,
    busy: bool,
    ready: bool,
    delivered: bool,
    written: usize,
    total: u64,
    status: Status,
}
impl Default for OutputState {
    fn default() -> Self {
        Self {
            phase: Phase::New,
            busy: false,
            ready: false,
            delivered: false,
            written: 0,
            total: 0,
            status: Status::Ok,
        }
    }
}
impl OutputState {
    fn reserve(
        &mut self,
        action: Action,
    ) -> Result<()> {
        if self.busy {
            return Err(Status::Busy);
        }
        self.phase = match (self.phase, action) {
            (Phase::New, Action::Head) | (Phase::Open, Action::Write) => {
                Phase::Open
            }
            (Phase::Open, Action::Finish) => Phase::Finished,
            _ => return Err(Status::Closed),
        };
        self.busy = true;
        Ok(())
    }
}

#[derive(Default)]
struct InputState {
    busy: bool,
    ready: bool,
    delivered: bool,
    data: Vec<u8>,
    status: Option<Status>,
}
struct Head {
    status: u16,
    headers: Headers,
    bytes: usize,
}
pub(super) enum Command {
    Head(u16, Headers),
    Write(Vec<u8>),
    Finish,
}

pub struct Exchange {
    pub application: Arc<State>,
    pub frame: Frame,
    head: Mutex<Option<Head>>,
    input: Mutex<InputState>,
    output: Mutex<OutputState>,
    reads: mpsc::Sender<usize>,
    writes: mpsc::Sender<Command>,
    pub cancel: CancellationToken,
    input_stop: CancellationToken,
    finished: AtomicBool,
    finished_delivered: AtomicBool,
}

impl Exchange {
    pub(super) fn new(
        application: Arc<State>,
        frame: Frame,
    ) -> (Arc<Self>, mpsc::Receiver<usize>, mpsc::Receiver<Command>) {
        // A slot is an operation owner, not an additional admission queue.
        let (reads, input) = mpsc::channel(1);
        let (writes, output) = mpsc::channel(1);
        let owner = Arc::new(Self {
            cancel: application.stop.child_token(),
            application,
            frame,
            head: Mutex::new(None),
            input: Mutex::new(InputState::default()),
            output: Mutex::new(OutputState::default()),
            reads,
            writes,
            input_stop: CancellationToken::new(),
            finished: AtomicBool::new(false),
            finished_delivered: AtomicBool::new(false),
        });
        (owner, input, output)
    }
    pub fn releasable(&self) -> bool {
        self.finished.load(Ordering::Acquire)
            && self.finished_delivered.load(Ordering::Acquire)
            && self.input.lock().is_ok_and(|slot| !slot.busy)
            && self.output.lock().is_ok_and(|slot| !slot.busy)
    }
    fn live(&self) -> Result<()> {
        if self.finished.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            Err(Status::Cancelled)
        } else {
            Ok(())
        }
    }
    pub fn begin(
        &self,
        status: u16,
    ) -> Result<()> {
        self.live()?;
        if !(200..=599).contains(&status) {
            return Err(Status::Unsupported);
        }
        let output = self.output.lock().map_err(|_| Status::Panic)?;
        if output.phase != Phase::New || output.busy {
            return Err(Status::Busy);
        }
        let mut head = self.head.lock().map_err(|_| Status::Panic)?;
        if head.is_some() {
            return Err(Status::Busy);
        }
        *head = Some(Head { status, headers: Vec::new(), bytes: 0 });
        Ok(())
    }
    pub fn header(
        &self,
        name: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.live()?;
        http::HeaderName::from_bytes(name)
            .map_err(|_| Status::InvalidArgument)?;
        http::HeaderValue::from_bytes(value)
            .map_err(|_| Status::InvalidArgument)?;
        let mut head = self.head.lock().map_err(|_| Status::Panic)?;
        let head = head.as_mut().ok_or(Status::Busy)?;
        let bytes = head
            .bytes
            .checked_add(name.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or(Status::Capacity)?;
        if bytes > LINK_INBOX_PAYLOAD_CAPACITY {
            return Err(Status::Capacity);
        }
        head.bytes = bytes;
        head.headers.push((name.to_vec(), value.to_vec()));
        Ok(())
    }
    pub fn head(&self) -> Result<()> {
        self.live()?;
        let mut output = self.output.lock().map_err(|_| Status::Panic)?;
        let mut head = self.head.lock().map_err(|_| Status::Panic)?;
        if head.is_none() {
            return Err(Status::Busy);
        }
        output.reserve(Action::Head)?;
        let head = head.take().expect("checked head");
        if self
            .writes
            .try_send(Command::Head(head.status, head.headers))
            .is_err()
        {
            output.busy = false;
            output.phase = Phase::Aborted;
            self.cancel.cancel();
            return Err(Status::Cancelled);
        }
        Ok(())
    }
    pub fn write(
        &self,
        data: &[u8],
    ) -> Result<()> {
        self.live()?;
        if data.len() > self.application.options.io_buffer_bytes {
            return Err(Status::Capacity);
        }
        let mut output = self.output.lock().map_err(|_| Status::Panic)?;
        let total =
            output.total.checked_add(data.len() as u64).ok_or(Status::Limit)?;
        if self.application.options.max_response_bytes != 0
            && total > self.application.options.max_response_bytes as u64
        {
            self.cancel.cancel();
            return Err(Status::Limit);
        }
        output.reserve(Action::Write)?;
        if self.writes.try_send(Command::Write(data.to_vec())).is_err() {
            output.busy = false;
            output.phase = Phase::Aborted;
            self.cancel.cancel();
            return Err(Status::Cancelled);
        }
        Ok(())
    }
    pub fn finish_output(&self) -> Result<()> {
        self.live()?;
        let mut output = self.output.lock().map_err(|_| Status::Panic)?;
        output.reserve(Action::Finish)?;
        if self.writes.try_send(Command::Finish).is_err() {
            output.busy = false;
            output.phase = Phase::Aborted;
            self.cancel.cancel();
            return Err(Status::Cancelled);
        }
        Ok(())
    }
    pub fn read(
        &self,
        capacity: usize,
    ) -> Result<()> {
        self.live()?;
        if self.input_stop.is_cancelled() {
            return Err(Status::Closed);
        }
        if capacity == 0 || capacity > self.application.options.io_buffer_bytes
        {
            return Err(Status::InvalidArgument);
        }
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if input.busy {
            return Err(Status::Busy);
        }
        input.busy = true;
        if self.reads.try_send(capacity).is_err() {
            input.busy = false;
            return Err(Status::Closed);
        }
        Ok(())
    }
    pub fn close_input(&self) {
        self.input_stop.cancel();
    }
    pub fn delivered(
        &self,
        kind: u32,
    ) -> Result<()> {
        if kind == EventKind::Complete as u32 {
            self.finished_delivered.store(true, Ordering::Release);
        } else if kind == EventKind::Read as u32 {
            let mut input = self.input.lock().map_err(|_| Status::Panic)?;
            if !input.ready || input.delivered {
                return Err(Status::Busy);
            }
            input.delivered = true;
        } else {
            let mut output = self.output.lock().map_err(|_| Status::Panic)?;
            if !output.ready || output.delivered {
                return Err(Status::Busy);
            }
            output.delivered = true;
        }
        Ok(())
    }
    pub fn read_result(
        &self,
        copy: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if !input.ready || !input.delivered {
            return Err(Status::Busy);
        }
        copy(&input.data)?;
        input.data.clear();
        input.ready = false;
        input.delivered = false;
        input.busy = false;
        let status = input.status.take().unwrap_or(Status::Ok);
        if status == Status::Ok { Ok(()) } else { Err(status) }
    }
    pub fn output_result(&self) -> Result<(usize, Status)> {
        let mut output = self.output.lock().map_err(|_| Status::Panic)?;
        if !output.ready || !output.delivered {
            return Err(Status::Busy);
        }
        output.ready = false;
        output.delivered = false;
        output.busy = false;
        Ok((output.written, output.status))
    }
    fn read_complete(
        &self,
        id: u64,
        data: Vec<u8>,
        status: Status,
    ) {
        let mut input = self.input.lock().expect("input state");
        if !input.busy || input.ready {
            return;
        }
        input.data = data;
        input.status = Some(status);
        input.ready = true;
        drop(input);
        self.application.event(EventKind::Read, id, status);
    }
    fn output_complete(
        &self,
        id: u64,
        written: usize,
        status: Status,
    ) {
        let mut output = self.output.lock().expect("output state");
        if !output.busy || output.ready {
            return;
        }
        output.written = written;
        output.status = status;
        output.ready = true;
        output.total = output.total.saturating_add(written as u64);
        if status != Status::Ok {
            output.phase = Phase::Aborted;
        }
        drop(output);
        self.application.event(EventKind::Output, id, status);
    }
    pub(super) async fn drive(
        self: Arc<Self>,
        id: u64,
        input: mpsc::Receiver<usize>,
        output: mpsc::Receiver<Command>,
    ) {
        let outcome = AssertUnwindSafe(self.perform(id, input, output))
            .catch_unwind()
            .await
            .unwrap_or(Err(Status::Panic));
        if outcome.is_err() {
            self.cancel.cancel();
        }
        self.input_stop.cancel();
        // A submission that raced owner exit still gets its reserved completion.
        self.read_complete(id, Vec::new(), Status::Cancelled);
        self.output_complete(id, 0, Status::Cancelled);
        if let Err(status) = outcome {
            self.application.event(EventKind::Cancelled, id, status);
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
        self: &Arc<Self>,
        id: u64,
        reads: mpsc::Receiver<usize>,
        writes: mpsc::Receiver<Command>,
    ) -> Result<()> {
        let (input, output) = match self.frame.body {
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
        let reader = Arc::clone(self);
        // Own and join the reader even if output fails; no detached I/O task.
        let read = async {
            if AssertUnwindSafe(reader.read_driver(id, input, reads))
                .catch_unwind()
                .await
                .is_err()
            {
                reader.read_complete(id, Vec::new(), Status::Panic);
                reader.cancel.cancel();
                return Err(Status::Panic);
            }
            Ok(())
        };
        let write = async {
            let result =
                AssertUnwindSafe(self.write_driver(id, output, writes))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(Status::Panic));
            self.input_stop.cancel();
            result
        };
        self.application.event(EventKind::Request, id, Status::Ok);
        let (read, write) = tokio::join!(read, write);
        read?;
        write
    }
    async fn read_driver(
        self: &Arc<Self>,
        id: u64,
        mut input: Option<ReadHalf>,
        mut commands: mpsc::Receiver<usize>,
    ) {
        let mut total = 0u64;
        let mut terminal = None;
        loop {
            let size = tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                () = self.input_stop.cancelled() => break,
                size = commands.recv() => match size { Some(size) => size, None => break },
            };
            let mut data = std::mem::take(
                &mut self.input.lock().expect("input state").data,
            );
            data.clear();
            let mut status = terminal.unwrap_or(Status::Ok);
            if terminal.is_none() {
                let maximum = self.application.options.max_request_bytes as u64;
                let size = if maximum != 0 {
                    size.min((maximum - total).saturating_add(1) as usize)
                } else {
                    size
                };
                data.resize(size, 0);
                let result = tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => Err(Status::Cancelled),
                    () = self.input_stop.cancelled() => Err(Status::Closed),
                    result = async { match input.as_mut() { Some(input) => input.read(&mut data).await.map_err(|_| Status::Cancelled), None => Ok(0) } } => result,
                };
                match result {
                    Ok(n) => {
                        data.truncate(n);
                        if maximum != 0 && n as u64 > maximum - total {
                            data.truncate((maximum - total) as usize);
                            status = Status::Limit;
                            terminal = Some(status);
                            input.take();
                        }
                        total = total.saturating_add(data.len() as u64);
                        if n == 0 {
                            input.take();
                        }
                    }
                    Err(error) => {
                        data.clear();
                        status = error;
                        if error == Status::Cancelled {
                            self.cancel.cancel();
                        }
                        terminal = Some(error);
                    }
                }
            }
            self.read_complete(id, data, status);
        }
        drop(commands);
        drop(input);
        self.read_complete(
            id,
            Vec::new(),
            if self.cancel.is_cancelled() {
                Status::Cancelled
            } else {
                Status::Closed
            },
        );
    }
    async fn write_driver(
        &self,
        id: u64,
        mut output: Option<WriteHalf>,
        mut commands: mpsc::Receiver<Command>,
    ) -> Result<()> {
        loop {
            let command = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return Err(Status::Cancelled),
                _ = peer_closed(&mut output) => return Err(Status::Cancelled),
                command = commands.recv() => command.ok_or(Status::Cancelled)?,
            };
            let mut written = 0usize;
            let finish = matches!(command, Command::Finish);
            let result = match command {
                Command::Head(status, headers) => {
                    self.application.publish_head(&self.frame, status, headers)
                }
                Command::Write(bytes) => tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => Err(Status::Cancelled),
                    result = async {
                        let output = output.as_mut().ok_or(Status::Unsupported)?;
                        while written < bytes.len() {
                            let n = output.write(&bytes[written..]).await.map_err(|_| Status::Cancelled)?;
                            if n == 0 { return Err(Status::Transport); }
                            written += n;
                        }
                        Ok(())
                    } => result,
                },
                Command::Finish => match output.as_mut() {
                    Some(output) => {
                        output.shutdown().await.map_err(|_| Status::Cancelled)
                    }
                    None => Ok(()),
                },
            };
            self.output_complete(
                id,
                written,
                result.err().unwrap_or(Status::Ok),
            );
            result?;
            if finish {
                return Ok(());
            }
        }
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
#[path = "../../tests/application/io.rs"]
mod tests;

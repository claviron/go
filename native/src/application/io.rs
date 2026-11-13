//! One input and one output owner per exchange; completion space is reserved
//! until both delivery and result consumption, including cancellation races.
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use claviron::orbit::link::application::body::ResponseWriter;
use claviron::orbit::link::application::{
    Exchange as Frame, ExchangeBody, Headers, LINK_INBOX_PAYLOAD_CAPACITY, Message,
    MessageReader, write_message,
};
use claviron::orbitive::stream::ReadHalf;
use futures_util::FutureExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::State;
use super::lifecycle::Generation;
use crate::abi::{EventKind, Result, Status};

// WebSocket control payloads have a protocol maximum of 125 bytes, even when
// an application's text/binary limit is smaller. This is not a tuning value.
const CONTROL_BYTES: usize = 125;
const MESSAGE_END: u32 = 0;
pub(crate) const MESSAGE_TEXT: u32 = 1;
pub(crate) const MESSAGE_BINARY: u32 = 2;
pub(crate) const MESSAGE_CLOSE: u32 = 3;

pub(super) type HttpBodies = (Option<ReadHalf>, Option<ResponseWriter>);

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
    message: (u32, u32),
}
struct Head {
    status: u16,
    headers: Headers,
    bytes: usize,
}
pub(super) enum Command {
    Head(u16, Headers),
    Write(Vec<u8>),
    Message(Message),
    Finish,
}

pub struct Exchange {
    admission: Option<claviron::orbit::link::application::WorkExecution>,
    pub application: Arc<State>,
    pub frame: Frame,
    pub generation: Arc<Generation>,
    head: Mutex<Option<Head>>,
    input: Mutex<InputState>,
    output: Mutex<OutputState>,
    reads: mpsc::Sender<usize>,
    writes: mpsc::Sender<Command>,
    pub cancel: CancellationToken,
    input_stop: CancellationToken,
    finished: AtomicBool,
    finished_delivered: AtomicBool,
    /// Set by acceptance: this exchange now carries Link message records.
    session_limit: OnceLock<u32>,
    /// A close the peer sent, for the output owner to answer.
    peer_close: Mutex<Option<(u16, String)>>,
    closing: Notify,
}

impl Drop for Exchange {
    fn drop(&mut self) {
        drop(self.admission.take());
        self.application.release_credit();
    }
}

impl Exchange {
    pub(super) fn new(
        application: Arc<State>,
        frame: Frame,
        generation: Arc<Generation>,
        admission: Option<claviron::orbit::link::application::WorkExecution>,
    ) -> (Arc<Self>, mpsc::Receiver<usize>, mpsc::Receiver<Command>) {
        // A slot is an operation owner, not an additional admission queue.
        let (reads, input) = mpsc::channel(1);
        let (writes, output) = mpsc::channel(1);
        let owner = Arc::new(Self {
            admission,
            cancel: generation.cancelled.child_token(),
            generation,
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
            session_limit: OnceLock::new(),
            peer_close: Mutex::new(None),
            closing: Notify::new(),
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
        // 101 is Link's acceptance and only begin_session sends it. A success
        // status cannot answer a session: it would authorize no message stream.
        if !(200..=599).contains(&status)
            || (self.frame.upgrade && status < 300)
        {
            return Err(Status::Unsupported);
        }
        self.begin_head(status)
    }
    /// Accept a message session. Native WebSocket and WebTransport handshakes
    /// stay at the edge; the 101 this begins is the Link acknowledgment.
    pub fn begin_session(
        &self,
        max_message_bytes: u32,
    ) -> Result<()> {
        self.live()?;
        if !self.frame.upgrade
            || self.frame.stream.is_none()
            || max_message_bytes == 0
        {
            return Err(Status::InvalidArgument);
        }
        self.begin_head(101)?;
        self.session_limit.set(max_message_bytes).map_err(|_| Status::Busy)
    }
    fn begin_head(
        &self,
        status: u16,
    ) -> Result<()> {
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
        if self.frame.upgrade {
            return Err(Status::Unsupported);
        }
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
    /// Queue one message. A close also ends this direction.
    pub fn write_message(
        &self,
        message: Message,
    ) -> Result<()> {
        self.live()?;
        let limit = *self.session_limit.get().ok_or(Status::Unsupported)?;
        let action = match &message {
            Message::Text(text) => {
                checked_length(text.len(), limit)?;
                Action::Write
            }
            Message::Binary(bytes) => {
                checked_length(bytes.len(), limit)?;
                Action::Write
            }
            Message::Close { code, reason } => {
                validate_close(*code, reason)?;
                Action::Finish
            }
        };
        let mut output = self.output.lock().map_err(|_| Status::Panic)?;
        output.reserve(action)?;
        if self.writes.try_send(Command::Message(message)).is_err() {
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
        if self.frame.upgrade {
            return Err(Status::Unsupported);
        }
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
    /// Start waiting for one message; completes through the read event.
    pub fn read_message(&self) -> Result<()> {
        self.live()?;
        if self.session_limit.get().is_none() {
            return Err(Status::Unsupported);
        }
        if self.input_stop.is_cancelled() {
            return Err(Status::Closed);
        }
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if input.busy {
            return Err(Status::Busy);
        }
        input.busy = true;
        if self.reads.try_send(0).is_err() {
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
    /// The delivered message's kind and code. `consume` copies and releases it;
    /// otherwise this only reports, so the caller can size its buffer.
    pub fn message_result(
        &self,
        consume: bool,
        copy: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<(u32, u32)> {
        let mut input = self.input.lock().map_err(|_| Status::Panic)?;
        if !input.ready || !input.delivered {
            return Err(Status::Busy);
        }
        copy(&input.data)?;
        let message = input.message;
        if !consume {
            return Ok(message);
        }
        input.data.clear();
        input.ready = false;
        input.delivered = false;
        input.busy = false;
        input.message = (MESSAGE_END, 0);
        let status = input.status.take().unwrap_or(Status::Ok);
        if status == Status::Ok { Ok(message) } else { Err(status) }
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
        bodies: HttpBodies,
    ) {
        let outcome = AssertUnwindSafe(self.perform(id, input, output, bodies))
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
        (input, mut output): HttpBodies,
    ) -> Result<()> {
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
                AssertUnwindSafe(self.write_driver(id, &mut output, writes))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(Status::Panic));
            if result.is_err()
                && let Some(output) = output.as_mut()
            {
                output.reset();
            }
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
        let mut inline = match &self.frame.body {
            claviron::orbit::link::application::ExchangeBody::Inline(bytes) => {
                bytes.clone()
            }
            _ => tokio_util::bytes::Bytes::new(),
        };
        if !matches!(
            self.frame.body,
            claviron::orbit::link::application::ExchangeBody::Stream
        ) {
            input.take();
        }
        let mut total = 0u64;
        let mut terminal = None;
        let mut records = None;
        loop {
            let size = tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                () = self.input_stop.cancelled() => break,
                size = commands.recv() => match size { Some(size) => size, None => break },
            };
            if let Some(limit) = self.session_limit.get().copied() {
                let records = records.get_or_insert_with(|| {
                    MessageReader::with_limit(
                        (limit as usize).max(CONTROL_BYTES),
                    )
                });
                self.read_record(id, limit, records, &mut input).await;
                continue;
            }
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
                    result = async { if !inline.is_empty() {
                        let length = data.len().min(inline.len());
                        data[..length].copy_from_slice(&inline.split_to(length));
                        Ok(length)
                    } else { match input.as_mut() { Some(input) => input.read(&mut data).await.map_err(|_| Status::Cancelled), None => Ok(0) } } } => result,
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
    async fn read_record(
        &self,
        id: u64,
        limit: u32,
        records: &mut MessageReader,
        input: &mut Option<ReadHalf>,
    ) {
        let result = match input.as_mut() {
            Some(half) => tokio::select! {
                biased;
                () = self.cancel.cancelled() => Err(Status::Cancelled),
                () = self.input_stop.cancelled() => Err(Status::Closed),
                result = records.read(half) => result.map_err(|error| {
                    // The Link's own records are well formed, so the invalid
                    // record a consumer meets is the one over its limit.
                    if error.kind() == std::io::ErrorKind::InvalidData {
                        Status::Limit
                    } else {
                        Status::Cancelled
                    }
                }),
            },
            None => Ok(None),
        };
        let (kind, code, data, status) = match result {
            Ok(Some(Message::Text(text))) => {
                match checked_length(text.len(), limit) {
                    Ok(()) => (MESSAGE_TEXT, 0, text.into_bytes(), Status::Ok),
                    Err(status) => (MESSAGE_END, 0, Vec::new(), status),
                }
            }
            Ok(Some(Message::Binary(bytes))) => {
                match checked_length(bytes.len(), limit) {
                    Ok(()) => (MESSAGE_BINARY, 0, bytes, Status::Ok),
                    Err(status) => (MESSAGE_END, 0, Vec::new(), status),
                }
            }
            Ok(Some(Message::Close { code, reason })) => {
                *self.peer_close.lock().expect("peer close") =
                    Some((code, reason.clone()));
                self.closing.notify_one();
                (
                    MESSAGE_CLOSE,
                    u32::from(code),
                    reason.into_bytes(),
                    Status::Ok,
                )
            }
            Ok(None) => {
                input.take();
                (MESSAGE_END, 0, Vec::new(), Status::Ok)
            }
            Err(status) => (MESSAGE_END, 0, Vec::new(), status),
        };
        if !matches!(status, Status::Ok | Status::Closed) {
            input.take();
            self.cancel.cancel();
        }
        self.input.lock().expect("input state").message = (kind, code);
        self.read_complete(id, data, status);
    }
    /// Answer the peer's close, then end this direction. 1005 and 1006 are
    /// observations, never codes sent on the wire.
    async fn answer_close(
        &self,
        output: &mut Option<ResponseWriter>,
    ) -> Result<()> {
        let close = self.peer_close.lock().map_err(|_| Status::Panic)?.take();
        self.output.lock().map_err(|_| Status::Panic)?.phase = Phase::Finished;
        let Some(ResponseWriter::ByteStream(output)) = output.as_mut() else { return Ok(()) };
        if let Some((code, reason)) = close
            && !matches!(code, 1005 | 1006)
        {
            let message = Message::Close { code, reason };
            write_message(output, &message)
                .await
                .map_err(|_| Status::Cancelled)?;
        }
        output.shutdown().await.map_err(|_| Status::Cancelled)
    }
    async fn publish_head(
        &self,
        status: u16,
        headers: Headers,
        output: &mut Option<ResponseWriter>,
    ) -> Result<()> {
        use claviron::orbit::link::application::Error as LinkError;

        let body = if output.is_some() { ExchangeBody::Stream } else { ExchangeBody::Empty };
        let bytes = State::encode_head(&self.frame, status, headers, body)?;
        loop {
            if self.cancel.is_cancelled() {
                return Err(Status::Cancelled);
            }
            match self
                .application
                .segment
                .publish_reply(self.frame.origin as usize, &[&bytes])
            {
                Ok(()) => return Ok(()),
                Err(LinkError::ProducerBusy) => {
                    tokio::select! {
                        biased;
                        () = self.cancel.cancelled() => return Err(Status::Cancelled),
                        () = peer_closed(output), if !self.frame.upgrade => return Err(Status::Cancelled),
                        result = self.application.segment.record_released() => {
                            result.map_err(|_| Status::Transport)?;
                        }
                    }
                }
                Err(_) => return Err(Status::Transport),
            }
        }
    }
    async fn write_driver(
        &self,
        id: u64,
        output: &mut Option<ResponseWriter>,
        mut commands: mpsc::Receiver<Command>,
    ) -> Result<()> {
        loop {
            let command = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return Err(Status::Cancelled),
                // A session's edge leaves right after its last record, which
                // the input owner has still to read: its end is not a cancel.
                _ = peer_closed(output), if !self.frame.upgrade => return Err(Status::Cancelled),
                command = commands.recv() => command.ok_or(Status::Cancelled)?,
                () = self.closing.notified() => {
                    // A peer already gone cannot receive the answer; the close
                    // it sent still ended this session cleanly.
                    tokio::select! {
                        biased;
                        () = self.cancel.cancelled() => {},
                        _ = self.answer_close(output) => {},
                    }
                    return Ok(());
                }
            };
            let mut written = 0usize;
            let finish = matches!(
                command,
                Command::Finish | Command::Message(Message::Close { .. })
            );
            let result = match command {
                Command::Head(status, headers) => async {
                    self.publish_head(status, headers, output).await?;
                    if let Some(output) = output.as_mut() {
                        output.start().map_err(|_| Status::Cancelled)?;
                    }
                    Ok(())
                }.await,
                Command::Write(bytes) => tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => Err(Status::Cancelled),
                    result = async {
                        let output = output.as_mut().ok_or(Status::Unsupported)?;
                        while written < bytes.len() {
                            let n = output.write_chunk(&bytes[written..]).await.map_err(|_| Status::Cancelled)?;
                            written += n;
                        }
                        Ok(())
                    } => result,
                },
                Command::Message(message) => tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => Err(Status::Cancelled),
                    result = async {
                        let Some(ResponseWriter::ByteStream(output)) = output.as_mut() else {
                            return Err(Status::Unsupported);
                        };
                        write_message(output, &message).await.map_err(|_| Status::Cancelled)?;
                        if finish {
                            output.shutdown().await.map_err(|_| Status::Cancelled)?;
                        }
                        Ok(())
                    } => result,
                },
                Command::Finish => tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => Err(Status::Cancelled),
                    result = async {
                        if let Some(output) = output.as_mut() {
                            output.finish().await.map_err(|_| Status::Cancelled)?;
                        }
                        Ok(())
                    } => result,
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

fn checked_length(
    length: usize,
    limit: u32,
) -> Result<()> {
    if length > limit as usize { Err(Status::Limit) } else { Ok(()) }
}

pub(crate) fn validate_close(
    code: u16,
    reason: &str,
) -> Result<()> {
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
        || reason.len() > CONTROL_BYTES - 2
    {
        return Err(Status::InvalidArgument);
    }
    Ok(())
}

async fn peer_closed(output: &mut Option<ResponseWriter>) {
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

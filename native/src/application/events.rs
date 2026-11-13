//! Private lifecycle fan-out using the existing application-event codec and
//! notified fleet ring. Public event subscription/delivery is a later stage.
use std::collections::VecDeque;
use std::sync::Arc;

use claviron::app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, ApplicationId, ApplicationName,
    ApplicationRuntime, EventValue,
};
use claviron::orbit::{ApplicationEventLayout, FleetEventCodec};
use claviron::orbitive::core::RingEventFd;
use claviron::orbitive::events::{FleetEventBus, FleetEventCursor};
use claviron::orbitive::{Fleet, NodeId};
use tokio::io::unix::AsyncFd;

use super::lifecycle::Action;
use crate::abi::{Result, Status};

// This protocol topic identifies lifecycle decisions among Go instances. It is
// not an operator setting: changing it creates a different event protocol.
const TOPIC: &str = "claviron-go.lifecycle.v1";

pub struct Events {
    bus: Arc<FleetEventBus<ApplicationEventLayout>>,
    target: ApplicationId,
}
pub struct Reader {
    events: Arc<Events>,
    cursor: FleetEventCursor,
    ready: AsyncFd<RingEventFd>,
    pending: VecDeque<Action>,
}
impl Events {
    pub fn open(
        fleet: &str,
        name: &str,
        lane: usize,
    ) -> Result<(Arc<Self>, Reader)> {
        let fleet = Arc::new(
            Fleet::join_shm_as(
                fleet,
                claviron::orbit::link::application::LINK_FLEET_CAPACITY as u16,
                NodeId::new(lane as u16),
            )
            .map_err(|_| Status::Transport)?,
        );
        let bus = Arc::new(
            FleetEventBus::<ApplicationEventLayout>::with_layout(fleet)
                .map_err(|_| Status::Transport)?,
        );
        let target = ApplicationId::new(
            ApplicationRuntime::new("go")
                .map_err(|_| Status::InvalidArgument)?,
            ApplicationName::new(name).map_err(|_| Status::InvalidArgument)?,
        );
        let events = Arc::new(Self { bus, target });
        let cursor = events.bus.cursor_at_head();
        let descriptor =
            events.bus.event_fd().map_err(|_| Status::Transport)?;
        // SAFETY: AsyncFd owns the descriptor and its stable fd identity until
        // deregistration; readiness is drained before the guard is cleared.
        let ready = unsafe { AsyncFd::register(descriptor) }
            .map_err(|_| Status::Transport)?;
        let reader = Reader {
            events: Arc::clone(&events),
            cursor,
            ready,
            pending: VecDeque::new(),
        };
        Ok((events, reader))
    }
    pub fn publish(
        &self,
        action: Action,
    ) -> Result<()> {
        let event = ApplicationEvent::for_application(
            self.target.clone(),
            TOPIC,
            EventValue::String(action.name().as_bytes().to_vec()),
        )
        .map_err(|_| Status::InvalidArgument)?;
        self.bus
            .publish(APPLICATION_EVENT_TOPIC, &event.encode_orbit())
            .map_err(|_| Status::Transport)?;
        Ok(())
    }
    fn decode(
        &self,
        bytes: &[u8],
    ) -> Result<Option<Action>> {
        let event = ApplicationEvent::decode_orbit(bytes)
            .map_err(|_| Status::InvalidArgument)?;
        if event.topic().as_str() != TOPIC
            || event.target() != Some(&self.target)
        {
            return Ok(None);
        }
        if let EventValue::String(name) = event.payload() {
            let name = std::str::from_utf8(name)
                .map_err(|_| Status::InvalidArgument)?;
            return Action::from_name(name)
                .map(Some)
                .ok_or(Status::InvalidArgument);
        }
        Err(Status::InvalidArgument)
    }
}
impl Reader {
    pub async fn next(&mut self) -> Result<Action> {
        loop {
            if let Some(action) = self.pending.pop_front() {
                return Ok(action);
            }
            let batch = self.events.bus.poll(&mut self.cursor);
            if batch.lagged != 0 {
                return Err(Status::Capacity);
            }
            for event in batch.events {
                if event.topic == APPLICATION_EVENT_TOPIC
                    && event.id.node() != self.events.bus.node_id().get()
                    && let Some(action) = self.events.decode(&event.payload)?
                {
                    self.pending.push_back(action);
                }
            }
            if !self.pending.is_empty() {
                continue;
            }
            let mut guard =
                self.ready.readable().await.map_err(|_| Status::Transport)?;
            // Clear before draining: a signal that lands after the
            // drain is the only one until the next drain, and
            // clearing after would erase it.
            guard.clear_ready();
            self.ready.get_ref().drain().map_err(|_| Status::Transport)?;
        }
    }
}

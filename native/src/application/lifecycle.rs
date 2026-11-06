//! Application generation ownership, independent of Link membership and process.
use crate::abi::{Result, Status};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Action {
    Start = 1,
    Stop = 2,
    Restart = 3,
}
impl Action {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
        }
    }
    pub fn from_code(code: u32) -> Result<Self> {
        match code {
            1 => Ok(Self::Start),
            2 => Ok(Self::Stop),
            3 => Ok(Self::Restart),
            _ => Err(Status::InvalidArgument),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Snapshot {
    pub generation: u64,
    pub revision: u64,
    pub running: u64,
}
pub struct Generation {
    pub id: u64,
    pub cancelled: CancellationToken,
}
struct Data {
    snapshot: Snapshot,
    generation: Arc<Generation>,
    notified: bool,
    delivered: bool,
}
pub struct Lifecycle {
    parent: CancellationToken,
    data: Mutex<Data>,
}
impl Lifecycle {
    pub fn new(parent: CancellationToken) -> Self {
        let generation =
            Arc::new(Generation { id: 1, cancelled: parent.child_token() });
        Self {
            parent,
            data: Mutex::new(Data {
                snapshot: Snapshot { generation: 1, revision: 1, running: 1 },
                generation,
                notified: false,
                delivered: false,
            }),
        }
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        Ok(self.data.lock().map_err(|_| Status::Panic)?.snapshot)
    }
    pub fn capture(&self) -> Result<Option<Arc<Generation>>> {
        let data = self.data.lock().map_err(|_| Status::Panic)?;
        Ok((data.snapshot.running != 0).then(|| Arc::clone(&data.generation)))
    }
    fn next(
        snapshot: Snapshot,
        action: Action,
    ) -> Result<Option<Snapshot>> {
        let running = snapshot.running != 0;
        if (action == Action::Start && running)
            || (action == Action::Stop && !running)
        {
            return Ok(None);
        }
        let revision =
            snapshot.revision.checked_add(1).ok_or(Status::Capacity)?;
        let generation = if action != Action::Stop {
            snapshot.generation.checked_add(1).ok_or(Status::Capacity)?
        } else {
            snapshot.generation
        };
        Ok(Some(Snapshot {
            generation,
            revision,
            running: u64::from(action != Action::Stop),
        }))
    }
    pub fn validate(
        &self,
        action: Action,
    ) -> Result<()> {
        Self::next(self.snapshot()?, action).map(|_| ())
    }
    pub fn apply(
        &self,
        action: Action,
    ) -> Result<bool> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        let Some(snapshot) = Self::next(data.snapshot, action)? else {
            return Ok(false);
        };
        data.generation.cancelled.cancel();
        if action != Action::Stop {
            data.generation = Arc::new(Generation {
                id: snapshot.generation,
                cancelled: self.parent.child_token(),
            });
        }
        data.snapshot = snapshot;
        // One reserved wake represents the latest state, never an action log.
        let signal = !data.notified;
        data.notified = true;
        Ok(signal)
    }
    pub fn delivered(&self) -> Result<()> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data.notified || data.delivered {
            return Err(Status::Busy);
        }
        data.delivered = true;
        Ok(())
    }
    pub fn consume(&self) -> Result<Snapshot> {
        let mut data = self.data.lock().map_err(|_| Status::Panic)?;
        if !data.notified || !data.delivered {
            return Err(Status::Busy);
        }
        data.delivered = false;
        data.notified = false;
        Ok(data.snapshot)
    }
}

#[cfg(test)]
#[path = "../../tests/application/lifecycle.rs"]
mod tests;

//! Initial reachability/status schema; application lifecycle changes follow later.
use std::time::SystemTime;

use claviron::app::fleet::{
    FleetApplicationListSnapshot, FleetApplicationPlacement,
    FleetApplicationStatus, encode_link_snapshot_rows,
};
use claviron::app::{
    ApplicationDescription, ApplicationId, ApplicationLifecycle,
    ApplicationName, ApplicationRuntime,
};
use claviron::orbit::service::{FleetMember, FleetMemberRole};

use super::State;
use crate::abi::{Result, Status};

impl State {
    pub(super) fn publish_status(&self) -> Result<()> {
        let now = SystemTime::now();
        let member = FleetMember {
            node_id: self.lane as u16,
            role: FleetMemberRole::Standalone,
        };
        let snapshot = FleetApplicationListSnapshot {
            captured_at: now,
            members_expected: 1,
            members_reporting: 1,
            applications: vec![FleetApplicationStatus {
                id: ApplicationId::new(
                    ApplicationRuntime::new("go")
                        .map_err(|_| Status::InvalidArgument)?,
                    ApplicationName::new(&*self.options.name)
                        .map_err(|_| Status::InvalidArgument)?,
                ),
                placements: vec![FleetApplicationPlacement {
                    member,
                    description: ApplicationDescription::new("go", "net/http"),
                    lifecycle: ApplicationLifecycle::Running,
                    enabled: true,
                    generation: 1,
                    pool: None,
                    last_error: None,
                    last_error_truncated: false,
                    observed_at: now,
                }],
            }],
        };
        let rows = encode_link_snapshot_rows(&snapshot, member)
            .map_err(|_| Status::Capacity)?;
        self.segment
            .publish_application_state(&self.options.name, &rows)
            .map_err(|_| Status::Transport)?;
        Ok(())
    }
}

//! Event-driven lifecycle acceptance. Status rows are barriers, not timing guesses.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::{App, Result, exchange, frame};
use claviron::app::{ApplicationLifecycle, fleet::decode_link_snapshot_rows};
use claviron::orbit::link::application::{
    ApplicationClient, ApplicationSegment, ApplicationStateNotice,
    ControlCommand, ControlTarget, FrameKind, Received, frame_kind,
};
use claviron::orbitive::link::InboxReceiver;
use claviron::orbitive::shm::LaneHold;
use tokio::io::AsyncReadExt;

struct Controller<'a> {
    segment: &'a ApplicationSegment,
    lane: usize,
    receiver: InboxReceiver,
    _hold: LaneHold,
    states: HashMap<usize, (u64, u64, bool)>,
}
impl<'a> Controller<'a> {
    fn new(segment: &'a ApplicationSegment) -> Result<Self> {
        let joined = segment.join("test-control", "edge", 1)?;
        let receiver = segment.receiver(joined.lane)?;
        Ok(Self {
            segment,
            lane: joined.lane,
            receiver,
            _hold: joined.hold,
            states: HashMap::new(),
        })
    }
    fn send(
        &self,
        lane: usize,
        name: &str,
        action: &str,
        target: Option<ControlTarget>,
    ) -> Result<()> {
        let frame = ControlCommand {
            origin: Some(self.lane as u16),
            application: name.into(),
            name: action.into(),
            payload: Vec::new(),
            target,
        }
        .encode()?;
        self.segment.inbox().write(lane, &frame)?;
        Ok(())
    }
    fn action(
        &self,
        lane: usize,
        name: &str,
        action: &str,
    ) -> Result<()> {
        self.send(
            lane,
            name,
            action,
            Some(ControlTarget { runtime: "go".into(), name: name.into() }),
        )
    }
    async fn wait(
        &mut self,
        lane: usize,
        generation: u64,
        running: bool,
        after: u64,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        loop {
            if self.states.get(&lane).is_some_and(
                |&(batch, current, enabled)| {
                    batch > after && current == generation && enabled == running
                },
            ) {
                return Ok(());
            }
            if !matches!(self.receiver.next(&mut bytes).await?, Received::Frame)
            {
                continue;
            }
            if !matches!(frame_kind(&bytes), Ok(FrameKind::ApplicationState)) {
                continue;
            }
            let notice = ApplicationStateNotice::decode(&bytes)?;
            if notice.row_count != 1 {
                return Err("fixture status must fit one row".into());
            }
            let snapshot = decode_link_snapshot_rows(&[notice.payload])?;
            let placement = &snapshot
                .applications
                .first()
                .ok_or("empty application status")?
                .placements[0];
            self.states.insert(
                notice.lane as usize,
                (
                    notice.batch,
                    placement.generation,
                    placement.lifecycle == ApplicationLifecycle::Running,
                ),
            );
        }
    }
}
impl Drop for Controller<'_> {
    fn drop(&mut self) {
        let _ = self.segment.inbox().release_lane(self.lane);
        // receiver is declared before the kernel hold and is dropped first.
    }
}

pub(super) async fn fixtures(
    binary: &PathBuf,
    fleet: &str,
    client: &Arc<ApplicationClient>,
    segment: &ApplicationSegment,
) -> Result<()> {
    let mut first = App::start_mode(binary, fleet, "app", true).await?;
    let mut second = App::start_mode(binary, fleet, "app", true).await?;
    let other = App::start_mode(binary, fleet, "other", true).await?;
    let first_pid = first.child.id().ok_or("first pid")?;
    let second_pid = second.child.id().ok_or("second pid")?;
    let mut control = Controller::new(segment)?;
    first.command("watch").await?;
    first.expect("WATCH 1").await?;
    control.action(first.lane, "app", "stop")?;
    control.wait(first.lane, 1, false, 0).await?;
    control.wait(second.lane, 1, false, 0).await?;
    first.expect("GEN_CANCELLED 1").await?;
    assert!(segment.inbox().consumers(first.lane)? > 0);
    assert!(segment.inbox().consumers(second.lane)? > 0);
    for _ in 0..4 {
        assert_eq!(exchange(client, frame("/"), b"").await?.0.status, 503);
    }
    let pending = client.send("other", frame("/generation"))?;
    let (_, mut output, reply, _) = pending.split();
    assert_eq!(reply.head().await?.status, 200);
    let mut value = String::new();
    output.read_to_string(&mut value).await?;
    assert_eq!(value, "1");

    // Wrong-runtime and wrong-name commands are followed by a status query on
    // the same inbox. A fresh status batch is an ordering barrier.
    for target in [
        ControlTarget { runtime: "java".into(), name: "app".into() },
        ControlTarget { runtime: "go".into(), name: "other".into() },
    ] {
        let before = control.states[&first.lane].0;
        control.send(first.lane, "app", "start", Some(target))?;
        control.send(first.lane, "app", "status", None)?;
        control.wait(first.lane, 1, false, before).await?;
    }
    control.action(first.lane, "app", "start")?;
    control.wait(first.lane, 2, true, 0).await?;
    control.wait(second.lane, 2, true, 0).await?;
    assert_eq!(exchange(client, frame("/generation"), b"").await?.1, b"2");
    println!(
        "PASS lifecycle stop/start fan-out, retained memberships, wrong-target isolation and generation context cancellation"
    );

    let old_a =
        super::send_on(client, segment, "app", first.lane, frame("/late"))?;
    let old_b =
        super::send_on(client, segment, "app", second.lane, frame("/late"))?;
    first.expect("LATE 2").await?;
    second.expect("LATE 2").await?;
    control.action(first.lane, "app", "restart")?;
    control.wait(first.lane, 3, true, 0).await?;
    control.wait(second.lane, 3, true, 0).await?;
    let (_, _old_body_a, old_head_a, _) = old_a.split();
    let (_, _old_body_b, old_head_b, _) = old_b.split();
    assert!(old_head_a.head().await.is_err());
    assert!(old_head_b.head().await.is_err());
    assert_eq!(exchange(client, frame("/generation"), b"").await?.1, b"3");
    first.command("release-late").await?;
    second.command("release-late").await?;
    first.expect("LATE_REJECTED 2").await?;
    second.expect("LATE_REJECTED 2").await?;
    println!(
        "PASS restart admits new work while delayed old handlers retain immutable generation and cannot reply"
    );

    first.command("stop-api").await?;
    first.expect("STATE 3 5 false").await?;
    control.wait(first.lane, 3, false, 0).await?;
    control.wait(second.lane, 3, false, 0).await?;
    first.command("start-api").await?;
    first.expect("STATE 4 6 true").await?;
    control.wait(first.lane, 4, true, 0).await?;
    control.wait(second.lane, 4, true, 0).await?;
    first.command("restart-api").await?;
    first.expect("STATE 5 7 true").await?;
    control.wait(first.lane, 5, true, 0).await?;
    control.wait(second.lane, 5, true, 0).await?;
    assert_eq!(segment.inbox().owner(first.lane)?, Some(first_pid));
    assert_eq!(segment.inbox().owner(second.lane)?, Some(second_pid));
    assert_eq!(exchange(client, frame("/generation"), b"").await?.1, b"5");
    first.close().await?;
    second.close().await?;
    other.close().await?;
    println!(
        "PASS Go Start/Stop/Restart API shares the same lifecycle, revisions and sibling fan-out without restarting processes"
    );
    Ok(())
}

pub(super) async fn example(
    client: &Arc<ApplicationClient>,
    segment: &ApplicationSegment,
    lane: usize,
    pid: u32,
) -> Result<()> {
    let mut control = Controller::new(segment)?;
    let pending = client.send("app", frame("/wait-generation"))?;
    let (_, mut output, reply, _) = pending.split();
    let head = reply.head().await?;
    assert_eq!(head.status, 200);
    assert!(
        head.headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(b"x-generation")
                && value == b"1")
    );
    control.action(lane, "app", "stop")?;
    control.wait(lane, 1, false, 0).await?;
    assert!(output.read_to_end(&mut Vec::new()).await.is_err());
    assert_eq!(
        exchange(client, frame("/generation"), b"").await?.0.status,
        503
    );
    control.action(lane, "app", "start")?;
    control.wait(lane, 2, true, 0).await?;
    assert_eq!(exchange(client, frame("/generation"), b"").await?.1, b"2");
    control.action(lane, "app", "restart")?;
    control.wait(lane, 3, true, 0).await?;
    assert_eq!(exchange(client, frame("/generation"), b"").await?.1, b"3");
    assert_eq!(segment.inbox().owner(lane)?, Some(pid));
    println!(
        "PASS actual Go example: stop cancels a streaming request, start/restart advance generations in the same process"
    );
    Ok(())
}

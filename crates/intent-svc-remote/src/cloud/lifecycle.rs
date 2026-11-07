//! The life of a running sandbox (monorepo `docs/protocol/methods/nodes.md`,
//! "Sandbox lifecycle"): keep its E2B timeout ahead while it lives, pause it
//! when idle, resume it before a dial that something asked for, pause and
//! resume it before E2B's continuous-run cap, delete it when it stayed
//! paused too long, notice when E2B removed it, and delete sandboxes with
//! this backend's metadata that no machine row knows.
//!
//! A supervisor task checks every cloud machine on a short tick. Pauses,
//! resumes and kills of one machine take turns on its transition lock; a
//! kill also holds the machine's lifecycle lock (lock order: lifecycle,
//! then transition). The connector's dial gate takes only the transition
//! lock, since a kill waits for stops that need a dial.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use intent_core::machines::{
    cloud_machine_unavailable, parse_rfc3339, CloudMachine, CloudSandboxState, ExposedMachine,
};
use intent_core::remote_launch::PlacementError;
use intent_core::{now_iso, Error, Result};
use intent_nodes::e2b::{E2bClient, E2bError, ListSandboxes, SandboxState};
use intent_services::feature_api::Services;

use super::config;
use crate::connect::ConnectFailure;
use crate::connector::Shared;
use crate::RemoteFeature;

/// The E2B timeout a live sandbox is kept ahead by. E2B pauses it (never
/// kills it: `autoPause`) when this runs out.
pub(crate) const SANDBOX_TIMEOUT_SECS: u32 = 30 * 60;

/// Renew the timeout this often while the sandbox runs.
const RENEW_MINUTES: u32 = 10;
/// Pause and resume a sandbox at a turn boundary once it ran this long
/// without a pause: before E2B's shortest continuous-run cap (1 h, Hobby).
const RUN_CAP_MINUTES: u32 = 50;
/// How often the reaper lists this backend's sandboxes.
const REAP_MINUTES: u32 = 10;
/// A sandbox no row knows is reaped only once it is this old, so a start in
/// flight (bounded by the start wait) keeps its new sandbox.
const REAP_GRACE_MINUTES: u32 = 15;

/// Test seams: the length of one minute and one day of the lifecycle
/// settings and timers, in milliseconds.
const MINUTE_MS_ENV: &str = "INTENTD_E2B_MINUTE_MS";
const DAY_MS_ENV: &str = "INTENTD_E2B_DAY_MS";

/// The turn error of an agent whose sandbox E2B removed.
pub(crate) const SANDBOX_LOST_MESSAGE: &str = "E2B removed this agent's sandbox, so its turn was interrupted. Work it pushed is kept; work it did not push is lost. The next message starts a new sandbox.";

/// The lifecycle's clock: the length of a settings minute and day.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clock {
    minute: Duration,
    day: Duration,
}

impl Clock {
    pub(crate) fn current() -> Self {
        let ms = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .map_or(Duration::from_millis(default), Duration::from_millis)
        };
        Self {
            minute: ms(MINUTE_MS_ENV, 60_000),
            day: ms(DAY_MS_ENV, 86_400_000),
        }
    }

    fn minutes(self, n: u32) -> Duration {
        self.minute * n
    }

    fn days(self, n: u32) -> Duration {
        self.day * n
    }

    /// How often the supervisor checks the machines.
    fn tick(self) -> Duration {
        self.minute / 2
    }
}

/// Wall-clock time since `iso`; zero when it cannot be read.
fn since(iso: Option<&str>) -> Duration {
    iso.and_then(parse_rfc3339).map_or(Duration::ZERO, |at| {
        (time::OffsetDateTime::now_utc() - at)
            .try_into()
            .unwrap_or(Duration::ZERO)
    })
}

/// What the supervisor remembers about a running sandbox.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Activity {
    last_busy: Instant,
    last_renew: Option<Instant>,
}

impl Activity {
    pub(super) fn fresh() -> Self {
        Self {
            last_busy: Instant::now(),
            last_renew: Some(Instant::now()),
        }
    }

    pub(super) fn touch(&mut self) {
        self.last_busy = Instant::now();
    }

    pub(super) fn renewed(&mut self) {
        self.last_renew = Some(Instant::now());
    }
}

/// A cloud machine's dial gate: a parked (paused) sandbox is dialed only on
/// a demand, and every dial first makes sure E2B runs the sandbox.
pub(crate) struct CloudGate {
    svc: Services,
    machine_id: String,
}

impl CloudGate {
    pub(crate) fn new(svc: Services, machine_id: &str) -> Arc<Self> {
        Arc::new(Self {
            svc,
            machine_id: machine_id.to_owned(),
        })
    }

    /// How many pauses of this machine began, so a dial that binds after
    /// one knows it reached a frozen sandbox.
    pub(crate) fn pauses(&self) -> u64 {
        super::feature(&self.svc).map_or(0, |feature| feature.cloud.pauses(&self.machine_id))
    }

    /// Whether the sandbox is paused: no dial until something asks.
    pub(crate) fn parked(&self) -> bool {
        super::feature(&self.svc).is_ok_and(|feature| feature.cloud.is_parked(&self.machine_id))
    }

    /// Before a dial: resume the sandbox when paused (or check that E2B
    /// still has it). A sandbox E2B removed ends the machine's agents with
    /// [`SANDBOX_LOST_MESSAGE`] and removes the machine. `asked` when
    /// something asked for this dial; a redial that waited here while a
    /// pause finished does not resume the sandbox.
    pub(crate) async fn before_dial(
        &self,
        shared: &Shared,
        asked: bool,
    ) -> std::result::Result<(), ConnectFailure> {
        let Ok(feature) = super::feature(&self.svc) else {
            return Ok(());
        };
        if feature.cloud.is_lost(&self.machine_id) {
            return Err(lost_failure());
        }
        let _transition = feature.cloud.transition(&self.machine_id).await;
        if !asked && feature.cloud.is_parked(&self.machine_id) {
            return Err(ConnectFailure::Local(
                "the sandbox was paused; it resumes when something asks for it".into(),
            ));
        }
        let row = match self.svc.store().get_exposed_machine(&self.machine_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(()),
            Err(error) => return Err(ConnectFailure::Local(error.to_string())),
        };
        if row
            .cloud
            .as_ref()
            .is_none_or(|cloud| cloud.state == CloudSandboxState::Stopping)
        {
            return Ok(());
        }
        // Without the key (a provider remove in progress), dial as is.
        let Ok(client) = config::saved_client(&self.svc).await else {
            return Ok(());
        };
        match resume(&self.svc, feature, &client, &row).await {
            Ok(()) => Ok(()),
            Err(Resumed::Lost) => {
                sandbox_lost(&self.svc, feature, Some(shared), &row);
                Err(lost_failure())
            }
            Err(Resumed::Refused(error)) => Err(match error {
                Error::RemoteMachine(failure) => ConnectFailure::Cloud {
                    message: failure.message,
                    data: failure.data,
                    lost: false,
                },
                other => ConnectFailure::Local(other.to_string()),
            }),
        }
    }
}

/// `cloud-machine-unavailable` `sandbox-lost`.
fn sandbox_lost_error() -> Error {
    cloud_machine_unavailable("sandbox-lost", true, None, SANDBOX_LOST_MESSAGE)
}

fn lost_failure() -> ConnectFailure {
    match sandbox_lost_error() {
        Error::RemoteMachine(failure) => ConnectFailure::Cloud {
            message: failure.message,
            data: failure.data,
            lost: true,
        },
        other => ConnectFailure::Local(other.to_string()),
    }
}

/// Why a resume did not happen.
enum Resumed {
    /// E2B no longer has the sandbox.
    Lost,
    /// E2B refused (the documented error).
    Refused(Error),
}

/// Make E2B run `row`'s sandbox: resume it when paused, else check it and
/// extend its timeout. The caller holds the machine's transition lock.
async fn resume(
    svc: &Services,
    feature: &RemoteFeature,
    client: &E2bClient,
    row: &ExposedMachine,
) -> std::result::Result<(), Resumed> {
    let Some(cloud) = &row.cloud else {
        return Ok(());
    };
    let paused = cloud.state != CloudSandboxState::Running;
    if paused {
        set_state(svc, feature, &row.id, CloudSandboxState::Resuming).await;
    }
    match client
        .connect(&cloud.sandbox_id, SANDBOX_TIMEOUT_SECS)
        .await
    {
        Ok(connected) => {
            if paused || connected.resumed {
                tracing::info!(machine = %row.id, sandbox = %cloud.sandbox_id, "cloud machine: sandbox resumed");
                set_state(svc, feature, &row.id, CloudSandboxState::Running).await;
            }
            feature.cloud.unpark(&row.id);
            feature.cloud.touch(&row.id);
            Ok(())
        }
        Err(E2bError::NotFound(_)) => Err(Resumed::Lost),
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: E2B refused the resume");
            if paused {
                set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
            }
            Err(Resumed::Refused(config::to_error(
                &error,
                config::Phase::Other,
            )))
        }
    }
}

/// Move cloud machine `machine_id` to `state`, keeping its running time and
/// pause time, then announce the row. Returns the updated row.
pub(crate) async fn set_state(
    svc: &Services,
    feature: &RemoteFeature,
    machine_id: &str,
    state: CloudSandboxState,
) -> Option<ExposedMachine> {
    let mut row = svc
        .store()
        .get_exposed_machine(machine_id)
        .await
        .ok()
        .flatten()?;
    let cloud = row.cloud.as_mut()?;
    if cloud.state == state {
        return Some(row);
    }
    apply_state(cloud, state);
    if let Err(error) = svc.store().update_cloud_machine(machine_id, cloud).await {
        tracing::warn!(machine = %machine_id, %error, "cloud machine: could not record its state");
        return None;
    }
    let live = feature.running_node(machine_id);
    crate::methods::publish_machine(svc, &row, live.as_ref().map(crate::RemoteNode::shared_ref))
        .await;
    Some(row)
}

fn apply_state(cloud: &mut CloudMachine, state: CloudSandboxState) {
    let now = time::OffsetDateTime::now_utc();
    match state {
        CloudSandboxState::Running => {
            cloud.running_since = Some(now_iso());
            cloud.paused_at = None;
        }
        CloudSandboxState::Paused => {
            cloud.running_seconds = cloud.running_seconds_at(now);
            cloud.running_since = None;
            if cloud.paused_at.is_none() {
                cloud.paused_at = Some(now_iso());
            }
        }
        CloudSandboxState::Pausing | CloudSandboxState::Stopping => {
            cloud.running_seconds = cloud.running_seconds_at(now);
            cloud.running_since = None;
        }
        CloudSandboxState::Resuming => {}
    }
    cloud.state = state;
}

/// E2B no longer has `row`'s sandbox: end the agents on it with
/// [`SANDBOX_LOST_MESSAGE`] (their turns fail at once), then remove the
/// machine in the background. Its agents become agents without a sandbox,
/// with `placementError` `sandbox-lost`; the next message starts a new one.
pub(crate) fn sandbox_lost(
    svc: &Services,
    feature: &RemoteFeature,
    shared: Option<&Shared>,
    row: &ExposedMachine,
) {
    if !feature.cloud.mark_lost(&row.id) {
        return;
    }
    tracing::warn!(machine = %row.id, sandbox = %row.cloud.as_ref().map(|c| c.sandbox_id.as_str()).unwrap_or_default(), "cloud machine: E2B removed the sandbox");
    let turn_error = format!("cloud-machine-unavailable (sandbox-lost): {SANDBOX_LOST_MESSAGE}");
    let node = feature.running_node(&row.id);
    let shared = shared.or_else(|| node.as_ref().map(crate::RemoteNode::shared_ref));
    if let Some(shared) = shared {
        // The sandbox took its agents with it: their stops are moot.
        shared.drop_stops();
        shared.agents.end_all(&turn_error);
    }
    let svc = svc.clone();
    let row = row.clone();
    tokio::spawn(async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        let lost = super::StartFailure::of(&sandbox_lost_error());
        let error = PlacementError::cloud(&lost.message, &lost.data);
        super::start::remove_machine(feature, &svc, &row, None, Some(&error)).await;
    });
}

/// Pause cloud machine `machine_id` now. An already paused sandbox is
/// returned unchanged.
///
/// # Errors
///
/// `machine-in-use` while an agent on it has a turn in flight; the E2B
/// refusal (`cloud-quota`, `cloud-machine-unavailable`); `sandbox-lost`.
pub(crate) async fn pause(
    svc: &Services,
    feature: &RemoteFeature,
    client: &E2bClient,
    machine_id: &str,
) -> Result<ExposedMachine> {
    let _lifecycle = feature.lifecycle(machine_id).await;
    let _transition = feature.cloud.transition(machine_id).await;
    let row = svc
        .store()
        .get_exposed_machine(machine_id)
        .await?
        .filter(ExposedMachine::is_cloud)
        .ok_or_else(|| super::methods::not_found(machine_id))?;
    let Some(cloud) = row.cloud.clone() else {
        return Ok(row);
    };
    match cloud.state {
        CloudSandboxState::Running | CloudSandboxState::Pausing => {}
        CloudSandboxState::Paused | CloudSandboxState::Resuming | CloudSandboxState::Stopping => {
            return Ok(row)
        }
    }
    let node = feature.running_node(machine_id);
    if let Some(node) = &node {
        let turns = node.shared_ref().agents.turns_in_flight();
        if turns > 0 {
            return Err(crate::methods::in_use(&row.name, turns));
        }
    }
    // No dial from now until a demand; the link drops with the pause anyway,
    // and a dial in flight is dropped when it binds.
    feature.cloud.park(machine_id);
    feature.cloud.count_pause(machine_id);
    if let Some(node) = &node {
        node.shared_ref().close_link();
    }
    set_state(svc, feature, machine_id, CloudSandboxState::Pausing).await;
    let paused = match client.pause(&cloud.sandbox_id).await {
        Ok(()) => Ok(()),
        // E2B may have paused it already (its timeout ran out).
        Err(E2bError::Conflict(_))
            if lookup(svc, client, &cloud).await == Ok(Some(SandboxState::Paused)) =>
        {
            Ok(())
        }
        Err(E2bError::NotFound(_)) => {
            sandbox_lost(svc, feature, None, &row);
            return Err(sandbox_lost_error());
        }
        Err(error) => Err(error),
    };
    match paused {
        Ok(()) => {
            tracing::info!(machine = %machine_id, sandbox = %cloud.sandbox_id, "cloud machine: sandbox paused");
            feature.cloud.forget_activity(machine_id);
            Ok(
                set_state(svc, feature, machine_id, CloudSandboxState::Paused)
                    .await
                    .unwrap_or(row),
            )
        }
        Err(error) => {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: E2B refused the pause");
            feature.cloud.unpark(machine_id);
            feature.cloud.touch(machine_id);
            set_state(svc, feature, machine_id, CloudSandboxState::Running).await;
            if let Some(node) = &node {
                if node.shared_ref().link_wanted() {
                    node.shared_ref().wake();
                }
            }
            Err(config::to_error(&error, config::Phase::Other))
        }
    }
}

/// The state E2B lists `cloud`'s sandbox in: `Ok(None)` when E2B lists it
/// in no state, `Err(())` when that cannot be known (the list failed).
async fn lookup(
    svc: &Services,
    client: &E2bClient,
    cloud: &CloudMachine,
) -> std::result::Result<Option<SandboxState>, ()> {
    let backend = svc.store().daemon_id().await.map_err(drop)?;
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend);
    metadata.insert("agentId".to_owned(), cloud.agent_id.clone());
    client
        .list(&ListSandboxes {
            metadata,
            states: vec![SandboxState::Running, SandboxState::Paused],
        })
        .await
        .map(|listed| {
            listed
                .into_iter()
                .find(|listed| listed.sandbox_id == cloud.sandbox_id)
                .map(|listed| listed.state)
        })
        .map_err(|error| {
            tracing::warn!(sandbox = %cloud.sandbox_id, %error, "cloud machine: could not list the sandbox's state");
        })
}

/// Resume a paused sandbox before its final stops. Returns whether it runs;
/// when not, the removal deletes it without a final stop.
pub(crate) async fn resume_for_stop(
    feature: &RemoteFeature,
    client: &E2bClient,
    row: &ExposedMachine,
) -> bool {
    let _transition = feature.cloud.transition(&row.id).await;
    let Some(cloud) = &row.cloud else {
        return true;
    };
    match client
        .connect(&cloud.sandbox_id, SANDBOX_TIMEOUT_SECS)
        .await
    {
        Ok(_) => {
            feature.cloud.unpark(&row.id);
            true
        }
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: could not resume the sandbox for its final stops; deleting it without them");
            false
        }
    }
}

/// Start the supervisor once per backend (not in node mode).
pub(crate) fn spawn_supervisor(svc: &Services) {
    let Ok(feature) = super::feature(svc) else {
        return;
    };
    if svc.node_mode() || feature.cloud.supervisor_started() {
        return;
    }
    let svc = svc.clone();
    intent_core::spawn_daemon(async move {
        let mut last_reap: Option<Instant> = None;
        loop {
            let clock = Clock::current();
            tokio::time::sleep(clock.tick()).await;
            let Ok(feature) = super::feature(&svc) else {
                return;
            };
            if let Err(error) = tick(&svc, feature, clock, &mut last_reap).await {
                tracing::debug!(%error, "cloud machines: lifecycle check failed");
            }
        }
    });
}

/// One check of every cloud machine, and the reaper when due.
async fn tick(
    svc: &Services,
    feature: &RemoteFeature,
    clock: Clock,
    last_reap: &mut Option<Instant>,
) -> Result<()> {
    let Some(saved) = config::load(svc).await? else {
        return Ok(());
    };
    let client = Arc::new(config::saved_client(svc).await?);
    let rows = svc.store().list_cloud_machines().await?;
    for row in &rows {
        check(svc, feature, &client, &saved, clock, row).await;
    }
    if last_reap.is_none_or(|at| at.elapsed() >= clock.minutes(REAP_MINUTES)) {
        *last_reap = Some(Instant::now());
        reap(svc, feature, &client, clock, &rows).await;
    }
    Ok(())
}

/// Decide for one machine: renew, pause when idle, pause and resume before
/// the run cap, delete when paused too long, finish a transition a backend
/// restart cut.
async fn check(
    svc: &Services,
    feature: &RemoteFeature,
    client: &Arc<E2bClient>,
    saved: &config::ProviderConfig,
    clock: Clock,
    row: &ExposedMachine,
) {
    let Some(cloud) = &row.cloud else { return };
    if feature.cloud.is_lost(&row.id) || feature.cloud.is_killing(&row.id) {
        return;
    }
    let node = feature.running_node(&row.id);
    let settled = !feature.cloud.in_transition(&row.id);
    match cloud.state {
        CloudSandboxState::Running => {
            let busy = node.as_ref().is_some_and(|node| node.shared_ref().busy());
            let activity = feature.cloud.activity(&row.id, busy);
            if activity
                .last_renew
                .is_none_or(|at| at.elapsed() >= clock.minutes(RENEW_MINUTES))
            {
                renew(svc, feature, client, row, cloud).await;
            }
            if busy || !settled {
                return;
            }
            let idle = activity.last_busy.elapsed() >= clock.minutes(saved.idle_pause_minutes);
            let capped = since(cloud.running_since.as_deref()) >= clock.minutes(RUN_CAP_MINUTES);
            if idle || capped {
                spawn_pause(svc, client, &row.id, !idle);
            }
        }
        CloudSandboxState::Paused => {
            if since(cloud.paused_at.as_deref()) >= clock.days(saved.paused_delete_days) {
                tracing::info!(machine = %row.id, "cloud machine: paused too long; deleting it");
                spawn_kill(svc, feature, client, row);
            }
        }
        // A transition a backend restart cut: finish it.
        CloudSandboxState::Pausing if settled => spawn_pause(svc, client, &row.id, false),
        CloudSandboxState::Resuming if settled => {
            feature.cloud.park(&row.id);
            set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
        }
        CloudSandboxState::Stopping if settled => spawn_kill(svc, feature, client, row),
        CloudSandboxState::Pausing | CloudSandboxState::Resuming | CloudSandboxState::Stopping => {}
    }
}

/// Keep the sandbox's timeout ahead.
async fn renew(
    svc: &Services,
    feature: &RemoteFeature,
    client: &E2bClient,
    row: &ExposedMachine,
    cloud: &CloudMachine,
) {
    match client
        .set_timeout(&cloud.sandbox_id, SANDBOX_TIMEOUT_SECS)
        .await
    {
        Ok(()) => feature.cloud.renewed(&row.id),
        Err(E2bError::NotFound(_)) => match lookup(svc, client, cloud).await {
            // E2B paused it itself (its timeout ran out while this backend
            // was away): a demand resumes it.
            Ok(Some(SandboxState::Paused)) => {
                feature.cloud.park(&row.id);
                set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
            }
            Ok(None) => sandbox_lost(svc, feature, None, row),
            // Still listed, or unknown (the list failed): the next renewal
            // asks again.
            Ok(Some(_)) | Err(()) => {}
        },
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: could not renew the sandbox's timeout");
        }
    }
}

/// Pause in the background; `planned` (the run cap) resumes at once when
/// the link is still wanted, so the cap resets without a visible pause.
fn spawn_pause(svc: &Services, client: &Arc<E2bClient>, machine_id: &str, planned: bool) {
    let svc = svc.clone();
    let client = client.clone();
    let machine_id = machine_id.to_owned();
    tokio::spawn(async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        if let Err(error) = pause(&svc, feature, &client, &machine_id).await {
            tracing::debug!(machine = %machine_id, %error, "cloud machine: not paused");
            return;
        }
        if planned {
            if let Some(node) = feature.running_node(&machine_id) {
                if node.shared_ref().link_wanted() {
                    tracing::info!(machine = %machine_id, "cloud machine: planned pause before the run cap; resuming");
                    if let Err(error) = node.connect().await {
                        tracing::warn!(machine = %machine_id, %error, "cloud machine: could not resume after the planned pause");
                    }
                }
            }
        }
    });
}

/// Delete a machine in the background (resume, final stops, delete).
fn spawn_kill(
    svc: &Services,
    feature: &RemoteFeature,
    client: &Arc<E2bClient>,
    row: &ExposedMachine,
) {
    if !feature.cloud.begin_kill(&row.id) {
        return;
    }
    let svc = svc.clone();
    let client = client.clone();
    let row = row.clone();
    tokio::spawn(async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        super::start::delete_machine(feature, &svc, &row, Some(&client)).await;
        feature.cloud.end_kill(&row.id);
    });
}

/// Delete every sandbox with this backend's metadata that no machine row
/// knows and no start in flight owns, once it is past the reap grace.
async fn reap(
    svc: &Services,
    feature: &RemoteFeature,
    client: &E2bClient,
    clock: Clock,
    rows: &[ExposedMachine],
) {
    let Ok(backend) = svc.store().daemon_id().await else {
        return;
    };
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend);
    let listed = match client
        .list(&ListSandboxes {
            metadata,
            states: vec![SandboxState::Running, SandboxState::Paused],
        })
        .await
    {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(%error, "cloud machines: could not list sandboxes for the reaper");
            return;
        }
    };
    let known: HashSet<&str> = rows
        .iter()
        .filter_map(|row| row.cloud.as_ref().map(|c| c.sandbox_id.as_str()))
        .collect();
    let starting = feature.cloud.starting_agents();
    feature
        .cloud
        .keep_orphans(listed.iter().map(|sandbox| sandbox.sandbox_id.as_str()));
    for sandbox in listed {
        if known.contains(sandbox.sandbox_id.as_str())
            || sandbox
                .metadata
                .get("agentId")
                .is_some_and(|agent| starting.contains(agent))
        {
            continue;
        }
        // An unreadable start time counts from when the reaper first saw
        // the sandbox, so it still gets the whole grace.
        let age = parse_rfc3339(&sandbox.started_at).map_or_else(
            || {
                feature
                    .cloud
                    .first_seen_orphan(&sandbox.sandbox_id)
                    .elapsed()
            },
            |at| {
                (time::OffsetDateTime::now_utc() - at)
                    .try_into()
                    .unwrap_or(Duration::ZERO)
            },
        );
        if age < clock.minutes(REAP_GRACE_MINUTES) {
            continue;
        }
        match client.delete(&sandbox.sandbox_id).await {
            Ok(_) => {
                feature.cloud.forget_orphan(&sandbox.sandbox_id);
                tracing::info!(sandbox = %sandbox.sandbox_id, "cloud machines: deleted a sandbox no machine knows");
            }
            Err(error) => {
                tracing::warn!(sandbox = %sandbox.sandbox_id, %error, "cloud machines: could not delete an orphan sandbox");
            }
        }
    }
}

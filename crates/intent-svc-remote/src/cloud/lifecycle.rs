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
use intent_nodes::cloud::{CloudProvider, InstanceState, ListInstances};
use intent_services::feature_api::Services;

use super::config::{self, Provider};
use super::start::Removal;
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
pub(crate) const SANDBOX_LOST_MESSAGE: &str = "The cloud provider removed this agent's sandbox, so its turn was interrupted. Work it pushed is kept; work it did not push is lost. The next message starts a new sandbox.";

/// The turn error of an agent whose sandbox could not link to this backend
/// (`machine-update-required`) and was deleted.
pub(crate) const SANDBOX_OUTDATED_MESSAGE: &str = "This agent's cloud sandbox runs an Intent version this backend can no longer link to, so the sandbox was deleted. Work it pushed is kept; work it did not push is lost. The next message starts a new sandbox.";

/// Why a machine's sandbox is gone or unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Loss {
    /// E2B no longer has it.
    Removed,
    /// It cannot link to this backend's version.
    Outdated,
}

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
    /// pause finished does not resume the sandbox. Returns the machine's
    /// pause count as seen under its transition lock, for the connector to
    /// tell whether a pause began after this check.
    pub(crate) async fn before_dial(
        &self,
        shared: &Shared,
        asked: bool,
    ) -> std::result::Result<u64, ConnectFailure> {
        let Ok(feature) = super::feature(&self.svc) else {
            return Ok(0);
        };
        match feature.cloud.loss(&self.machine_id) {
            Some(Loss::Removed) => return Err(lost_failure()),
            Some(Loss::Outdated) => return Err(ConnectFailure::Local(outdated_turn_error())),
            None => {}
        }
        let _transition = feature.cloud.transition(&self.machine_id).await;
        let pauses = feature.cloud.pauses(&self.machine_id);
        if !asked && feature.cloud.is_parked(&self.machine_id) {
            return Err(ConnectFailure::Local(
                "the sandbox was paused; it resumes when something asks for it".into(),
            ));
        }
        let row = match self.svc.store().get_exposed_machine(&self.machine_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(pauses),
            Err(error) => return Err(ConnectFailure::Local(error.to_string())),
        };
        if row
            .cloud
            .as_ref()
            .is_none_or(|cloud| cloud.state == CloudSandboxState::Stopping)
        {
            return Ok(pauses);
        }
        // Without the key (an entry remove in progress), dial as is.
        let Ok(client) = config::saved_client(&self.svc, super::methods::row_entry(&row)).await
        else {
            return Ok(pauses);
        };
        match resume(&self.svc, feature, &*client, &row).await {
            Ok(()) => Ok(pauses),
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

    /// The sandbox answered the dial with `machine-update-required`: it
    /// runs an intentd this backend shares no link protocol with, so it
    /// can never link again. See [`sandbox_outdated`].
    pub(crate) fn outdated(&self, shared: &Shared) {
        if let Ok(feature) = super::feature(&self.svc) {
            sandbox_outdated(&self.svc, feature, shared, &self.machine_id);
        }
    }
}

pub(crate) fn outdated_turn_error() -> String {
    format!(
        "{}: {SANDBOX_OUTDATED_MESSAGE}",
        intent_core::machines::MACHINE_UPDATE_REQUIRED_CODE
    )
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
    client: &dyn CloudProvider,
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
            // A machine that keeps only its disk runs no intentd after a
            // resume or a container restart: start it, and tell each agent
            // that ran there what it lost at its next launch.
            if !client.keeps_memory() {
                match super::bootstrap::start_daemon(client, &connected.instance).await {
                    Ok(true) => {
                        tracing::info!(machine = %row.id, "cloud machine: intentd started again from the machine's disk");
                        disk_resumed(svc, feature, &row.id).await;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        if paused {
                            set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
                        }
                        return Err(Resumed::Refused(error));
                    }
                }
            }
            if paused || connected.resumed {
                tracing::info!(machine = %row.id, sandbox = %cloud.sandbox_id, "cloud machine: sandbox resumed");
                set_state(svc, feature, &row.id, CloudSandboxState::Running).await;
            }
            feature.cloud.unpark(&row.id);
            feature.cloud.touch(&row.id);
            Ok(())
        }
        Err(error) if error.is_not_found() => Err(Resumed::Lost),
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: the provider refused the resume");
            if paused {
                set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
            }
            Err(Resumed::Refused(config::to_error(
                &error,
                config::Phase::Other,
                client.kind(),
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
    if !feature.cloud.mark_lost(&row.id, Loss::Removed) {
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
        super::start::remove_machine(feature, &svc, &row, None, Removal::Lost(&error)).await;
    });
}

/// Machine `machine_id`'s sandbox cannot link to this backend's version
/// (for example a paused sandbox after the backend was updated past its
/// link protocols): end its agents' turns with [`SANDBOX_OUTDATED_MESSAGE`],
/// then delete the sandbox without a final stop (none can reach it) and
/// remove the machine in the background. Its agents become agents without
/// a sandbox; the next message starts a new one from their pushed work.
fn sandbox_outdated(svc: &Services, feature: &RemoteFeature, shared: &Shared, machine_id: &str) {
    if !feature.cloud.mark_lost(machine_id, Loss::Outdated) {
        return;
    }
    tracing::warn!(machine = %machine_id, "cloud machine: the sandbox cannot link to this backend's version; deleting it");
    // A prompt waiting for the link fails with this refusal. Ending the
    // agents here would close their transport first, and a prompt closed
    // before any output is silently redriven instead of told.
    shared.refuse_turns(&outdated_turn_error());
    shared.drop_stops();
    let svc = svc.clone();
    let machine_id = machine_id.to_owned();
    tokio::spawn(async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        let row = match svc.store().get_exposed_machine(&machine_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                feature.cloud.forget(&machine_id);
                return;
            }
            Err(error) => {
                tracing::warn!(machine = %machine_id, %error, "cloud machine: could not read it to delete its outdated sandbox");
                feature.cloud.forget(&machine_id);
                return;
            }
        };
        // Without the key the sandbox stays; the reaper deletes it once a
        // working key is saved.
        let client = config::saved_client(&svc, super::methods::row_entry(&row))
            .await
            .ok();
        super::start::remove_machine(feature, &svc, &row, client.as_deref(), Removal::Unlinkable)
            .await;
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
    client: &dyn CloudProvider,
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
        Err(error)
            if error.is_conflict()
                && lookup(svc, client, &cloud).await == Ok(Some(InstanceState::Paused)) =>
        {
            Ok(())
        }
        Err(error) if error.is_not_found() => {
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
            Err(config::to_error(
                &error,
                config::Phase::Other,
                client.kind(),
            ))
        }
    }
}

/// Machine `machine_id` started again from its disk: each agent recorded
/// on it gets [`resumedFromDisk`](intent_core::node_link::LaunchCommand::resumed_from_disk)
/// at its next launch there.
async fn disk_resumed(svc: &Services, feature: &RemoteFeature, machine_id: &str) {
    match super::start::placed_agents(svc.store(), machine_id).await {
        Ok(agents) => {
            for agent in &agents {
                feature.cloud.mark_disk_resumed(agent.agent_id.as_str());
            }
            // The new daemon runs none of them: their runtimes here end, so
            // a waiting prompt is redriven on a relaunch instead of reaching
            // an agent the machine does not know.
            if let Some(node) = feature.running_node(machine_id) {
                let ids: Vec<_> = agents.into_iter().map(|agent| agent.agent_id).collect();
                node.shared_ref().processes_lost(
                    &ids,
                    "transport closed: the machine started again from its disk",
                );
            }
        }
        Err(error) => {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: could not read the agents to tell about the disk resume");
        }
    }
}

/// The state E2B lists `cloud`'s sandbox in: `Ok(None)` when E2B lists it
/// in no state, `Err(())` when that cannot be known (the list failed).
async fn lookup(
    svc: &Services,
    client: &dyn CloudProvider,
    cloud: &CloudMachine,
) -> std::result::Result<Option<InstanceState>, ()> {
    let backend = svc.store().daemon_id().await.map_err(drop)?;
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend);
    metadata.insert("agentId".to_owned(), cloud.agent_id.clone());
    client
        .list(&ListInstances {
            tags: metadata,
            states: vec![InstanceState::Running, InstanceState::Paused],
        })
        .await
        .map(|listed| {
            listed
                .into_iter()
                .find(|listed| listed.id == cloud.sandbox_id)
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
    client: &dyn CloudProvider,
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
        if let Err(error) = config::migrate(&svc).await {
            tracing::warn!(%error, "cloud machines: could not migrate the saved E2B provider");
        }
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

/// One check of every cloud machine, and the reaper when due, per E2B
/// entry with that entry's key. A machine whose entry is gone is left to
/// the entry's removal.
async fn tick(
    svc: &Services,
    feature: &RemoteFeature,
    clock: Clock,
    last_reap: &mut Option<Instant>,
) -> Result<()> {
    let entries = config::load_all(svc).await?;
    let retired = config::load_retired(svc).await?;
    if entries.is_empty() && retired.is_empty() {
        return Ok(());
    }
    let rows = svc.store().list_cloud_machines().await?;
    let reap_due = last_reap.is_none_or(|at| at.elapsed() >= clock.minutes(REAP_MINUTES));
    // Every sandbox the reaper listed this tick, in every project; `None`
    // once a listing failed, so no first sighting is forgotten on a guess.
    let mut listed: Option<Vec<String>> = Some(Vec::new());
    let mut note = |more: Option<Vec<String>>| match (&mut listed, more) {
        (Some(all), Some(more)) => all.extend(more),
        _ => listed = None,
    };
    if reap_due {
        *last_reap = Some(Instant::now());
        for (row_id, from, saved) in &retired {
            note(reap_retired(svc, feature, clock, &rows, (row_id, from), saved).await);
        }
    }
    for (id, saved) in &entries {
        let client = match config::saved_client(svc, id).await {
            Ok(client) => client,
            Err(error) => {
                tracing::debug!(entry = %id, %error, "cloud machines: no client for an E2B entry");
                note(None);
                continue;
            }
        };
        for row in rows
            .iter()
            .filter(|row| super::methods::row_entry(row) == id)
        {
            check(svc, feature, &client, saved, clock, row).await;
        }
        // Every row counts as known: entries may share an E2B project.
        if reap_due {
            note(reap(svc, feature, &*client, clock, &rows).await);
        }
    }
    if let (true, Some(listed)) = (reap_due, listed) {
        feature
            .cloud
            .keep_orphans(listed.iter().map(String::as_str));
    }
    Ok(())
}

/// Decide for one machine: renew, pause when idle, pause and resume before
/// the run cap, delete when paused too long, finish a transition a backend
/// restart cut.
async fn check(
    svc: &Services,
    feature: &RemoteFeature,
    client: &Provider,
    saved: &config::ProviderConfig,
    clock: Clock,
    row: &ExposedMachine,
) {
    let Some(cloud) = &row.cloud else { return };
    if feature.cloud.loss(&row.id).is_some() || feature.cloud.is_killing(&row.id) {
        return;
    }
    let node = feature.running_node(&row.id);
    let settled = !feature.cloud.in_transition(&row.id);
    match cloud.state {
        CloudSandboxState::Running => {
            let busy = node.as_ref().is_some_and(|node| node.shared_ref().busy());
            let activity = feature.cloud.activity(&row.id, busy);
            // A provider without a lease has nothing to renew and no
            // continuous-run cap: only the idle pause below stops it.
            let lease = client.has_lease();
            if lease
                && activity
                    .last_renew
                    .is_none_or(|at| at.elapsed() >= clock.minutes(RENEW_MINUTES))
            {
                renew(svc, feature, &**client, row, cloud).await;
            }
            if busy || !settled {
                return;
            }
            let idle = activity.last_busy.elapsed() >= clock.minutes(saved.idle_pause_minutes);
            let capped =
                lease && since(cloud.running_since.as_deref()) >= clock.minutes(RUN_CAP_MINUTES);
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
    client: &dyn CloudProvider,
    row: &ExposedMachine,
    cloud: &CloudMachine,
) {
    match client
        .keep_alive(&cloud.sandbox_id, SANDBOX_TIMEOUT_SECS)
        .await
    {
        Ok(()) => feature.cloud.renewed(&row.id),
        Err(error) if error.is_not_found() => match lookup(svc, client, cloud).await {
            // E2B paused it itself (its timeout ran out while this backend
            // was away): a demand resumes it.
            Ok(Some(InstanceState::Paused)) => {
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
fn spawn_pause(svc: &Services, client: &Provider, machine_id: &str, planned: bool) {
    let svc = svc.clone();
    let client = client.clone();
    let machine_id = machine_id.to_owned();
    tokio::spawn(async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        if let Err(error) = pause(&svc, feature, &*client, &machine_id).await {
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
fn spawn_kill(svc: &Services, feature: &RemoteFeature, client: &Provider, row: &ExposedMachine) {
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
        super::start::delete_machine(feature, &svc, &row, Some(&*client)).await;
        feature.cloud.end_kill(&row.id);
    });
}

/// The reaper's pass with the key a removed entry `from` left behind
/// (`row_id`): delete its orphans, then forget the key once E2B has no
/// sandbox of the entry left.
async fn reap_retired(
    svc: &Services,
    feature: &RemoteFeature,
    clock: Clock,
    rows: &[ExposedMachine],
    (row_id, from): (&str, &str),
    saved: &config::ProviderConfig,
) -> Option<Vec<String>> {
    let key = match config::saved_key(svc, row_id).await {
        Ok(Some(key)) => key,
        // The key is really gone: nothing can delete the sandboxes now.
        Ok(None) => {
            let _ = svc.store().delete_cloud_machine_provider(row_id).await;
            return Some(Vec::new());
        }
        // Unreadable for now: keep the row for the next pass.
        Err(error) => {
            tracing::warn!(%error, "cloud machines: could not read a removed entry's key");
            return None;
        }
    };
    let Ok(client) = config::client(saved, key) else {
        return None;
    };
    let listed = reap(svc, feature, &*client, clock, rows).await;
    if entry_sandboxes_left(svc, &*client, from).await {
        return listed;
    }
    if let Err(error) = svc.delete_cloud_machine_key(row_id).await {
        tracing::warn!(%error, "cloud machines: could not forget a removed entry's key");
        return listed;
    }
    if let Err(error) = svc.store().delete_cloud_machine_provider(row_id).await {
        tracing::warn!(%error, "cloud machines: could not forget a removed entry");
        return listed;
    }
    tracing::info!(entry = %from, "cloud machines: no sandbox of the removed entry is left; its key is gone");
    listed
}

/// Whether E2B still lists a sandbox of this backend started with entry
/// `id` (one from before entries counts for `"e2b"`). `true` when E2B cannot
/// answer, so a key is never dropped too soon.
pub(crate) async fn entry_sandboxes_left(
    svc: &Services,
    client: &dyn CloudProvider,
    id: &str,
) -> bool {
    let Ok(backend) = svc.store().daemon_id().await else {
        return true;
    };
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend);
    match client
        .list(&ListInstances {
            tags: metadata,
            states: vec![InstanceState::Running, InstanceState::Paused],
        })
        .await
    {
        // A sandbox without an entry is one from before entries (E2B), or a
        // leftover that names none (a Northflank volume): it keeps the key
        // of the legacy entry, or of every entry of a provider other than E2B.
        Ok(listed) => listed.iter().any(|sandbox| {
            sandbox.tags.get("intentEntryId").map_or(
                id == config::legacy_entry()
                    || client.kind() != intent_core::machines::CLOUD_PROVIDER_E2B,
                |entry| entry == id,
            )
        }),
        Err(error) => {
            tracing::warn!(%error, "cloud machines: could not list an entry's sandboxes");
            true
        }
    }
}

/// Delete every sandbox with this backend's metadata that no machine row
/// knows and no start in flight owns, once it is past the reap grace.
async fn reap(
    svc: &Services,
    feature: &RemoteFeature,
    client: &dyn CloudProvider,
    clock: Clock,
    rows: &[ExposedMachine],
) -> Option<Vec<String>> {
    let Ok(backend) = svc.store().daemon_id().await else {
        return None;
    };
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend);
    let listed = match client
        .list(&ListInstances {
            tags: metadata,
            states: vec![InstanceState::Running, InstanceState::Paused],
        })
        .await
    {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(%error, "cloud machines: could not list sandboxes for the reaper");
            return None;
        }
    };
    let known: HashSet<&str> = rows
        .iter()
        .filter_map(|row| row.cloud.as_ref().map(|c| c.sandbox_id.as_str()))
        .collect();
    let starting = feature.cloud.starting_agents();
    let ids = listed.iter().map(|sandbox| sandbox.id.clone()).collect();
    for sandbox in listed {
        if known.contains(sandbox.id.as_str())
            || sandbox
                .tags
                .get("agentId")
                .is_some_and(|agent| starting.contains(agent))
        {
            continue;
        }
        // An unreadable start time counts from when the reaper first saw
        // the sandbox, so it still gets the whole grace.
        let age = parse_rfc3339(&sandbox.started_at).map_or_else(
            || feature.cloud.first_seen_orphan(&sandbox.id).elapsed(),
            |at| {
                (time::OffsetDateTime::now_utc() - at)
                    .try_into()
                    .unwrap_or(Duration::ZERO)
            },
        );
        if age < clock.minutes(REAP_GRACE_MINUTES) {
            continue;
        }
        match client.delete(&sandbox.id).await {
            Ok(_) => {
                feature.cloud.forget_orphan(&sandbox.id);
                tracing::info!(sandbox = %sandbox.id, "cloud machines: deleted a sandbox no machine knows");
            }
            Err(error) => {
                tracing::warn!(sandbox = %sandbox.id, %error, "cloud machines: could not delete an orphan sandbox");
            }
        }
    }
    Some(ids)
}

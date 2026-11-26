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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use intent_core::machines::{
    cloud_machine_unavailable, parse_rfc3339, CloudMachine, CloudSandboxState, ExposedMachine,
};
use intent_core::remote_launch::PlacementError;
use intent_core::{now_iso, Error, Result};
use intent_nodes::cloud::{CloudErrorKind, CloudProvider, InstanceState, ListInstances};
use intent_services::feature_api::Services;

use super::config::{self, Provider};
use super::start::Removal;
use crate::connect::ConnectFailure;
use crate::link::gate::GateLink;
use crate::RemoteFeature;

/// The E2B timeout a live sandbox is kept ahead by. E2B pauses it (never
/// kills it: `autoPause`) when this runs out.
pub(crate) const SANDBOX_TIMEOUT_SECS: u32 = 30 * 60;

/// Renew the timeout this often while the sandbox runs.
const RENEW_MINUTES: u32 = 10;
/// Pause and resume a sandbox at a turn boundary once it ran this long
/// without a pause: before E2B's shortest continuous-run cap (1 h, Hobby).
const RUN_CAP_MINUTES: u32 = 50;
/// Idle pauses of a sandbox holding a home skipped in a row because a
/// push before them failed; the next one pauses anyway, and the failed
/// pushes count toward the home's `backupWarning`.
const MAX_SKIPPED_PAUSES: u32 = 2;
/// How often the reaper lists this backend's sandboxes.
const REAP_MINUTES: u32 = 10;
/// A sandbox no row knows is reaped only once it is this old, so a start in
/// flight (bounded by the start wait) keeps its new sandbox.
const REAP_GRACE_MINUTES: u32 = 15;

/// Test seams: the length of one minute and one day of the lifecycle
/// settings and timers, in milliseconds.
const MINUTE_MS_ENV: &str = "INTENTD_E2B_MINUTE_MS";
const DAY_MS_ENV: &str = "INTENTD_E2B_DAY_MS";
/// Test seam: the run cap in settings minutes, so a test can keep planned
/// pauses out of steps that do not test them.
const RUN_CAP_MINUTES_ENV: &str = "INTENTD_E2B_RUN_CAP_MINUTES";
/// Test seam: a wait between an idle pause's home pushes and the pause, in
/// milliseconds, so a test can try to use the sandbox in between.
const AFTER_PUSH_MS_ENV: &str = "INTENTD_E2B_AFTER_PUSH_MS";

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

/// The lifecycle's clock: the length of a settings minute and day, and
/// the run cap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clock {
    minute: Duration,
    day: Duration,
    run_cap_minutes: u32,
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
            run_cap_minutes: std::env::var(RUN_CAP_MINUTES_ENV)
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(RUN_CAP_MINUTES),
        }
    }

    fn minutes(self, n: u32) -> Duration {
        self.minute * n
    }

    fn days(self, n: u32) -> Duration {
        self.day * n
    }

    fn run_cap(self) -> Duration {
        self.minutes(self.run_cap_minutes)
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
    /// The provider refused to keep the session alive longer (its session
    /// cap): pause and resume at the next idle moment.
    capped: bool,
}

impl Activity {
    pub(super) fn fresh() -> Self {
        Self {
            last_busy: Instant::now(),
            last_renew: Some(Instant::now()),
            capped: false,
        }
    }

    pub(super) fn touch(&mut self) {
        self.last_busy = Instant::now();
    }

    /// The machine had activity at `at`; an earlier time changes nothing.
    pub(super) fn active_at(&mut self, at: Instant) {
        self.last_busy = self.last_busy.max(at);
    }

    pub(super) fn renewed(&mut self) {
        self.last_renew = Some(Instant::now());
    }

    pub(super) fn cap(&mut self) {
        self.capped = true;
    }

    pub(super) fn is_capped(&self) -> bool {
        self.capped
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
}

/// Why a dial-gate call was refused: the tenant's links are closed.
const LINKS_CLOSED: &str = "this backend's machine links are closed";

impl crate::link::gate::DialGate for CloudGate {
    /// How many pauses of this machine began, so a dial that binds after
    /// one knows it reached a frozen sandbox.
    fn pauses(&self) -> u64 {
        super::feature(&self.svc).map_or(0, |feature| feature.cloud.pauses(&self.machine_id))
    }

    /// Whether the sandbox is paused: no dial until something asks.
    fn parked(&self) -> bool {
        super::feature(&self.svc).is_ok_and(|feature| feature.cloud.is_parked(&self.machine_id))
    }

    /// Before a dial: resume the sandbox when paused (or check that E2B
    /// still has it). A sandbox E2B removed ends the machine's agents with
    /// [`SANDBOX_LOST_MESSAGE`] and removes the machine. `asked` when
    /// something asked for this dial; a redial that waited here while a
    /// pause finished does not resume the sandbox. Returns the machine's
    /// pause count as seen under its transition lock, for the connector to
    /// tell whether a pause began after this check, and whether the sandbox
    /// was resumed now.
    fn before_dial<'a>(
        &'a self,
        link: &'a dyn GateLink,
        asked: bool,
    ) -> intent_core::BoxFuture<'a, std::result::Result<Dialable, ConnectFailure>> {
        Box::pin(async move {
            // A fenced tenant's connector makes no provider call: the
            // resume is dropped once the links close, and the fence waits
            // for it.
            let Ok(feature) = super::feature(&self.svc) else {
                return self.before_dial_inner(link, asked).await;
            };
            feature
                .until_closed(self.before_dial_inner(link, asked))
                .await
                .unwrap_or_else(|| Err(ConnectFailure::Local(LINKS_CLOSED.to_owned())))
        })
    }

    /// The sandbox runs an intentd this backend shares no link protocol
    /// with, so it can never link again. See [`sandbox_outdated`].
    fn outdated(&self, link: &dyn GateLink) {
        if let Ok(feature) = super::feature(&self.svc) {
            sandbox_outdated(&self.svc, feature, link, &self.machine_id);
        }
    }

    /// Nothing answered the dials right after a resume: restart the
    /// sandbox's tunnel through the provider. See [`revive`].
    fn revive<'a>(&'a self, link: &'a dyn GateLink) -> intent_core::BoxFuture<'a, bool> {
        Box::pin(async move {
            let Ok(feature) = super::feature(&self.svc) else {
                return false;
            };
            // As for before_dial: no provider call once the links close.
            feature
                .until_closed(async {
                    let revived = revive(&self.svc, feature, &self.machine_id).await;
                    if revived {
                        self.refresh_address(link).await;
                    }
                    revived
                })
                .await
                .unwrap_or(false)
        })
    }
}

impl CloudGate {
    /// A relay change gives the sandbox a new tunnel address: dial that.
    async fn refresh_address(&self, link: &dyn GateLink) {
        if let Ok(Some(fresh)) = self.svc.store().get_exposed_machine(&self.machine_id).await {
            let mut target = link.target();
            if target.tc_address != fresh.tc_address {
                target.tc_address = fresh.tc_address;
                link.set_target(target);
            }
        }
    }

    async fn before_dial_inner(
        &self,
        link: &dyn GateLink,
        asked: bool,
    ) -> std::result::Result<Dialable, ConnectFailure> {
        let Ok(feature) = super::feature(&self.svc) else {
            return Ok(Dialable {
                pauses: 0,
                resumed: false,
            });
        };
        match feature.cloud.loss(&self.machine_id) {
            Some(Loss::Removed) => return Err(lost_failure()),
            Some(Loss::Outdated) => return Err(ConnectFailure::Local(outdated_turn_error())),
            None => {}
        }
        let _transition = feature.cloud.transition(&self.machine_id).await;
        let pauses = feature.cloud.pauses(&self.machine_id);
        let as_is = Dialable {
            pauses,
            resumed: false,
        };
        if !asked && feature.cloud.is_parked(&self.machine_id) {
            return Err(ConnectFailure::Local(
                "the sandbox was paused; it resumes when something asks for it".into(),
            ));
        }
        let relay = super::bootstrap::wanted_relay(&self.svc);
        if let Some(refused) = held_relay_refusal(&self.machine_id, relay.as_deref(), None) {
            return Err(refused);
        }
        let row = match self.svc.store().get_exposed_machine(&self.machine_id).await {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(as_is),
            Err(error) => return Err(ConnectFailure::Local(error.to_string())),
        };
        if row
            .cloud
            .as_ref()
            .is_none_or(|cloud| cloud.state == CloudSandboxState::Stopping)
        {
            return Ok(as_is);
        }
        // Without the key (an entry remove in progress), dial as is.
        let Ok(client) = config::saved_client(&self.svc, super::methods::row_entry(&row)).await
        else {
            return Ok(as_is);
        };
        match resume(&self.svc, feature, &*client, &row).await {
            Ok(resumed) => {
                forget_relay_refusal(&self.machine_id);
                self.refresh_address(link).await;
                Ok(Dialable { pauses, resumed })
            }
            Err(Resumed::Lost) => {
                sandbox_lost(&self.svc, feature, Some(link), &row);
                Err(lost_failure())
            }
            Err(Resumed::Refused(error)) => {
                let failure = match error {
                    Error::RemoteMachine(failure) => ConnectFailure::Cloud {
                        message: failure.message,
                        data: failure.data,
                        lost: false,
                    },
                    other => ConnectFailure::Local(other.to_string()),
                };
                if matches!(&failure, ConnectFailure::Cloud { data, .. } if data["reason"] == "relay-unreachable")
                {
                    held_relay_refusal(&self.machine_id, relay.as_deref(), Some(&failure));
                }
                Err(failure)
            }
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
pub(crate) enum Resumed {
    /// E2B no longer has the sandbox.
    Lost,
    /// E2B refused (the documented error).
    Refused(Error),
}

use crate::link::gate::Dialable;

/// Make E2B run `row`'s sandbox: resume it when paused, else check it and
/// extend its timeout. The caller holds the machine's transition lock.
/// Returns whether it resumed the sandbox or started its intentd again.
pub(crate) async fn resume(
    svc: &Services,
    feature: &RemoteFeature,
    client: &dyn CloudProvider,
    row: &ExposedMachine,
) -> std::result::Result<bool, Resumed> {
    let Some(cloud) = &row.cloud else {
        return Ok(false);
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
            let relay = super::bootstrap::wanted_relay(svc);
            if let Some(Err(error)) = relay
                .as_deref()
                .map(|url| super::bootstrap::check_relay_url(url, super::bootstrap::Moment::Resume))
            {
                if paused || connected.resumed {
                    pause_after_relay_failure(svc, feature, client, &row.id, &cloud.sandbox_id)
                        .await;
                }
                return Err(Resumed::Refused(
                    intent_core::machines::with_cloud_provider(error, client.kind()),
                ));
            }
            // A machine that keeps only its disk runs no intentd after a
            // resume or a container restart: start it, and tell each agent
            // that ran there what it lost at its next launch.
            let mut restarted = false;
            if !client.keeps_memory() {
                match super::bootstrap::start_daemon(client, &connected.instance, relay.as_deref())
                    .await
                {
                    Ok(true) => {
                        tracing::info!(machine = %row.id, "cloud machine: intentd started again from the machine's disk");
                        disk_resumed(svc, feature, &row.id).await;
                        restarted = true;
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
            // The backend's relay setting reaches the sandbox here, at each
            // resume or restart, when it changed, or after a failed attempt
            // (which leaves the tunnel off on a relay the row does not name).
            // The sandbox's tunnel is moved to it and its relay checked.
            let unsettled = relay_unsettled(&row.id, None);
            if (relay.is_some() || cloud.relay_url.is_some() || unsettled)
                && (relay != cloud.relay_url
                    || paused
                    || connected.resumed
                    || restarted
                    || unsettled)
            {
                match super::bootstrap::ensure_relay(
                    client,
                    &connected.instance,
                    relay.as_deref(),
                    true,
                    super::bootstrap::Moment::Resume,
                )
                .await
                {
                    Ok(address) => {
                        relay_unsettled(&row.id, Some(false));
                        record_relay(svc, feature, &row.id, relay, address).await;
                    }
                    Err(error) => {
                        relay_unsettled(&row.id, Some(true));
                        tracing::warn!(machine = %row.id, %error, "cloud machine: the sandbox could not use the backend's relay");
                        if paused || connected.resumed {
                            pause_after_relay_failure(
                                svc,
                                feature,
                                client,
                                &row.id,
                                &cloud.sandbox_id,
                            )
                            .await;
                        }
                        return Err(Resumed::Refused(
                            intent_core::machines::with_cloud_provider(error, client.kind()),
                        ));
                    }
                }
            }
            let resumed = paused || connected.resumed;
            if resumed {
                tracing::info!(machine = %row.id, sandbox = %cloud.sandbox_id, "cloud machine: sandbox resumed");
                set_state(svc, feature, &row.id, CloudSandboxState::Running).await;
            }
            feature.cloud.unpark(&row.id);
            if resumed || restarted {
                // A new session: the provider's session cap starts again.
                feature.cloud.touch_new_session(&row.id);
            } else {
                feature.cloud.touch(&row.id);
            }
            Ok(resumed || restarted)
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

/// The dials right after a resume of cloud machine `machine_id` got no
/// answer: restart its sandbox's tunnel through the provider's process API
/// (the relay step, which turns the tunnel off and on). A tailcat that
/// starts while its network still looks down never picks its home relay
/// and, with no peers, never looks again: it serves an address nothing can
/// reach (intent#9895). Takes the machine's transition lock; a sandbox that
/// is not running is left alone. Returns whether the tunnel was restarted.
pub(crate) async fn revive(svc: &Services, feature: &RemoteFeature, machine_id: &str) -> bool {
    let _transition = feature.cloud.transition(machine_id).await;
    if feature.cloud.is_parked(machine_id) {
        return false;
    }
    let Ok(Some(row)) = svc.store().get_exposed_machine(machine_id).await else {
        return false;
    };
    let Some(cloud) = row
        .cloud
        .as_ref()
        .filter(|cloud| cloud.state == CloudSandboxState::Running)
    else {
        return false;
    };
    let Ok(client) = config::saved_client(svc, super::methods::row_entry(&row)).await else {
        return false;
    };
    let connected = match client
        .connect(&cloud.sandbox_id, SANDBOX_TIMEOUT_SECS)
        .await
    {
        Ok(connected) => connected,
        Err(error) => {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: could not reach the sandbox to restart its tunnel");
            return false;
        }
    };
    let relay = super::bootstrap::wanted_relay(svc);
    // The relay step turns the tunnel off first: until it is back on, the
    // next resume must run the relay step again, even if this one is dropped.
    relay_unsettled(machine_id, Some(true));
    match super::bootstrap::ensure_relay(
        &*client,
        &connected.instance,
        relay.as_deref(),
        true,
        super::bootstrap::Moment::Resume,
    )
    .await
    {
        Ok(address) => {
            tracing::info!(machine = %machine_id, "cloud machine: no answer after the resume; the sandbox's tunnel was restarted");
            relay_unsettled(machine_id, Some(false));
            record_relay(svc, feature, machine_id, relay, address).await;
            true
        }
        Err(error) => {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: the sandbox's tunnel could not be restarted");
            false
        }
    }
}

/// The provider resumed the sandbox but it cannot use the backend's relay:
/// pause it again so it stops billing, and park it until something asks for
/// it. When the provider refuses the pause, the row says it runs.
async fn pause_after_relay_failure(
    svc: &Services,
    feature: &RemoteFeature,
    client: &dyn CloudProvider,
    machine_id: &str,
    sandbox_id: &str,
) {
    feature.cloud.park(machine_id);
    feature.cloud.count_pause(machine_id);
    match client.pause(sandbox_id).await {
        Ok(()) => {
            feature.cloud.forget_activity(machine_id);
            set_state(svc, feature, machine_id, CloudSandboxState::Paused).await;
        }
        Err(error) => {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: the provider refused the pause after a relay failure; it stays running");
            feature.cloud.unpark(machine_id);
            feature.cloud.touch(machine_id);
            set_state(svc, feature, machine_id, CloudSandboxState::Running).await;
        }
    }
}

/// How long a relay refusal answers new dials of its machine without
/// another resume, while the relay setting is unchanged: one message asks
/// for the link again after a refusal (a redriven turn relaunches its
/// agent), and each ask resumed and paused the sandbox again (intent#9825).
const RELAY_REFUSAL_HOLD: Duration = Duration::from_secs(60);

type RelayRefusals = HashMap<String, (Option<String>, Instant, ConnectFailure)>;

fn relay_refusals() -> std::sync::MutexGuard<'static, RelayRefusals> {
    static REFUSALS: std::sync::LazyLock<std::sync::Mutex<RelayRefusals>> =
        std::sync::LazyLock::new(Default::default);
    REFUSALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The relay refusal `machine_id` had for `relay` within
/// [`RELAY_REFUSAL_HOLD`]; `hold` records a new one first.
fn held_relay_refusal(
    machine_id: &str,
    relay: Option<&str>,
    hold: Option<&ConnectFailure>,
) -> Option<ConnectFailure> {
    let mut refusals = relay_refusals();
    if let Some(failure) = hold {
        refusals.insert(
            machine_id.to_owned(),
            (relay.map(str::to_owned), Instant::now(), failure.clone()),
        );
    }
    let (refused_for, at, failure) = refusals.get(machine_id)?;
    (refused_for.as_deref() == relay && at.elapsed() < RELAY_REFUSAL_HOLD).then(|| failure.clone())
}

fn forget_relay_refusal(machine_id: &str) {
    relay_refusals().remove(machine_id);
}

/// Whether a relay step failed for `machine_id` since its last success;
/// `set` records a new outcome first.
fn relay_unsettled(machine_id: &str, set: Option<bool>) -> bool {
    static UNSETTLED: std::sync::LazyLock<std::sync::Mutex<HashSet<String>>> =
        std::sync::LazyLock::new(Default::default);
    let mut unsettled = UNSETTLED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match set {
        Some(true) => {
            unsettled.insert(machine_id.to_owned());
        }
        Some(false) => {
            unsettled.remove(machine_id);
        }
        None => {}
    }
    unsettled.contains(machine_id)
}

/// Record the relay cloud machine `machine_id` now uses, and its new tunnel
/// address, and announce the row.
async fn record_relay(
    svc: &Services,
    feature: &RemoteFeature,
    machine_id: &str,
    relay: Option<String>,
    address: Option<String>,
) {
    let Ok(Some(mut row)) = svc.store().get_exposed_machine(machine_id).await else {
        return;
    };
    if let Some(address) = address.filter(|address| row.tc_address.as_ref() != Some(address)) {
        if let Err(error) = svc
            .store()
            .record_exposed_machine_connection(machine_id, &row.hosts, Some(&address), &now_iso())
            .await
        {
            tracing::warn!(machine = %machine_id, %error, "cloud machine: could not record its tunnel address");
        }
    }
    let Some(cloud) = row.cloud.as_mut() else {
        return;
    };
    cloud.relay_url = relay;
    if let Err(error) = svc.store().update_cloud_machine(machine_id, cloud).await {
        tracing::warn!(machine = %machine_id, %error, "cloud machine: could not record its relay");
        return;
    }
    if let Ok(Some(row)) = svc.store().get_exposed_machine(machine_id).await {
        let live = feature.running_node(machine_id);
        crate::methods::publish_machine(svc, &row, live.as_ref().map(|node| node.view())).await;
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
    crate::methods::publish_machine(svc, &row, live.as_ref().map(|node| node.view())).await;
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
    link: Option<&dyn GateLink>,
    row: &ExposedMachine,
) {
    if !feature.cloud.mark_lost(&row.id, Loss::Removed) {
        return;
    }
    tracing::warn!(machine = %row.id, sandbox = %row.cloud.as_ref().map(|c| c.sandbox_id.as_str()).unwrap_or_default(), "cloud machine: E2B removed the sandbox");
    let turn_error = format!("cloud-machine-unavailable (sandbox-lost): {SANDBOX_LOST_MESSAGE}");
    let node = feature.running_node(&row.id);
    let link = link.or_else(|| node.as_ref().map(|node| &**node as &dyn GateLink));
    if let Some(link) = link {
        // The sandbox took its agents with it: their stops are moot.
        link.drop_stops();
        link.end_all_agents(&turn_error);
    }
    let svc = svc.clone();
    let row = row.clone();
    crate::spawn_until_closed(svc.clone(), async move {
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
fn sandbox_outdated(
    svc: &Services,
    feature: &RemoteFeature,
    link: &dyn GateLink,
    machine_id: &str,
) {
    if !feature.cloud.mark_lost(machine_id, Loss::Outdated) {
        return;
    }
    tracing::warn!(machine = %machine_id, "cloud machine: the sandbox cannot link to this backend's version; deleting it");
    // A prompt waiting for the link fails with this refusal. Ending the
    // agents here would close their transport first, and a prompt closed
    // before any output is silently redriven instead of told.
    link.refuse_turns(&outdated_turn_error());
    link.drop_stops();
    let svc = svc.clone();
    let machine_id = machine_id.to_owned();
    crate::spawn_until_closed(svc.clone(), async move {
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
        let turns = node.turns_in_flight();
        if turns > 0 {
            return Err(crate::methods::in_use(&row.name, turns));
        }
    }
    // No dial from now until a demand; the link drops with the pause anyway,
    // and a dial in flight is dropped when it binds.
    feature.cloud.park(machine_id);
    if let Some(node) = &node {
        node.close_link();
    }
    feature.cloud.count_pause(machine_id);
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
                if node.link_wanted() {
                    node.wake();
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
                node.processes_lost(
                    &ids,
                    &format!(
                        "transport closed: {}",
                        intent_core::node_link::DISK_RESUME_TRANSPORT_CLOSED
                    ),
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
    let running = RunningFlag::set(&feature.cloud.supervisor_running);
    intent_core::spawn_daemon(async move {
        let _running = running;
        // A fenced tenant's links close: its supervisor ends with them,
        // mid-check included, so it makes no more E2B calls.
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        let _task = crate::BackgroundTask::of(feature);
        tokio::select! {
            () = feature.closed() => {}
            () = supervise(&svc) => {}
        }
    });
}

/// `true` while held.
struct RunningFlag(Arc<std::sync::atomic::AtomicBool>);

impl RunningFlag {
    fn set(flag: &Arc<std::sync::atomic::AtomicBool>) -> Self {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Self(flag.clone())
    }
}

impl Drop for RunningFlag {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The supervisor's work: one-time startup steps, then a check every tick.
async fn supervise(svc: &Services) {
    if let Err(error) = config::migrate(svc).await {
        tracing::warn!(%error, "cloud machines: could not migrate the saved E2B provider");
    }
    if svc.workspace_homes() {
        super::home::reset_after_restart(svc).await;
    }
    let mut last_reap: Option<Instant> = None;
    loop {
        let clock = Clock::current();
        tokio::time::sleep(clock.tick()).await;
        let Ok(feature) = super::feature(svc) else {
            return;
        };
        if let Err(error) = tick(svc, feature, clock, &mut last_reap).await {
            tracing::debug!(%error, "cloud machines: lifecycle check failed");
        }
    }
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
            let busy = node.as_ref().is_some_and(|node| node.busy());
            let last_active = node.as_ref().map(|node| node.last_activity());
            let activity = feature.cloud.activity(&row.id, busy, last_active);
            // A provider without a lease has nothing to renew: only the
            // idle pause below stops it. E2B caps a continuous run; Vercel
            // refuses a renewal past its session cap.
            let lease = client.has_lease();
            if lease
                && activity
                    .last_renew
                    .is_none_or(|at| at.elapsed() >= clock.minutes(RENEW_MINUTES))
            {
                renew(svc, feature, &**client, row, cloud).await;
            }
            if busy && !feature.cloud.is_pushing(&row.id) {
                // Failed pushes before idle pauses count in a row only; the
                // pushes themselves keep the sandbox busy.
                feature.cloud.reset_skipped_pauses(&row.id);
            }
            if busy || !settled {
                return;
            }
            let idle = activity.last_busy.elapsed() >= clock.minutes(saved.idle_pause_minutes);
            let capped = activity.capped
                || (client.caps_run() && since(cloud.running_since.as_deref()) >= clock.run_cap());
            if idle || capped {
                spawn_pause(
                    svc,
                    client,
                    &row.id,
                    if idle { PauseFor::Idle } else { PauseFor::Cap },
                );
            }
        }
        CloudSandboxState::Paused => {
            if since(cloud.paused_at.as_deref()) >= clock.days(saved.paused_delete_days) {
                tracing::info!(machine = %row.id, "cloud machine: paused too long; deleting it");
                spawn_kill(svc, feature, client, row);
            }
        }
        // A transition a backend restart cut: finish it. `row` was read
        // before this check, so a pause or resume may have ended since:
        // act only on the state read again under the transition lock.
        CloudSandboxState::Pausing | CloudSandboxState::Resuming if settled => {
            let Some(transition) = feature.cloud.try_transition(&row.id) else {
                return;
            };
            match current_state(svc, &row.id).await {
                Some(CloudSandboxState::Pausing) => {
                    drop(transition);
                    spawn_pause(svc, client, &row.id, PauseFor::Restart);
                }
                Some(CloudSandboxState::Resuming) => {
                    feature.cloud.park(&row.id);
                    set_state(svc, feature, &row.id, CloudSandboxState::Paused).await;
                }
                _ => {}
            }
        }
        CloudSandboxState::Stopping if settled => spawn_kill(svc, feature, client, row),
        CloudSandboxState::Pausing | CloudSandboxState::Resuming | CloudSandboxState::Stopping => {}
    }
}

/// Machine `machine_id`'s sandbox state as stored now.
async fn current_state(svc: &Services, machine_id: &str) -> Option<CloudSandboxState> {
    let row = svc.store().get_exposed_machine(machine_id).await.ok()??;
    row.cloud.map(|cloud| cloud.state)
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
        // The session cannot outlive the provider's cap: a planned pause
        // and resume at the next idle moment starts a new one.
        Err(error) if error.kind() == CloudErrorKind::SessionCap => {
            tracing::info!(machine = %row.id, "cloud machine: the session reached the provider's cap; pausing and resuming when idle");
            feature.cloud.renewed(&row.id);
            feature.cloud.cap(&row.id);
        }
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

/// Why the supervisor pauses a sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PauseFor {
    /// It was idle: its homes push first, and it pauses only while still
    /// idle.
    Idle,
    /// The run cap: it resumes at once when the link is still wanted, so
    /// the cap resets without a visible pause.
    Cap,
    /// A pause a backend restart cut: finish it.
    Restart,
}

/// Pause in the background, as `why` says.
fn spawn_pause(svc: &Services, client: &Provider, machine_id: &str, why: PauseFor) {
    let idle_since = super::feature(svc)
        .ok()
        .and_then(|feature| feature.running_node(machine_id))
        .map(|node| node.last_activity());
    let svc = svc.clone();
    let client = client.clone();
    let machine_id = machine_id.to_owned();
    crate::spawn_until_closed(svc.clone(), async move {
        let Ok(feature) = super::feature(&svc) else {
            return;
        };
        let paused = if why == PauseFor::Idle {
            // One idle pause at a time, from its pushes to its end: a later
            // check must not push (and so resume) a sandbox it paused.
            if !feature.cloud.begin_push(&machine_id) {
                return;
            }
            let paused = idle_pause(&svc, feature, &*client, &machine_id, idle_since).await;
            feature.cloud.end_push(&machine_id);
            paused
        } else {
            pause(&svc, feature, &*client, &machine_id).await.map(drop)
        };
        if let Err(error) = paused {
            tracing::debug!(machine = %machine_id, %error, "cloud machine: not paused");
            return;
        }
        if why == PauseFor::Cap {
            if let Some(node) = feature.running_node(&machine_id) {
                if node.link_wanted() {
                    tracing::info!(machine = %machine_id, "cloud machine: planned pause before the run cap; resuming");
                    if let Err(error) = node.connect().await {
                        tracing::warn!(machine = %machine_id, %error, "cloud machine: could not resume after the planned pause");
                    }
                }
            }
        }
    });
}

/// How long an idle pause waits for the uses of its machine in flight to
/// end before it gives up (the idle time starts again).
const USE_WAIT: Duration = Duration::from_secs(5);

/// An idle pause. It holds its machine's use gate exclusively
/// ([`intent_node_connections::MachineLifecycle::quiesce`]): the uses in flight (home calls,
/// terminal and script opens, turns) end first, and new ones wait until the
/// link is closed, then dial, which resumes the sandbox. Under the gate it
/// checks the sandbox is still idle (nothing busy, no activity since the
/// check that found it idle), the homes push ([`pushed_before_pause`]), and
/// the sandbox pauses.
async fn idle_pause(
    svc: &Services,
    feature: &RemoteFeature,
    client: &dyn CloudProvider,
    machine_id: &str,
    idle_since: Option<Instant>,
) -> Result<()> {
    let node = feature.running_node(machine_id);
    // Held until the pause ends: new uses wait for it.
    let mut quiet = None;
    if let Some(node) = &node {
        quiet = node.quiesce(USE_WAIT).await;
        if quiet.is_none() {
            feature.cloud.touch(machine_id);
            return Err(Error::Internal("the sandbox kept being used".into()));
        }
    }
    let used = node
        .as_ref()
        .is_some_and(|node| node.busy() || Some(node.last_activity()) != idle_since);
    if used {
        feature.cloud.touch(machine_id);
        return Err(Error::Internal("the sandbox is in use".into()));
    }
    if !pushed_before_pause(svc, feature, machine_id).await {
        return Err(Error::Internal(
            "a home did not push before the idle pause".into(),
        ));
    }
    if let Some(wait) = std::env::var(AFTER_PUSH_MS_ENV)
        .ok()
        .and_then(|ms| ms.parse::<u64>().ok())
    {
        // timing-guard: test seam only; holds the gap a slow push leaves open
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }
    let paused = pause(svc, feature, client, machine_id).await.map(drop);
    drop(quiet);
    paused
}

/// Before an idle pause (nodes.md "Cloud homes" 4): each home the machine
/// holds pushes `home/head` and `home/wip`. Whether to pause now: a failed
/// push skips this pause and the idle time starts again, up to
/// [`MAX_SKIPPED_PAUSES`] in a row; then the sandbox pauses anyway. Only a
/// running sandbox pushes: a push dials it, and a dial resumes a paused one.
async fn pushed_before_pause(svc: &Services, feature: &RemoteFeature, machine_id: &str) -> bool {
    let running = matches!(
        svc.store().get_exposed_machine(machine_id).await,
        Ok(Some(row)) if row.cloud.as_ref().is_some_and(|cloud| cloud.state == CloudSandboxState::Running)
    );
    if !running || feature.cloud.is_parked(machine_id) {
        return false;
    }
    let homes = svc.store().homes_on_machine(machine_id).await;
    let mut failed = homes.is_err();
    for (workspace_id, home) in homes.unwrap_or_default() {
        if let Err(error) = crate::home_calls::push_home(svc, &home, &workspace_id).await {
            tracing::warn!(machine = %machine_id, workspace = %workspace_id, %error, "cloud machine: the home did not push before the idle pause");
            failed = true;
        }
    }
    if !failed {
        feature.cloud.reset_skipped_pauses(machine_id);
        return true;
    }
    if feature.cloud.skip_pause(machine_id) > MAX_SKIPPED_PAUSES {
        tracing::warn!(machine = %machine_id, "cloud machine: its homes keep failing to push; pausing anyway");
        feature.cloud.reset_skipped_pauses(machine_id);
        return true;
    }
    feature.cloud.touch(machine_id);
    false
}

/// Delete a machine in the background (resume, final stops, delete).
fn spawn_kill(svc: &Services, feature: &RemoteFeature, client: &Provider, row: &ExposedMachine) {
    if !feature.cloud.begin_kill(&row.id) {
        return;
    }
    let svc = svc.clone();
    let client = client.clone();
    let row = row.clone();
    crate::spawn_until_closed(svc.clone(), async move {
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

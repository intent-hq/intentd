//! Placing a remote agent across the machines exposed to this backend
//! (monorepo `docs/protocol/model-platform-routing.md`, *Exposed machines*):
//! which machines can take the agent, in which order, and the error the
//! delegating model gets when none can.
//!
//! The backend's machine list and its on-demand connect are behind
//! [`PlacementMachines`], so the rules are tested without a link.

use std::cmp::Reverse;
use std::fmt::Write as _;

use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use crate::machines::{
    ExposedMachine, MachineModels, MachineUpdateRequired, ModelUnavailable, RemoteMachineState,
};
use crate::node_link::{MachineCapabilities, ProviderCapability, ProviderStatus};
use crate::placement::{
    AgentPlacement, CapacityRefusal, MachineCapacityRefusal, MachineRef, PlacementUnavailable,
    PlacementUnavailableReason, PlatformAvailability,
};
use crate::{BoxFuture, Error, Result};

/// A cached report older than this is refreshed before placement decides.
pub const REPORT_MAX_AGE: Duration = Duration::hours(1);

/// Why a machine's report could not be refreshed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshFailure {
    /// No route to the machine answered.
    Unreachable,
    /// The machine no longer accepts this backend's credential.
    Revoked,
    /// The machine and this backend share no link protocol version.
    UpdateRequired(Box<MachineUpdateRequired>),
    /// The machine is reachable but did not report in time (its first
    /// report can take a while); its cached report, if any, is used.
    Slow,
}

/// The backend's view of its exposed machines.
pub trait PlacementMachines: Send + Sync {
    /// Every machine exposed to this backend, with its cached report.
    fn machines(&self) -> BoxFuture<'_, Result<Vec<ExposedMachine>>>;

    /// Connect to `machine_id` if needed and read a fresh report.
    fn refresh<'a>(
        &'a self,
        machine_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<MachineCapabilities, RefreshFailure>>;
}

/// What the agent needs from a machine.
#[derive(Debug, Clone, Copy)]
pub struct PlacementRequest<'a> {
    pub placement: &'a AgentPlacement,
    /// The provider id, as the machines report it (`claude-code`, `codex`).
    pub provider: &'a str,
    /// The requested model; `None` when the provider's default is used.
    pub model: Option<&'a str>,
    /// The launch sends a selected profile's environment, so the machine's
    /// own sign-in does not apply.
    pub with_profile: bool,
}

/// Whether `report` is missing, unreadable or older than
/// [`REPORT_MAX_AGE`] at `now`.
#[must_use]
pub fn report_is_stale(observed_at: Option<&str>, now: OffsetDateTime) -> bool {
    observed_at
        .and_then(|at| OffsetDateTime::parse(at, &Rfc3339).ok())
        .is_none_or(|at| now - at > REPORT_MAX_AGE)
}

/// The machines that can take the agent now, best first: platform, provider
/// installed, the machine's own sign-in usable (unless a profile is
/// selected), model listed and free capacity; ordered by fewest running
/// agents, then most recently used. Machines without a report, with one
/// listing no providers yet, a stale model list lacking the requested
/// model, or one older than [`REPORT_MAX_AGE`], or last seen needing an
/// update are refreshed first. `placement.nodeId` names the only candidate.
///
/// # Errors
///
/// [`Error::ModelUnavailable`] when no candidate offers the provider and
/// model; [`Error::MachineUpdateRequired`] for a version gap;
/// [`Error::PlacementUnavailable`] otherwise (`unsupported-platform`,
/// `no-capacity`, `machine-unknown`, `machine-revoked`,
/// `machine-unreachable`); the source's own error when it cannot list. A
/// machine that answers too slowly keeps its cached report, and a refusal
/// it decides is retryable.
pub async fn select_machines(
    source: &dyn PlacementMachines,
    request: PlacementRequest<'_>,
    now: OffsetDateTime,
) -> Result<Vec<ExposedMachine>> {
    let rows = source.machines().await?;
    let placement = request.placement;
    let named = placement.node_id.as_deref();
    let observed = now.format(&Rfc3339).unwrap_or_default();

    let candidates: Vec<ExposedMachine> = match named {
        Some(id) => {
            let Some(row) = rows.iter().find(|row| row.id == id).cloned() else {
                return Err(machine_error(
                    placement,
                    PlacementUnavailableReason::MachineUnknown,
                    MachineRef {
                        node_id: id.to_owned(),
                        name: id.to_owned(),
                    },
                    PlatformAvailability::none(observed),
                ));
            };
            if row.state == RemoteMachineState::Revoked {
                return Err(machine_error(
                    placement,
                    PlacementUnavailableReason::MachineRevoked,
                    machine_ref(&row),
                    PlatformAvailability::none(observed),
                ));
            }
            vec![row]
        }
        None => rows
            .iter()
            .filter(|row| row.state != RemoteMachineState::Revoked)
            .cloned()
            .collect(),
    };
    if candidates.is_empty() {
        let reason = if rows.is_empty() {
            None
        } else {
            Some(PlacementUnavailableReason::MachineRevoked)
        };
        let failure = PlatformAvailability::none(observed).unavailable(placement.clone());
        return Err(Error::PlacementUnavailable(Box::new(match reason {
            Some(reason) => PlacementUnavailable { reason, ..failure },
            None => failure,
        })));
    }

    // Refresh what placement cannot decide on, keeping the failures.
    let mut reported = Vec::new();
    let mut failed: Vec<(ExposedMachine, RefreshFailure)> = Vec::new();
    let mut slow = false;
    for mut row in candidates {
        let maybe_matches = row
            .platform
            .is_none_or(|platform| placement.matches_platform(platform));
        // A report without providers was sent before the machine observed
        // them (its welcome right after remote agents were turned on),
        // unless it names them all as switched off there.
        let must_refresh = row.capabilities.as_ref().is_none_or(not_observed)
            || row.state == RemoteMachineState::UpdateRequired
            || report_is_stale(row.observed_at.as_deref(), now)
            || models_pending(&row, &request);
        if maybe_matches && must_refresh {
            match source.refresh(&row.id).await {
                Ok(capabilities) => {
                    row.platform = Some(capabilities.platform);
                    row.observed_at = Some(capabilities.observed_at.clone());
                    row.capabilities = Some(capabilities);
                    if row.state != RemoteMachineState::Connected {
                        row.state = RemoteMachineState::Disconnected;
                    }
                }
                // Slow with a cached report: decide on that report.
                Err(RefreshFailure::Slow) if row.capabilities.is_some() => slow = true,
                Err(failure) => {
                    failed.push((row, failure));
                    continue;
                }
            }
        }
        if row.capabilities.is_some() {
            reported.push(row);
        }
    }

    let availability = availability(&reported, &failed, observed);
    let matching: Vec<&ExposedMachine> = reported
        .iter()
        .filter(|row| {
            row.platform
                .is_some_and(|platform| placement.matches_platform(platform))
        })
        .collect();
    if matching.is_empty() {
        let named_row = named.and(reported.first().or(failed.first().map(|(row, _)| row)));
        return Err(no_platform(placement, named_row, &failed, availability));
    }

    let offering: Vec<&ExposedMachine> = matching
        .iter()
        .copied()
        .filter(|row| offers(row, &request))
        .collect();
    if offering.is_empty() {
        let retryable = slow
            || !failed.is_empty()
            || matching.iter().any(|row| {
                provider_of(row, request.provider).is_some_and(|provider| provider.models_stale)
            });
        let machines = matching
            .iter()
            .filter_map(|row| machine_models(row, request.provider));
        return Err(Error::ModelUnavailable(Box::new(ModelUnavailable::new(
            request.provider,
            request.model.unwrap_or_default(),
            machines,
            retryable,
        ))));
    }

    let mut free: Vec<ExposedMachine> = offering
        .iter()
        .filter(|row| capacity_refusal(row).is_none())
        .map(|row| (*row).clone())
        .collect();
    if free.is_empty() {
        let refusals: Vec<(&ExposedMachine, CapacityRefusal)> = offering
            .iter()
            .filter_map(|row| capacity_refusal(row).map(|refusal| (*row, refusal)))
            .collect();
        return Err(Error::PlacementUnavailable(Box::new(no_capacity(
            placement,
            &refusals,
            availability,
        ))));
    }
    free.sort_by_key(|row| {
        let running = row
            .capabilities
            .as_ref()
            .map_or(u32::MAX, |caps| caps.capacity.running_agents);
        let used = row
            .last_connected_at
            .as_deref()
            .and_then(|at| OffsetDateTime::parse(at, &Rfc3339).ok());
        (running, Reverse(used))
    });
    Ok(free)
}

/// Whether a running remote agent on `node_id` can switch to `request`'s
/// provider and model there (`agent.setModel`): judged on the machine's
/// cached report with placement's rules (provider installed, its own
/// sign-in usable unless a profile is selected, model listed or an alias of
/// it), without connecting. A machine that is offline keeps its report; the
/// relaunch then fails at start like any start.
///
/// # Errors
///
/// [`Error::PlacementUnavailable`] `machine-unknown` / `machine-revoked`
/// when the machine is gone; [`Error::ModelUnavailable`] naming that
/// machine only when its report does not offer the provider and model.
pub fn check_switch_on_machine(
    rows: &[ExposedMachine],
    node_id: &str,
    request: PlacementRequest<'_>,
    now: OffsetDateTime,
) -> Result<()> {
    let observed = now.format(&Rfc3339).unwrap_or_default();
    let Some(row) = rows.iter().find(|row| row.id == node_id) else {
        return Err(machine_error(
            request.placement,
            PlacementUnavailableReason::MachineUnknown,
            MachineRef {
                node_id: node_id.to_owned(),
                name: node_id.to_owned(),
            },
            PlatformAvailability::none(observed),
        ));
    };
    if row.state == RemoteMachineState::Revoked {
        return Err(machine_error(
            request.placement,
            PlacementUnavailableReason::MachineRevoked,
            machine_ref(row),
            PlatformAvailability::none(observed),
        ));
    }
    if offers(row, &request) {
        return Ok(());
    }
    let retryable =
        provider_of(row, request.provider).is_some_and(|provider| provider.models_stale);
    Err(Error::ModelUnavailable(Box::new(ModelUnavailable::new(
        request.provider,
        request.model.unwrap_or_default(),
        machine_models(row, request.provider),
        retryable,
    ))))
}

fn machine_ref(row: &ExposedMachine) -> MachineRef {
    MachineRef {
        node_id: row.id.clone(),
        name: row.name.clone(),
    }
}

fn machine_error(
    placement: &AgentPlacement,
    reason: PlacementUnavailableReason,
    machine: MachineRef,
    availability: PlatformAvailability,
) -> Error {
    Error::PlacementUnavailable(Box::new(availability.machine_unavailable(
        placement.clone(),
        reason,
        machine,
        None,
    )))
}

/// Supported: every platform a reachable or cached machine reported.
/// Available: those of reachable machines with free capacity.
fn availability(
    reported: &[ExposedMachine],
    failed: &[(ExposedMachine, RefreshFailure)],
    observed: String,
) -> PlatformAvailability {
    let supported = reported
        .iter()
        .chain(failed.iter().map(|(row, _)| row))
        .filter_map(|row| row.platform);
    let available = reported
        .iter()
        .filter(|row| capacity_refusal(row).is_none())
        .filter_map(|row| row.platform);
    PlatformAvailability::new(supported, available, observed)
}

/// The error when no reachable machine runs the requested platform: the
/// refresh failure of a machine that might have, or `unsupported-platform`.
fn no_platform(
    placement: &AgentPlacement,
    named: Option<&ExposedMachine>,
    failed: &[(ExposedMachine, RefreshFailure)],
    availability: PlatformAvailability,
) -> Error {
    let update = failed.iter().find_map(|(_, failure)| match failure {
        RefreshFailure::UpdateRequired(update) => Some(update.clone()),
        _ => None,
    });
    // A machine that has not reported yet and is slow to is, for now, as
    // retryable as one that cannot be reached.
    let unreachable = failed
        .iter()
        .find(|(_, failure)| matches!(failure, RefreshFailure::Unreachable | RefreshFailure::Slow));
    let revoked = failed
        .iter()
        .find(|(_, failure)| *failure == RefreshFailure::Revoked);
    // A reachable machine beats an update; an update beats a revocation.
    let reason = if let Some((row, _)) = unreachable {
        Some((PlacementUnavailableReason::MachineUnreachable, row))
    } else if let Some(update) = update {
        return Error::MachineUpdateRequired(update);
    } else {
        revoked.map(|(row, _)| (PlacementUnavailableReason::MachineRevoked, row))
    };
    match (reason, named) {
        (Some((reason, row)), Some(_)) => {
            machine_error(placement, reason, machine_ref(row), availability)
        }
        (Some((reason, _)), None) => {
            let failure = availability.unavailable(placement.clone());
            Error::PlacementUnavailable(Box::new(PlacementUnavailable { reason, ..failure }))
        }
        (None, Some(row)) => machine_error(
            placement,
            PlacementUnavailableReason::UnsupportedPlatform,
            machine_ref(row),
            availability,
        ),
        (None, None) => {
            let failure = availability.unavailable(placement.clone());
            Error::PlacementUnavailable(Box::new(PlacementUnavailable {
                reason: PlacementUnavailableReason::UnsupportedPlatform,
                ..failure
            }))
        }
    }
}

/// Whether the requested provider's model list is still being discovered
/// (stale, as in the machine's quick first report) and does not list the
/// requested model yet, so only a fresh report can answer.
fn models_pending(row: &ExposedMachine, request: &PlacementRequest<'_>) -> bool {
    request.model.is_some_and(|model| {
        provider_of(row, request.provider).is_some_and(|provider| {
            provider.models_stale
                && !provider
                    .models
                    .iter()
                    .any(|listed| model_listed(listed, model))
        })
    })
}

fn provider_of<'a>(row: &'a ExposedMachine, provider: &str) -> Option<&'a ProviderCapability> {
    row.capabilities
        .as_ref()?
        .providers
        .iter()
        .find(|candidate| candidate.id == provider)
}

/// Whether the provider can launch there, which the sign-in decides only for
/// a launch without a profile.
fn provider_usable(status: ProviderStatus, with_profile: bool) -> bool {
    match status {
        ProviderStatus::CliMissing
        | ProviderStatus::PrerequisiteMissing
        | ProviderStatus::Disabled => false,
        ProviderStatus::SignedOut | ProviderStatus::AuthFailed => with_profile,
        ProviderStatus::SignedIn | ProviderStatus::Unknown => true,
    }
}

fn offers(row: &ExposedMachine, request: &PlacementRequest<'_>) -> bool {
    provider_of(row, request.provider).is_some_and(|provider| {
        provider_usable(provider.status, request.with_profile)
            && request.model.is_none_or(|model| {
                provider
                    .models
                    .iter()
                    .any(|listed| model_listed(listed, model))
            })
    })
}

/// Whether `requested` names the listed model id: the same id ignoring
/// case, or its words in order within the id, as provider aliases do
/// (`opus` and `opus-4-1` name `claude-opus-4-1`).
#[must_use]
pub fn model_listed(listed: &str, requested: &str) -> bool {
    if listed.eq_ignore_ascii_case(requested) {
        return true;
    }
    let words = |id: &str| -> Vec<String> {
        id.split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|word| !word.is_empty())
            .map(str::to_ascii_lowercase)
            .collect()
    };
    let (listed, requested) = (words(listed), words(requested));
    !requested.is_empty()
        && listed
            .windows(requested.len())
            .any(|window| window == requested.as_slice())
}

/// A report that says nothing of the machine's providers yet: sent before
/// the machine first looked at them.
fn not_observed(caps: &MachineCapabilities) -> bool {
    caps.providers.is_empty() && caps.disabled_providers.is_empty()
}

/// The status of a provider the machine's report does not list: `disabled`
/// when the report names it as switched off there, `unknown` without a
/// report or with one that predates the machine's first look at its
/// providers, else `cli-missing`.
fn unlisted_status(caps: Option<&MachineCapabilities>, provider: &str) -> ProviderStatus {
    match caps {
        Some(caps) if caps.disabled_providers.iter().any(|id| id == provider) => {
            ProviderStatus::Disabled
        }
        Some(caps) if !not_observed(caps) => ProviderStatus::CliMissing,
        _ => ProviderStatus::Unknown,
    }
}

fn machine_models(row: &ExposedMachine, provider: &str) -> Option<MachineModels> {
    let offered = provider_of(row, provider);
    Some(MachineModels {
        node_id: row.id.clone(),
        name: row.name.clone(),
        platform: row.platform?,
        status: offered.map_or_else(
            || unlisted_status(row.capabilities.as_ref(), provider),
            |provider| provider.status,
        ),
        models: offered
            .map(|provider| provider.models.clone())
            .unwrap_or_default(),
        observed_at: row.observed_at.clone().unwrap_or_default(),
    })
}

/// The shared limit a machine's last report says is used up.
fn capacity_refusal(row: &ExposedMachine) -> Option<CapacityRefusal> {
    let capacity = row.capabilities.as_ref()?.capacity;
    if capacity.running_agents >= capacity.agent_cap {
        return Some(CapacityRefusal::Count {
            running_agents: capacity.running_agents,
            agent_cap: capacity.agent_cap,
        });
    }
    capacity
        .memory_budget_bytes
        .filter(|budget| capacity.charged_memory_bytes >= *budget)
        .map(|budget| CapacityRefusal::Memory {
            charged_memory_bytes: capacity.charged_memory_bytes,
            memory_budget_bytes: budget,
        })
}

fn no_capacity(
    placement: &AgentPlacement,
    refusals: &[(&ExposedMachine, CapacityRefusal)],
    availability: PlatformAvailability,
) -> PlacementUnavailable {
    if let [(row, refusal)] = refusals {
        return availability.machine_unavailable(
            placement.clone(),
            PlacementUnavailableReason::NoCapacity,
            machine_ref(row),
            Some(*refusal),
        );
    }
    let failure = availability.unavailable(placement.clone());
    PlacementUnavailable {
        reason: PlacementUnavailableReason::NoCapacity,
        ..failure
    }
    .with_machines(
        refusals
            .iter()
            .map(|(row, refusal)| MachineCapacityRefusal {
                node_id: row.id.clone(),
                refusal: *refusal,
            })
            .collect(),
    )
}

/// Most machines the discovery text lists.
pub const HELP_MAX_MACHINES: usize = 8;
/// Most models per provider the discovery text lists.
pub const HELP_MAX_MODELS: usize = 10;
/// The discovery text's budget for its machine lines, in bytes.
pub const HELP_MAX_BYTES: usize = 4096;

/// The profile list's cap in the discovery text; the rest are counted.
pub const HELP_MAX_PROFILES: usize = 20;

/// Whether a provider profile (or Default configuration) can run on
/// another machine, as the discovery text shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileRemote {
    Compatible,
    /// It sets these account-folder keys, which tie it to this backend.
    Incompatible {
        keys: Vec<String>,
    },
    /// Its saved settings could not be read; placement decides.
    Unknown,
}

/// One provider profile held by this backend, without its settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementProfile {
    /// `<base>@<profileId>`, what `provider` takes.
    pub provider_ref: String,
    pub base_provider: String,
    pub name: String,
    /// Quota failover may switch to it.
    pub backup: bool,
    pub remote: ProfileRemote,
}

/// What the backend contributes to the remote-placement text: its
/// profiles, the Default configurations that cannot run remotely, and the
/// providers it has turned off.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementBackendFacts {
    pub profiles: Vec<PlacementProfile>,
    pub incompatible_defaults: Vec<(String, ProfileRemote)>,
    pub disabled_providers: Vec<String>,
}

/// What the remote-placement text is rendered from.
#[derive(Clone, Copy)]
pub struct PlacementHelpInputs<'a> {
    pub machines: &'a [ExposedMachine],
    /// Whether a machine's link is up now.
    pub connected: &'a dyn Fn(&str) -> bool,
    /// Whether this backend has a provider turned off (Settings → Providers).
    pub disabled: &'a dyn Fn(&str) -> bool,
    pub profiles: &'a [PlacementProfile],
    /// Base providers whose Default configuration is not remote-compatible
    /// (or could not be read).
    pub incompatible_defaults: &'a [(String, ProfileRemote)],
}

impl<'a> PlacementHelpInputs<'a> {
    /// The machines alone: no profiles, every provider enabled.
    #[must_use]
    pub fn new(machines: &'a [ExposedMachine], connected: &'a dyn Fn(&str) -> bool) -> Self {
        Self {
            machines,
            connected,
            disabled: &|_| false,
            profiles: &[],
            incompatible_defaults: &[],
        }
    }
}

/// How to place an agent on an exposed machine: the end of the 1.0–3.9
/// prompt text, and of the 3.10 `ws.help("agent")` text.
const PLACEMENT_HOW_TO: &str = "To run an agent on one, pass `placement: { target: \"remote\" }` (optionally `nodeId`, `os`, `arch`) in the options of `ws.agent.create` or `ws.agent.delegate`, or as `ws.agent.wakeOrCreate(taskNoteId, contextMessage, model, messageMetadata, reasoningEffort, { create: { placement } })`. Without `nodeId` the backend picks a machine that runs the platform, has the provider signed in (or unknown) and the model listed, with free capacity. When none lists the model the call fails with `model-unavailable`; other refusals are `placement-unavailable` or `machine-update-required`. A remote agent's own sub-agents (`ws.agent.create`, `ws.agent.delegate`, `ws.agent.wakeOrCreate`) run on its machine when no `placement` is given, with the same checks and no fallback; pass `placement: { target: \"local\" }` to run one on the backend. A batch `ws.agent.delegate({ tasks })` takes one `placement` for all its tasks, and an object entry `{ taskNoteId, placement }` its own.\n\
         A remote agent starts from the workspace's last commit (uncommitted changes are not carried), runs with the selected provider profile's environment, or the provider's Default configuration settings when no profile is selected (an API key or other settings; a profile or Default that sets an account folder such as `CODEX_HOME` / `CLAUDE_CONFIG_DIR` is refused with `profile-not-remote-compatible`), uses the machine's own provider login only when that configuration sets nothing, and pushes its work to `refs/intent/ws/<workspaceId>/agents/<agentId>/head` (commits) and `.../wip` (its working tree): right after each `ws.git.commit` (its result has `pushed`) and before each `ws.agent.reportToParent` is delivered, and at the end of each turn, which is when a plain `git commit` reaches the backend. `ws.agent.setModel` can switch it to another model or remote-compatible profile its machine offers; its next turn relaunches it there with the conversation history.\n";

/// Renders the remote-placement text. Each harness version picks one;
/// [`exposed_machines_help`] is the renderer of harness 1.0 through 3.9.
pub type PlacementHelpRenderer = fn(inputs: &PlacementHelpInputs<'_>) -> Option<String>;

/// What a remote agent is told about long commands, in the placement help
/// and its first turn on the machine: its provider's own background shells
/// are off there, and nothing reports them back to wake it.
pub const REMOTE_LONG_COMMANDS_HINT: &str = "On a remote machine, run anything over 30 s with `ws.script.start`; shell background jobs never wake you.";

/// The `ws.agent` discovery text for the exposed machines: name, id,
/// platform, connected or last seen, each usable provider with its sign-in
/// status and up to [`HELP_MAX_MODELS`] models, and when the report was
/// observed. At most [`HELP_MAX_MACHINES`] machines within
/// [`HELP_MAX_BYTES`]; the rest are counted. Then the backend's provider
/// profiles (up to [`HELP_MAX_PROFILES`]) and Default configurations that
/// cannot run remotely. Never names routes, fingerprints, credentials or
/// setting values. `None` with no usable machine.
#[must_use]
pub fn exposed_machines_help(inputs: &PlacementHelpInputs<'_>) -> Option<String> {
    let listed: Vec<&ExposedMachine> = inputs
        .machines
        .iter()
        .filter(|row| row.state != RemoteMachineState::Revoked)
        .collect();
    if listed.is_empty() {
        return None;
    }
    let mut lines = String::new();
    let mut shown = 0;
    for row in &listed {
        let line = help_line(row, (inputs.connected)(&row.id), inputs.disabled);
        if shown == HELP_MAX_MACHINES || lines.len() + line.len() > HELP_MAX_BYTES {
            break;
        }
        lines.push_str(&line);
        shown += 1;
    }
    let mut text = format!(
        "Remote placement (experimental): machines exposed to this backend ({}):\n{lines}",
        listed.len()
    );
    if shown < listed.len() {
        let _ = writeln!(
            text,
            "- … {} more not shown; a `model-unavailable` error lists every machine's models.",
            listed.len() - shown
        );
    }
    push_profiles(&mut text, inputs);
    text.push_str(PLACEMENT_HOW_TO);
    text.push_str(REMOTE_LONG_COMMANDS_HINT);
    Some(text)
}

/// The 3.10 lookup binding, as agents call it.
pub const MACHINE_MODELS_SIGNATURE: &str = "ws.agent.machineModels(nodeId?, { provider?, model? }?)";

/// The harness 3.10 prompt text: each exposed machine by stable identity
/// only (name, nodeId, platform), in name order, plus the instruction to
/// confirm with [`MACHINE_MODELS_SIGNATURE`] and a pointer to
/// `ws.help("agent")`. No state, times, sign-in status or models, so it
/// changes only when a machine is added, removed or first reports its
/// platform. `None` with no usable machine.
#[must_use]
pub fn stable_machines_help(inputs: &PlacementHelpInputs<'_>) -> Option<String> {
    let mut text = identity_list(inputs.machines)?;
    let _ = writeln!(
        text,
        "Before you choose a machine, provider or model for a remote agent, confirm it with `{MACHINE_MODELS_SIGNATURE}`: each machine's state, provider sign-in status and full model list. How to place one: `ws.help(\"agent\")`."
    );
    text.push_str(REMOTE_LONG_COMMANDS_HINT);
    Some(text)
}

/// The profiles block: each profile's reference, name, backup flag and
/// remote compatibility, then each Default that cannot run remotely.
fn push_profiles(text: &mut String, inputs: &PlacementHelpInputs<'_>) {
    if inputs.profiles.is_empty() && inputs.incompatible_defaults.is_empty() {
        return;
    }
    text.push_str(
        "Provider profiles (held by this backend, usable on every machine that has the base provider; select one with `provider: \"<base>@<profileId>\"`):\n",
    );
    for profile in inputs.profiles.iter().take(HELP_MAX_PROFILES) {
        let _ = write!(text, "- `{}` \"{}\"", profile.provider_ref, profile.name);
        if profile.backup {
            text.push_str(" (backup)");
        }
        push_remote(text, &profile.remote);
        text.push('\n');
    }
    if inputs.profiles.len() > HELP_MAX_PROFILES {
        let _ = writeln!(
            text,
            "- … {} more profiles not shown.",
            inputs.profiles.len() - HELP_MAX_PROFILES
        );
    }
    for (base, remote) in inputs.incompatible_defaults {
        let _ = write!(text, "- Default `{base}`");
        push_remote(text, remote);
        text.push('\n');
    }
}

fn push_remote(text: &mut String, remote: &ProfileRemote) {
    match remote {
        ProfileRemote::Compatible => {}
        ProfileRemote::Incompatible { keys } => {
            let keys: Vec<String> = keys.iter().map(|key| format!("`{key}`")).collect();
            let _ = write!(
                text,
                ": not remote-compatible (sets {}, an account folder on this backend)",
                keys.join(", ")
            );
        }
        ProfileRemote::Unknown => {
            text.push_str(": remote compatibility unknown (settings unreadable)");
        }
    }
}

/// The harness 3.10 `ws.help("agent")` text: the same identity list as
/// [`stable_machines_help`], the lookup's doc line, the backend's provider
/// profiles and the placement how-to.
/// No live facts: those come only from the lookup.
#[must_use]
pub fn stable_machines_reference(inputs: &PlacementHelpInputs<'_>) -> Option<String> {
    let mut text = identity_list(inputs.machines)?;
    let _ = writeln!(
        text,
        "  {MACHINE_MODELS_SIGNATURE} → {{ machines }}  // Experimental. Per machine: `nodeId`, `name`, `platform`, `state`, `lastConnectedAt`, `observedAt`, `runningAgents`, `providers: [{{ id, status, models, modelsStale }}]` and `disabledProviders`, from its last report (up to 1 hour old; no connect). `nodeId` keeps one machine (unknown → `machine-unknown`); `provider` keeps that provider; `model` (exact id) keeps providers listing it. Each kept provider has its full model list. Confirm the machine, provider and model here before placing a remote agent."
    );
    push_profiles(&mut text, inputs);
    text.push_str(PLACEMENT_HOW_TO);
    text.push_str(REMOTE_LONG_COMMANDS_HINT);
    Some(text)
}

/// The header and one identity line per listed machine (not revoked), in
/// name order, within [`HELP_MAX_MACHINES`] and [`HELP_MAX_BYTES`].
fn identity_list(machines: &[ExposedMachine]) -> Option<String> {
    let mut listed: Vec<&ExposedMachine> = machines
        .iter()
        .filter(|row| row.state != RemoteMachineState::Revoked)
        .collect();
    if listed.is_empty() {
        return None;
    }
    listed.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
    let mut lines = String::new();
    let mut shown = 0;
    for row in &listed {
        let platform = row
            .platform
            .map_or_else(|| "platform unknown".to_owned(), |p| p.to_string());
        let line = format!("- \"{}\" (nodeId `{}`, {platform})\n", row.name, row.id);
        if shown == HELP_MAX_MACHINES || lines.len() + line.len() > HELP_MAX_BYTES {
            break;
        }
        lines.push_str(&line);
        shown += 1;
    }
    let mut text =
        format!("Remote placement (experimental): machines exposed to this backend:\n{lines}");
    if shown < listed.len() {
        let _ = writeln!(
            text,
            "- … {} more; `ws.agent.machineModels()` lists them all.",
            listed.len() - shown
        );
    }
    Some(text)
}

fn help_line(row: &ExposedMachine, connected: bool, disabled: &dyn Fn(&str) -> bool) -> String {
    let platform = row
        .platform
        .map_or_else(|| "platform unknown".to_owned(), |p| p.to_string());
    let state = match row.state {
        _ if connected => "connected".to_owned(),
        RemoteMachineState::UpdateRequired => "needs an Intent update".to_owned(),
        RemoteMachineState::Unreachable => "unreachable".to_owned(),
        _ => row.last_connected_at.as_deref().map_or_else(
            || "not connected yet".to_owned(),
            |at| format!("last seen {at}"),
        ),
    };
    let mut line = format!(
        "- \"{}\" (nodeId `{}`, {platform}, {state}",
        row.name, row.id
    );
    if let Some(at) = &row.observed_at {
        let _ = write!(line, ", report {at}");
    }
    line.push(')');
    let providers: Vec<String> = row
        .capabilities
        .iter()
        .flat_map(|caps| caps.providers.iter())
        .filter(|provider| {
            provider.visible
                && !matches!(
                    provider.status,
                    ProviderStatus::CliMissing | ProviderStatus::PrerequisiteMissing
                )
        })
        .map(|provider| {
            let shown = provider.models.len().min(HELP_MAX_MODELS);
            let mut models = provider.models[..shown].join(", ");
            if provider.models.len() > shown {
                let _ = write!(models, ", +{} more", provider.models.len() - shown);
            }
            if models.is_empty() {
                models.push_str("no models listed");
            }
            let mut shown = format!(
                "{} {} [{models}]",
                provider.id,
                crate::machines::status_name(provider.status)
            );
            if disabled(&provider.id) {
                shown.push_str(", not enabled on this backend (Settings → Providers)");
            }
            shown
        })
        .collect();
    if providers.is_empty() {
        line.push_str(": no usable providers reported");
    } else {
        let _ = write!(line, ": {}", providers.join("; "));
    }
    line.push('\n');
    line
}

#[cfg(test)]
mod tests;

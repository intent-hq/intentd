//! `agent.*` RPC helpers (PROTOCOL §5.5).
//!
//! Pure projections + the model-catalog helpers that back the `agent.*`
//! `WorkspaceApi` methods (the trait bodies live in `lib.rs`). The
//! [`AgentLite`] derivation (`lastAgentResponse`/`digest`) ports the TS
//! `agent.list`/`agent.get` post-processing; [`parse_model_list_output`]
//! ports the auggie CLI model-list parser.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex};

use intent_core::events::{
    AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_MESSAGE, AGENT_QUEUE_PROCESSING,
    AGENT_QUEUE_UPDATED, AGENT_UPDATED,
};
use intent_core::{
    now_iso, parse_iso, ActorType, AgentCreateExtra, AgentId, AgentLite, AgentMessage,
    AgentSession, AgentStatus, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    ConversationProjection, Error, Event, EventActor, NoteId, PullRequestInfo, PullRequestStatus,
    Result, SessionStats, TaskStatus, WorkspaceApi, WorkspaceId, MAX_DELEGATION_DEPTH,
    PROPOSAL_OUTCOME_APPLIED, PROPOSAL_OUTCOME_DISMISSED, SLIM_PAGE_BUDGET_BYTES,
};
/// Default `agent.diagnostics` stale-responding threshold (10 minutes), matching
/// the TS `DEFAULT_STALE_RESPONDING_AFTER_MS`.
const DEFAULT_STALE_RESPONDING_AFTER_MS: i64 = 10 * 60 * 1000;

/// Age past which an undelivered ready-to-send queue entry on an agent that is
/// not actively running counts as a `stale-queue-entry` stuck-risk in
/// `agent.diagnostics` (intent-hq/monorepo#1897): during the monorepo#1791
/// incident a settlement wake sat undelivered at position 0 for ~13 minutes on
/// an idle agent while diagnostics reported zero stuck risks. Five minutes is
/// comfortably past any normal drain latency (drains trigger in seconds) while
/// catching a wedged queue well before the incident's timescale.
const STALE_QUEUE_ENTRY_AFTER_MS: i64 = 5 * 60 * 1000;

/// Persisted-conversation size past which `agent.diagnostics` raises a
/// `large-conversation` stuck-risk warning (intent-hq/monorepo#2669): in that
/// incident, turns started silently dying (bare `end_turn` after an 11-13 min
/// silent gap) once the conversation reached ~5.3-5.5 MB. 4 MiB is well past
/// the transport's 1 MiB large-frame WARN while still comfortably before the
/// observed failure sizes, so coordinators can rotate to a fresh agent BEFORE
/// turns start dying. Diagnostics-only: the byte total never rides the hot
/// `agent.list`/`agent.get` payloads.
const LARGE_CONVERSATION_WARN_BYTES: u64 = 4 * 1024 * 1024;

/// Per-entry `content` cap (chars) for the shared queue *preview* projection
/// ([`Services::queue_snapshot_preview`]) used by surfaces that embed other
/// agents' queues (e.g. `agent.diagnostics`). `agent.getQueue` itself stays
/// untruncated.
const QUEUE_PREVIEW_MAX_CHARS: usize = 200;

/// Maximum length for caller-supplied message IDs to prevent unbounded storage
/// and `DoS` via oversized persisted IDs.
pub(crate) const MAX_MESSAGE_ID_LEN: usize = 256;

use crate::agent_subscriptions::CompletionWatch;

/// `waitMode` value that defers the completion watch into an `after_all`
/// delegation group (AS-4) rather than registering a standalone ungrouped
/// watch here.
const WAIT_MODE_AFTER_ALL: &str = "after_all";

/// Marker `metadata.source` written on new agents created by
/// `agent.wakeOrCreate` (C1d-10a). Mirrors the FE tool's own tag so downstream
/// consumers (activity feeds, filters) can trace provenance.
const WAKE_OR_CREATE_SOURCE: &str = "wake_or_create_task_agent";

use serde_json::{json, Value};
use uuid::Uuid;

use crate::Services;

/// Row scope selecting which sessions an `agent.list` read serves (§5.5).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AgentListScope {
    /// Default read: active (not soft-retired) sessions only.
    Active,
    /// `includeRetired: true`: every session, retired rows carrying `retiredAt`.
    All,
    /// `retiredOnly: true`: soft-retired sessions only.
    RetiredOnly,
}

/// "Running a turn" statuses for the retire guard (§5.5, confirmed
/// decision): a descendant in `pending`/`active`/`Processing` blocks
/// `ws.agent.retire`; idle/waiting/settled children are cascade-retired.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}

/// Terminal statuses the retire cascade never touches (the same set
/// `count_child_agents` treats as terminal).
fn is_terminal_status(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Completed | AgentStatus::Error | AgentStatus::Deleted
    )
}

/// Per-agent ordering gate for pending-question marker writes and their
/// matching `agent:updated` events. Different agents never contend.
#[derive(Clone, Default)]
pub(crate) struct PendingQuestionMutationLocks {
    locks: Arc<Mutex<HashMap<AgentId, Arc<tokio::sync::Mutex<()>>>>>,
}

impl PendingQuestionMutationLocks {
    fn lock_for(&self, agent_id: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .expect("pending-question lock map poisoned")
            .entry(agent_id.clone())
            .or_default()
            .clone()
    }

    /// Drop the agent's entry from the map (agent deletion —
    /// monorepo#3179): entries otherwise accumulate for the daemon's
    /// lifetime. Safe against in-flight mutations: a holder keeps its own
    /// `Arc` clone of the mutex, so removal never invalidates a held guard,
    /// and mutations already serialized on the old mutex complete in order.
    /// A mutation that starts AFTER removal gets a fresh mutex, but by then
    /// the session row is gone, so it no-ops on the store read — the two
    /// mutexes never guard conflicting writes.
    fn remove(&self, agent_id: &AgentId) {
        self.locks
            .lock()
            .expect("pending-question lock map poisoned")
            .remove(agent_id);
    }

    /// Whether the map currently holds an entry for the agent (test-only).
    #[cfg(test)]
    pub(crate) fn contains(&self, agent_id: &AgentId) -> bool {
        self.locks
            .lock()
            .expect("pending-question lock map poisoned")
            .contains_key(agent_id)
    }
}

/// The concise per-agent state digest behind `ws.agent.snapshot()` and the
/// per-turn prompt injection (`current ws.agent.snapshot() => {...}`).
/// Serialized as single-line camelCase JSON with zero-count and null fields
/// omitted to preserve tokens; `time` is always present.
///
/// `num_questions_asked` counts this agent's structured questions currently
/// pending: questions registered via `ws.app.question.ask` in the current
/// turn that are still waiting for the turn-end drain (turn-attachment
/// registry), plus questions already presented on the trailing assistant
/// message and not yet answered or dismissed (the counting form of the
/// question-hold derivation) — see [`Services::pending_question_count`].
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentSnapshot {
    /// Current UTC timestamp (whole-second RFC-3339).
    pub(crate) time: String,
    /// Active (scheduled/running) background hooks owned by this agent.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) hooks: usize,
    /// Active sub-agent completion watches registered by this agent.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) agent_watches: usize,
    /// Pending messages in this agent's own delivery queue.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) queued_messages: usize,
    /// Active workspace event subscriptions owned by this agent.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) event_subscriptions: usize,
    /// Delegated children that are executing a live runtime turn now.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) active_sub_agents: usize,
    /// Delegated children not yet settled, including idle/background waiters.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) unsettled_sub_agents: usize,
    /// Legacy compatibility count for children in an in-flight status.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) running_sub_agents: usize,
    /// Structured questions still pending presentation/answer.
    #[serde(skip_serializing_if = "is_zero")]
    pub(crate) num_questions_asked: usize,
    /// Active PR monitors owned by this agent as `"<owner>/<name>#<number>"`
    /// labels, suffixed with `" (changes pending)"` while a debounced emit is
    /// accumulating. Omitted when the agent monitors nothing, so prompts stay
    /// byte-identical for agents that never use the feature.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) pr_monitors: Vec<String>,
    /// Tracked open PRs (not merged/closed) as `"<owner>/<name>#<number>"`
    /// labels grouped by state, from the workspace repo plus registered git
    /// roots. Omitted when the workspace tracks no open PR, so prompts stay
    /// byte-identical for workspaces without PRs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prs: Option<AgentSnapshotPrs>,
    /// `"blocker"` / `"discussion"` when this agent has raised an attention
    /// request that is still unresolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pending_attention: Option<String>,
}

/// The snapshot's `prs` object: tracked open PRs grouped by state, built
/// from persisted columns only (the workspace row's discovered
/// `pull_requests` plus each registered git root's) — no forge calls, no
/// per-PR statements. Group precedence per PR: `draft` > `blocked` >
/// `mergeable` > `unknown`; empty groups are omitted from the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AgentSnapshotPrs {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) draft: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) blocked: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) mergeable: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) unknown: Vec<String>,
}

impl AgentSnapshotPrs {
    fn is_empty(&self) -> bool {
        self.draft.is_empty()
            && self.blocked.is_empty()
            && self.mergeable.is_empty()
            && self.unknown.is_empty()
    }

    /// The group vec `pr` belongs to, or `None` when the PR is merged or
    /// closed (excluded from the snapshot entirely).
    fn group_for(&mut self, pr: &PullRequestInfo) -> Option<&mut Vec<String>> {
        if matches!(
            pr.status,
            PullRequestStatus::Merged | PullRequestStatus::Closed
        ) {
            return None;
        }
        let state = pr.mergeable_state.as_deref();
        if pr.is_draft == Some(true)
            || pr.status == PullRequestStatus::Draft
            || state == Some("draft")
        {
            return Some(&mut self.draft);
        }
        if matches!(state, Some("blocked" | "dirty" | "behind")) || pr.mergeable == Some(false) {
            return Some(&mut self.blocked);
        }
        if matches!(state, Some("clean" | "unstable" | "has_hooks")) || pr.mergeable == Some(true) {
            return Some(&mut self.mergeable);
        }
        Some(&mut self.unknown)
    }
}

/// Group tracked PR pools into the snapshot's `prs` object. `pools` yields
/// `(owner, name, prs)` per repo; a pool with a blank (empty/whitespace)
/// owner or name is skipped entirely — no meaningful label can be formed —
/// matching the identity-less-root skip upstream. A merged/closed entry in
/// ANY pool suppresses that `(repo, number)` entirely: the freshest terminal
/// state wins over a stale open duplicate regardless of which pool carries
/// it. Among surviving open duplicates the workspace pool (yielded first)
/// wins the grouping. Returns `None` when no open PR survives (the field is
/// then omitted).
fn grouped_open_prs<'a>(
    pools: impl IntoIterator<Item = (&'a str, &'a str, &'a [PullRequestInfo])>,
) -> Option<AgentSnapshotPrs> {
    let pools: Vec<(&str, &str, &[PullRequestInfo])> = pools
        .into_iter()
        .filter(|(owner, name, _)| !owner.trim().is_empty() && !name.trim().is_empty())
        .collect();
    // Seed the seen-set with every merged/closed key so a terminal state in
    // any pool suppresses stale open duplicates of the same PR.
    let mut seen: HashSet<(&str, &str, u64)> = HashSet::new();
    for &(owner, name, prs) in &pools {
        for pr in prs {
            if matches!(
                pr.status,
                PullRequestStatus::Merged | PullRequestStatus::Closed
            ) {
                seen.insert((owner, name, pr.number));
            }
        }
    }
    let mut groups = AgentSnapshotPrs::default();
    for &(owner, name, prs) in &pools {
        for pr in prs {
            if !seen.insert((owner, name, pr.number)) {
                continue;
            }
            if let Some(group) = groups.group_for(pr) {
                group.push(crate::harness::latest().pr_monitor_label(
                    owner,
                    name,
                    pr.number.cast_signed(),
                ));
            }
        }
    }
    (!groups.is_empty()).then_some(groups)
}

// serde's `skip_serializing_if` requires a `fn(&T) -> bool` signature.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl AgentSnapshot {
    /// `true` when every field other than `time` is zero/absent — the
    /// injection-skip condition (`time` alone never forces an injection).
    pub(crate) fn is_trivial(&self) -> bool {
        self.hooks == 0
            && self.agent_watches == 0
            && self.queued_messages == 0
            && self.event_subscriptions == 0
            && self.active_sub_agents == 0
            && self.unsettled_sub_agents == 0
            && self.running_sub_agents == 0
            && self.num_questions_asked == 0
            && self.pr_monitors.is_empty()
            && self.prs.is_none()
            && self.pending_attention.is_none()
    }
}

/// Outcome of [`Services::scan_assigned_agents`]: the newest live/resumable
/// session (occupancy), the newest known session (inheritance source), plus
/// the stale (`cleaned_up`) and poisoned assignment ids the wakeOrCreate
/// branches prune/migrate.
#[derive(Debug, Default)]
pub(crate) struct AssignedAgentScan {
    pub(crate) live_session: Option<AgentSession>,
    pub(crate) inheritance_source: Option<AgentSession>,
    pub(crate) cleaned_up: Vec<AgentId>,
    pub(crate) poisoned: Vec<AgentId>,
}

mod batch;

// Delivery-time ready-set delta for completion wakes
// (intent-hq/monorepo#2044): enqueue-time trigger stamping + the pure delta
// helper the delivery paths render fresh at flush time.
pub(crate) mod ready_delta;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_stab28;

#[cfg(test)]
mod tests_stab115;

#[cfg(test)]
mod tests_specialist_frontmatter;

#[cfg(test)]
mod tests_delegate_provider_resolution;

#[cfg(test)]
mod tests_settings_default_effort;

#[cfg(test)]
mod tests_catalog_default_model;

/// Resolve the default model from settings when no explicit model is supplied
/// at agent creation time. Precedence chain (the per-workspace override tier
/// was removed in monorepo#1000; the background-agent tier was removed in
/// monorepo#1729 — the renamed `quickActions.*` keys scope to single-shot
/// quick actions and never to agent sessions, delegated ones included):
/// 1. `model.providerDefaults[resolved provider]`
/// 2. `model.default`
/// 3. None → CLI default (current behavior, last resort)
///
/// The resolved model is persisted to `session.model` at creation time, pinning
/// it for the agent's lifetime. Later settings changes never affect existing
/// sessions; only new agents pick up the new default.
fn resolve_default_model_from_settings(
    services: &Services,
    provider: Option<&str>,
) -> Option<String> {
    let settings = services.effective_settings();

    // 1. Check provider defaults. With no explicit provider, key the lookup
    // by the settings-derived default (model.defaultProvider); with neither,
    // there is no provider to key on (monorepo#3044: no positional last
    // resort) and the step is skipped.
    let derived;
    let provider_key = if let Some(p) = provider {
        Some(p)
    } else {
        derived = crate::agent_session::derived_default_provider(&settings);
        derived.as_deref()
    };
    if let Some(model) = provider_key.and_then(|k| settings.model.provider_defaults.get(k)) {
        if !model.is_empty() {
            return Some(model.clone());
        }
    }

    // 2. Check global default
    if let Some(model) = settings.model.default.as_deref().filter(|m| !m.is_empty()) {
        return Some(model.to_string());
    }

    // 3. None → CLI default (session.model stays None)
    None
}

/// Which rung of [`resolve_agent_default_model`] produced the resolved model.
/// Creation-time reasoning-effort resolution needs the provenance: the
/// settings default effort (`model.defaultReasoningEffort`) is a strict
/// companion to the settings default *model*, so it applies only when the
/// model itself came from [`Settings`](DefaultModelSource::Settings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefaultModelSource {
    /// Step 1 — a caller-supplied `model`. Never returned by the resolver
    /// (which only runs when the caller supplied none); stamped by the create
    /// seam so the effort resolution can tell a pinned model apart.
    Explicit,
    /// Step 2 — specialist frontmatter `model`.
    Specialist,
    /// Step 3 — the settings chain ([`resolve_default_model_from_settings`]).
    Settings,
    /// Step 4 — the cached provider catalog's `isDefault` row
    /// ([`crate::model_catalog::ModelCatalogCache::cached_default_model`]).
    /// Not a settings default: the settings default reasoning effort does
    /// NOT apply to it.
    CatalogDefault,
    /// Step 5 — nothing resolved; the provider CLI default applies.
    CliDefault,
}

/// Single daemon-side default-model resolver (spec "New resolution policy").
/// Applied by every creation path — `agent.create`, `agent.delegate`,
/// `agent.wakeOrCreate`, `workspace.create` initialAgent — via
/// [`Services::agent_create_op`]; step 1 (explicit client `model`) is handled
/// by the caller. Also reusable standalone (e.g. `specialist.get/list`
/// `resolvedModel`) so previews match what a no-model create actually pins.
///
/// Precedence (steps 2–5; the former `modelTier` step is retired — the key
/// is tolerated-and-ignored in frontmatter/wire specs, PROTOCOL §5.11):
/// 2. Specialist frontmatter `model` — only if it belongs to the resolved
///    provider.
/// 3. Settings chain ([`resolve_default_model_from_settings`]) —
///    provider-guarded.
/// 4. The **cached** provider catalog's `isDefault` row (PROTOCOL §5.30) —
///    cache-only, never a probe: pinning it freezes the model even if the
///    provider later changes its default. Cold cache or no marked row falls
///    through.
/// 5. `None` → provider CLI default (`session.model` stays unset).
///
/// The chain is background-agnostic (monorepo#1729): the quick-action model
/// settings apply to single-shot quick actions only, so a delegated
/// specialist resolves the provider default exactly like an interactive
/// session does.
pub(crate) fn resolve_agent_default_model(
    services: &Services,
    specialist: Option<&str>,
    workspace_path: Option<&Path>,
    provider: Option<&str>,
) -> Option<String> {
    resolve_agent_default_model_with_source(services, specialist, workspace_path, provider).0
}

/// [`resolve_agent_default_model`] plus the [`DefaultModelSource`] rung that
/// produced the value, for callers that must distinguish a settings-chain
/// default from a specialist pin (creation-time effort resolution).
pub(crate) fn resolve_agent_default_model_with_source(
    services: &Services,
    specialist: Option<&str>,
    workspace_path: Option<&Path>,
    provider: Option<&str>,
) -> (Option<String>, DefaultModelSource) {
    // Normalize through provider_config so legacy default-provider aliases
    // guard as the provider the spawn would actually run. With no explicit
    // provider, guard against the settings-derived default
    // (model.defaultProvider). With neither there is no effective provider
    // (monorepo#3044: no positional last resort) — the provider-keyed steps
    // below are skipped.
    let derived;
    let effective_provider: Option<&str> = if let Some(p) = provider {
        Some(intent_providers::provider_config(p).id)
    } else {
        derived = crate::agent_session::derived_default_provider(&services.effective_settings());
        derived
            .as_deref()
            .map(|p| intent_providers::provider_config(p).id)
    };

    if let Some(spec_id) = specialist {
        let specialists_svc = services.specialists_service();

        // Step 2: specialist frontmatter `model` (3-tier: project > user >
        // bundled) — only if it belongs to the resolved provider; a model
        // owned by another provider falls through instead of leaking.
        if let Some(m) = specialists_svc.resolve_model(spec_id, workspace_path) {
            if default_model_belongs_to_provider(services, effective_provider, &m) {
                return (Some(m), DefaultModelSource::Specialist);
            }
            tracing::debug!(
                model = m,
                provider = effective_provider.unwrap_or_default(),
                specialist = spec_id,
                "specialist frontmatter model belongs to another provider; ignoring"
            );
        }
    }

    // Step 3: settings chain, provider-guarded — a configured default owned
    // by another provider must not be pinned (monorepo#607); drop to the
    // catalog/CLI default instead of rejecting a model the caller never sent.
    if let Some(m) = resolve_default_model_from_settings(services, provider) {
        if default_model_belongs_to_provider(services, effective_provider, &m) {
            return (Some(m), DefaultModelSource::Settings);
        }
        tracing::warn!(
            model = m,
            provider = effective_provider.unwrap_or_default(),
            "configured default model belongs to another provider; \
             falling back to the catalog/CLI default"
        );
    }

    // Step 4: the cached catalog's `isDefault` row for the resolved provider
    // (PROTOCOL §5.5). Cache-only — never a probe on the creation path; the
    // row is the provider's own by construction, so no ownership guard is
    // needed. Pinning it to session.model freezes the model for the
    // session's lifetime even if the provider later changes its default.
    if let Some(m) =
        effective_provider.and_then(|p| services.models_catalog.cached_default_model(p))
    {
        return (Some(m), DefaultModelSource::CatalogDefault);
    }

    // Step 5: None → provider CLI default.
    (None, DefaultModelSource::CliDefault)
}

/// Ownership guard for resolver-derived defaults (never explicit client
/// models). Models are bare ids and reuse
/// [`ensure_bare_model_matches_provider`]'s asymmetric evidence rules
/// (cached dynamic catalogs; absence of evidence passes). A legacy compound
/// `provider:model` value (e.g. an old on-disk specialist frontmatter or
/// settings default that predates the wire-level rejection) never passes —
/// `session.model` must stay a bare id. With no effective provider at all
/// (monorepo#3044), a bare id has nothing to be validated against and falls
/// through.
fn default_model_belongs_to_provider(
    services: &Services,
    effective_provider: Option<&str>,
    model: &str,
) -> bool {
    if model.contains(':') {
        return false;
    }
    let Some(effective_provider) = effective_provider else {
        return false;
    };
    ensure_bare_model_matches_provider(
        "agent.create",
        &services.models_catalog,
        effective_provider,
        model,
    )
    .is_ok()
}

/// Reject a provider id that is not in the ACP registry with `-32602`
/// (`InvalidParams`). Persisting an unknown provider would make the spawn path
/// silently fall back to the default binary; hard-fail at the front door
/// instead (PROTOCOL §5.5). `method` names the rejecting RPC in the message.
fn ensure_known_provider(method: &str, provider_id: &str) -> Result<()> {
    if intent_providers::find_provider(provider_id).is_none() {
        return Err(Error::InvalidParams(format!(
            "{method}: unknown provider: {provider_id} (known providers: {})",
            intent_providers::all_provider_ids().join(", ")
        )));
    }
    Ok(())
}

/// Reject a bare model id that provably belongs to a different provider with
/// `-32602` (`InvalidParams`). Persisting the mismatch would make the spawn
/// path feed another provider's model id to `provider_id`'s binary
/// (monorepo#607). Ownership evidence is deterministic and probe-free: the
/// in-memory last-good `ModelCatalogCache` entries under each provider's
/// current registry version key — never a live fetch, so create/setModel
/// cannot block on a catalog probe. (The former static-tier evidence path
/// went with the tier tables.)
///
/// Evidence stays asymmetric: a cached-catalog claim by another provider
/// rejects only when the requested provider's ownership is affirmatively
/// *disproven* — its own cached catalog exists but lacks the id. With no
/// cache entry for the requested provider (cold start, bumped pin), the
/// bare id passes — absence of evidence is not a mismatch.
///
/// Two spawn-parity carve-outs:
/// - the literal `"default"` id is a "use the CLI default" *sentinel*, not
///   an ownership claim — it passes for every provider;
/// - `provider_id` is normalized through `provider_config` first, so legacy
///   default-provider aliases persisted on old sessions (`default`/`acp`/
///   `augment` — see `DEFAULT_PROVIDER_ALIASES`) compare as the provider the
///   spawn would actually run, not as the raw alias string.
pub(crate) fn ensure_bare_model_matches_provider(
    method: &str,
    cache: &crate::model_catalog::ModelCatalogCache,
    provider_id: &str,
    model_id: &str,
) -> Result<()> {
    if model_id == "default" {
        return Ok(());
    }
    let effective = intent_providers::provider_config(provider_id).id;
    // The requested provider provably owns the id via its own current cached
    // catalog — any other claim is a shared id, not a mismatch.
    let requested_cache = cache.cached_catalog_claims(effective, model_id);
    if requested_cache == Some(true) {
        return Ok(());
    }
    let cached_owners = cache.providers_claiming_model_cached(model_id);
    if !cached_owners.is_empty() && requested_cache == Some(false) {
        return Err(Error::InvalidParams(format!(
            "{method}: model {model_id} does not belong to provider {effective} \
             (providers with this model: {}); pass providerId to select the \
             intended provider",
            cached_owners.join(", ")
        )));
    }
    Ok(())
}

/// Reject a `reasoningEffort` level the resolved model provably does not
/// support with `-32602` (`InvalidParams`), naming the valid values. Evidence is
/// the cached model catalog's `effortLevels` for `model_id`
/// ([`crate::model_catalog::ModelCatalogCache::cached_effort_levels`]) — the
/// same probe-free, read-only rule as the bare-model ownership guard: with no
/// evidence (no model, no cached row, or a row declaring no levels) the value
/// passes through unvalidated, since providers own the effort vocabulary
/// (PROTOCOL §5.5). Matching is case-insensitive; the stored value is the
/// caller's spelling.
fn ensure_effort_supported_by_model(
    method: &str,
    cache: &crate::model_catalog::ModelCatalogCache,
    model_id: Option<&str>,
    effort: &str,
) -> Result<()> {
    let Some(levels) = model_id.and_then(|m| cache.cached_effort_levels(m)) else {
        return Ok(());
    };
    if levels.iter().any(|l| l.eq_ignore_ascii_case(effort)) {
        return Ok(());
    }
    Err(Error::InvalidParams(format!(
        "{method}: reasoningEffort {effort} is not supported by model {} (valid values: {})",
        model_id.unwrap_or_default(),
        levels.join(", ")
    )))
}

/// Pick the settings default reasoning effort (`model.defaultReasoningEffort`)
/// for a creation whose effort is still unresolved after the explicit /
/// model-option / frontmatter rungs.
///
/// The setting is a strict companion to the settings *default model*: it
/// applies only when the session's model itself resolved from the settings
/// chain ([`DefaultModelSource::Settings`]) — a caller-supplied model, a
/// specialist pin, or a fall-through to the provider CLI default all leave the
/// session effort unset.
///
/// Settings-chain leniency (mirroring the bare-model settings-chain fallback):
/// a level the resolved model's cached `effortLevels` provably does not list
/// is dropped with a warn, never a `-32602` — only caller-supplied efforts
/// reject. With no cached evidence the level passes through, matching
/// [`ensure_effort_supported_by_model`].
fn resolve_settings_default_reasoning_effort(
    services: &Services,
    model_source: DefaultModelSource,
    resolved_model: Option<&str>,
) -> Option<String> {
    if model_source != DefaultModelSource::Settings {
        return None;
    }
    let settings = services.effective_settings();
    let level = settings
        .model
        .default_reasoning_effort
        .as_deref()
        .map(str::trim)
        .filter(|l| !l.is_empty())?;
    match ensure_effort_supported_by_model(
        "agent.create",
        &services.models_catalog,
        resolved_model,
        level,
    ) {
        Ok(()) => Some(level.to_string()),
        Err(e) => {
            tracing::warn!(
                effort = level,
                model = resolved_model.unwrap_or_default(),
                error = %e,
                "configured default reasoning effort is not supported by the \
                 resolved default model; leaving the session effort unset"
            );
            None
        }
    }
}

/// Resolve the effective `reasoningEffort` for a delegated/woken child
/// (PROTOCOL §5.11), in precedence order: the caller's explicit `param`, then
/// the chosen model option's declared effort, then the specialist's
/// `reasoningEffort` frontmatter scalar, then unset.
///
/// An empty/whitespace-only `param` is an explicit clear and is returned
/// verbatim rather than as `None`: the create seam reads a present-but-blank
/// value as "the caller decided", so it neither falls through to the
/// specialist rungs here nor to the settings default
/// ([`resolve_settings_default_reasoning_effort`]) there. `None` means "no
/// rung resolved", which is what lets the settings default apply.
fn resolve_delegate_reasoning_effort(
    services: &Services,
    param: Option<&str>,
    specialist: Option<&str>,
    model: Option<&str>,
    workspace_path: Option<&Path>,
) -> Option<String> {
    if let Some(param) = param {
        return Some(param.to_string());
    }
    let spec_id = specialist?;
    let specialists_svc = services.specialists_service();
    model
        .and_then(|m| specialists_svc.resolve_model_option_effort(spec_id, workspace_path, m))
        .or_else(|| specialists_svc.resolve_reasoning_effort(spec_id, workspace_path))
}

/// Resolve the provider `agent.delegate` should spawn on when the caller
/// supplies no explicit `provider` param and no explicit `model` (spec
/// Decision D2). An explicit `provider` param (PROTOCOL §5.5) short-circuits
/// this derivation entirely at the call site; without it the daemon must
/// derive one itself instead of leaving `AgentCreateExtra.provider` unset —
/// which would fall through to the spawn path's positional last resort
/// regardless of the user's actual configured default.
///
/// 1. The specialist's frontmatter `codingAgent` (3-tier resolution). It
///    must be a known, available provider or the delegate fails with a
///    clear error (never silently substituted).
/// 2. The settings-derived default (`model.defaultProvider` —
///    [`crate::agent_session::derived_default_provider`]), with the same
///    known/available requirement.
/// 3. Neither is set: a clear `-32602` (monorepo#3044) — the former residual
///    `Ok(None)` left the session's `provider` unset, and spawn-time
///    resolution bottomed out at the first registered provider (auggie),
///    silently spawning a binary that may not be installed. Resolution that
///    falls through entirely now fails loudly at the front door instead.
fn resolve_delegate_provider(
    services: &Services,
    specialist: Option<&str>,
    workspace_path: Option<&Path>,
) -> Result<Option<String>> {
    let settings = services.effective_settings();

    if let Some(spec_id) = specialist {
        let specialists_svc = services.specialists_service();
        let explicit = specialists_svc.resolve_coding_agent(spec_id, workspace_path);
        if let Some(provider_id) = explicit {
            ensure_known_provider("agent.delegate", &provider_id)?;
            ensure_provider_available("agent.delegate", &provider_id, &settings.providers)?;
            return Ok(Some(provider_id));
        }
    }

    match crate::agent_session::derived_default_provider(&settings) {
        Some(derived) => {
            ensure_known_provider("agent.delegate", &derived)?;
            ensure_provider_available("agent.delegate", &derived, &settings.providers)?;
            Ok(Some(derived))
        }
        None => Err(crate::agent_session::no_default_provider_error(
            "agent.delegate",
        )),
    }
}

/// Preview-only mirror of [`resolve_delegate_provider`]'s resolution order —
/// the specialist's frontmatter `codingAgent` when it names a *known*
/// provider, else the settings-derived
/// default — but tolerant of an unknown/unavailable provider instead of
/// erroring, since a preview must never fail. `None` when nothing resolves
/// (monorepo#3044: no positional last resort) — the preview then shows the
/// provider CLI default. Used by [`Services::specialist_model_options`] so
/// the delegate-docs hint names the default each specialist's *own* provider
/// override would actually pin, instead of assuming every specialist spawns
/// on the shared settings-derived provider (a specialist pinned to another
/// provider previously showed that other provider's fallback/`None`).
pub(crate) fn resolve_delegate_provider_preview(
    services: &Services,
    specialist: Option<&str>,
    workspace_path: Option<&Path>,
) -> Option<String> {
    if let Some(spec_id) = specialist {
        let specialists_svc = services.specialists_service();
        let explicit = specialists_svc.resolve_coding_agent(spec_id, workspace_path);
        if let Some(provider_id) = explicit {
            if intent_providers::find_provider(&provider_id).is_some() {
                return Some(provider_id);
            }
        }
    }
    crate::agent_session::derived_default_provider(&services.effective_settings())
}

/// Reject a known provider id that the user explicitly disabled in the
/// `providers.enabled` settings map (`enabled[id] == false`, for providers
/// with [`intent_providers::ProviderConfig::can_be_disabled`]) with a
/// distinct "not enabled" `-32602` (monorepo#3178). Disabled beats installed:
/// a disabled provider must fail fast at every create/delegate front door
/// regardless of whether its binary (or npx) would resolve. An absent map or
/// absent entry means enabled (the settings default).
fn ensure_provider_enabled(
    method: &str,
    provider_id: &str,
    enabled: Option<&std::collections::BTreeMap<String, bool>>,
) -> Result<()> {
    let disableable =
        intent_providers::find_provider(provider_id).is_some_and(|p| p.can_be_disabled);
    if disableable && enabled.is_some_and(|m| m.get(provider_id) == Some(&false)) {
        let display = intent_providers::provider_config(provider_id).display_name;
        return Err(Error::InvalidParams(format!(
            "{method}: provider \"{provider_id}\" ({display}) is not enabled — enable it in \
             Settings > Agents."
        )));
    }
    Ok(())
}

/// Reject a known provider id that is disabled in settings, whose cached
/// auth verdict is hard-false, or that the daemon's own provider discovery
/// reports as unrunnable (not installed, or gated off by a missing env
/// var/feature code) with a clear, caller-surfaceable `-32602` — so the FE
/// can toast it — instead of letting the delegate succeed and the spawn fail
/// later with a raw "No such file or directory" (spec Decision D2 step 3).
/// The funnel runs disabled ([`ensure_provider_enabled`], monorepo#3178) →
/// not-authenticated ([`ensure_provider_authenticated`]) → unrunnable, each
/// with its own distinct message. Mirrors `resolve_spawn`'s override-aware resolution
/// (monorepo#1065) via [`intent_providers::provider_availability_for`], keyed
/// by the same `providers.paths` settings, and aligns "installed" with what
/// the spawn path can actually run: a provider whose only runnable path is
/// its npx fallback (`fallback_npx_package`, e.g. codex) counts as runnable
/// when npx resolves — exactly like `resolve_spawn`'s fallback tier.
fn ensure_provider_available(
    method: &str,
    provider_id: &str,
    providers: &intent_core::settings_file::ProvidersSettings,
) -> Result<()> {
    ensure_provider_enabled(method, provider_id, providers.enabled.as_ref())?;
    ensure_provider_authenticated(
        method,
        provider_id,
        crate::provider_auth::cached_auth_verdict(provider_id),
    )?;
    let availability = intent_providers::provider_availability_for(provider_id, &|key| {
        providers.paths.get(key).cloned()
    });
    ensure_provider_runnable(method, provider_id, availability, &|| {
        intent_providers::find_npx().is_some()
    })
}

/// Reject a provider whose cached auth verdict is explicitly `false` — the
/// daemon's last probe observed "not logged in" — with an actionable
/// `-32602` naming the CLI login remedy, before any session row is
/// persisted or adapter spawned. Rides the same create/delegate seam as the
/// disabled-provider gate (monorepo#3178): without it, a hard-false
/// provider passes every front door and the agent dies on its first turn
/// with a raw `-32000 Authentication required` from the adapter.
///
/// Gates ONLY on a hard `false`: an absent, expired, or inconclusive
/// (unknown) verdict stays permissive — inconclusive probes must never
/// block creates — and no probe runs here; the verdict is whatever the
/// `host.providerAuthStatus` cache last stored
/// ([`crate::provider_auth::cached_auth_verdict`]). The remedy names the
/// catalog login hint ([`intent_providers::login_command`]); for
/// claude-code it also spells out the desktop-app caveat — a Claude
/// desktop-app sign-in does not carry over to the CLI credential chain
/// (intent-hq/intent#3941), the exact trap that produced dead agents.
///
/// Accepted staleness: logging in does not invalidate a cached hard-false
/// verdict, so a retry within the cache TTL (60s) can still reject until
/// the entry expires or a forced `host.providerAuthStatus` refresh (the
/// FE's recheck) overwrites it — bounded and deemed acceptable.
fn ensure_provider_authenticated(
    method: &str,
    provider_id: &str,
    auth_verdict: Option<bool>,
) -> Result<()> {
    if auth_verdict != Some(false) {
        return Ok(());
    }
    let display = intent_providers::provider_config(provider_id).display_name;
    let login_cmd = intent_providers::login_command(provider_id);
    let caveat = if provider_id == "claude-code" {
        " Note: signing into the Claude desktop app does not carry over to the CLI — run \
         \"claude\" in a terminal, then \"/login\"."
    } else {
        ""
    };
    Err(Error::InvalidParams(format!(
        "{method}: provider \"{provider_id}\" ({display}) is not authenticated — run \
         \"{login_cmd}\" in a terminal, then retry.{caveat}"
    )))
}

/// The runnability half of [`ensure_provider_available`], with the npx probe
/// injected so unit tests can exercise the fallback arm deterministically
/// (the real probe scans the host's enhanced PATH). `npx_present` is only
/// consulted when the provider is not installed but declares an npx fallback.
fn ensure_provider_runnable(
    method: &str,
    provider_id: &str,
    availability: Option<intent_providers::ProviderAvailability>,
    npx_present: &dyn Fn() -> bool,
) -> Result<()> {
    let display = intent_providers::provider_config(provider_id).display_name;
    let Some(availability) = availability else {
        // Unknown ids are rejected by `ensure_known_provider` before this;
        // kept defensive for any future caller that skips it.
        return Err(Error::InvalidParams(format!(
            "{method}: provider \"{provider_id}\" ({display}) is not available — it is not a \
             registered provider."
        )));
    };
    if let Some(reason) = &availability.gated_off {
        return Err(Error::InvalidParams(format!(
            "{method}: provider \"{provider_id}\" ({display}) is not available — {reason}."
        )));
    }
    let runnable = availability.installed || (availability.has_npx_fallback && npx_present());
    if !runnable {
        return Err(Error::InvalidParams(format!(
            "{method}: provider \"{provider_id}\" ({display}) is not available — it is not \
             installed. Choose an available provider in Settings > Agents, or install {display}."
        )));
    }
    Ok(())
}

/// One pending message in an agent's in-memory send queue (`agent.getQueue`).
///
/// `editing` marks the entry as "under edit" — excluded from the **ready-to-send**
/// queue so the drain skips it (PROTOCOL §5.5/§6.5). The agent may go idle only
/// when every remaining queued entry has `editing == true`; setting `editing`
/// back to `false` re-includes the message and self-drains.
///
/// Serializes to camelCase JSON as the durable `agent_queue.payload` shape
/// (write-through persistence; see [`Services::persist_queue_snapshot`]). The
/// bool fields take `#[serde(default)]` so older payloads missing a later
/// flag still rehydrate.
// The independent bool flags ARE the durable payload shape; grouping them
// would break persisted-payload rehydration.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueuedMessage {
    pub id: String,
    /// Turn correlation id (monorepo#1022): stable across terminal-failure
    /// requeues so retries of the same logical turn share one id. Fresh
    /// enqueues set `turn_id = id`; `publish_error_status_and_requeue` mints
    /// a new entry `id` but carries the failed turn's original `turn_id`
    /// forward.
    /// `#[serde(default)]` keeps legacy persisted payloads decodable —
    /// rehydration backfills an empty `turn_id` with the entry `id`.
    #[serde(default)]
    pub turn_id: String,
    pub content: String,
    pub image_blocks: Option<Value>,
    pub file_blocks: Option<Value>,
    pub queued_at: String,
    #[serde(default)]
    pub editing: bool,
    /// `true` when the user-message row already reached the transcript before
    /// this entry was (re)queued. The terminal-failure requeue (STAB-112)
    /// carries the CONFIRMED durability of the pre-turn `persist_user` append
    /// (STAB-51) — `false` when that append failed, so the retry drain
    /// re-attempts it. Drain paths skip `persist_user` for such entries so a
    /// retry does not duplicate the user message in chat history. (The
    /// zero-output interrupt no longer requeues: it delivers the preempted
    /// message combined with the interrupt turn, monorepo#1014.)
    #[serde(default)]
    pub persisted: bool,
    /// `true` when this is a terminal-failure requeue (STAB-112); `to_value`
    /// emits `requeuedAfterFailure: true` on the wire so the FE shows
    /// "failed — will retry" only for genuine failures.
    #[serde(default)]
    pub requeued_after_failure: bool,
    /// Per-message `messageMetadata` captured at enqueue time (e.g. the
    /// `event_notification` payload of a parent wake that arrived while a turn
    /// was in flight). `to_value` emits it as `messageMetadata` when present,
    /// and drain paths persist it on the user message row so the transcript
    /// carries the same metadata as a directly-delivered wake.
    pub message_metadata: Option<Value>,
    /// Combined-delivery carry-over (monorepo#1014): the preempted message's
    /// text captured by a zero-output interrupt. Populated on two paths: at
    /// initial enqueue when the interrupt falls back to the queue instead of
    /// streaming (quarantine park, concurrent-send slot race, append-failure
    /// auto-queue — threaded via `QueuedPrepend`, monorepo#1034), and by the
    /// terminal-failure requeue after the interrupt turn failed. Either way
    /// the drain's rebuilt `TurnOptions` still delivers the preempted content
    /// ahead of the interrupt message. Prompt-only (both user rows are
    /// already persisted); not emitted on the `agent.getQueue` wire shape.
    #[serde(default)]
    pub prepend_content: Option<String>,
    /// Preempted message's image attachments, carried like `prepend_content`.
    #[serde(default)]
    pub prepend_image_blocks: Option<Value>,
    /// Preempted message's file attachments, carried like `prepend_content`.
    #[serde(default)]
    pub prepend_file_blocks: Option<Value>,
    /// `true` when this entry was enqueued with `priority: "interrupt"`
    /// (question hold / PROTOCOL §5.5): interrupt entries ALWAYS enter the
    /// queue ahead of all normal entries, preserving arrival order among
    /// themselves ([`Services::enqueue_message`]). Persisted so the ordering
    /// survives daemon restarts; `to_value` emits `interruptPriority: true`
    /// so queue snapshots reflect the marker.
    #[serde(default)]
    pub interrupt_priority: bool,
    /// `true` when the entry carries a USER-originated `agent.sendMessage`
    /// that was parked by a queue-fallback path (busy race, quarantine,
    /// append-failure). The question hold never blocks user messages
    /// (PROTOCOL §5.5), so the hold-gated drain paths deliver the first
    /// user-origin entry instead of suspending — without this marker a user
    /// answer parked by the turn-end busy race would deadlock against the
    /// hold its answer tag is supposed to release. Persisted so the bypass
    /// survives daemon restarts.
    #[serde(default)]
    pub user_origin: bool,
    /// Debounce-hold marker: `Some` marks the entry **held** — excluded from
    /// the ready-to-send queue (like `editing`) until `hold_until` passes or
    /// the hold is released/retracted via the `(agent, child_agent_id,
    /// hold_kind)` key. Persisted so holds survive daemon restarts
    /// ([`Services::rehydrate_agent_queues`] re-arms the release timers).
    #[serde(default)]
    pub hold_kind: Option<String>,
    /// RFC-3339 deadline at which the hold expires: the per-entry release
    /// timer ([`Services::arm_hold_release_timer`]) flushes the hold at this
    /// time and kicks delivery. A held entry with a missing or unparseable
    /// deadline counts as already expired (fail open) so corrupt data can
    /// never strand a message.
    #[serde(default)]
    pub hold_until: Option<String>,
    /// Child agent whose activity this held entry debounces; together with
    /// `hold_kind` it forms the release/retract key.
    #[serde(default)]
    pub child_agent_id: Option<String>,
}

/// `hold_kind` for a debounced `agent.reportToParent` progress wake parked on
/// the parent's queue (spec Design §2): flushed as-is when the window expires,
/// retracted and folded into the terminal wake when the child settles first.
pub(crate) const REPORT_DEBOUNCE_HOLD_KIND: &str = "report-debounce";

impl QueuedMessage {
    /// The camelCase wire shape for `agent.getQueue` / queue results, matching the
    /// TS `QueuedMessage` and the iOS decoder (`{id, content, queuedAt, position,
    /// imageBlocks?, fileBlocks?, editing?, requeuedAfterFailure?}`). `position` is
    /// the entry's 0-based index in the queue (0 = next to be sent) and is supplied
    /// by the caller since it is positional. `editing` is only present when `true`
    /// (a client that hasn't migrated still sees the legacy shape unchanged).
    /// `requeuedAfterFailure` is only present when `true` (STAB-112: backward-compatible
    /// marker for terminal-failure requeues). `messageMetadata` is only present
    /// when the entry was enqueued with metadata (e.g. a parent wake's
    /// `event_notification` payload) — entries without it keep the legacy shape.
    /// `turnId` is only present when set (monorepo#1022: correlation id stable
    /// across requeues; entries rehydrated from legacy payloads always have one
    /// backfilled).
    pub(crate) fn to_value(&self, position: usize) -> Value {
        let mut v = json!({
            "id": self.id,
            "content": self.content,
            "queuedAt": self.queued_at,
            "position": position,
        });
        if !self.turn_id.is_empty() {
            v["turnId"] = Value::String(self.turn_id.clone());
        }
        if let Some(blocks) = &self.image_blocks {
            v["imageBlocks"] = blocks.clone();
        }
        if let Some(blocks) = &self.file_blocks {
            v["fileBlocks"] = blocks.clone();
        }
        if self.editing {
            v["editing"] = Value::Bool(true);
        }
        if self.requeued_after_failure {
            v["requeuedAfterFailure"] = Value::Bool(true);
        }
        if let Some(md) = &self.message_metadata {
            v["messageMetadata"] = md.clone();
        }
        if self.interrupt_priority {
            v["interruptPriority"] = Value::Bool(true);
        }
        if let Some(kind) = &self.hold_kind {
            v["holdKind"] = Value::String(kind.clone());
        }
        if let Some(until) = &self.hold_until {
            v["holdUntil"] = Value::String(until.clone());
        }
        if let Some(child) = &self.child_agent_id {
            v["childAgentId"] = Value::String(child.clone());
        }
        v
    }

    /// `true` while the entry carries an **unexpired** hold marker: excluded
    /// from the ready-to-send queue until `hold_until` passes or the hold is
    /// released/retracted. A hold with a missing or unparseable `hold_until`
    /// counts as expired (fail open) so corrupt data can never strand a
    /// message.
    pub(crate) fn is_held(&self) -> bool {
        self.hold_kind.is_some()
            && self
                .hold_until
                .as_deref()
                .and_then(parse_iso)
                .is_some_and(|t| t > time::OffsetDateTime::now_utc())
    }

    /// The ready-to-send predicate shared by every drain path and idle gate
    /// (PROTOCOL §5.5/§6.5): not under edit and not held.
    pub(crate) fn ready_to_send(&self) -> bool {
        !self.editing && !self.is_held()
    }
}

/// Combined-delivery carry-over bundle (monorepo#1014 / monorepo#1034): the
/// preempted message's content threaded from the caller's `TurnOptions` into
/// an enqueued entry, so the queue-fallback paths (concurrent-send slot race,
/// quarantine park, append-failure auto-queue) still deliver the preempted
/// message ahead of the interrupt message when the entry drains.
///
/// Serializes to camelCase JSON as the durable `agent_stop_redelivery.payload`
/// shape (intent-hq/monorepo#1899): the zero-output stop-redelivery arm mirrors
/// the in-memory payload write-through so it survives a daemon restart. The
/// fields take `#[serde(default)]` so older payloads missing a later field
/// still rehydrate.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueuedPrepend {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub image_blocks: Option<Value>,
    #[serde(default)]
    pub file_blocks: Option<Value>,
}

/// Collect the `text` of every `type: "text"` content block in a message's
/// `content` (a JSON array of blocks; non-arrays yield nothing).
fn text_blocks(content: &Value) -> Vec<String> {
    let Some(blocks) = content.as_array() else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Remove all `start..=end` delimited spans from `s` (inclusive of the markers).
fn strip_spans(s: &str, start: &str, end: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(start) {
        out.push_str(&rest[..i]);
        let after = &rest[i + start.len()..];
        if let Some(j) = after.find(end) {
            rest = &after[j + end.len()..];
        } else {
            rest = "";
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Drop `<group:…>` / `</group…>` tags (the streamed response group markers).
fn strip_group_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('<') {
        let tail = &rest[i..];
        let is_group = tail.starts_with("<group:") || tail.starts_with("</group");
        if is_group {
            if let Some(j) = tail.find('>') {
                out.push_str(&rest[..i]);
                rest = &tail[j + 1..];
                continue;
            }
        }
        out.push_str(&rest[..=i]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
    out
}

/// Clean an assistant text block of digest/suggested-prompts/group markers,
/// mirroring the TS `agent.list` cleaning before the last-line extraction.
fn clean_response_text(text: &str) -> String {
    let mut cleaned = strip_spans(text, "<agent_digest>", "</agent_digest>");
    cleaned = strip_spans(&cleaned, "<!-- suggested-prompts", "-->");
    cleaned = strip_group_tags(&cleaned);
    cleaned.trim().to_string()
}

/// Derive `(lastAgentResponse, digest)` from the most-recent assistant message,
/// porting the TS `agent.list`/`agent.get` post-processing (PROTOCOL §5.5).
pub(crate) fn last_response_and_digest(
    messages: &[AgentMessage],
) -> (Option<String>, Option<String>) {
    for msg in messages.iter().rev() {
        if msg.role != "assistant" {
            continue;
        }
        return last_response_and_digest_from_blocks(&text_blocks(&msg.content));
    }
    (None, None)
}

/// [`last_response_and_digest`] over pre-extracted text-block strings — the
/// shared core also fed by the store's text-only projection (P1b), whose
/// capped blocks keep their tails so the last-line/digest extraction here is
/// unaffected by the cap, and by the `agent:stream:activity` /
/// `agent:stream:end` live-preview payloads (`agent_session.rs`).
pub(crate) fn last_response_and_digest_from_blocks(
    blocks: &[String],
) -> (Option<String>, Option<String>) {
    let mut digest: Option<String> = None;
    let mut last_response: Option<String> = None;
    for block in blocks {
        let text = block.trim();
        if text.is_empty() {
            continue;
        }
        if digest.is_none() {
            if let Some(d) = strip_spans_capture(text, "<agent_digest>", "</agent_digest>") {
                digest = Some(d.trim().to_string());
            }
        }
        let cleaned = clean_response_text(text);
        if !cleaned.is_empty() {
            let line = cleaned.lines().rfind(|l| !l.trim().is_empty()).map_or_else(
                || cleaned.chars().take(200).collect(),
                |l| l.trim().to_string(),
            );
            last_response = Some(line);
        }
    }
    (last_response, digest)
}

/// Live/mid-turn variant of [`last_response_and_digest_from_blocks`] for the
/// in-flight preview surfaces (`agent:stream:activity` and the `AgentLite`
/// live-turn overlay): when the final text block is still streaming
/// (`final_block_open`), its trailing partial line (text after the last
/// newline) is clipped before the last-line extraction — the preview advances
/// on newline boundaries and never surfaces a partially-streamed line. A turn
/// that has not completed any non-empty line yet yields `None` (same as the
/// pre-first-token case). A final text block CLOSED by a non-text block
/// boundary (e.g. a tool call flushed it and no new text has streamed since)
/// is complete even without a trailing newline, so the caller passes
/// `final_block_open: false` and no clipping applies. The digest is derived
/// from the UNCLIPPED text: its capture already requires the closing tag, so
/// a fully-streamed `<agent_digest>…</agent_digest>` surfaces immediately
/// while a partial span never leaks (the cleaning strips an unclosed opener
/// to end-of-text). Terminal/persisted callers keep using
/// [`last_response_and_digest_from_blocks`] unchanged.
pub(crate) fn live_response_and_digest_from_blocks(
    blocks: &[String],
    final_block_open: bool,
) -> (Option<String>, Option<String>) {
    if !final_block_open {
        return last_response_and_digest_from_blocks(blocks);
    }
    let (_, digest) = last_response_and_digest_from_blocks(blocks);
    let (last_response, _) =
        last_response_and_digest_from_blocks(&clip_trailing_partial_line(blocks));
    (last_response, digest)
}

/// Drop the still-streaming trailing partial line from live-turn text blocks:
/// only the FINAL block is mid-stream (earlier blocks were closed by a
/// non-text block boundary and pass through unchanged), so its text is
/// clipped at the last newline (inclusive); a final block with no newline at
/// all is dropped entirely.
fn clip_trailing_partial_line(blocks: &[String]) -> Vec<String> {
    let mut out = blocks.to_vec();
    if let Some(last) = out.pop() {
        if let Some(i) = last.rfind('\n') {
            out.push(last[..=i].to_string());
        }
    }
    out
}

/// Derive `lastUserMessage` from the most-recent `user` message's text blocks
/// (joined), porting the TS `agent.list`/`agent.get` activity field.
pub(crate) fn last_user_message(messages: &[AgentMessage]) -> Option<String> {
    for msg in messages.iter().rev() {
        if msg.role != "user" {
            continue;
        }
        return user_text_from_blocks(&text_blocks(&msg.content));
    }
    None
}

/// [`last_user_message`] over pre-extracted text-block strings — the shared
/// core also fed by the store's text-only projection (P1b).
fn user_text_from_blocks(blocks: &[String]) -> Option<String> {
    let text = blocks.join("\n").trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

/// Capture the first `start..end` span's inner text (for digest extraction).
fn strip_spans_capture(s: &str, start: &str, end: &str) -> Option<String> {
    let i = s.find(start)?;
    let after = &s[i + start.len()..];
    let j = after.find(end)?;
    Some(after[..j].to_string())
}

/// Parse `auggie model list` output into `(value, label, description?)` rows,
/// porting the TS `parseModelListOutput` (`- Label [model-id]` + an optional
/// indented description on the next line).
pub(crate) fn parse_model_list_output(stdout: &str) -> Vec<(String, String, Option<String>)> {
    let lines: Vec<&str> = stdout.split('\n').collect();
    let mut models = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed.is_empty() || trimmed.starts_with("Available models") {
            i += 1;
            continue;
        }
        if let Some((label, value)) = parse_model_line(trimmed) {
            let mut description = None;
            if i + 1 < lines.len() {
                let next = lines[i + 1].trim();
                if !next.is_empty() && !next.starts_with('-') && !next.starts_with("Available") {
                    description = Some(next.to_string());
                    i += 1;
                }
            }
            models.push((value, label, description));
        }
        i += 1;
    }
    models
}

/// Parse a single `- Label [model-id]` line into `(label, value)`.
fn parse_model_line(line: &str) -> Option<(String, String)> {
    let rest = line.strip_prefix('-')?.trim_start();
    let open = rest.find('[')?;
    let close = rest[open + 1..].find(']')? + open + 1;
    let label = rest[..open].trim().to_string();
    let value = rest[open + 1..close].trim().to_string();
    if label.is_empty() || value.is_empty() {
        return None;
    }
    Some((label, value))
}

/// The per-provider `models.list` fallback response (PROTOCOL §5.30) when no
/// dynamic discovery succeeded: an empty list labeled `source: "static"` with
/// a `warning`, never an error. (The former static tier catalog went with the
/// tier tables — the provider CLI owns model discovery.)
pub(crate) fn static_provider_response(provider_id: &str, warning: &str) -> Value {
    json!({
        "providerId": provider_id,
        "models": [],
        "source": "static",
        "warning": warning,
    })
}

/// Resolve the auggie binary for daemon-side CLI fetches with an injectable
/// discovery step (unit-test seam for the resolution order): the explicit
/// override wins, else `discover` is consulted.
fn resolve_auggie_bin_with<F>(
    auggie_bin: Option<std::path::PathBuf>,
    discover: F,
) -> Option<std::path::PathBuf>
where
    F: FnOnce() -> Option<std::path::PathBuf>,
{
    auggie_bin.or_else(discover)
}

/// Resolve the auggie binary for daemon-side CLI fetches: the explicit
/// override (the [`crate::Services::with_auggie_bin`] test seam) wins, else
/// canonical discovery ([`intent_context::discovery::find_auggie`] —
/// auggie's own managed install (`~/.augment/bin`) first, then the
/// enhanced-PATH scan) so a packaged app with a minimal process PATH still
/// finds the CLI. Returns `None` when discovery fails, so callers keep
/// their static/transcript fallbacks.
fn resolve_auggie_bin(auggie_bin: Option<std::path::PathBuf>) -> Option<std::path::PathBuf> {
    resolve_auggie_bin_with(auggie_bin, intent_context::discovery::find_auggie)
}

/// Best-effort `agent.getModels` dynamic fetch: run `auggie model list`, parse
/// stdout (then stderr), and map to wire models. Returns `Ok(None)` when the
/// CLI is unavailable, hangs past [`AUGGIE_MODELS_TIMEOUT`], or yields
/// nothing, so the caller can degrade to an empty model list. The binary
/// comes from [`resolve_auggie_bin`] and runs via [`auggie_output`] (bounded,
/// exec PATH) so its co-located `node` resolves in a packaged-app
/// environment.
pub(crate) async fn fetch_auggie_models(
    auggie_bin: Option<std::path::PathBuf>,
) -> Result<Option<Vec<Value>>> {
    let Some(auggie) = resolve_auggie_bin(auggie_bin) else {
        return Ok(None);
    };
    let Some(output) = auggie_output(&auggie, &["model", "list"]).await else {
        return Ok(None);
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut parsed = parse_model_list_output(&stdout);
    if parsed.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        parsed = parse_model_list_output(&stderr);
    }
    if parsed.is_empty() {
        return Ok(None);
    }
    let models = parsed
        .into_iter()
        .map(|(value, label, description)| {
            let mut m = json!({ "id": value, "name": label, "provider": "auggie" });
            if let Some(d) = description {
                m["description"] = Value::String(d);
            }
            m
        })
        .collect();
    Ok(Some(models))
}

/// Parse `auggie model list --json` output into rich wire `ModelInfo` rows
/// (PROTOCOL §5.30), porting the TS `parseModelListJson`: expects
/// `{ models: [...] }`, maps `id` ← `shortName` and `name` ← `displayName`,
/// and skips rows missing either string. Optional picker metadata
/// (`description`, `modelGroupPriority`, `costTier`, `badges`, `effortLevels`,
/// `isDefault`, `priority`, `isLegacyModel`) is copied only when
/// present/non-empty; boolean flags are emitted only when true. Returns `None`
/// when the payload is not the expected JSON shape, so the caller falls back
/// to plain text.
pub(crate) fn parse_model_list_json(stdout: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(stdout.trim()).ok()?;
    let models = parsed.get("models")?.as_array()?;
    let mut out = Vec::new();
    for m in models {
        let (Some(short), Some(display)) = (
            m.get("shortName").and_then(Value::as_str),
            m.get("displayName").and_then(Value::as_str),
        ) else {
            continue;
        };
        let mut row = json!({ "id": short, "name": display, "provider": "auggie" });
        if let Some(d) = m.get("description").and_then(Value::as_str) {
            if !d.is_empty() {
                row["description"] = Value::String(d.to_string());
            }
        }
        for key in ["modelGroupPriority", "costTier", "priority"] {
            if let Some(v) = m.get(key).filter(|v| v.is_number()) {
                row[key] = v.clone();
            }
        }
        for key in ["badges", "effortLevels"] {
            if let Some(v) = m.get(key).and_then(Value::as_array) {
                if !v.is_empty() {
                    row[key] = Value::Array(v.clone());
                }
            }
        }
        if m.get("isDefault").and_then(Value::as_bool) == Some(true) {
            row["isDefault"] = Value::Bool(true);
        }
        if m.get("isLegacyModel").and_then(Value::as_bool) == Some(true) {
            row["isLegacyModel"] = Value::Bool(true);
        }
        out.push(row);
    }
    Some(out)
}

/// Post-process parsed `models.list` rows (PROTOCOL §5.30): preserve every row
/// and its metadata, then sort by `modelGroupPriority`, `priority`, and `name`
/// — missing priorities sort last (`999`).
pub(crate) fn finalize_model_rows(rows: Vec<Value>) -> Vec<Value> {
    fn priority(row: &Value, key: &str) -> f64 {
        row.get(key).and_then(Value::as_f64).unwrap_or(999.0)
    }
    fn name(row: &Value) -> &str {
        row.get("name").and_then(Value::as_str).unwrap_or("")
    }
    let mut rows = rows;
    rows.sort_by(|a, b| {
        priority(a, "modelGroupPriority")
            .total_cmp(&priority(b, "modelGroupPriority"))
            .then_with(|| priority(a, "priority").total_cmp(&priority(b, "priority")))
            .then_with(|| name(a).cmp(name(b)))
    });
    rows
}

/// Upper bound on one auggie CLI invocation (model list / session stats).
/// The models fetch is single-flighted, so a wedged CLI (e.g. blocked on a
/// TTY prompt) would otherwise stall every `models.list` caller — including
/// `forceRefresh` — daemon-wide instead of just its own. Matches the bounded
/// ACP (15s overall) and opencode (10s) probe sources.
const AUGGIE_MODELS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// [`tokio::process::Command::output`] bounded by
/// [`AUGGIE_MODELS_TIMEOUT`]; a timeout counts as fetch failure (`None`).
/// `kill_on_drop` reaps the child when the timeout cancels the output
/// future, so a wedged CLI does not leak past the failed probe. The child
/// runs with the exec PATH (`intent_context::discovery::exec_path`) so the
/// `.mjs` shim's `#!/usr/bin/env node` resolves in a packaged-app
/// environment.
async fn auggie_output(auggie: &std::path::Path, args: &[&str]) -> Option<std::process::Output> {
    tokio::time::timeout(
        AUGGIE_MODELS_TIMEOUT,
        tokio::process::Command::new(auggie)
            .args(args)
            .env("PATH", intent_context::discovery::exec_path(auggie))
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()
}

/// Best-effort `models.list` dynamic fetch (PROTOCOL §5.30), porting the
/// reference `fetchAuggieModels`: try `auggie model list --json` for the rich
/// rows, fall back to the plain-text parser ([`parse_model_list_output`]),
/// then preserve all rows and sort ([`finalize_model_rows`]). Returns
/// `None` when the CLI is unavailable, hangs past
/// [`AUGGIE_MODELS_TIMEOUT`], or yields nothing parseable, so the caller can
/// degrade to an empty model list. `auggie_bin` overrides discovery
/// (the [`crate::Services::with_auggie_bin`] test seam); otherwise the
/// binary comes from [`resolve_auggie_bin`].
pub(crate) async fn fetch_auggie_models_rich(
    auggie_bin: Option<std::path::PathBuf>,
) -> Option<Vec<Value>> {
    let auggie = resolve_auggie_bin(auggie_bin)?;
    let mut rows: Option<Vec<Value>> = None;
    if let Some(output) = auggie_output(&auggie, &["model", "list", "--json"]).await {
        rows = parse_model_list_json(&String::from_utf8_lossy(&output.stdout))
            .filter(|r| !r.is_empty());
    }
    if rows.is_none() {
        let output = auggie_output(&auggie, &["model", "list"]).await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut parsed = parse_model_list_output(&stdout);
        if parsed.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            parsed = parse_model_list_output(&stderr);
        }
        if !parsed.is_empty() {
            rows = Some(
                parsed
                    .into_iter()
                    .map(|(value, label, description)| {
                        let mut m = json!({ "id": value, "name": label, "provider": "auggie" });
                        if let Some(d) = description {
                            m["description"] = Value::String(d);
                        }
                        m
                    })
                    .collect(),
            );
        }
    }
    let finalized = finalize_model_rows(rows?);
    if finalized.is_empty() {
        None
    } else {
        Some(finalized)
    }
}

/// Parse the JSON emitted by `auggie session stats <sessionId> --json` into a
/// [`SessionStats`] (PROTOCOL §5.24). Tolerant of the CLI's richer shape:
/// `creditsUsed` is nullable (absent/non-numeric → `None`, i.e. not yet
/// computed), and the message/tool counts default to 0 when absent. Returns
/// `None` when the payload is not a JSON object (e.g. the plain-text line the
/// CLI prints when the command is unavailable), so the caller degrades
/// gracefully rather than failing.
pub(crate) fn parse_session_stats_output(stdout: &str) -> Option<SessionStats> {
    let value: Value = serde_json::from_str(stdout.trim()).ok()?;
    let obj = value.as_object()?;
    Some(SessionStats {
        credits_used: obj.get("creditsUsed").and_then(Value::as_f64),
        message_count: obj.get("messageCount").and_then(Value::as_u64).unwrap_or(0),
        tool_count: obj.get("toolCount").and_then(Value::as_u64).unwrap_or(0),
    })
}

/// Best-effort `agent.getSessionStats` CLI refresh: run
/// `auggie session stats <sessionId> --json` and parse stdout (then stderr).
/// Returns `None` when the CLI is unavailable, hangs past
/// [`AUGGIE_MODELS_TIMEOUT`], or emits nothing parseable, so the caller can
/// fall back to transcript-derived counts with `creditsUsed = null`.
/// The binary comes from [`resolve_auggie_bin`] (`auggie_bin` is the
/// [`crate::Services::with_auggie_bin`] test seam) and runs via
/// [`auggie_output`] (bounded, exec PATH) so its co-located `node` resolves.
pub(crate) async fn fetch_session_stats(
    auggie_bin: Option<std::path::PathBuf>,
    session_id: &AgentId,
) -> Option<SessionStats> {
    let auggie = resolve_auggie_bin(auggie_bin)?;
    let output = auggie_output(
        &auggie,
        &["session", "stats", session_id.0.as_str(), "--json"],
    )
    .await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_session_stats_output(&stdout).or_else(|| {
        let stderr = String::from_utf8_lossy(&output.stderr);
        parse_session_stats_output(&stderr)
    })
}

/// Transcript-derived `(messageCount, toolCount)` fallback used when the auggie
/// CLI is unavailable: every logged message counts, and every `tool_use` content
/// block counts as one tool call.
fn transcript_counts(messages: &[AgentMessage]) -> (u64, u64) {
    let mut tool_count = 0u64;
    for msg in messages {
        if let Some(blocks) = msg.content.as_array() {
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    tool_count += 1;
                }
            }
        }
    }
    (messages.len() as u64, tool_count)
}

/// Mint a stable user-message id (`user-msg-{uuid}`), mirroring the TS
/// `agent.sendMessage` `messageId` default.
pub(crate) fn new_message_id() -> String {
    format!("user-msg-{}", Uuid::new_v4())
}

/// The `agent:message` event payload for a persisted transcript row
/// (PROTOCOL §6.5): `{ agentId, messageId, role }` plus `appMessageId` when
/// the row carries a client-minted `userAppMessageId` (lifted from the row
/// metadata at append time) — the echo the FE dedup guard matches its
/// optimistic user message against. Rows without a client id keep the
/// pre-existing three-field shape (backward compatible). `turn_id` is the
/// turn correlation id (monorepo#1022) — present on user-row echoes emitted
/// by a turn that carries one, omitted otherwise (never `null`).
pub(crate) fn agent_message_event_payload(
    agent_id: &AgentId,
    message: &AgentMessage,
    turn_id: Option<&str>,
) -> Value {
    let mut payload = json!({
        "agentId": agent_id.0,
        "messageId": message.id,
        "role": message.role,
    });
    if let Some(app_id) = &message.app_message_id {
        payload["appMessageId"] = json!(app_id);
    }
    if let Some(tid) = turn_id {
        payload["turnId"] = json!(tid);
    }
    payload
}

/// The `agent:last-message` event payload (PROTOCOL §6.5): the id-only
/// `agent:message` echo enriched with the persisted preview projections the
/// write just computed — derived from the appended row itself, no extra
/// queries. A `user`/`assistant` row (by construction the session's newest
/// such message) carries `lastMessageRole`/`lastMessageId` plus its
/// role-specific preview: `lastAgentResponse` (assistant) or
/// `lastUserMessage` (user), and `lastToolUse` (the
/// [`intent_core::last_tool_use_preview`] of the row's content — present
/// only when the row carries a `tool_use` block; its ABSENCE on a
/// user/assistant echo means the session's `lastToolUse` is now cleared,
/// matching the overwritten 0098 column). System (and other) rows keep the
/// base echo shape — the preview columns were not touched, so no preview
/// fields ride along. Every optional field is omitted (never `null`).
pub(crate) fn agent_last_message_event_payload(
    agent_id: &AgentId,
    message: &AgentMessage,
    turn_id: Option<&str>,
) -> Value {
    let mut payload = agent_message_event_payload(agent_id, message, turn_id);
    if message.role != "user" && message.role != "assistant" {
        return payload;
    }
    payload["lastMessageRole"] = json!(message.role);
    payload["lastMessageId"] = json!(message.id);
    let blocks = text_blocks(&message.content);
    if message.role == "assistant" {
        if let (Some(last_response), _) = last_response_and_digest_from_blocks(&blocks) {
            payload["lastAgentResponse"] = json!(last_response);
        }
    } else if let Some(last_user) = user_text_from_blocks(&blocks) {
        payload["lastUserMessage"] = json!(last_user);
    }
    if let Some(preview) = intent_core::last_tool_use_preview(&message.content) {
        payload["lastToolUse"] = preview;
    }
    payload
}

/// Whether a wire `priority` requests interrupt delivery (PROTOCOL §5.5):
/// `"interrupt"` preempts the in-flight turn keep-alive; anything else (or
/// absent) is normal queue-vs-stream delivery.
pub(crate) fn is_interrupt_priority(priority: Option<&str>) -> bool {
    priority == Some("interrupt")
}

/// A2A sender-attribution header (intent-hq/intent#3721, monorepo#1015): prepends
/// [`crate::harness::Harness::a2a_sender_note`] (+ blank line) to an
/// agent-origin send's content when `message_metadata` is an object carrying
/// a string `fromAgentId` — daemon-stamped by the MCP bindings, never
/// caller-controlled (the wire front doors strip caller-supplied attribution
/// at the router ingress) — using `fromAgentName` when present. No-op
/// otherwise, so user/FE sends stay byte-identical. Applied at the send
/// front doors (BEFORE persist/enqueue), so immediate deliveries, auto-queue
/// fallbacks and queued entries all inherit the header and
/// drain/flush/redrive never need to re-annotate. Idempotency is
/// **exact-match**: the daemon rebuilds the header it would emit from THIS
/// entry's stamped attribution and skips only when the content already
/// starts with exactly that header + blank line — byte-stable across the
/// layered front doors and requeues because the name is read from the same
/// stamped metadata, never a live lookup (same contract as the dequeue-wait
/// note). A caller-authored lookalike first line (any other `[MESSAGE FROM
/// AGENT…`) does NOT suppress annotation: the genuine header is prepended
/// ABOVE it, so the spoof visibly appears below the real attribution.
/// `persisted: true` requeues are untouched by construction (the annotation
/// never runs at drain time). Metadata is not modified: the
/// single-pending-message guard, `removeQueuedMessage` ownership and
/// `question_answers` intake all key on metadata and are unaffected.
pub(crate) fn annotate_sender_attribution(content: &mut String, message_metadata: Option<&Value>) {
    let Some(from_agent_id) = message_metadata
        .and_then(|md| md.get("fromAgentId"))
        .and_then(Value::as_str)
    else {
        return;
    };
    let name = message_metadata
        .and_then(|md| md.get("fromAgentName"))
        .and_then(Value::as_str);
    let note = crate::harness::latest().a2a_sender_note(name, from_agent_id);
    let annotated_head = format!("{note}\n\n");
    if content.starts_with(&annotated_head) {
        return;
    }
    *content = format!("{annotated_head}{content}");
}

/// Validate an FE-supplied `fileBlocks` array (PROTOCOL §5.5): every entry
/// must carry EXACTLY one of inline `data` (base64 payload) or an
/// attachment-registry `attachmentId` reference, both non-empty strings when
/// present. Both-or-neither is `Error::InvalidParams` (→ `-32602`) naming the
/// offending index. A non-array payload and non-object entries are tolerated
/// (skipped downstream like every other malformed attachment entry) so
/// legacy callers keep their fail-soft behavior.
pub(crate) fn validate_file_blocks(method: &str, file_blocks: Option<&Value>) -> Result<()> {
    let Some(files) = file_blocks.and_then(Value::as_array) else {
        return Ok(());
    };
    for (i, file) in files.iter().enumerate() {
        let Some(obj) = file.as_object() else {
            continue;
        };
        let has_data = obj.get("data").and_then(Value::as_str).is_some();
        let has_ref = obj
            .get("attachmentId")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty());
        if has_data == has_ref {
            return Err(Error::InvalidParams(format!(
                "{method}: fileBlocks[{i}] must carry exactly one of `data` or `attachmentId`"
            )));
        }
    }
    Ok(())
}

/// Validate an FE-supplied `imageBlocks` array (PROTOCOL §5.5,
/// monorepo#3338): every entry must carry EXACTLY one of inline `data`
/// (base64 payload) or an attachment-registry `attachmentId` reference.
/// Both-or-neither is `Error::InvalidParams` (→ `-32602`) naming the
/// offending index. A non-array payload and non-object entries are tolerated
/// (skipped downstream like every other malformed attachment entry) so
/// legacy callers keep their fail-soft behavior — mirrors
/// [`validate_file_blocks`].
pub(crate) fn validate_image_blocks(method: &str, image_blocks: Option<&Value>) -> Result<()> {
    let Some(imgs) = image_blocks.and_then(Value::as_array) else {
        return Ok(());
    };
    for (i, img) in imgs.iter().enumerate() {
        let Some(obj) = img.as_object() else {
            continue;
        };
        let has_data = obj.get("data").and_then(Value::as_str).is_some();
        let has_ref = obj
            .get("attachmentId")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty());
        if has_data == has_ref {
            return Err(Error::InvalidParams(format!(
                "{method}: imageBlocks[{i}] must carry exactly one of `data` or `attachmentId`"
            )));
        }
    }
    Ok(())
}

/// Byte cap for attachment-registry image references resolved into a turn
/// (monorepo#3338). 30 MiB of raw bytes base64-encode to exactly 40 MiB —
/// the transport's inbound frame cap that already bounds INLINE image
/// blocks — so a reference can never carry a larger image than the inline
/// arm could.
pub(crate) const IMAGE_REF_MAX_BYTES: u64 = 30 * 1024 * 1024;

/// The non-blank `attachmentId` values of reference-arm image entries
/// (entries WITHOUT inline `data`) — the same either-or reading as
/// [`validate_image_blocks`] / [`user_message_blocks`]: inline `data` wins,
/// and a blank reference counts as absent.
pub(crate) fn image_block_ref_ids(image_blocks: Option<&Value>) -> Vec<String> {
    image_blocks
        .and_then(Value::as_array)
        .map(|imgs| {
            imgs.iter()
                .filter_map(|img| {
                    let obj = img.as_object()?;
                    if obj.get("data").and_then(Value::as_str).is_some() {
                        return None;
                    }
                    obj.get("attachmentId")
                        .and_then(Value::as_str)
                        .filter(|s| !s.trim().is_empty())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The persisted content-block array for a user message: one `text` block
/// followed by any FE-supplied `image` / `file` attachment blocks (STAB-133:
/// attachments must reach the transcript so the conversation view can render
/// them). Image entries require EITHER inline `data` + `mimeType` OR an
/// attachment-registry `attachmentId` reference (monorepo#3338) and file
/// entries require `fileName` plus the same either-or (PROTOCOL §5.5) — the
/// same attachment contract prompt assembly (`append_attachment_blocks`)
/// enforces; malformed entries are silently skipped so a partial attachment
/// array never breaks the persist. Reference blocks persist AS references —
/// the bytes never ride the transcript row, keeping `agent.getConversation`
/// payloads constant-size.
pub(crate) fn user_message_blocks(
    content: &str,
    image_blocks: Option<&Value>,
    file_blocks: Option<&Value>,
) -> Value {
    let mut blocks = vec![json!({ "type": "text", "text": content })];
    if let Some(imgs) = image_blocks.and_then(Value::as_array) {
        for img in imgs {
            let data = img.get("data").and_then(Value::as_str);
            let mime = img.get("mimeType").and_then(Value::as_str);
            // Same non-blank filter as `validate_image_blocks` and prompt
            // assembly — a whitespace attachmentId must not shadow inline
            // data into a dangling blank reference.
            let attachment_id = img
                .get("attachmentId")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty());
            if data.is_none() {
                if let Some(id) = attachment_id {
                    let mut block = json!({ "type": "image", "attachmentId": id });
                    if let Some(mime) = mime {
                        block["mimeType"] = json!(mime);
                    }
                    blocks.push(block);
                    continue;
                }
            }
            if let (Some(data), Some(mime)) = (data, mime) {
                blocks.push(json!({ "type": "image", "data": data, "mimeType": mime }));
            }
        }
    }
    if let Some(files) = file_blocks.and_then(Value::as_array) {
        for file in files {
            let data = file.get("data").and_then(Value::as_str);
            let mime = file.get("mimeType").and_then(Value::as_str);
            let name = file.get("fileName").and_then(Value::as_str);
            // Same non-blank filter as `validate_file_blocks` and prompt
            // assembly — a whitespace attachmentId must not shadow inline
            // data into a dangling blank reference.
            let attachment_id = file
                .get("attachmentId")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty());
            if let (Some(id), Some(name)) = (attachment_id, name) {
                let mut block = json!({
                    "type": "file",
                    "attachmentId": id,
                    "fileName": name,
                });
                if let Some(mime) = mime {
                    block["mimeType"] = json!(mime);
                }
                if let Some(size) = file.get("size").and_then(Value::as_u64) {
                    block["size"] = json!(size);
                }
                blocks.push(block);
            } else if let (Some(data), Some(mime), Some(name)) = (data, mime, name) {
                blocks.push(json!({
                    "type": "file",
                    "data": data,
                    "mimeType": mime,
                    "fileName": name,
                }));
            }
        }
    }
    Value::Array(blocks)
}

/// Number of question resource blocks (`application/vnd.intent.question+json`
/// — the MIME type `ws.app.question.ask` emits; reused from `intent-acp` so
/// hold detection cannot drift from the binding) in a message's content-block
/// array. Non-array content counts zero.
pub(crate) fn question_block_count(content: &Value) -> usize {
    content
        .as_array()
        .map_or(0, |blocks| question_block_count_in(blocks))
}

/// [`question_block_count`] over an already-borrowed block slice, for callers
/// holding the blocks before they are wrapped into a `Value` (the turn-end
/// persist).
pub(crate) fn question_block_count_in(blocks: &[Value]) -> usize {
    blocks
        .iter()
        .filter(|b| {
            b.get("type").and_then(Value::as_str) == Some("resource")
                && b.pointer("/resource/mimeType").and_then(Value::as_str)
                    == Some(intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE)
        })
        .count()
}

/// `true` iff a message's content-block array carries at least one pending
/// question resource block.
pub(crate) fn has_question_blocks(content: &Value) -> bool {
    question_block_count(content) > 0
}

/// `messageMetadata.type` marker the FE's question wizard stamps on the
/// flattened `Q:`/`A:` answer message (PROTOCOL §5.5, question hold). The daemon
/// keys the pending-questions marker clear on this structured tag plus
/// [`ANSWERED_QUESTIONS_MESSAGE_ID_FIELD`] — never on the answer TEXT.
pub(crate) const QUESTION_ANSWERS_METADATA_TYPE: &str = "question_answers";

/// Field on a `question_answers` `messageMetadata` naming the assistant
/// message whose questions the row answers.
pub(crate) const ANSWERED_QUESTIONS_MESSAGE_ID_FIELD: &str = "answeredQuestionsMessageId";

/// The assistant message id a user row's `messageMetadata` claims to answer:
/// `Some` only for an object tagged `type: "question_answers"` carrying a
/// non-empty `answeredQuestionsMessageId` string. Pure; the daemon never
/// inspects the answer text.
pub(crate) fn answered_questions_message_id(metadata: Option<&Value>) -> Option<&str> {
    let md = metadata?;
    if md.get("type").and_then(Value::as_str) != Some(QUESTION_ANSWERS_METADATA_TYPE) {
        return None;
    }
    md.get(ANSWERED_QUESTIONS_MESSAGE_ID_FIELD)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// `messageMetadata.type` marker on the questions-dismissed system notice
/// (PROTOCOL §5.5, `agent.dismissQuestions`) — the FE keys on it to render
/// the notice as a system chip instead of a plain user message. Follows the
/// `hook_wake` / `event_notification` metadata conventions.
pub(crate) const QUESTIONS_DISMISSED_METADATA_TYPE: &str = "questions_dismissed";

/// `messageMetadata.type` marker on the proposal-resolved system notice
/// (PROTOCOL §5.5, `agent.resolveProposal`) — carried on BOTH outcomes
/// (`applied` and `dismissed`) so the FE renders the notice as a system chip
/// naming the proposal instead of a plain user message. Follows the
/// [`QUESTIONS_DISMISSED_METADATA_TYPE`] conventions.
pub(crate) const PROPOSAL_RESOLVED_METADATA_TYPE: &str = "proposal_resolved";

/// Cap on the caller-supplied `agent.resolveProposal` `detail` string — it is
/// appended verbatim to the applied notice, so bound it against oversized
/// payloads riding into the transcript.
const MAX_DETAIL_LEN: usize = 2_000;

/// Retention cap on the persisted `proposalResolutions` map: past it the
/// OLDEST entries are evicted on insert. Bounds both the session metadata
/// blob and the `AgentLite` projection that lifts the map into the hot
/// `agent.list` / `agent.get` payloads (the RPC cost contract). Idempotent
/// re-resolution of an evicted id degrades to `NotFound` — acceptable, the
/// entry is long-resolved and no longer renderable as a pending card.
const MAX_PROPOSAL_RESOLUTIONS: usize = 100;

/// Build the persisted `agent_session.metadata` blob for the create branch of
/// `agent.wakeOrCreate` (C1d-10a). Starts from any caller-supplied
/// `create.metadata` object (or `{}`), overlays the FE provenance fields the
/// tool guarantees (`createdByAgentId`, `delegationDepth`, `taskNoteId`,
/// `isBackground`, `source`), and folds `create.contextReferences` /
/// `create.agentType` in when present so a child's `agent.wakeOrCreate` can
/// read them back without a follow-up round-trip. Caller-supplied fields for
/// `taskNoteId`/`source`/`delegationDepth`/`createdByAgentId` are honored
/// verbatim only when the wake input did not supply the corresponding hint.
fn build_create_metadata(
    create_opts: &AgentWakeCreateOptions,
    input: &AgentWakeOrCreateInput,
    task_note_id: &NoteId,
    parent_depth: Option<i64>,
    agent_type: Option<String>,
) -> Option<Value> {
    let mut obj = create_opts
        .metadata
        .clone()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    if !obj.contains_key("taskNoteId") {
        obj.insert("taskNoteId".to_string(), json!(task_note_id.0));
    }
    if !obj.contains_key("source") {
        obj.insert("source".to_string(), json!(WAKE_OR_CREATE_SOURCE));
    }
    if !obj.contains_key("isBackground") {
        obj.insert("isBackground".to_string(), json!(true));
    }
    let child_depth = parent_depth.map_or(0, |d| d + 1);
    obj.entry("delegationDepth".to_string())
        .or_insert(json!(child_depth));
    if let Some(caller) = input.caller_agent_id.as_ref() {
        obj.entry("createdByAgentId".to_string())
            .or_insert(json!(caller.0));
    }
    if let Some(refs) = create_opts.context_references.as_ref() {
        obj.entry("contextReferences".to_string())
            .or_insert(refs.clone());
    }
    if let Some(agent_type) = agent_type {
        obj.entry("agentType".to_string())
            .or_insert(json!(agent_type));
    }
    if let Some(skip) = create_opts.skip_auto_commit {
        obj.entry("skipAutoCommit".to_string())
            .or_insert(json!(skip));
    }
    if obj.is_empty() {
        None
    } else {
        Some(Value::Object(obj))
    }
}

/// Build the `agent.wakeOrCreate` response envelope (C1d-10a). `action` is one
/// of `message_queued_to_active_agent` / `woke_existing` / `created_new` — the
/// 3-way discriminator the FE tool exposes. `cleanedUpAgentIds` is omitted
/// when empty so pre-widening callers that only inspect `ok`/`agentId`/
/// `created`/`result` stay wire-compatible.
fn build_wake_response(
    agent_id: &AgentId,
    agent_name: &str,
    created: bool,
    action: &str,
    task_title: &str,
    result: &Value,
    cleaned_up: &[AgentId],
) -> Value {
    let mut out = json!({
        "ok": true,
        "agentId": agent_id,
        "agentName": agent_name,
        "created": created,
        "action": action,
        "taskTitle": task_title,
        "result": result,
    });
    if !cleaned_up.is_empty() {
        out["cleanedUpAgentIds"] = json!(cleaned_up);
    }
    out
}

/// TS `isDelegatedBackgroundTaskSession`: a background session that was
/// delegated by another agent onto a task note. Both the delegator id and the
/// task linkage are read from the persisted `metadata` blob (the shape the
/// delegate/create writers populate), matching the reference field-for-field.
fn is_delegated_background_task_session(session: &AgentSession) -> bool {
    let md = session.metadata.as_ref();
    let md_is_background = md
        .and_then(|m| m.get("isBackground"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let md_has_str = |key: &str| {
        md.and_then(|m| m.get(key))
            .and_then(Value::as_str)
            .is_some()
    };
    (session.is_background || md_is_background)
        && md_has_str("createdByAgentId")
        && md_has_str("taskNoteId")
}

/// Project an [`AgentSession`] (with its loaded messages) into [`AgentLite`].
fn project_lite(session: AgentSession) -> AgentLite {
    let (last_response, digest) = last_response_and_digest(&session.messages);
    let last_user = last_user_message(&session.messages);
    let (last_role, last_id) = last_message_role_and_id(&session.messages);
    let last_tool_use = last_tool_use_from_messages(&session.messages);
    let count = session.messages.len() as u64;
    let mut lite = AgentLite::from_session(
        session,
        count,
        last_response,
        last_user,
        digest,
        last_role,
        last_id,
    );
    lite.last_tool_use = last_tool_use;
    lite
}

/// Derive `lastToolUse` from a loaded transcript: the newest
/// `user`/`assistant` message's last `tool_use` block preview — the same
/// [`intent_core::last_tool_use_preview`] the write path persists into the
/// 0098 column, so this loaded-transcript path and the projection paths
/// (which serve the persisted column) agree. `None` when that message
/// carries no `tool_use` block or no user/assistant message exists.
fn last_tool_use_from_messages(messages: &[AgentMessage]) -> Option<Value> {
    messages
        .iter()
        .rev()
        .find(|m| m.role == "user" || m.role == "assistant")
        .and_then(|m| intent_core::last_tool_use_preview(&m.content))
}

/// Derive `lastMessageRole`/`lastMessageId` from a loaded transcript: the
/// role and row id of the newest `user`/`assistant` message — system (and
/// any other) rows are transparent. `(None, None)` when no such message
/// exists (both wire fields are omitted). The projection paths serve the
/// same values from the persisted `agent_session.last_message_role` (0070)
/// and `last_message_id` (0088) columns.
fn last_message_role_and_id(messages: &[AgentMessage]) -> (Option<String>, Option<String>) {
    messages
        .iter()
        .rev()
        .find(|m| m.role == "user" || m.role == "assistant")
        .map_or((None, None), |m| (Some(m.role.clone()), Some(m.id.clone())))
}

/// Project a metadata-only [`AgentSession`] summary plus its bounded
/// [`intent_store::SessionMessageProjection`] into [`AgentLite`] — the
/// transcript-free equivalent of [`project_lite`] (monorepo#958). The derived
/// fields (`lastAgentResponse`/digest/`lastUserMessage`) only ever read the
/// newest assistant/user rows' text blocks, so feeding them the projection's
/// SQL-extracted capped blocks (P1b) yields output identical to a
/// full-transcript projection of the same data for any message within the
/// cap.
fn project_lite_from_projection(
    session: AgentSession,
    projection: &intent_store::SessionMessageProjection,
) -> AgentLite {
    let (last_response, digest) = projection
        .last_assistant_text_blocks
        .as_deref()
        .map_or((None, None), last_response_and_digest_from_blocks);
    let last_user = projection
        .last_user_text_blocks
        .as_deref()
        .and_then(user_text_from_blocks);
    let mut lite = AgentLite::from_session(
        session,
        projection.message_count,
        last_response,
        last_user,
        digest,
        projection.last_message_role.clone(),
        projection.last_message_id.clone(),
    );
    lite.last_tool_use.clone_from(&projection.last_tool_use);
    lite
}

/// Whether `blocks` contains a `tool_use` block with no matching `tool_result`
/// (matched by `tool_use_id == toolCallId`). The daemon-side port of the FE
/// `hasUnresolvedToolUse` content-block branch: a tool call that has been
/// emitted but whose result block has not yet been appended is "unresolved"
/// (the agent is blocked awaiting the tool).
fn has_unresolved_tool_use(blocks: &[Value]) -> bool {
    blocks.iter().any(|block| {
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            return false;
        }
        let Some(id) = block.get("toolCallId").and_then(Value::as_str) else {
            return false;
        };
        !blocks.iter().any(|candidate| {
            candidate.get("type").and_then(Value::as_str) == Some("tool_result")
                && candidate.get("tool_use_id").and_then(Value::as_str) == Some(id)
        })
    })
}

/// STAB-124: strip anonymous `tool_use` blocks (`name` missing/empty) and the
/// `tool_result` blocks paired to them (matched by `tool_use_id == toolCallId`)
/// from a message before serving it. Pre-fix daemons persisted this malformed
/// pair when an interrupt landed mid-tool-call; the FE conversation load chokes
/// on the empty name. Messages without such blocks pass through unchanged.
fn strip_anonymous_tool_blocks(mut message: AgentMessage) -> AgentMessage {
    fn is_anonymous_tool_use(b: &Value) -> bool {
        b.get("type").and_then(Value::as_str) == Some("tool_use")
            && b.get("name")
                .and_then(Value::as_str)
                .is_none_or(|n| n.trim().is_empty())
    }
    let Some(blocks) = message.content.as_array() else {
        return message;
    };
    if !blocks.iter().any(is_anonymous_tool_use) {
        return message;
    }
    let anonymous_ids: HashSet<&str> = blocks
        .iter()
        .filter(|b| is_anonymous_tool_use(b))
        .filter_map(|b| b.get("toolCallId").and_then(Value::as_str))
        .collect();
    let kept: Vec<Value> = blocks
        .iter()
        .filter(|b| match b.get("type").and_then(Value::as_str) {
            Some("tool_use") => !is_anonymous_tool_use(b),
            Some("tool_result") => b
                .get("tool_use_id")
                .and_then(Value::as_str)
                .is_none_or(|id| !anonymous_ids.contains(id)),
            _ => true,
        })
        .cloned()
        .collect();
    message.content = Value::Array(kept);
    message
}

/// monorepo#1114: stamp the stable synthetic `{messageId}:{index}` id onto any
/// content block that persisted without one, so `agent.getConversation`, the
/// seq-0 chat snapshot, and the §7.1 delta path (which re-reads through this
/// op) agree byte-for-byte on block identity. Assistant blocks always persist
/// with ids, so the pass is a no-op for them; non-assistant rows (user /
/// system / tool) gain the same id the delta path stamps. Serve-time only —
/// the stored rows are untouched, so the read stays idempotent. Runs AFTER
/// [`strip_anonymous_tool_blocks`] so indices match the served array.
fn stamp_synthetic_block_ids(mut message: AgentMessage) -> AgentMessage {
    let message_id = message.id.clone();
    let Some(blocks) = message.content.as_array_mut() else {
        return message;
    };
    for (index, block) in blocks.iter_mut().enumerate() {
        // An empty-string id is treated as missing — it can't serve as a
        // stable upsert key, so it gets the synthetic id like an absent one.
        if block
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty())
        {
            continue;
        }
        if let Some(obj) = block.as_object_mut() {
            obj.insert(
                "id".to_string(),
                Value::String(format!("{message_id}:{index}")),
            );
        }
    }
    message
}

/// Apply the slim conversation projection (PROTOCOL §5.5, opt-in via
/// `projection: "slim"`) to one served message. The block bounding itself
/// lives in [`crate::tool_block::slim_message_blocks`], shared with the live
/// `chat.subscribe` stream so deltas and snapshots agree byte-for-byte. Runs
/// AFTER [`stamp_synthetic_block_ids`] so the flags land on the final served
/// block identity.
fn apply_slim_projection(mut message: AgentMessage, thumbnails: Option<&Value>) -> AgentMessage {
    if let Some(blocks) = message.content.as_array_mut() {
        crate::tool_block::slim_message_blocks(blocks, thumbnails);
    }
    message
}

impl Services {
    /// `agent.listActive` (PROTOCOL §5.5): daemon-global mid-turn agents from
    /// the runtime manager's busy set. Only the small busy set reaches `SQLite`,
    /// and each lookup selects `updated_at` alone.
    pub(crate) async fn agent_list_active_op(&self) -> Result<Value> {
        let Some(manager) = self.agent_manager() else {
            return Ok(json!({ "streams": [] }));
        };
        let busy = manager.list_busy();
        if busy.is_empty() {
            return Ok(json!({ "streams": [] }));
        }

        let mut streams = Vec::with_capacity(busy.len());
        for (agent_id, workspace_id) in busy {
            // A busy agent whose session row is gone (e.g. a concurrent
            // `agent.delete` — an expected race elsewhere in the manager/store
            // paths) is skipped rather than failing the whole response.
            let updated_at = match self.store.get_agent_session_updated_at(&agent_id).await {
                Ok(updated_at) => updated_at,
                Err(Error::NotFound(_)) => {
                    tracing::debug!(
                        agent = %agent_id,
                        "agent.listActive: busy agent has no session row (likely \
                         deleted mid-turn); skipping"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            streams.push(json!({
                "agentId": agent_id,
                "sessionId": agent_id,
                "workspaceId": workspace_id,
                // `startTime` is derived from the session's `updated_at`:
                // `try_begin` persists the Active status transition (touching
                // `updated_at`) when the turn is claimed, so it approximates
                // the turn start without a dedicated column. The wire name is
                // part of the 4.1 contract (consumed by FE) — do not rename.
                "startTime": iso_ms(&updated_at),
            }));
        }
        Ok(json!({ "streams": streams }))
    }

    /// `agent.list` (PROTOCOL §5.5). Reads metadata-only session summaries
    /// plus the bounded per-workspace message projections (monorepo#958):
    /// a fixed number of store queries regardless of session count, and no
    /// message row beyond each session's newest user/assistant pair is ever
    /// fetched or decoded — full transcripts are never hydrated.
    ///
    /// List-payload cost contract (monorepo#2932): `metadata.initialMessage`
    /// — the full spawn-time first message, the single largest per-session
    /// field on real workspaces — is stripped from the whole [`AgentLite`]
    /// projection (`agent.list` AND `agent.get`; no client reads it off
    /// agent rows) and is served by `agent.getSession` only. The strip is
    /// SQL-deep: the summary SELECT behind these reads omits the
    /// `initial_message` column entirely (like `system_prompt` /
    /// `image_blocks`), so the bytes are never fetched or decoded.
    /// The remaining preview fields (`lastAgentResponse`,
    /// `lastUserMessage`, `lastToolUse`, `digest`,
    /// `metadata.completionReport` — read by list consumers like the HUD, so
    /// capped rather than omitted) are bounded per row to the render-sized
    /// [`intent_core::AGENT_LIST_PREVIEW_BUDGET_BYTES`]
    /// ([`AgentLite::cap_list_previews`]); the detail reads keep full values.
    /// Together these keep a ~250-session response well under the 1 MiB
    /// outbound frame warn threshold.
    ///
    /// Deliberate asymmetry: the agent channel's seq-0 snapshot goes through
    /// this op (capped rows), while per-agent deltas re-read via `agent.get`
    /// (full values) — the bound is a property of list-shaped reads, not a
    /// channel invariant. Delta frames are single-agent, so the size goal
    /// holds.
    ///
    /// Soft retire: the default read excludes retired sessions (SQL-side
    /// `retired_at IS NULL` filter — cost stays O(rows returned)); the wire
    /// `includeRetired: true` variant is
    /// [`Self::agent_list_including_retired_op`].
    pub(crate) async fn agent_list_op(&self, workspace_id: WorkspaceId) -> Result<Vec<AgentLite>> {
        self.agent_list_impl(workspace_id, AgentListScope::Active)
            .await
    }

    /// `agent.list` with `includeRetired: true` (PROTOCOL §5.5): every
    /// session including soft-retired ones, whose rows carry `retiredAt`.
    pub(crate) async fn agent_list_including_retired_op(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<AgentLite>> {
        self.agent_list_impl(workspace_id, AgentListScope::All)
            .await
    }

    /// `agent.list` with `retiredOnly: true` (PROTOCOL §5.5): ONLY
    /// soft-retired sessions, whose rows carry `retiredAt`. SQL-side
    /// `retired_at IS NOT NULL` filter on both the summary read AND the
    /// message-projection aggregate (served from the partial covering index
    /// `idx_agent_workspace_retired`), and the hook/PR-monitor overlays are
    /// skipped (retire cancels them, so retired rows always read empty) —
    /// cost stays O(retired rows returned), never touching the workspace's
    /// active sessions.
    pub(crate) async fn agent_list_retired_only_op(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<AgentLite>> {
        self.agent_list_impl(workspace_id, AgentListScope::RetiredOnly)
            .await
    }

    /// `retiredCount` (PROTOCOL §5.5): number of soft-retired sessions in
    /// the workspace — one SQL COUNT answered from the partial covering
    /// index `idx_agent_workspace_retired` (O(retired rows), O(1) when the
    /// bin is empty), attached to every `agent.list` response variant by
    /// the router.
    pub(crate) async fn agent_retired_count_op(&self, workspace_id: WorkspaceId) -> Result<u64> {
        self.store.count_retired_agent_sessions(&workspace_id).await
    }

    async fn agent_list_impl(
        &self,
        workspace_id: WorkspaceId,
        scope: AgentListScope,
    ) -> Result<Vec<AgentLite>> {
        let sessions = match scope {
            AgentListScope::All => {
                self.store
                    .list_agent_session_summaries(&workspace_id)
                    .await?
            }
            AgentListScope::Active => {
                self.store
                    .list_active_agent_session_summaries(&workspace_id)
                    .await?
            }
            AgentListScope::RetiredOnly => {
                self.store
                    .list_retired_agent_session_summaries(&workspace_id)
                    .await?
            }
        };
        // Message projections are the expensive half (full-workspace COUNT
        // aggregate + preview columns). Each scope loads a projection set
        // matching exactly its row set (RPC cost contract — O(rows
        // returned)): the default read serves the active-only set from the
        // per-workspace cache (invalidated on transcript writes and session
        // create/delete); `retiredOnly` loads the retired-only SQL variant
        // directly (never the full-workspace superset); `includeRetired`
        // loads the full set directly, bypassing the cache in both
        // directions.
        let mut projections = match scope {
            AgentListScope::Active => {
                self.agent_list_cache
                    .get_or_load(&self.store, &workspace_id)
                    .await?
            }
            AgentListScope::RetiredOnly => {
                self.store
                    .get_retired_agent_session_message_projections(&workspace_id)
                    .await?
            }
            AgentListScope::All => {
                self.store
                    .get_agent_session_message_projections(&workspace_id)
                    .await?
            }
        };
        // Idle-visibility: overlay each agent's active-hook metadata
        // (`waitingOnHooks`) and active PR monitors (`waitingOnPrMonitors`),
        // each omitted when empty, from one workspace-wide query apiece.
        // Retire cancels the owner's hooks and PR monitors
        // (`retire_one_session`), so retired rows always read empty — the
        // `retiredOnly` scope skips both workspace-wide queries entirely
        // rather than hydrating every active agent's hook history for rows
        // that cannot carry any (RPC cost contract).
        let (mut hooks_by_agent, mut pr_monitors_by_agent) = if scope == AgentListScope::RetiredOnly
        {
            (HashMap::new(), HashMap::new())
        } else {
            (
                self.active_hooks_by_agent(&workspace_id).await,
                self.active_pr_monitors_by_agent(&workspace_id).await,
            )
        };
        Ok(sessions
            .into_iter()
            .map(|s| {
                let projection = projections.remove(&s.id.0).unwrap_or_default();
                let waiting_on_hooks = hooks_by_agent.remove(&s.id.0).unwrap_or_default();
                let waiting_on_pr_monitors =
                    pr_monitors_by_agent.remove(&s.id.0).unwrap_or_default();
                let mut lite = self.project_lite_with_flags_from_projection(s, &projection);
                lite.waiting_on_hooks = waiting_on_hooks;
                lite.waiting_on_pr_monitors = waiting_on_pr_monitors;
                // List-payload cost contract: bound the render-preview
                // fields per row (see the doc comment above); the detail
                // reads keep full values. Applied AFTER the runtime overlay
                // so live-turn preview text is capped like persisted text.
                lite.cap_list_previews();
                lite
            })
            .collect())
    }

    /// Drop the cached agent.list message projections for `workspace_id`.
    /// Call after any successful `agent_message` write or session create/delete
    /// in this workspace so the next list reloads from `SQLite`.
    pub(crate) fn invalidate_agent_list_cache(&self, workspace_id: &WorkspaceId) {
        self.agent_list_cache.invalidate(&workspace_id.0);
    }

    /// `agent.get` (PROTOCOL §5.5). `NotFound` is surfaced to the router which
    /// maps it to `-32602 "Agent not found"`. When `workspace_id` is supplied
    /// the caller's workspace must match the session's; a mismatch surfaces as
    /// `NotFound` (defense-in-depth against bare-id probes across workspaces).
    ///
    /// Uses the metadata-only session lookup plus the bounded per-session
    /// message projection (monorepo#958, monorepo#981) — the transcript is
    /// never hydrated and no other session in the workspace is projected;
    /// only this session's newest user/assistant rows are fetched and decoded.
    pub(crate) async fn agent_get_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<AgentLite> {
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let projection = self
            .store
            .get_agent_session_message_projection(&agent_id)
            .await?;
        // Idle-visibility: overlay the agent's active-hook metadata
        // (`waitingOnHooks`, omitted when empty).
        let waiting_on_hooks = self.active_hooks_for_agent(&agent_id).await;
        // Idle-visibility (unified external-wait): same overlay for active
        // PR monitors (`waitingOnPrMonitors`, omitted when empty).
        let waiting_on_pr_monitors = self
            .active_pr_monitors_for_agent(&agent_id)
            .await
            .iter()
            .map(crate::pr_monitor::waiting_on_pr_monitors_entry)
            .collect();
        let mut lite = self.project_lite_with_flags_from_projection(session, &projection);
        lite.waiting_on_hooks = waiting_on_hooks;
        lite.waiting_on_pr_monitors = waiting_on_pr_monitors;
        Ok(lite)
    }

    /// Project an [`AgentSession`] into [`AgentLite`] and overlay the daemon-owned
    /// runtime activity flags (PROTOCOL §5.5/§7.1): `isResponding`,
    /// `isWaitingOnTool`, `isWaitingForOtherAgents`, `waitingForAgentIds`, plus
    /// the STAB-125 turn-liveness pair `turnInFlight`/`lastStreamActivityAt`.
    /// See [`agent_activity_flags_for`] and [`live_turn_liveness_for`]. The
    /// liveness pair reuses `is_responding` as its busy signal, so within one
    /// projection `turnInFlight` implies `isResponding` (the converse need not
    /// hold — a busy worker may not have opened its live-turn slot yet).
    pub(crate) fn project_lite_with_flags(&self, session: AgentSession) -> AgentLite {
        self.project_lite_with_flags_inner(session, project_lite)
    }

    /// [`project_lite_with_flags`](Self::project_lite_with_flags) over a
    /// metadata-only session summary plus its bounded message projection
    /// (monorepo#958) — same runtime-flag overlay, no transcript required.
    pub(crate) fn project_lite_with_flags_from_projection(
        &self,
        session: AgentSession,
        projection: &intent_store::SessionMessageProjection,
    ) -> AgentLite {
        self.project_lite_with_flags_inner(session, |s| project_lite_from_projection(s, projection))
    }

    /// The current effective `agentFeatures` values as the JSON snapshot
    /// shape stamped on new sessions (intent-hq/monorepo#2459). Used to
    /// project a value for legacy (pre-0096) rows whose `harness_features`
    /// is NULL; best-effort — an encode failure yields `None` and the wire
    /// field stays omitted.
    fn current_agent_features_snapshot(&self) -> Option<serde_json::Value> {
        serde_json::to_value(&self.effective_settings().agent_features).ok()
    }

    /// The `agentFeatures` values governing `session`'s runtime surface: the
    /// snapshot captured at creation (`harness_features`, monorepo#2459) when
    /// present and decodable, else the live effective settings (legacy
    /// pre-0096 rows, or a snapshot written by a newer daemon this build
    /// cannot decode). Respawns read this instead of the live settings so a
    /// settings change never alters an existing session's tools/prompt —
    /// matching what `harnessFeatures` reports on the wire.
    pub(crate) fn session_agent_features(
        &self,
        session: &AgentSession,
    ) -> intent_core::settings_file::AgentFeaturesSettings {
        session
            .harness_features
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_else(|| self.effective_settings().agent_features)
    }

    /// Lazy legacy freeze (intent-hq/monorepo#2459): when a legacy (pre-0096)
    /// session whose `harness_features` is still NULL is ACTIVATED — reaches
    /// `ensure_started`, the single choke point every turn (first spawn,
    /// resume, respawn, wake) funnels through — materialize the snapshot
    /// once: persist the currently-resolved `agentFeatures` values, with the
    /// legacy per-session taskGraph pin (0095 column) folded in — the pin
    /// wins over the live setting for that key, matching the pre-freeze
    /// COALESCE read. From then on the session reads its frozen snapshot like
    /// any new session. `harness_version` stays "1.0" — only the flags
    /// freeze. A no-op for rows that already carry a snapshot; before first
    /// activation NULL keeps the read-time live projection. Best-effort: a
    /// failed write WARNs and leaves the live-settings fallback in place.
    pub(crate) async fn materialize_legacy_harness_features(&self, session: &mut AgentSession) {
        if session.harness_features.is_some() {
            return;
        }
        let mut features = self.effective_settings().agent_features;
        match self
            .store
            .get_agent_session_task_graph_enabled(&session.id)
            .await
        {
            Ok(pinned) => features.task_graph = pinned,
            Err(e) => {
                tracing::warn!(agent_id = %session.id, error = %e,
                    "legacy feature freeze: taskGraph pin read failed; skipping");
                return;
            }
        }
        let Ok(snapshot) = serde_json::to_value(&features) else {
            tracing::warn!(agent_id = %session.id,
                "legacy feature freeze: agentFeatures snapshot encode failed; skipping");
            return;
        };
        match self
            .store
            .materialize_agent_session_harness_features(&session.id, &snapshot)
            .await
        {
            Ok(persisted) => session.harness_features = persisted,
            Err(e) => {
                tracing::warn!(agent_id = %session.id, error = %e,
                    "legacy feature freeze: snapshot write failed; session keeps live fallback");
            }
        }
    }

    /// Shared runtime-flag overlay behind the two `project_lite_with_flags*`
    /// entry points: compute the flags from the session, project it via
    /// `project`, then overlay.
    fn project_lite_with_flags_inner(
        &self,
        mut session: AgentSession,
        project: impl FnOnce(AgentSession) -> AgentLite,
    ) -> AgentLite {
        // Legacy rows (pre-0096) carry no captured agentFeatures snapshot:
        // project the CURRENT effective settings on read so the wire always
        // carries a value. This read path never persists — the row stays NULL
        // until its first activation freezes the snapshot
        // (`materialize_legacy_harness_features`).
        if session.harness_features.is_none() {
            session.harness_features = self.current_agent_features_snapshot();
        }
        let (is_responding, is_waiting_on_tool, is_waiting_for_other_agents, waiting_for_agent_ids) =
            self.agent_activity_flags_for(&session);
        let (turn_in_flight, last_stream_activity_at) =
            self.live_turn_liveness_for(&session, is_responding);
        // monorepo#940: derived on emit (never persisted) so a client
        // rehydrating via agent.list/agent.get after the failure event still
        // sees the corrupted flag.
        let session_corrupted = self.session_poisoned(&session);
        // Live-turn overlay: while a worker is draining an in-flight turn,
        // derive `lastAgentResponse`/`digest` from the live slot's streamed
        // text blocks so `agent.get`/`agent.list` track the
        // turn instead of staying pinned on the previous turn's persisted
        // preview. The live variant clips the trailing partial line only
        // while the final text block is still streaming; a final block
        // closed by a tool-call boundary surfaces its last line unclipped.
        // Per-field: a turn that has streamed no completed line yet (or no
        // digest yet) yields `None` for that field and the persisted-preview
        // value is kept.
        let live_overlay = if is_responding {
            self.live_turn_text_blocks(&session.id)
                .map(|(blocks, open)| live_response_and_digest_from_blocks(&blocks, open))
        } else {
            None
        };
        // Delete grace window (§5.5): overlay the pending-deletion deadline
        // from the in-memory registry (O(1) map lookup, never persisted).
        let pending_delete_at = self.pending_agent_deletes.deadline(session.id.0.as_str());
        let mut lite = project(session);
        lite.is_responding = is_responding;
        lite.is_waiting_on_tool = is_waiting_on_tool;
        lite.is_waiting_for_other_agents = is_waiting_for_other_agents;
        lite.waiting_for_agent_ids = waiting_for_agent_ids;
        lite.turn_in_flight = turn_in_flight;
        lite.last_stream_activity_at = last_stream_activity_at;
        // Context-occupancy overlay (intent-hq/intent#3797): the latest ACP
        // `usage_update` `used`/`size` for this agent, from the in-memory
        // registry (O(1) map lookup, never persisted). Omitted when no live
        // report exists (fresh session, daemon restart).
        lite.context_usage = self.context_usage_for(&lite.id);
        // Liveness overlay on `lastActivity` (monorepo#3647): the persisted
        // `updated_at` freezes at turn start (nothing persists until the turn
        // ends), so mid-turn the live-turn slot's stream stamp is the newer
        // truth. Serve the max of the two so pollers watching `lastActivity`
        // see a long-but-alive turn advance instead of reading as stalled.
        // Compare parsed instants — RFC-3339 strings carry variable
        // sub-second precision, so lexicographic order is not chronological.
        // An exact-instant tie prefers the live stamp (refreshed on every
        // stream event, so at least as fresh), matching the diagnostics path.
        if let Some(live) = lite.last_stream_activity_at.as_ref() {
            let newer = match lite.last_activity.as_deref() {
                None => true,
                Some(persisted) => match (parse_iso(live), parse_iso(persisted)) {
                    (Some(l), Some(p)) => l >= p,
                    _ => false,
                },
            };
            if newer {
                lite.last_activity = Some(live.clone());
            }
        }
        lite.session_corrupted = session_corrupted;
        lite.pending_delete_at = pending_delete_at;
        if let Some((live_response, live_digest)) = live_overlay {
            if live_response.is_some() {
                // The in-flight turn has derivable streamed text: the newest
                // (live) message is the assistant's, so `lastMessageRole`
                // flips with the response overlay. Pre-first-token the
                // persisted value (typically "user") is served unchanged.
                lite.last_message_role = Some("assistant".to_string());
                lite.last_agent_response = live_response;
            }
            if live_digest.is_some() {
                lite.digest = live_digest;
            }
        }
        lite
    }

    /// Compute the daemon-owned runtime activity flags for `session` — the port
    /// of the FE agent-state selectors so clients render verbatim (PROTOCOL
    /// §5.5/§7.1). Returns
    /// `(isResponding, isWaitingOnTool, isWaitingForOtherAgents, waitingForAgentIds)`:
    ///
    /// - `isResponding` — a worker is draining an in-flight turn for this agent
    ///   ([`agent_is_busy`], the authoritative "active worker" signal; mirrors the
    ///   FE `selectAgentIsResponding`). Builds on the existing busy/live-turn state
    ///   rather than adding a parallel notion of "busy".
    /// - `isWaitingOnTool` — that in-flight turn has an unresolved `tool_use` block
    ///   (a tool call awaiting its result; the port of FE `hasUnresolvedToolUse`).
    /// - `isWaitingForOtherAgents` — the agent parents one or more pending
    ///   completion watches (the port of FE `isAgentWaitingForOtherAgents`).
    ///   `report_delivered` watches are excluded via the shared
    ///   [`Services::waiting_watches_for_parent`] filter (issue
    ///   intent-hq/monorepo#1649), so the projection agrees with the
    ///   settlement predicate ([`Services::agent_is_waiting_on_agents`]).
    /// - `waitingForAgentIds` — the distinct `child_agent_id`s of those pending
    ///   watches, in registration order. Always returned (defaults to empty);
    ///   non-empty iff `isWaitingForOtherAgents` is `true`, so clients can render
    ///   the waiting-on names verbatim without consulting `metadata`.
    ///
    /// Terminal agents (completed/error/deleted) report all flags `false` and an
    /// empty `waitingForAgentIds`, mirroring the FE selectors' terminal-status
    /// short-circuit.
    pub(crate) fn agent_activity_flags_for(
        &self,
        session: &AgentSession,
    ) -> (bool, bool, bool, Vec<AgentId>) {
        let terminal = matches!(
            session.status,
            AgentStatus::Completed | AgentStatus::Error | AgentStatus::Deleted
        );
        if terminal {
            return (false, false, false, Vec::new());
        }
        let is_responding = self.agent_is_busy(session.id.clone());
        let is_waiting_on_tool = is_responding && self.live_turn_has_unresolved_tool(&session.id);
        let watches = self.waiting_watches_for_parent(&session.id);
        // Distinct child ids in registration order — a parent can register
        // multiple watches against the same child (e.g. successive `immediate`
        // delegates), but the FE only wants each waiting-on agent once.
        let mut waiting_for_agent_ids: Vec<AgentId> = Vec::with_capacity(watches.len());
        for w in &watches {
            if !waiting_for_agent_ids.contains(&w.child_agent_id) {
                waiting_for_agent_ids.push(w.child_agent_id.clone());
            }
        }
        let is_waiting_for_other_agents = !waiting_for_agent_ids.is_empty();
        (
            is_responding,
            is_waiting_on_tool,
            is_waiting_for_other_agents,
            waiting_for_agent_ids,
        )
    }

    /// Whether the agent's in-flight live turn (if any) is blocked on an
    /// unresolved tool call. `false` when no turn is streaming.
    fn live_turn_has_unresolved_tool(&self, agent_id: &AgentId) -> bool {
        self.live_turn(agent_id)
            .is_some_and(|live| has_unresolved_tool_use(&live.blocks))
    }

    /// Turn-liveness for `session` (STAB-125): `(turnInFlight,
    /// lastStreamActivityAt)`. `turnInFlight` is `true` while an active worker
    /// is draining a `session/prompt` turn whose live-turn slot is open;
    /// `lastStreamActivityAt` is the slot's most-recent stream-event timestamp,
    /// so a poller can tell a long-but-alive turn (timestamp advancing) from a
    /// wedged agent (timestamp pinned) even when nothing has persisted yet.
    /// (The stamp only refreshes on mapped `session/update` traffic, so during
    /// a long silent tool call it pins too — combine with `isWaitingOnTool` to
    /// avoid misclassifying a healthy-but-slow tool turn.)
    ///
    /// `is_busy` is the caller's [`agent_is_busy`](Self::agent_is_busy) read —
    /// the same authoritative "active worker" signal behind `isResponding` and
    /// `chat_snapshot`'s live-turn merge — threaded through so an orphan slot
    /// with no worker never reports a phantom in-flight turn and the pair stays
    /// consistent with `isResponding` within one projection. Terminal agents
    /// (completed/error/deleted) report `(false, None)`, mirroring
    /// [`agent_activity_flags_for`](Self::agent_activity_flags_for)'s terminal
    /// short-circuit.
    pub(crate) fn live_turn_liveness_for(
        &self,
        session: &AgentSession,
        is_busy: bool,
    ) -> (bool, Option<String>) {
        let terminal = matches!(
            session.status,
            AgentStatus::Completed | AgentStatus::Error | AgentStatus::Deleted
        );
        if terminal || !is_busy {
            return (false, None);
        }
        match self.live_turn_activity_at(&session.id) {
            Some(stamp) => (true, Some(stamp)),
            None => (false, None),
        }
    }

    /// `agent.getConversation` (PROTOCOL §5.5). Paginated per the TA-2 contract:
    /// the limit clamps to `[1,200]` (default 50) and an opaque `nextToken`
    /// walks backward to older pages. The `messages` array stays oldest→newest
    /// within a page (wire parity with the TS handler); `nextToken` is additive
    /// and is `null` once the oldest message has been returned.
    ///
    /// STAB-124 loading tolerance: rows persisted by pre-fix daemons can carry
    /// an anonymous `tool_use` block (`name: ""`, the fabricated echo of a
    /// tool call aborted by an interrupt) that breaks FE conversation loading.
    /// The served page strips those blocks (and their paired `tool_result`s)
    /// non-destructively — the stored rows are untouched, so the read is
    /// idempotent and covers old rows and restored backups alike.
    ///
    /// monorepo#1114: the served page also stamps the stable synthetic
    /// `{messageId}:{index}` id onto blocks that persisted without one
    /// ([`stamp_synthetic_block_ids`]), so snapshot and delta consumers see
    /// identical block identities.
    ///
    /// Pagination happens SQL-side (monorepo#958): the window is resolved
    /// against the row count and only the requested page is selected and
    /// decoded, so a `limit=N` read touches at most N rows regardless of
    /// transcript size. The token contract is unchanged from the in-memory
    /// implementation (`page_window` over the same oldest→newest indexing),
    /// so previously minted tokens still resolve to the same rows.
    ///
    /// Seek (`aroundMessageId` / `aroundIndex`): when present either takes
    /// precedence over any token and resolves to the page containing the
    /// target (`page_window_around`). `aroundMessageId` targets the row with
    /// that id (unknown id is `-32602` naming the id); `aroundIndex` targets
    /// the 0-based ordinal from the OLDEST message, clamped into
    /// `[0, totalMessages - 1]` so approximate client estimates never reject
    /// (negative values are rejected `-32602` at the transport boundary, as
    /// is supplying both seek params). Seek pages — and the forward
    /// continuations minted from them — additionally carry a `prevToken`
    /// cursor that walks newer toward the live tail (`null` once the newest
    /// message has been returned); their `nextToken` is the standard backward
    /// cursor, so older continuation is ordinary paging. Absent both params
    /// (and any seek-minted forward token), the response is byte-identical
    /// to before — no `prevToken` key is added.
    ///
    /// Slim page budget (§5.5): under `projection: "slim"` the served page is
    /// additionally bounded by [`SLIM_PAGE_BUDGET_BYTES`] total serialized
    /// bytes — `limit` counts messages, but a message can carry hundreds of
    /// (individually capped) blocks, so a message-counted slim page could
    /// still serialize to multiple MB. The trim keeps the page's anchor end
    /// (newest for legacy backward pages, oldest for forward continuations,
    /// the target for seek pages — always at least one message) and re-mints
    /// the continuation cursor(s) at the first excluded row, so existing
    /// token loops resume seamlessly with more round-trips. `totalMessages`
    /// / `truncated` semantics are unchanged (transcript-wide, not
    /// page-length). Slim is the wire default since v8.0; an unbudgeted full
    /// read (`projection: None`) survives only as an internal test seam.
    ///
    /// In-progress tail (monorepo#3647): with `include_in_progress` set and
    /// an in-flight turn (`turnInFlight` true), a page that ends at the live
    /// tail additionally carries the in-flight turn's partial assistant
    /// message — the live-turn slot's streamed blocks so far — appended as a
    /// trailing row marked `inProgress: true`. The row is serve-time only
    /// (nothing persists until the turn ends), runs through the same slim
    /// bounding as persisted rows, and is excluded from `totalMessages` /
    /// pagination (`truncated`, tokens). Non-tail pages (older continuations
    /// and seeks that do not reach the tail) never carry it. Absent the
    /// param, responses are byte-identical to before. Its `created_at` is
    /// the slot's stream stamp, so unlike persisted rows it advances across
    /// successive reads of the same turn — deliberate: the row IS the
    /// liveness signal, and the id is the stable reconciliation key.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn agent_get_conversation_op(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        workspace_id: Option<WorkspaceId>,
        page_token: Option<String>,
        around_message_id: Option<String>,
        around_index: Option<i64>,
        projection: Option<ConversationProjection>,
        include_in_progress: bool,
    ) -> Result<Value> {
        // Metadata-only scope check — the transcript is never hydrated here.
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        // Turn-liveness indicator (STAB-125): a long in-flight turn persists
        // nothing until it ends, so without these additive fields a
        // conversation read mid-turn is indistinguishable from a wedged agent.
        let is_busy = self.agent_is_busy(session.id.clone());
        let (turn_in_flight, last_stream_activity_at) =
            self.live_turn_liveness_for(&session, is_busy);
        // Count + page are two separate reads; consistent because the message
        // log is insert-only (appends only extend the tail, so existing row
        // positions never shift — a racing `replace_agent_messages` degrades
        // no worse than an already-stale page token).
        let total = usize::try_from(self.store.count_agent_messages(&agent_id).await?.max(0))
            .expect("value fits in usize");
        // Seek resolution: `aroundMessageId` / `aroundIndex` win over any
        // token (mutual exclusivity is enforced at the transport boundary);
        // a forward (`prevToken`-minted) cursor is recognized next; otherwise
        // the legacy backward contract applies unchanged. `prev_token` is
        // `Some(..)` only on seek/forward pages — the legacy path never adds
        // the `prevToken` key, keeping absent-param responses byte-identical.
        // The slim page budget's admission anchor mirrors the direction the
        // page's consumer walks (see `budget_page`): legacy backward pages
        // keep the newest suffix, forward continuations keep the oldest
        // prefix, seek pages keep the target and grow outward. Tracked here
        // (as a global target index for seeks) so a budget trim below can
        // re-mint the right cursor(s).
        let mut slim_anchor = crate::pagination::BudgetAnchor::Newest;
        let seek_win = if let Some(mid) = around_message_id {
            let index = self
                .store
                .get_agent_message_index(&agent_id, &mid)
                .await?
                .ok_or_else(|| Error::InvalidParams(format!("unknown message id: {mid}")))?;
            let index = usize::try_from(index.max(0)).expect("value fits in usize");
            slim_anchor = crate::pagination::BudgetAnchor::Target(index);
            Some(crate::pagination::page_window_around(total, limit, index))
        } else if let Some(idx) = around_index {
            // Ordinal seek: clamp into [0, total - 1] — client estimates are
            // approximate, so an overshooting index lands on the newest page
            // instead of erroring (negatives were rejected at the boundary).
            let clamped = usize::try_from(idx.max(0))
                .expect("non-negative")
                .min(total.saturating_sub(1));
            slim_anchor = crate::pagination::BudgetAnchor::Target(clamped);
            Some(crate::pagination::page_window_around(total, limit, clamped))
        } else {
            let w = page_token
                .as_deref()
                .and_then(|t| crate::pagination::forward_page_window(total, limit, t));
            if w.is_some() {
                slim_anchor = crate::pagination::BudgetAnchor::Oldest;
            }
            w
        };
        let (start, end, mut next_token, mut prev_token) = if let Some(w) = seek_win {
            (w.start, w.end, w.next_token, Some(w.prev_token))
        } else {
            let w = crate::pagination::page_window(total, limit, page_token.as_deref());
            (w.start, w.end, w.next_token, None)
        };
        // Global indices of the served page — updated by a budget trim
        // below; `page_end == total` means the page ends at the live tail
        // (where the in-progress row may be appended), and `page_start`
        // anchors token re-minting after the in-progress append's own
        // re-budget pass.
        let mut page_start = start;
        let mut page_end = end;
        // The slim projection reads the page AS STORED: an externalized
        // heavy body's stored placeholder IS the slim preview
        // (`intent_core::slim_heavy_body`, shared write/serve transform), so
        // hydrating multi-MB bodies only for `apply_slim_projection` to
        // re-truncate them would be pure waste — this is the hot
        // default-slim path's "no full-blob hydration" guarantee. The
        // full-fidelity (absent-projection) read hydrates as before.
        let page_offset = i64::try_from(start).expect("value fits in i64");
        let page_limit = i64::try_from(end - start).expect("value fits in i64");
        let raw_page = if projection == Some(ConversationProjection::Slim) {
            self.store
                .get_agent_messages_page_as_stored(&agent_id, page_offset, page_limit)
                .await?
        } else {
            self.store
                .get_agent_messages_page(&agent_id, page_offset, page_limit)
                .await?
        };
        let mut page: Vec<AgentMessage> = raw_page
            .into_iter()
            .map(strip_anonymous_tool_blocks)
            .map(stamp_synthetic_block_ids)
            .collect();
        if projection == Some(ConversationProjection::Slim) {
            // One bounded thumbnails read sized by the page (RPC cost
            // contract: O(rows returned); the common all-text page selects
            // nothing). Absent-projection reads never reach here, keeping
            // them byte-identical to before.
            let ids: Vec<String> = page.iter().map(|m| m.id.clone()).collect();
            let thumbnails = self
                .store
                .get_agent_message_thumbnails(&agent_id, &ids)
                .await?;
            page = page
                .into_iter()
                .map(|m| {
                    let thumbs = thumbnails.get(&m.id);
                    apply_slim_projection(m, thumbs)
                })
                .collect();
            // Page byte budget (§5.5): the per-block bound above caps each
            // body, but `limit` counts messages and a message can carry
            // hundreds of blocks, so a slim page could still serialize to
            // multiple MB. Trim the page to `SLIM_PAGE_BUDGET_BYTES` from
            // its anchor (the anchor row always serves, so a single
            // over-budget message still pages through) and re-mint the
            // continuation cursor(s) at the first excluded row, so existing
            // `nextToken`/`prevToken` loops resume with no gaps or
            // duplicates — they just make more round-trips. The full
            // (absent-projection) read never reaches here.
            let anchor = match slim_anchor {
                crate::pagination::BudgetAnchor::Target(global) => {
                    crate::pagination::BudgetAnchor::Target(global.saturating_sub(start))
                }
                other => other,
            };
            let sizes: Vec<usize> = page
                .iter()
                .map(crate::pagination::serialized_size)
                .collect();
            let (lo, hi) = crate::pagination::budget_page(&sizes, anchor, SLIM_PAGE_BUDGET_BYTES);
            if (lo, hi) != (0, page.len()) {
                page.truncate(hi);
                page.drain(..lo);
                page_start = start + lo;
                page_end = start + hi;
                next_token = crate::pagination::remint_backward_token(start + lo);
                if prev_token.is_some() {
                    prev_token = Some(crate::pagination::remint_forward_token(start + hi, total));
                }
            }
        }
        let mut messages = serde_json::to_value(&page).expect("messages serialize");
        // In-progress tail (monorepo#3647): append the in-flight turn's
        // partial assistant message when the caller opted in and this page
        // ends at the live tail, so a mid-turn read shows the tool calls and
        // text streamed so far instead of only persisted completed messages.
        // `turn_in_flight` already gates on a busy worker + open slot and
        // terminal statuses; the row is serve-time only and deliberately
        // outside `totalMessages`/pagination. Idempotent against the
        // persist/slot-clear race: if the turn's message already persisted
        // (id present in the served page) the append is skipped, mirroring
        // the seq-0 snapshot merge (`merge_live_turn`), so a read in that
        // window never duplicates the row.
        if include_in_progress && turn_in_flight && page_end == total {
            if let Some(live) = self.live_turn(&agent_id) {
                let arr = messages.as_array_mut().expect("messages is an array");
                let already_persisted = arr
                    .iter()
                    .any(|m| m.get("id").and_then(Value::as_str) == Some(live.message_id.as_str()));
                if !already_persisted {
                    let row = AgentMessage {
                        id: live.message_id.clone(),
                        agent_id: agent_id.clone(),
                        seq: i64::try_from(total).unwrap_or(i64::MAX),
                        role: "assistant".to_string(),
                        content: Value::Array(live.blocks),
                        metadata: None,
                        app_message_id: None,
                        created_at: live.last_activity_at.clone(),
                    };
                    let mut row = stamp_synthetic_block_ids(strip_anonymous_tool_blocks(row));
                    if projection == Some(ConversationProjection::Slim) {
                        row = apply_slim_projection(row, None);
                    }
                    let mut row = serde_json::to_value(row).expect("in-progress row serialize");
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert("inProgress".to_string(), json!(true));
                    }
                    arr.push(row);
                    // Re-apply the slim page budget after the append (§5.5),
                    // mirroring the seq-0 snapshot's `rebudget_merged_page`:
                    // `apply_slim_projection` caps block bodies but not block
                    // count, and the motivating scenario — a long tool-heavy
                    // turn — is exactly one that accumulates hundreds of
                    // capped blocks in the live slot, so appended beside an
                    // at-budget persisted page the response could blow the
                    // budget the read path just enforced. The live row is the
                    // newest and always serves; oldest persisted rows are
                    // evicted until the page fits, with `nextToken` re-minted
                    // at the first evicted row. The page's own anchor (a
                    // forward page's oldest row, a seek's target) is never
                    // evicted — dropping it would re-mint a cursor at the
                    // anchor itself and loop the client — so those pages
                    // bound at their already-budgeted anchor..tail suffix
                    // plus the live row. Full (absent-projection) reads are
                    // never budgeted, as above.
                    if projection == Some(ConversationProjection::Slim) {
                        let protect_rel = match slim_anchor {
                            crate::pagination::BudgetAnchor::Newest => None,
                            crate::pagination::BudgetAnchor::Oldest => Some(0),
                            crate::pagination::BudgetAnchor::Target(global) => {
                                Some(global.saturating_sub(page_start))
                            }
                        };
                        let sizes: Vec<usize> =
                            arr.iter().map(crate::pagination::serialized_size).collect();
                        let (lo, _hi) = crate::pagination::budget_page(
                            &sizes,
                            crate::pagination::BudgetAnchor::Newest,
                            SLIM_PAGE_BUDGET_BYTES,
                        );
                        let lo = protect_rel.map_or(lo, |p| lo.min(p));
                        if lo > 0 {
                            arr.drain(..lo);
                            next_token = crate::pagination::remint_backward_token(page_start + lo);
                        }
                    }
                }
            }
        }
        let mut result = json!({
            "agentId": agent_id,
            "messages": messages,
            "truncated": next_token.is_some(),
            "totalMessages": total,
            "nextToken": next_token,
            "turnInFlight": turn_in_flight,
            "lastStreamActivityAt": last_stream_activity_at,
        });
        if let Some(prev) = prev_token {
            result
                .as_object_mut()
                .expect("conversation result is an object")
                .insert("prevToken".to_string(), json!(prev));
        }
        Ok(result)
    }

    /// `agent.getMessageBlock` (PROTOCOL §5.5): one FULL content block of one
    /// persisted message, by block id — the on-demand counterpart of the slim
    /// conversation projection. The row is served through the same
    /// [`strip_anonymous_tool_blocks`] + [`stamp_synthetic_block_ids`] passes
    /// as `agent.getConversation` (NEVER the slim bounding), so block identity
    /// matches the served conversation byte-for-byte — persisted assistant ids
    /// and serve-time synthetic `{messageId}:{index}` ids both resolve — and
    /// the returned block is always the full, unprojected body. Bounded cost
    /// (RPC cost contract): a metadata-only session read plus ONE primary-key
    /// message row read; the transcript is never hydrated.
    ///
    /// In-progress rows (monorepo#3647): when the message id is not persisted
    /// but matches the live-turn slot's in-flight message, the block resolves
    /// from the slot's streamed blocks (O(1) in-memory read) — so a
    /// slim-truncated block served on the `includeInProgress` tail row is
    /// hydratable mid-turn, same contract as persisted rows. The persisted
    /// row wins once the turn ends (checked first), and an unknown id stays
    /// `InvalidParams` exactly as before.
    ///
    /// Frame-size note: a block whose serialized response exceeds the
    /// transport's 40 MiB outbound frame cap (`MAX_OUTBOUND_MESSAGE_BYTES`)
    /// surfaces as the standard `-32010` oversized-response error naming
    /// `responseBytes` and the limit — explicit, not a hang. Such a block is
    /// rare by construction (the matching inbound cap bounds what clients can
    /// persist; base64 attachments top out ~33.4 MiB) and is equally
    /// unservable via unprojected `agent.getConversation`, where it takes the
    /// whole page down rather than just itself. The slim flags
    /// (`inputBytes`/`outputBytes`/`dataBytes`) carry the full body size, so
    /// a client can predict the fetch size before calling.
    pub(crate) async fn agent_get_message_block_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        block_id: String,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        // Metadata-only scope check — same fail-closed contract as
        // `agent_get_conversation_op`: a cross-workspace mismatch is
        // `NotFound`, indistinguishable from an unknown agent.
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let message = match self
            .store
            .get_agent_message_by_id(&agent_id, &message_id)
            .await?
        {
            Some(m) => m,
            // In-progress fallback (monorepo#3647): an unpersisted id that
            // matches the live-turn slot's in-flight message resolves from
            // the slot's streamed blocks, so blocks slim-truncated on the
            // `includeInProgress` tail row hydrate mid-turn too.
            None => match self
                .live_turn(&agent_id)
                .filter(|l| l.message_id == message_id)
            {
                Some(live) => AgentMessage {
                    id: live.message_id,
                    agent_id: agent_id.clone(),
                    seq: i64::MAX,
                    role: "assistant".to_string(),
                    content: Value::Array(live.blocks),
                    metadata: None,
                    app_message_id: None,
                    created_at: live.last_activity_at,
                },
                None => {
                    return Err(Error::InvalidParams(format!(
                        "unknown message id: {message_id}"
                    )))
                }
            },
        };
        let message = stamp_synthetic_block_ids(strip_anonymous_tool_blocks(message));
        let block = message
            .content
            .as_array()
            .and_then(|blocks| {
                blocks
                    .iter()
                    .find(|b| b.get("id").and_then(Value::as_str) == Some(block_id.as_str()))
            })
            .cloned()
            .ok_or_else(|| Error::InvalidParams(format!("unknown block id: {block_id}")))?;
        Ok(json!({ "block": block }))
    }

    /// `agent.listUserMessages` (PROTOCOL §5.5): all user-role messages of
    /// one agent as lightweight index items, oldest→newest. Previews are
    /// bounded to `preview_chars` characters (absent → 300, clamped into
    /// [1, 2000]); `metadata` is passed through verbatim when present so
    /// clients can distinguish automated rows. Bounded cost (RPC cost
    /// contract): a metadata-only session read plus ONE role-filtered index
    /// read whose previews are extracted and truncated inside SQL — full
    /// content blobs never leave the database and the transcript is never
    /// hydrated.
    pub(crate) async fn agent_list_user_messages_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        preview_chars: Option<i64>,
    ) -> Result<Value> {
        const DEFAULT_PREVIEW_CHARS: i64 = 300;
        const MAX_PREVIEW_CHARS: i64 = 2000;
        // Metadata-only scope check — same fail-closed contract as
        // `agent_get_message_block_op`: a cross-workspace mismatch is
        // `NotFound`, indistinguishable from an unknown agent.
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let preview_chars = usize::try_from(
            preview_chars
                .unwrap_or(DEFAULT_PREVIEW_CHARS)
                .clamp(1, MAX_PREVIEW_CHARS),
        )
        .expect("clamped to [1, MAX_PREVIEW_CHARS]");
        let items = self
            .store
            .get_agent_user_message_index(&agent_id, preview_chars)
            .await?;
        let total = items.len();
        let items: Vec<Value> = items
            .into_iter()
            .map(|item| {
                let mut obj = json!({
                    "id": item.id,
                    "preview": item.preview,
                    "createdAt": item.created_at,
                });
                if let Some(metadata) = item.metadata {
                    obj.as_object_mut()
                        .expect("item is an object")
                        .insert("metadata".to_string(), metadata);
                }
                obj
            })
            .collect();
        Ok(json!({ "agentId": agent_id.0, "items": items, "total": total }))
    }

    /// Publish an `agent:*` session-mutation event (P3-1.2b): every persisted
    /// session mutation emits an invalidation event so subscribed clients
    /// re-read the projection instead of relying on a local cache.
    pub(crate) async fn publish_agent_mutation_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_type: &str,
        data: Value,
    ) {
        crate::publish_event(
            self.event_bus.as_ref(),
            intent_store::NewEvent {
                workspace_id: workspace_id.clone(),
                timestamp: now_iso(),
                event_type: event_type.to_string(),
                actor: crate::system_actor(),
                session_id: Some(agent_id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
    }

    /// Publish the persisted-row event pair (PROTOCOL §6.5): the lean
    /// `agent:message` echo followed by the content-bearing
    /// `agent:last-message` companion — every transcript persist emits both,
    /// so clients that only know the old echo are untouched while preview
    /// consumers converge with zero follow-up RPCs. Payloads derive from the
    /// appended row itself ([`agent_message_event_payload`] /
    /// [`agent_last_message_event_payload`]); no extra queries.
    pub(crate) async fn publish_agent_message_events(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        message: &AgentMessage,
        turn_id: Option<&str>,
    ) {
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            AGENT_MESSAGE,
            agent_message_event_payload(agent_id, message, turn_id),
        )
        .await;
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_LAST_MESSAGE,
            agent_last_message_event_payload(agent_id, message, turn_id),
        )
        .await;
    }

    /// Publish `agent:subscriptions-changed` for `parent_agent_id`, carrying the
    /// refreshed waiting flags derived from its live completion-watch set
    /// (`isWaitingForOtherAgents` / `waitingForAgentIds`, the same projection
    /// `agent.get` serves) so clients converge on watch-set changes without
    /// polling (PROTOCOL §6.5). Emitted when watches are added (delegate /
    /// watchCompletion) and when wake delivery removes them (fired watch /
    /// delegation-group clear).
    ///
    /// The watch set also feeds the anchor workspace's derived
    /// `displayStatus` and its orthogonal `waiting` flag (an idle parent
    /// still waiting on delegated children reads as pending work), so every
    /// publish recomputes both — transition-only and best-effort
    /// ([`Services::maybe_emit_display_status_changed`] /
    /// [`Services::maybe_emit_waiting_changed`] dedupe against the last
    /// observation and swallow errors), so a no-op recompute stays silent
    /// and can never break the watch lifecycle.
    pub(crate) async fn publish_subscriptions_changed(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: &AgentId,
    ) {
        let watches = self.waiting_watches_for_parent(parent_agent_id);
        let mut waiting: Vec<AgentId> = Vec::with_capacity(watches.len());
        for w in &watches {
            if !waiting.contains(&w.child_agent_id) {
                waiting.push(w.child_agent_id.clone());
            }
        }
        self.publish_agent_mutation_event(
            workspace_id,
            parent_agent_id,
            intent_core::events::AGENT_SUBSCRIPTIONS_CHANGED,
            json!({
                "agentId": parent_agent_id.0,
                "isWaitingForOtherAgents": !waiting.is_empty(),
                "waitingForAgentIds": waiting,
            }),
        )
        .await;
        self.maybe_emit_display_status_changed(workspace_id).await;
        self.maybe_emit_waiting_changed(workspace_id).await;
    }

    /// `agent.create`: persist a new session; the process spawns lazily on first
    /// turn (PROTOCOL §5.5). `task_note_id`/`skip_auto_commit` are set by
    /// `agent.delegate` so the auto-commit-on-idle subscriber (LNI-1) can
    /// resolve the `Linked-Note-Id:` trailer and honor the opt-out.
    ///
    /// Agent ids are server-assigned: the op always mints a fresh
    /// `agent-{uuid}` id (client-supplied ids are rejected `-32602` at the
    /// transport boundary before this op runs).
    ///
    /// `extra` carries the widened FE-facing spawn hints. `provider` lands on
    /// the persisted [`AgentSession`]; `metadata` is harvested for the
    /// persistence-gap fields (`delegationDepth`, `initialMessage`,
    /// `contextReferences`, `imageBlocks`; P3-1.2b — plus `isBackground`,
    /// G-A1/P3-1.2c) with the top-level `contextReferences`/`imageBlocks`/
    /// `isBackground` params winning over the `metadata` fallback.
    /// `agentType`/`workspacePath`/`workspaceContext` remain
    /// accepted-but-unpersisted (P2-12a audit).
    ///
    /// Emits `agent:created` after the insert.
    ///
    /// Returns `{ agent: <AgentLite> }` — the full projection so the FE can
    /// upsert the created session without a follow-up `agent.get` round-trip.
    /// This is a superset of the earlier `{ id, name }` shape, so existing
    /// callers that only read `agent.id` / `agent.name` stay green.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn agent_create_op(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist: Option<String>,
        parent_agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        skip_auto_commit: bool,
        extra: AgentCreateExtra,
    ) -> Result<Value> {
        // Depth guard at the service layer (LC-1): mirror the MCP `create_agent`
        // front-door check so every path that spawns a child for a parent
        // already at `MAX_DELEGATION_DEPTH` is refused — including RPC/service
        // callers that bypass the dispatch-level guard. An unknown parent reads
        // as depth 0 (same leniency as the dispatch check).
        if let Some(parent) = &parent_agent_id {
            let parent_depth = self
                .store
                .get_agent_session(parent)
                .await
                .ok()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0);
            if parent_depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "Cannot create sub-agent: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {parent_depth}. Please complete this task directly instead of delegating further."
                )));
            }
        }
        // Specialist alias canonicalization (PROTOCOL §5.11): a `specialist`
        // naming an alias (e.g. `"coordinator"`) is rewritten to the claiming
        // specialist's CANONICAL id (`"spec-writer"`) before any downstream
        // rung runs, so display-name/model/effort resolution, the prompt
        // snapshot, and the persisted `session.specialist` (surfaced as
        // `metadata.specialist`) all see the canonical id. A directly-known
        // id passes through unchanged, and an UNKNOWN id is rejected with
        // `-32602` naming the id and the known catalog ids (monorepo#3497)
        // — BEFORE any side effect, so no session row is persisted. Every
        // spawn seam funnels through here (`agent.create`, the MCP
        // `create_agent`/`ws.agent.create` tools, `agent.delegate`,
        // `agent.wakeOrCreate`'s create branch, `workspace.create`'s
        // `initialAgent`), so the validation covers them all.
        // SECURITY: the project tier resolves against the stored workspace
        // record's path, never a client-supplied one (same rationale as the
        // model resolution below).
        let specialist = match specialist {
            Some(spec_id) => {
                let wp = self
                    .store
                    .get_workspace(&workspace_id)
                    .await
                    .ok()
                    .and_then(|w| crate::git_ops::worktree_path(&w));
                // Canonicalization walks the specialist tier directories —
                // blocking pool (monorepo#4148).
                let services = self.clone();
                Some(
                    tokio::task::spawn_blocking(move || {
                        services
                            .specialists_service()
                            .canonical_id_or_err(&spec_id, wp.as_deref())
                    })
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "agent.create specialist resolution task failed: {e}"
                        ))
                    })??,
                )
            }
            None => None,
        };
        let now = now_iso();
        // Derive an omitted name from the specialist's resolved display name
        // (frontmatter `name`, 3-tier project > user > bundled — the same
        // workspace-path-aware seam the model resolution below uses) so a
        // name-less `agent.create` / `workspace.create.initialAgent` carrying
        // a specialist surfaces e.g. "Coordinator" for `spec-writer` instead
        // of the generic `Agent {6-hex}` fallback. Resolution failure or an
        // unknown specialist never fails the create — the fallback below
        // still applies.
        let specialist_display_name = match (&name, specialist.as_deref()) {
            (None, Some(spec_id)) => {
                // SECURITY: derive workspace_path from the stored workspace
                // record, never the client-supplied value (same rationale as
                // the model resolution below).
                let wp = self
                    .store
                    .get_workspace(&workspace_id)
                    .await
                    .ok()
                    .and_then(|w| crate::git_ops::worktree_path(&w));
                // Display-name resolution walks the specialist tiers —
                // blocking pool (monorepo#4148); a JoinError degrades to the
                // generic name fallback, never failing the create.
                let services = self.clone();
                let spec_id = spec_id.to_string();
                tokio::task::spawn_blocking(move || {
                    services
                        .specialists_service()
                        .resolve_display_name(&spec_id, wp.as_deref())
                })
                .await
                .ok()
                .flatten()
            }
            _ => None,
        };
        // `name_explicitly_set` defaults to `name.is_some()` so an explicit
        // `agent.create` with a client-supplied name still becomes
        // renameable-with-guard. A specialist-derived default counts as
        // explicitly set too — the desktop FE resolves the display name
        // client-side and sends it as an explicit `name`, so the daemon-side
        // derivation must survive the agent's opening-turn
        // `ws.workspace.setAgentName` (`skipIfExplicitlySet: true`) the same
        // way. Delegate flows override to `Some(false)` via
        // `AgentCreateExtra.name_explicitly_set` so their task-derived name
        // stays renameable by that opening-turn rename.
        let name_explicitly_set = extra
            .name_explicitly_set
            .unwrap_or(name.is_some() || specialist_display_name.is_some());
        let name = name
            .or(specialist_display_name)
            .unwrap_or_else(|| format!("Agent {}", &Uuid::new_v4().simple().to_string()[..6]));
        let id = AgentId(format!("agent-{}", Uuid::new_v4()));
        // `metadata` is persisted (C1d-10a, closes the metadata half of the
        // P2-12a deferral) so `agent.wakeOrCreate` chains can read back the
        // parent's `delegationDepth`/`createdByAgentId`/`taskNoteId`/
        // `isBackground`/`source`/`skipAutoCommit` without a follow-up round-trip.
        // `workspace_path` is now used for project-tier specialist resolution;
        // `agent_type` and `workspace_context` remain deferred.
        let AgentCreateExtra {
            provider,
            reasoning_effort,
            agent_type: _,
            mut metadata,
            workspace_path: _, // Ignored; derived from workspace record for security
            workspace_context: _,
            context_references,
            image_blocks,
            file_blocks,
            is_background,
            name_explicitly_set: _,
        } = extra;
        // A present value — even blank — means the effort was decided by the
        // caller or by an upstream rung (`resolve_delegate_reasoning_effort`),
        // so the settings default below must not fill it in. Empty/whitespace
        // then collapses to None (an explicit clear); a non-empty level is
        // validated against the resolved model once the model resolution has
        // settled.
        let reasoning_effort_decided = reasoning_effort.is_some();
        let reasoning_effort = reasoning_effort.filter(|e| !e.trim().is_empty());
        // Harvest the persistence-gap fields the FE writer kept under
        // `metadata` (P3-1.2b). Top-level params win over the metadata copy.
        let meta = metadata.as_ref().and_then(Value::as_object);
        let meta_get = |key: &str| meta.and_then(|m| m.get(key)).cloned();
        let delegation_depth = meta_get("delegationDepth").and_then(|v| v.as_i64());
        let initial_message = meta_get("initialMessage")
            .and_then(|v| v.as_str().map(str::to_string))
            .filter(|s| !s.trim().is_empty());
        let context_references = context_references
            .or_else(|| meta_get("contextReferences"))
            .or_else(|| meta_get("contextRefs"))
            .filter(|v| !v.is_null());
        let image_blocks = image_blocks
            .or_else(|| meta_get("imageBlocks"))
            .filter(|v| !v.is_null());
        let file_blocks = file_blocks
            .or_else(|| meta_get("fileBlocks"))
            .filter(|v| !v.is_null());
        // Attachment-reference validation (PROTOCOL §5.5): every file and
        // image block must carry exactly one of `data` / `attachmentId`, and
        // image references must name registered attachments in this
        // workspace (monorepo#3338). Runs before any side effect so a
        // `-32602` rejection persists nothing.
        validate_file_blocks("agent.create", file_blocks.as_ref())?;
        validate_image_blocks("agent.create", image_blocks.as_ref())?;
        self.validate_image_block_refs("agent.create", image_blocks.as_ref())
            .await?;
        let is_background = is_background
            .or_else(|| meta_get("isBackground").and_then(|v| v.as_bool()))
            .unwrap_or(false);

        // Resolve the default model when none is explicitly supplied, via the
        // single daemon-side resolver (steps 2–5; see
        // `resolve_agent_default_model`). The resolved model is persisted to
        // session.model, pinning it for the agent's lifetime. Settings changes
        // only affect new agents created afterwards; existing agents change
        // model only via explicit agent.setModel.
        let model_explicit = model.is_some();
        // SECURITY: derive workspace_path from the stored workspace record
        // rather than trusting the client-supplied value (review thread
        // PRRT_kwDOS9Wxuc6SIhDc). A malicious client could supply a spoofed
        // workspacePath and read specialist files from other workspaces.
        // Use worktree_path if available, otherwise repository_path. Read once
        // and only when a specialist tier is actually consulted (model
        // resolution, the specialist reasoning-effort rungs, and/or the
        // specialist prompt snapshot below).
        let spec_wp = if model.is_none() || specialist.is_some() {
            self.store
                .get_workspace(&workspace_id)
                .await
                .ok()
                .and_then(|w| crate::git_ops::worktree_path(&w))
        } else {
            None
        };
        // Step 1: explicit model from the client (user picked it); otherwise
        // default-model resolution walks the specialist tier directories —
        // blocking pool (monorepo#4148).
        let (mut resolved_model, mut model_source) = if let Some(m) = model {
            (Some(m), DefaultModelSource::Explicit)
        } else {
            let services = self.clone();
            let specialist = specialist.clone();
            let spec_wp = spec_wp.clone();
            let provider = provider.clone();
            tokio::task::spawn_blocking(move || {
                resolve_agent_default_model_with_source(
                    &services,
                    specialist.as_deref(),
                    spec_wp.as_deref(),
                    provider.as_deref(),
                )
            })
            .await
            .map_err(|e| {
                Error::Internal(format!("agent.create model resolution task failed: {e}"))
            })?
        };

        // Validate the explicit provider before persisting anything: an
        // unknown provider is -32602, never a session row that would
        // silently spawn the default binary. Absent provider (defaulting)
        // remains valid.
        if let Some(p) = provider.as_deref() {
            ensure_known_provider("agent.create", p)?;
        }
        // monorepo#3044: with no explicit provider and no settings-derived
        // default, no spawn provider could ever resolve for this session.
        // Fail loudly at the front door — the former behavior persisted the
        // row and the spawn silently bottomed out at the first registered
        // provider (auggie), installed or not.
        let derived_default =
            crate::agent_session::derived_default_provider(&self.effective_settings());
        if provider.is_none() && derived_default.is_none() {
            return Err(crate::agent_session::no_default_provider_error(
                "agent.create",
            ));
        }
        // monorepo#3178: the provider this session would spawn on must not be
        // one the user explicitly disabled in `providers.enabled` — fail fast
        // with the distinct "not enabled" -32602 before any session row is
        // persisted. Resolve it with the spawn path's own precedence
        // (`resolve_provider_id`: `provider` field → settings-derived
        // default). This one gate covers every create seam (`agent.create`,
        // `agent.wakeOrCreate`, and delegate's child creation). The
        // hard-false auth-verdict gate (`ensure_provider_authenticated`)
        // rides the same seam: a provider the daemon already observed as
        // not-logged-in must fail fast with the login remedy instead of
        // persisting a session that dies auth-required on its first turn.
        // Installed-ness stays delegate-only (`ensure_provider_available`):
        // direct creates on a known, enabled-but-uninstalled provider keep
        // their existing spawn-time failure mode.
        if let Some(p) = crate::agent_session::resolve_provider_id(
            provider.as_deref(),
            derived_default.as_deref(),
        ) {
            ensure_provider_enabled(
                "agent.create",
                &p,
                self.effective_settings().providers.enabled.as_ref(),
            )?;
            ensure_provider_authenticated(
                "agent.create",
                &p,
                crate::provider_auth::cached_auth_verdict(&p),
            )?;
        }
        // A bare model that provably belongs to a different provider (cached
        // dynamic catalogs) must not be persisted: the spawn would feed the
        // effective provider another provider's model id (monorepo#607). The
        // effective provider mirrors `resolve_provider_id`: provider field →
        // settings-derived default (guaranteed present by the guard above).
        // Bare ids with no ownership evidence pass — ownership cannot be
        // proven for model lists that were never fetched.
        //
        // Only a *client-supplied* mismatch hard-fails. A mismatch in a
        // derived default (specialist frontmatter / settings chain — e.g. a
        // global `model.default` naming an auggie model while the caller
        // asked for `provider: "grok"` with no model param) would reject a
        // model the caller never sent and make the provider uncreatable
        // until settings change; drop it to the CLI default instead
        // (session.model stays None).
        if let Some(m) = resolved_model.as_deref() {
            let effective = provider
                .as_deref()
                .or(derived_default.as_deref())
                .expect("guarded above: provider or derived default present");
            match ensure_bare_model_matches_provider(
                "agent.create",
                &self.models_catalog,
                effective,
                m,
            ) {
                Ok(()) => {}
                Err(e) if model_explicit => return Err(e),
                Err(e) => {
                    tracing::warn!(
                        model = m,
                        provider = effective,
                        error = %e,
                        "configured default model belongs to another provider; \
                         falling back to the CLI default"
                    );
                    resolved_model = None;
                    model_source = DefaultModelSource::CliDefault;
                }
            }
        }
        // Reasoning effort (PROTOCOL §5.11), specialist rungs: a *direct*
        // `agent.create` naming a specialist consults the same model-option >
        // frontmatter order the delegate/wakeOrCreate seams do, keyed on the
        // model that was actually resolved above. Those seams pre-decide the
        // effort and pass it down as a param, so this only fires for callers
        // that did not (`reasoning_effort_decided == false`) — which is also
        // what keeps the specialist rungs ahead of the settings default below.
        let reasoning_effort = if reasoning_effort_decided {
            reasoning_effort
        } else {
            // Effort resolution re-reads specialist frontmatter — blocking
            // pool (monorepo#4148).
            let services = self.clone();
            let specialist = specialist.clone();
            let resolved_model = resolved_model.clone();
            let spec_wp = spec_wp.clone();
            tokio::task::spawn_blocking(move || {
                resolve_delegate_reasoning_effort(
                    &services,
                    None,
                    specialist.as_deref(),
                    resolved_model.as_deref(),
                    spec_wp.as_deref(),
                )
            })
            .await
            .map_err(|e| {
                Error::Internal(format!("agent.create effort resolution task failed: {e}"))
            })?
        };
        // Validate the requested level (PROTOCOL §5.5) against the *resolved*
        // model's cached `effortLevels`, with the same probe-free,
        // evidence-only rule the delegate/wakeOrCreate seams use — no evidence
        // means the value passes through, since providers own the vocabulary.
        // Runs before the session is persisted so a `-32602` rejection is
        // side-effect free.
        if let Some(effort) = reasoning_effort.as_deref() {
            ensure_effort_supported_by_model(
                "agent.create",
                &self.models_catalog,
                resolved_model.as_deref(),
                effort,
            )?;
        }
        // Last rung: the settings default effort, applied only when no rung
        // above decided the effort AND the model itself came from the settings
        // default chain (see `resolve_settings_default_reasoning_effort`).
        let reasoning_effort = if reasoning_effort.is_some() || reasoning_effort_decided {
            reasoning_effort
        } else {
            resolve_settings_default_reasoning_effort(self, model_source, resolved_model.as_deref())
        };
        // Specialist prompt snapshot: freeze the resolved specialist injection
        // for the session's lifetime by persisting it into the metadata JSON,
        // so later edits/deletes of user/project-tier specialist files never
        // change this agent on respawn. The resolved body reuses the
        // `behaviorPrompt` override slot — written only when the caller
        // supplied no explicit override (a caller override is left untouched
        // and is itself the frozen body); the resolved identity lands in
        // `specialistName` / `specialistRoleReminder`. The bundled floor
        // consulted here is the latest bundle — the same doctrine the
        // `harnessVersion` stamp below pins — so no H2 interplay changes.
        // Unknown specialist / resolution failure writes no snapshot and
        // never fails the create; a non-object caller `metadata` is left
        // untouched.
        if let Some(spec_id) = specialist.as_deref() {
            // Both snapshot resolutions below walk the specialist tier
            // directories — blocking pool (monorepo#4148).
            let services = self.clone();
            let spec_id_owned = spec_id.to_string();
            let wp = spec_wp.clone();
            let (injection, frozen_is_orchestrator) = tokio::task::spawn_blocking(move || {
                (
                    services
                        .specialists_service()
                        .resolve_prompt_injection(&spec_id_owned, wp.as_deref()),
                    services
                        .specialists_service()
                        .resolve_is_orchestrator(&spec_id_owned, wp.as_deref()),
                )
            })
            .await
            .map_err(|e| {
                Error::Internal(format!("agent.create specialist snapshot task failed: {e}"))
            })?;
            if let Some((body, spec_name, reminder)) = injection {
                let meta_value =
                    metadata.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
                if let Some(obj) = meta_value.as_object_mut() {
                    let has_override = obj
                        .get("behaviorPrompt")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty());
                    if !has_override {
                        if let Some(body) = body {
                            obj.insert("behaviorPrompt".to_string(), json!(body));
                        }
                    }
                    obj.insert("specialistName".to_string(), json!(spec_name));
                    // The reminder key always reflects the resolution
                    // outcome: a resolved reminder overwrites, a None
                    // resolution REMOVES any caller-supplied key so the
                    // frozen readers never consume free-form caller input
                    // as a trusted reminder.
                    match reminder {
                        Some(reminder) => {
                            obj.insert("specialistRoleReminder".to_string(), json!(reminder));
                        }
                        None => {
                            obj.remove("specialistRoleReminder");
                        }
                    }
                }
            }
            // Orchestrator-role snapshot: freeze the creation-time role
            // decision alongside the identity snapshot so later edits or
            // deletes of specialist files never flip this agent's tool
            // denylist on respawn/session-open
            // (`session_specialist_is_orchestrator` prefers this key).
            // Written for EVERY specialist session — including one whose id
            // did not resolve above (the fail-closed name fallback decides)
            // — and always overwrites any caller-supplied value, so the
            // frozen readers never consume caller input as a trusted role.
            // (Resolved in the blocking task above.)
            let meta_value = metadata.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
            if let Some(obj) = meta_value.as_object_mut() {
                obj.insert(
                    "specialistIsOrchestrator".to_string(),
                    json!(frozen_is_orchestrator),
                );
            }
        }
        // Harness stamp (intent-hq/monorepo#2459): every new session gets the
        // CURRENT harness version and a snapshot of the effective
        // agentFeatures values, captured once here and immutable for the
        // session's life. Creation time is the only input — delegated /
        // wakeOrCreate children funnel through this op and mint the latest
        // version, never inheriting the parent's pinned one.
        let settings = self.effective_settings();
        let harness_features = serde_json::to_value(&settings.agent_features)
            .map_err(|e| Error::Internal(format!("encode agentFeatures snapshot failed: {e}")))?;
        let session = AgentSession {
            id,
            workspace_id,
            parent_agent_id,
            backend_session_id: None,
            acp_session_id: None,
            name,
            name_explicitly_set,
            model: resolved_model,
            reasoning_effort,
            effort_levels: None,
            provider,
            system_prompt: None,
            specialist,
            // Reference parity: `agent-factory.ts:435` persists `AgentStatus.Idle`
            // — the legacy capitalized `"Idle"` variant — on session creation, so
            // a freshly persisted session must be `Idle`, not `Pending`. On
            // `agent.get`/`agent.list` the `AgentLite.status` field is serialized
            // directly by serde, so `Pending` surfaces on the wire as `"pending"`
            // (not `"waiting"` — the `"waiting"` string only comes from the
            // separate `agent_status_wire` normalization used by the diagnostics
            // / subscription snapshots); the FE's session hydration and idle
            // selectors treat `"pending"` as a non-idle initial state, whereas
            // `Idle` correctly hydrates as idle. Both idle wire values,
            // capitalized `"Idle"` (`AgentStatus.Idle`) and lowercase `"idle"`
            // (`AgentStatus.RuntimeIdle`), are accepted equivalently by the FE
            // (see `consolidated-backend.service.ts:1207`
            // `status === AgentStatus.Idle || status === AgentStatus.RuntimeIdle`),
            // so persisting `Idle` here and later rewriting to `RuntimeIdle` at
            // end-of-turn presents a single idle state to the UI. Heal-on-
            // startup is unaffected: `is_stale_in_flight_status` only matches
            // `Active`/`Processing`/`Waiting`, so `Idle` (like the former
            // `Pending`) is left untouched by the sweep at
            // `crates/intent-services/src/lib.rs:653`.
            status: AgentStatus::Idle,
            is_active: false,
            messages: Vec::new(),
            stats: None,
            task_note_id,
            skip_auto_commit,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth,
            initial_message,
            context_references,
            image_blocks,
            file_blocks,
            is_background,
            metadata,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: Some(harness_features),
            created_at: now.clone(),
            updated_at: now,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        // The legacy per-session taskGraph pin (0095) keeps being written for
        // fallback reads; its value equals the snapshot's `taskGraph`.
        let task_graph_enabled = settings.agent_features.task_graph;
        self.store
            .insert_agent_session_with_task_graph(&session, task_graph_enabled)
            .await?;
        self.invalidate_agent_list_cache(&session.workspace_id);
        // Global usage-stats (D2): count this session start in the current UTC
        // hour bucket under the session's stats model key (normalized model,
        // falling back to the provider id when no model is resolved yet;
        // "unknown" only when the provider is unknowable too — D13).
        // Best-effort — never fails agent.create.
        crate::usage_stats::record_session_started(
            &self.store,
            session.model.as_deref(),
            session.provider.as_deref(),
            crate::agent_session::derived_default_provider(&settings).as_deref(),
        )
        .await;
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &session.id,
            intent_core::events::AGENT_CREATED,
            json!({ "agentId": session.id.0, "name": session.name }),
        )
        .await;
        // Project into `AgentLite` so the wire returns the full agent object
        // (superset of `{ id, name }`). A fresh session has no messages, so the
        // derived counts/last-* fields are `None`/0; runtime activity flags stay
        // at their `AgentLite::from_session` defaults (no live runtime state).
        let lite = AgentLite::from_session(session, 0, None, None, None, None, None);
        Ok(json!({ "agent": lite }))
    }

    /// `agent.rename` (PROTOCOL §5.5). A missing agent surfaces as `-32603`
    /// (matching the TS `renameAgentOnDisk` failure path). When
    /// `skip_if_explicitly_set` is `true` and the session's name was already
    /// explicitly set, the rename is a no-op returning the existing name with
    /// `skipped: true` (the FE `renameAgent` semantics). An applied rename
    /// emits `agent:renamed`.
    pub(crate) async fn agent_rename_op(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> Result<Value> {
        let mut session = self.load_session_internal(&agent_id).await?;
        if skip_if_explicitly_set && session.name_explicitly_set {
            return Ok(json!({ "success": true, "name": session.name, "skipped": true }));
        }
        session.name = name.clone();
        session.name_explicitly_set = true;
        session.updated_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            intent_core::events::AGENT_RENAMED,
            json!({ "agentId": agent_id.0, "name": name }),
        )
        .await;
        Ok(json!({ "success": true, "name": name }))
    }

    /// `agent.setModel` (PROTOCOL §5.5). Emits `agent:updated`.
    ///
    /// `model_id` is always a bare id (compound `provider:model` ids are
    /// rejected at the wire).
    ///
    /// `provider_id` is the optional explicit provider (additive param,
    /// monorepo#1657): absent keeps the historical behavior byte-for-byte;
    /// present it must name a registered provider, and `model_id` is
    /// validated against — and session.provider reconciled to — the GIVEN
    /// provider instead of the session's effective one (a cross-provider
    /// switch spawns the new provider's binary on the next turn).
    pub(crate) async fn agent_set_model_op(
        &self,
        agent_id: AgentId,
        model_id: String,
        provider_id: Option<String>,
    ) -> Result<Value> {
        let session = self.load_session_internal(&agent_id).await?;
        // An explicit providerId must be a registered provider before any
        // mutation (persisting an unknown one would make the next spawn
        // silently fall back to the default binary, -32602).
        if let Some(pid) = provider_id.as_deref() {
            ensure_known_provider("agent.setModel", pid)?;
        }
        let model_provider = if let Some(pid) = provider_id {
            // Explicit providerId: ownership is validated against the GIVEN
            // provider (not the session's effective one), and
            // session.provider is reconciled to it below so the next spawn
            // runs the intended binary (monorepo#1657).
            ensure_bare_model_matches_provider(
                "agent.setModel",
                &self.models_catalog,
                &pid,
                &model_id,
            )?;
            Some(pid)
        } else {
            // Without an explicit providerId the model is validated against
            // the session's effective provider (same precedence as
            // `resolve_provider_id`: session.provider → settings-derived
            // default): a bare id provably owned by another provider (cached
            // dynamic catalogs) is the same misroute vector (monorepo#607).
            // With neither set the session could never spawn — fail loudly
            // instead of validating against a positional default
            // (monorepo#3044).
            let derived;
            let effective = if let Some(p) = session.provider.as_deref().filter(|p| !p.is_empty()) {
                p
            } else {
                derived =
                    crate::agent_session::derived_default_provider(&self.effective_settings())
                        .ok_or_else(|| {
                            crate::agent_session::no_default_provider_error("agent.setModel")
                        })?;
                derived.as_str()
            };
            ensure_bare_model_matches_provider(
                "agent.setModel",
                &self.models_catalog,
                effective,
                &model_id,
            )?;
            None
        };
        // Reconcile the provider to the explicit providerId (models without
        // one keep the session's provider). The
        // write goes through the narrow
        // `set_agent_session_model` — the ONE writer allowed to change
        // `provider` after first real use — because a cross-provider switch
        // after `acp_session_id` is persisted would otherwise trip
        // `update_agent_session`'s immutability guard (monorepo#882); the
        // next turn then respawns the new provider's binary, opens a fresh
        // `session/new`, and replays history as `<supervisor>` XML.
        let provider = model_provider.or(session.provider);
        let workspace_id = session.workspace_id;
        self.store
            .set_agent_session_model(
                &workspace_id,
                &agent_id,
                &model_id,
                provider.as_deref(),
                &now_iso(),
            )
            .await?;
        // The stored model changed, so any persisted display resolution (D14)
        // now names the wrong model — clear it; the next session open
        // re-resolves against the new id. Best-effort: the setModel itself
        // already landed. Benign race: a session open interleaving between
        // the model UPDATE above and this clear can persist a fresh (valid)
        // resolution for the NEW id which this then wipes — the only cost is
        // a lost resolution until the next open (stats fall back to
        // normalizing the raw id), never a stale mis-attribution.
        if let Err(e) = self
            .store
            .clear_agent_session_resolved_model(&workspace_id, &agent_id)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "clear resolved display model failed");
        }
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "modelId": model_id }),
        )
        .await;
        Ok(json!({ "success": true, "modelId": model_id }))
    }

    /// `agent.delete`: idempotent session delete (PROTOCOL §5.5). When
    /// `workspace_id` is supplied the caller's workspace must match the
    /// session's; a mismatch surfaces as `NotFound` (defense-in-depth against
    /// bare-id probes across workspaces).
    pub(crate) async fn agent_delete_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        // Capture the workspace (and name, for the event's `agentName`
        // enrichment — intent-hq/monorepo#2869) before deleting so the
        // post-delete agent:deleted emit can be workspace-scoped. If the
        // session is already gone, skip the emit gracefully rather than
        // failing the idempotent delete. When the caller declares a
        // workspace, reject a cross-workspace bare-id probe by mapping to
        // `NotFound` before touching the store.
        let session_meta = self
            .store
            .get_agent_session(&agent_id)
            .await
            .ok()
            .map(|s| (s.workspace_id, s.name, s.parent_agent_id));
        let session_workspace_id = session_meta.as_ref().map(|(ws, _, _)| ws.clone());
        if let (Some(ws), Some(session_ws)) = (workspace_id.as_ref(), session_workspace_id.as_ref())
        {
            if session_ws != ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        // Immediate-delete-while-pending (§5.5): an immediate delete
        // supersedes a running grace window — drop the pending entry and
        // abort its timer, then commit now. The timer-fired commit path has
        // already claimed (removed) its entry before calling here, so this
        // is a no-op for it.
        self.pending_agent_deletes.cancel(agent_id.0.as_str());
        // Route the DELETE through the workspace guard so a stale-caller with the
        // wrong workspace cannot mutate the row even if the pre-check above races
        // with a concurrent workspace move.
        if let Some(session_ws) = session_workspace_id.as_ref() {
            self.store
                .delete_agent_session(session_ws, &agent_id)
                .await?;
            self.invalidate_agent_list_cache(session_ws);
        }
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .remove(&agent_id);
        // Silent-tail record (intent-hq/monorepo#2669): in-memory, keyed by
        // agent — drop it with the session so the map never leaks entries
        // for deleted agents. The truncation-redrive counter and handoff
        // flag (monorepo#2863) are dropped on the same terms.
        self.clear_turn_silent_tail(&agent_id);
        self.clear_truncation_redrives(&agent_id);
        self.take_truncation_redrive(&agent_id);
        // A parked mid-turn attention raise dies with the agent — dropped on
        // the same terms as the other per-agent in-memory registries.
        self.take_deferred_attention(&agent_id);
        // Context-occupancy registry (intent-hq/intent#3797): in-memory,
        // keyed by agent — dropped on the same terms.
        self.clear_context_usage(&agent_id);
        // Registry hygiene (monorepo#840): drop the failure streak and any
        // failure-wake dedup records naming the deleted agent as parent OR
        // child — delegation churns short-lived agents in both roles, so a
        // child-only sweep would leak (deleted_parent, child) entries for the
        // daemon's lifetime. The streaming path's terminal-error stash
        // (monorepo#2050) is dropped on the same terms.
        self.clear_failure_streak(&agent_id);
        self.clear_failure_wake_dedup_all_roles(&agent_id);
        self.discard_pending_terminal_error(&agent_id);
        // Pending-question registries (monorepo#3179): the per-agent marker
        // mutation lock and the dismissal-notice dedup set are both in-memory
        // and keyed by agent — drop them with the session so neither map
        // grows unboundedly on a long-lived daemon. In-flight marker
        // mutations keep their own Arc clone of the mutex, so ordering is
        // preserved (see `PendingQuestionMutationLocks::remove`).
        self.pending_question_mutation_locks.remove(&agent_id);
        self.dismissal_notices_sent
            .lock()
            .expect("dismissal notice registry poisoned")
            .remove(&agent_id);
        // Drop the deleted agent's event subscriptions (monorepo#937): the
        // wake target is gone, so matching/batching for it is pure leak.
        self.remove_event_subscriptions_for_agent(&agent_id).await;
        if let Some((workspace_id, agent_name, parent_agent_id)) = session_meta {
            // intent-hq/monorepo#3906: the session row is already deleted, so
            // the watch-wake label pass cannot resolve the delegation parent
            // from the store — stamp it on the event (present only for
            // delegated agents, mirroring the `agent:failed` `parentAgentId?`
            // enrichment) so a genuine child's deletion wake still renders
            // "Child agent".
            let mut data = json!({ "agentId": agent_id.0, "agentName": agent_name });
            if let Some(parent) = parent_agent_id {
                data["parentAgentId"] = json!(parent.0);
            }
            crate::publish_event(
                self.event_bus.as_ref(),
                intent_store::NewEvent {
                    workspace_id: workspace_id.clone(),
                    timestamp: now_iso(),
                    event_type: intent_core::events::AGENT_DELETED.to_string(),
                    actor: crate::system_actor(),
                    session_id: Some(agent_id.0.clone()),
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data,
                },
            )
            .await;
            // Deleting a session can retire a needs_attention hold (a pending
            // attention request or unanswered question dies with the row):
            // recompute-and-compare (§6.5 step 0); the dedup cache suppresses
            // the no-op when nothing derived from this session.
            self.maybe_emit_display_status_changed(&workspace_id).await;
        }
        Ok(json!({ "success": true }))
    }

    /// `agent.delete` with `undoDelayMs > 0` (PROTOCOL §5.5): register an
    /// in-memory pending deletion with deadline `now + undo_delay_ms`
    /// (clamped to the 60s cap) and return the ISO `deleteAt` deadline.
    /// Scheduling does NOT stop the agent — the deadline commit runs the
    /// ordinary [`Services::agent_delete_op`] cascade (which does). Emits
    /// `agent:delete-scheduled`. Re-scheduling while pending is idempotent;
    /// nothing is persisted, so a daemon restart drops the pending deletion.
    pub(crate) async fn agent_schedule_delete_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        undo_delay_ms: u64,
    ) -> Result<String> {
        // Validate existence up front so scheduling a delete for an unknown
        // session is the standard `NotFound` error, not a timer that fails
        // later. When the caller declares a workspace, reject a
        // cross-workspace bare-id probe the same way `agent_delete_op` does.
        let session_ws = self
            .store
            .get_agent_session_summary(&agent_id)
            .await?
            .workspace_id;
        if let Some(ws) = workspace_id.as_ref() {
            if session_ws != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let delay_ms = crate::delete_grace::clamp_undo_delay_ms(undo_delay_ms);
        let delete_at = intent_core::iso_ms_from_now(delay_ms);
        let key = agent_id.0.clone();
        let timer_services = self.clone();
        let timer_id = agent_id.clone();
        let timer_ws = session_ws.clone();
        // Idempotent re-schedule (§5.5): the registry arms the timer only
        // when nothing is pending for this key — the check runs under the
        // registry lock, so concurrent schedules converge on one deadline
        // and only the arming call emits `agent:delete-scheduled`.
        if let Some(existing) =
            self.pending_agent_deletes
                .schedule(key, delete_at.clone(), move |generation| {
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        // Claim-or-abstain: only the timer that still owns the
                        // entry commits. A cancel or an immediate delete that
                        // raced ahead removed/superseded the entry — do nothing.
                        if !timer_services
                            .pending_agent_deletes
                            .claim(timer_id.0.as_str(), generation)
                        {
                            return;
                        }
                        // Commit via the existing immediate-delete cascade (same
                        // events). Best-effort: the caller is long gone, so a
                        // failure is logged, not surfaced.
                        if let Err(e) = timer_services
                            .agent_delete_op(timer_id.clone(), Some(timer_ws))
                            .await
                        {
                            tracing::warn!(
                                agent = %timer_id.0,
                                error = %e,
                                "scheduled agent delete failed at commit"
                            );
                        }
                    })
                })
        {
            return Ok(existing);
        }
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::agent_delete_scheduled_event(&session_ws, &agent_id, &delete_at),
        )
        .await;
        Ok(delete_at)
    }

    /// `agent.cancelDelete` (PROTOCOL §5.5): cancel a pending agent-session
    /// deletion. `true` when something was pending (emits
    /// `agent:delete-cancelled`), `false` otherwise (already committed, or
    /// never scheduled) — the non-error, race-safe outcome. A caller-declared
    /// workspace mismatch is rejected as `NotFound` BEFORE touching the
    /// registry, mirroring `agent_delete_op`/`agent_schedule_delete_op`, so
    /// a stale or cross-workspace scoped cancel cannot remove another
    /// workspace's pending deletion.
    pub(crate) async fn agent_cancel_delete_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<bool> {
        let session_ws = match self.store.get_agent_session_summary(&agent_id).await {
            Ok(summary) => summary.workspace_id,
            // Row already gone: the deletion committed (or the session never
            // existed) — nothing pending, the race-safe non-error outcome.
            Err(Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };
        if let Some(ws) = workspace_id.as_ref() {
            if session_ws != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let cancelled = self.pending_agent_deletes.cancel(agent_id.0.as_str());
        if cancelled {
            crate::publish_event(
                self.event_bus.as_ref(),
                crate::agent_delete_cancelled_event(&session_ws, &agent_id),
            )
            .await;
        }
        Ok(cancelled)
    }

    /// Abort any pending agent-session deletions in `workspace_id` without
    /// emitting per-agent cancel events: the caller is the workspace delete
    /// cascade (immediate or committed-from-pending), which supersedes them —
    /// every session is deleted right after, emitting `agent:deleted` per
    /// session (§5.5 cascade interaction).
    pub(crate) fn abort_pending_agent_deletes(&self, sessions: &[AgentSession]) {
        for session in sessions {
            self.pending_agent_deletes.cancel(session.id.0.as_str());
        }
    }

    /// Soft retire (`ws.agent.retire`): set `retired_at` on the session,
    /// keeping the row and its full conversation intact (still searchable).
    /// The retired session is INERT — `require_agent_session` rejects every
    /// interaction path and default `agent.list` reads exclude it — until
    /// the user/FE-initiated `agent.restore` clears the mark. Idempotent on
    /// an already-retired session (the existing timestamp is preserved, no
    /// event re-emitted). Emits `agent:retired` with
    /// `{ agentId, agentName, retiredAt, reason? }`.
    ///
    /// GUARDED on active children: the call FAILS (`InvalidParams`, nothing
    /// mutated) while any descendant — transitive over `parent_agent_id` —
    /// is still running a turn. On success the whole descendant subtree is
    /// cascade-retired (see the inline comments), and restoring the parent
    /// does NOT restore the cascaded children.
    pub(crate) async fn agent_retire_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        reason: Option<String>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        if let Some(existing) = session.retired_at {
            return Ok(json!({ "success": true, "retiredAt": existing, "alreadyRetired": true }));
        }
        // Guard (confirmed decision): retire fails while any descendant —
        // grandchildren included — is still running a turn
        // (pending/active/Processing). NOTHING is mutated on rejection (no
        // partial cascade). Retired descendants are inert and never count;
        // idle/waiting children pass the guard and are cascade-retired
        // below.
        let descendants = self.collect_retire_descendants(&agent_id).await?;
        let active: Vec<String> = descendants
            .iter()
            .filter(|c| c.retired_at.is_none() && is_running_turn(c.status))
            .map(|c| format!("{} ({})", c.name, c.id.0))
            .collect();
        if !active.is_empty() {
            return Err(Error::InvalidParams(format!(
                "cannot retire: {} active child agent(s) still running a turn: {}. \
                 Stop them or wait for them to finish, then retire again.",
                active.len(),
                active.join(", ")
            )));
        }
        let mut session = session;
        let now = loop {
            if let Some(now) = self.retire_one_session(&session, reason.as_deref()).await? {
                break now;
            }
            // Lost the CAS to a concurrent retire: re-read. While the
            // winner's mark is set, report `alreadyRetired` with its real
            // timestamp; if a concurrent restore already cleared it again,
            // retry the retire instead of fabricating a timestamp for an
            // agent that is not actually retired.
            session = self.store.get_agent_session_summary(&agent_id).await?;
            if let Some(existing) = session.retired_at.clone() {
                return Ok(
                    json!({ "success": true, "retiredAt": existing, "alreadyRetired": true }),
                );
            }
        };
        // Cascade (confirmed decision): every non-terminal,
        // not-already-retired descendant retires with the parent, each
        // through the same full cleanup — hooks/PR monitors cancelled,
        // subscriptions dropped, watches resolved via its own
        // `agent:retired` emit. Restoring the parent does NOT restore them;
        // each is individually restorable via `agent.restore`.
        // Guard-vs-cascade race: a child that started a turn after the
        // guard passed is SKIPPED (left running, un-retired) rather than
        // stopped or failed late — the parent's own retire already landed,
        // and the racing child can retire itself later. Best-effort: a
        // per-child failure logs and moves on.
        let cascade_reason = format!("parent {} retired", session.name);
        for child in &descendants {
            // Row gone since the snapshot → skip.
            let Ok(fresh) = self.store.get_agent_session_summary(&child.id).await else {
                continue;
            };
            if fresh.retired_at.is_some()
                || is_terminal_status(fresh.status)
                || is_running_turn(fresh.status)
            {
                continue;
            }
            if let Err(e) = self.retire_one_session(&fresh, Some(&cascade_reason)).await {
                tracing::warn!(
                    parent = %agent_id,
                    child = %fresh.id,
                    error = %e,
                    "retire cascade: child retire failed; continuing"
                );
            }
        }
        Ok(json!({ "success": true, "retiredAt": now }))
    }

    /// Snapshot the retiring agent's descendant tree, transitive over
    /// `parent_agent_id` (cross-workspace, like
    /// [`intent_store::Store::count_child_agents`]). Deleted rows are
    /// excluded from the returned set (the cascade never touches them) but
    /// still traversed, so grandchildren under a deleted intermediary are
    /// found. Cycle-safe via a visited set (parent links cannot cycle in
    /// practice, but a corrupt row must not hang the retire).
    async fn collect_retire_descendants(&self, root: &AgentId) -> Result<Vec<AgentSession>> {
        let mut seen: HashSet<String> = HashSet::from([root.0.clone()]);
        let mut frontier = vec![root.clone()];
        let mut out = Vec::new();
        while let Some(id) = frontier.pop() {
            for child in self.store.list_child_agent_summaries(&id).await? {
                if !seen.insert(child.id.0.clone()) {
                    continue;
                }
                frontier.push(child.id.clone());
                if !matches!(child.status, AgentStatus::Deleted) {
                    out.push(child);
                }
            }
        }
        Ok(out)
    }

    /// One session's retire transition + cleanup, shared by the caller path
    /// and the child cascade of [`Services::agent_retire_op`]. Returns
    /// `Ok(Some(retiredAt))` when this call performed the transition,
    /// `Ok(None)` when a concurrent retire won the CAS (nothing re-emitted).
    async fn retire_one_session(
        &self,
        session: &AgentSession,
        reason: Option<&str>,
    ) -> Result<Option<String>> {
        let now = now_iso();
        // CAS write: only the request that actually flips NULL → set emits
        // the event.
        let transitioned = self
            .store
            .set_agent_session_retired_at(&session.workspace_id, &session.id, Some(&now), &now)
            .await?;
        if !transitioned {
            return Ok(None);
        }
        self.invalidate_agent_list_cache(&session.workspace_id);
        // Drop the retired agent's event subscriptions: the wake target is
        // inert, so matching/batching for it is pure leak (same rationale
        // as the delete cascade — monorepo#937). The queue entry is kept:
        // restore may drain it later.
        self.remove_event_subscriptions_for_agent(&session.id).await;
        // Cancel the retiring agent's active hooks and PR monitors (mirrors
        // the workspace-archive sweeps): the owner is inert and retired
        // itself, so NO wake notice is queued. `agent.restore` does NOT
        // resurrect them (unarchive precedent) — the agent re-registers if
        // the condition still matters.
        self.cancel_agent_hooks(&session.id).await;
        self.cancel_agent_pr_monitors(&session.id).await;
        let mut data = json!({
            "agentId": session.id.0,
            "agentName": session.name,
            "retiredAt": now,
        });
        if let Some(r) = reason {
            data["reason"] = json!(r);
        }
        crate::publish_event(
            self.event_bus.as_ref(),
            intent_store::NewEvent {
                workspace_id: session.workspace_id.clone(),
                timestamp: now.clone(),
                event_type: intent_core::events::AGENT_RETIRED.to_string(),
                actor: crate::system_actor(),
                session_id: Some(session.id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
        // Retiring a session can retire a needs_attention hold too (a
        // pending attention request or unanswered question goes inert with
        // the row): recompute-and-compare (§6.5 step 0).
        self.maybe_emit_display_status_changed(&session.workspace_id)
            .await;
        Ok(Some(now))
    }

    /// `agent.restore` (wire-only; PROTOCOL §5.5): clear `retired_at`,
    /// returning the session to normal service. Restoring a non-retired
    /// session is a documented no-op (`{ success: true, restored: false }`)
    /// — idempotent-friendly for double-clicks/replays. Emits
    /// `agent:restored` with `{ agentId, agentName }` when a mark was
    /// actually cleared.
    pub(crate) async fn agent_restore_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        if session.retired_at.is_none() {
            return Ok(json!({ "success": true, "restored": false }));
        }
        let now = now_iso();
        // CAS write: only the request that actually clears the mark emits
        // the event; a concurrent restore that lost the race reports the
        // documented no-op shape.
        let transitioned = self
            .store
            .set_agent_session_retired_at(&session.workspace_id, &agent_id, None, &now)
            .await?;
        if !transitioned {
            return Ok(json!({ "success": true, "restored": false }));
        }
        self.invalidate_agent_list_cache(&session.workspace_id);
        crate::publish_event(
            self.event_bus.as_ref(),
            intent_store::NewEvent {
                workspace_id: session.workspace_id.clone(),
                timestamp: now,
                event_type: intent_core::events::AGENT_RESTORED.to_string(),
                actor: crate::system_actor(),
                session_id: Some(agent_id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({ "agentId": agent_id.0, "agentName": session.name }),
            },
        )
        .await;
        self.maybe_emit_display_status_changed(&session.workspace_id)
            .await;
        // Re-engage the queue parked by the retired gates (`try_drain_queue`
        // / `deliver_wake_message`): nothing kicks the restored agent's drain
        // organically, so without this a wake parked during retirement would
        // stay stranded until someone happened to message the agent (mirrors
        // `unarchive_workspace`'s kick). Best-effort: `try_drain_queue`
        // re-checks its own gates (busy, question hold, Error park).
        if let Some(manager) = self.agent_manager() {
            if self.has_ready_to_send(&agent_id) {
                manager
                    .clone()
                    .try_drain_queue(agent_id.clone(), session.workspace_id.clone())
                    .await;
            }
        }
        Ok(json!({ "success": true, "restored": true }))
    }

    /// `agent.getSession` (PROTOCOL §5.5). Full [`AgentSession`] projection —
    /// the superset that `agent.get`/[`AgentLite`] strips (`systemPrompt`,
    /// `specialist`, persisted metadata block, full `messages` log). Used by
    /// the FE-side agent-backend-handler retirement (C1d/C1e) so a `loadAgent`
    /// caller can rehydrate the full session shape from the daemon. Emits no
    /// events (a pure read). `NotFound` when the session is unknown.
    pub(crate) async fn agent_get_session_op(&self, agent_id: AgentId) -> Result<AgentSession> {
        let mut session = self.store.get_agent_session(&agent_id).await?;
        // monorepo#940: derived on emit (never persisted); see
        // `project_lite_with_flags`.
        session.session_corrupted = self.session_poisoned(&session);
        // Delete grace window (§5.5): overlay the pending-deletion deadline
        // from the in-memory registry (O(1) map lookup, never persisted).
        session.pending_delete_at = self.pending_agent_deletes.deadline(agent_id.0.as_str());
        // Legacy rows (pre-0096): project the current effective agentFeatures
        // on read (never persisted); see `project_lite_with_flags_inner`.
        if session.harness_features.is_none() {
            session.harness_features = self.current_agent_features_snapshot();
        }
        Ok(session)
    }

    /// `agent.update` (PROTOCOL §5.5). Partial update from a `changes` object —
    /// only listed fields are touched; omitted fields are preserved. The store
    /// enforces the write-once (`acpSessionId`) and immutable (`provider`)
    /// invariants; malformed values in `changes` surface as `InvalidParams`.
    /// Emits `agent:updated` (or `agent:renamed` when `name` is the only field
    /// mutated) so subscribed clients invalidate their cached projection.
    pub(crate) async fn agent_update_op(&self, agent_id: AgentId, changes: Value) -> Result<Value> {
        let Value::Object(obj) = changes else {
            return Err(Error::InvalidParams(
                "agent.update: `changes` must be an object".to_string(),
            ));
        };
        let mut session = self.store.get_agent_session(&agent_id).await?;
        let prior_model = session.model.clone();
        let allowed = [
            "status",
            "isActive",
            "acpSessionId",
            "backendSessionId",
            "name",
            "nameExplicitlySet",
            "model",
            "reasoningEffort",
            "provider",
            "systemPrompt",
            "specialist",
            "taskNoteId",
            "skipAutoCommit",
            "completionReport",
            "completionReportTimestamp",
            "delegationDepth",
            "initialMessage",
            "contextReferences",
            "imageBlocks",
            "fileBlocks",
            "isBackground",
        ];
        for key in obj.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(Error::InvalidParams(format!(
                    "agent.update: unknown field `{key}` in `changes`"
                )));
            }
        }
        let mut mutated_only_name = obj.contains_key("name");
        for (key, value) in &obj {
            if key != "name" {
                mutated_only_name = false;
            }
            match key.as_str() {
                "status" => {
                    session.status = serde_json::from_value(value.clone()).map_err(|e| {
                        Error::InvalidParams(format!("agent.update: invalid status: {e}"))
                    })?;
                }
                "isActive" => {
                    session.is_active = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `isActive` must be a boolean".to_string(),
                        )
                    })?;
                }
                "acpSessionId" => {
                    session.acp_session_id = update_optional_string(value, "acpSessionId")?;
                }
                "backendSessionId" => {
                    session.backend_session_id = update_optional_string(value, "backendSessionId")?
                        .map(|s| AgentId::from(s.as_str()));
                }
                "name" => {
                    session.name = value
                        .as_str()
                        .ok_or_else(|| {
                            Error::InvalidParams(
                                "agent.update: `name` must be a string".to_string(),
                            )
                        })?
                        .to_string();
                    session.name_explicitly_set = true;
                }
                "nameExplicitlySet" => {
                    session.name_explicitly_set = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `nameExplicitlySet` must be a boolean".to_string(),
                        )
                    })?;
                }
                "model" => {
                    session.model = update_optional_string(value, "model")?;
                }
                "reasoningEffort" => {
                    session.reasoning_effort = update_optional_string(value, "reasoningEffort")?
                        .filter(|e| !e.trim().is_empty());
                }
                "provider" => {
                    session.provider = update_optional_string(value, "provider")?;
                }
                "systemPrompt" => {
                    session.system_prompt = update_optional_string(value, "systemPrompt")?;
                }
                "specialist" => {
                    // Same alias canonicalization as `agent_create_op`
                    // (PROTOCOL §5.11): an alias is rewritten to the claiming
                    // specialist's canonical id before persistence so
                    // `metadata.specialist` never carries an alias; a
                    // directly-known id passes through unchanged and an
                    // UNKNOWN id is rejected with `-32602` naming the id and
                    // the known catalog ids (monorepo#3497) — the session is
                    // left untouched. Null still clears the field.
                    session.specialist = if let Some(spec_id) =
                        update_optional_string(value, "specialist")?
                    {
                        let wp = self
                            .store
                            .get_workspace(&session.workspace_id)
                            .await
                            .ok()
                            .and_then(|w| crate::git_ops::worktree_path(&w));
                        // Canonicalization + the refreshed orchestrator-role
                        // snapshot (`specialistIsOrchestrator`, written at
                        // create, re-resolved here so the session-open
                        // denylist decision tracks the identity change) both
                        // walk the specialist tiers — blocking pool
                        // (monorepo#4148).
                        let services = self.clone();
                        let (canonical, is_orchestrator) =
                            tokio::task::spawn_blocking(move || -> Result<(String, bool)> {
                                let canonical = services
                                    .specialists_service()
                                    .canonical_id_or_err(&spec_id, wp.as_deref())?;
                                let is_orchestrator = services
                                    .specialists_service()
                                    .resolve_is_orchestrator(&canonical, wp.as_deref());
                                Ok((canonical, is_orchestrator))
                            })
                            .await
                            .map_err(|e| {
                                Error::Internal(format!(
                                    "agent.update specialist resolution task failed: {e}"
                                ))
                            })??;
                        let meta = session
                            .metadata
                            .get_or_insert_with(|| json!(serde_json::Map::new()));
                        if let Some(obj) = meta.as_object_mut() {
                            obj.insert(
                                "specialistIsOrchestrator".to_string(),
                                json!(is_orchestrator),
                            );
                        }
                        Some(canonical)
                    } else {
                        // A cleared specialist retires the snapshot: a
                        // plain agent has no role and the stale key must
                        // not survive a later re-assignment.
                        if let Some(obj) = session.metadata.as_mut().and_then(Value::as_object_mut)
                        {
                            obj.remove("specialistIsOrchestrator");
                        }
                        None
                    };
                }
                "taskNoteId" => {
                    session.task_note_id =
                        update_optional_string(value, "taskNoteId")?.map(NoteId::from);
                }
                "skipAutoCommit" => {
                    session.skip_auto_commit = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `skipAutoCommit` must be a boolean".to_string(),
                        )
                    })?;
                }
                "completionReport" => {
                    session.completion_report = update_optional_string(value, "completionReport")?;
                }
                "completionReportTimestamp" => {
                    session.completion_report_timestamp =
                        update_optional_string(value, "completionReportTimestamp")?;
                }
                "delegationDepth" => {
                    session.delegation_depth = if value.is_null() {
                        None
                    } else {
                        Some(value.as_i64().ok_or_else(|| {
                            Error::InvalidParams(
                                "agent.update: `delegationDepth` must be an integer".to_string(),
                            )
                        })?)
                    };
                }
                "initialMessage" => {
                    session.initial_message = update_optional_string(value, "initialMessage")?;
                }
                "contextReferences" => {
                    session.context_references = if value.is_null() {
                        None
                    } else {
                        Some(value.clone())
                    };
                }
                "imageBlocks" => {
                    session.image_blocks = if value.is_null() {
                        None
                    } else {
                        validate_image_blocks("agent.update", Some(value))?;
                        self.validate_image_block_refs("agent.update", Some(value))
                            .await?;
                        Some(value.clone())
                    };
                }
                "fileBlocks" => {
                    session.file_blocks = if value.is_null() {
                        None
                    } else {
                        validate_file_blocks("agent.update", Some(value))?;
                        Some(value.clone())
                    };
                }
                "isBackground" => {
                    session.is_background = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `isBackground` must be a boolean".to_string(),
                        )
                    })?;
                }
                _ => unreachable!("guarded by allow-list above"),
            }
        }
        session.updated_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        // The stored model changed, so any persisted display resolution now
        // names the wrong model — clear it, same anti-staleness contract as
        // `agent.setModel` (the next session open re-resolves). Best-effort:
        // the update itself already landed.
        if obj.contains_key("model") && session.model != prior_model {
            if let Err(e) = self
                .store
                .clear_agent_session_resolved_model(&workspace_id, &agent_id)
                .await
            {
                tracing::warn!(agent = %agent_id, error = %e, "clear resolved display model failed");
            }
        }
        let event_type = if mutated_only_name {
            intent_core::events::AGENT_RENAMED
        } else {
            AGENT_UPDATED
        };
        let mut event_data = serde_json::Map::new();
        event_data.insert("agentId".into(), json!(agent_id.0));
        for (k, v) in &obj {
            event_data.insert(k.clone(), v.clone());
        }
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            event_type,
            Value::Object(event_data),
        )
        .await;
        // `status` / `isBackground` feed the needs_attention derivation (a
        // deleted or background session's pending request/question no longer
        // counts): recompute-and-compare (§6.5 step 0); other fields skip the
        // probe entirely.
        if obj.contains_key("status") || obj.contains_key("isBackground") {
            self.maybe_emit_display_status_changed(&workspace_id).await;
        }
        let lite = self.project_lite_with_flags(session);
        Ok(json!({ "success": true, "agent": lite }))
    }

    /// `agent.appendMessage` (PROTOCOL §5.5). Append a single message to the
    /// transcript. Rejected with `InvalidParams` when the agent is mid-turn
    /// (message-log mutation must not race the daemon's streaming writer).
    /// `metadata` is persisted verbatim on the row. Emits `agent:message`.
    pub(crate) async fn agent_append_message_op(
        &self,
        agent_id: AgentId,
        role: String,
        content: Value,
        metadata: Option<Value>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&agent_id).await?;
        if self.agent_is_busy(agent_id.clone()) {
            return Err(Error::InvalidParams(format!(
                "agent.appendMessage: session {} is busy — cannot mutate transcript during an active turn",
                agent_id.0
            )));
        }
        validate_message_role(&role)?;
        let created_at = now_iso();
        let message = self
            .store
            .append_agent_message_with_metadata(
                &agent_id,
                &role,
                &content,
                metadata.as_ref(),
                &created_at,
            )
            .await?;
        self.invalidate_agent_list_cache(&session.workspace_id);
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(&session.workspace_id, &agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        } else if role == "user" {
            // Schedule debounced lastActivity event (§10.1) only for
            // user-role appends: lastActivity moves at turn boundaries —
            // agent/system transcript writes are mid-turn noise for the
            // workspace ordering.
            self.schedule_last_activity_event(session.workspace_id.clone());
        }
        self.publish_agent_message_events(&session.workspace_id, &agent_id, &message, None)
            .await;
        // Stored-on-write question-hold markers (PROTOCOL §5.5), same contract
        // as the turn-end and user-send persists: an appended assistant row
        // bearing question blocks arms the pending marker, an appended user
        // row tagged `question_answers` for the marked message clears it.
        // Only those two transitions move the hold — a plain user row leaves
        // it pending — so they also gate the displayStatus recompute below.
        let hold_moved = if role == "assistant" && has_question_blocks(&content) {
            self.record_pending_questions_marker(&session.workspace_id, &agent_id, &message.id)
                .await
        } else if role == "user"
            && self
                .resolve_pending_questions_for_answer(
                    &session.workspace_id,
                    &agent_id,
                    metadata.as_ref(),
                )
                .await
        {
            // This path only persists a row (no turn of its own), so the
            // released hold needs an explicit drain kick for the entries it
            // parked — same kick `agent.dismissQuestions` performs.
            if let Some(manager) = self.agent_manager() {
                manager
                    .try_drain_queue(agent_id.clone(), session.workspace_id.clone())
                    .await;
            }
            true
        } else {
            false
        };
        // A moved question-hold derivation — an answered question set retires
        // the hold, an assistant row with a trailing question block raises it
        // — flips the workspace's needs_attention displayStatus (§6.5 step 0):
        // recompute-and-compare (monorepo#1266).
        if hold_moved {
            self.maybe_emit_display_status_changed(&session.workspace_id)
                .await;
        }
        Ok(json!({ "success": true, "message": message }))
    }

    /// `agent.replaceMessages` (PROTOCOL §5.5). Atomically swap the transcript
    /// with `messages`. Rejected with `InvalidParams` when the agent is mid-turn
    /// (same rationale as [`Services::agent_append_message_op`]). Row ids are
    /// minted by the store — callers cannot smuggle stale ids across the swap.
    /// Emits `agent:updated` with `{ replacedCount }`.
    pub(crate) async fn agent_replace_messages_op(
        &self,
        agent_id: AgentId,
        messages: Value,
    ) -> Result<Value> {
        struct Parsed {
            role: String,
            content: Value,
            metadata: Option<Value>,
            created_at: String,
        }
        let session = self.store.get_agent_session(&agent_id).await?;
        if self.agent_is_busy(agent_id.clone()) {
            return Err(Error::InvalidParams(format!(
                "agent.replaceMessages: session {} is busy — cannot mutate transcript during an active turn",
                agent_id.0
            )));
        }
        let raw = messages.as_array().ok_or_else(|| {
            Error::InvalidParams("agent.replaceMessages: `messages` must be an array".to_string())
        })?;
        let mut parsed: Vec<Parsed> = Vec::with_capacity(raw.len());
        let fallback_ts = now_iso();
        for (i, entry) in raw.iter().enumerate() {
            let obj = entry.as_object().ok_or_else(|| {
                Error::InvalidParams(format!(
                    "agent.replaceMessages: `messages[{i}]` must be an object"
                ))
            })?;
            let role = obj
                .get("role")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].role` is required"
                    ))
                })?
                .to_string();
            validate_message_role(&role)?;
            let content = obj
                .get("contentBlocks")
                .or_else(|| obj.get("content"))
                .cloned()
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].contentBlocks` is required"
                    ))
                })?;
            let metadata = match obj.get("metadata") {
                Some(Value::Null) | None => None,
                Some(v) => Some(v.clone()),
            };
            let created_at = match obj.get("timestamp").or_else(|| obj.get("createdAt")) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Null) | None => fallback_ts.clone(),
                Some(_) => {
                    return Err(Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].timestamp` must be a string"
                    )))
                }
            };
            parsed.push(Parsed {
                role,
                content,
                metadata,
                created_at,
            });
        }
        let batch: Vec<intent_store::ReplaceMessage<'_>> = parsed
            .iter()
            .map(|p| intent_store::ReplaceMessage {
                role: p.role.as_str(),
                content: &p.content,
                metadata: p.metadata.as_ref(),
                created_at: p.created_at.as_str(),
            })
            .collect();
        let inserted = self.store.replace_agent_messages(&agent_id, &batch).await?;
        self.invalidate_agent_list_cache(&session.workspace_id);
        let replaced_count = inserted.len();
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "replacedCount": replaced_count }),
        )
        .await;
        // The swap re-mints row ids, so any surviving pending-questions marker
        // is dangling: re-derive it from the new transcript (same contract as
        // the `agent.editAndRegenerate` truncation). The swapped transcript can
        // move the question-hold derivation in either direction, so the
        // re-derivation also recomputes the workspace's needs_attention
        // displayStatus (§6.5 step 0, monorepo#1266) and kicks the queue drain
        // for entries a now-released hold parked.
        self.reconcile_pending_questions_marker(&session.workspace_id, &agent_id, &inserted)
            .await;
        // Same re-mint hazard for the pending-proposals list: every entry's
        // `messageId` names a pre-swap row id, so remap each entry to the
        // newest post-swap assistant row still carrying its proposal block
        // (dropping entries whose blocks are gone).
        self.reconcile_pending_proposals(&session.workspace_id, &agent_id, &inserted)
            .await;
        Ok(json!({ "success": true, "messages": inserted }))
    }

    /// Locate the `agent.editAndRegenerate` target in an already-fetched
    /// transcript: `message_id` must reference an existing **user** message.
    /// Returns its 0-based index into `messages`; `InvalidParams` (→ `-32602`)
    /// otherwise. Pure, so validate + truncate can share ONE transcript fetch
    /// (no TOCTOU between the index and the slice it cuts).
    fn find_edit_target(messages: &[intent_core::AgentMessage], message_id: &str) -> Result<usize> {
        let idx = messages
            .iter()
            .position(|m| m.id == message_id)
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "agent.editAndRegenerate: messageId {message_id} not found in transcript"
                ))
            })?;
        if messages[idx].role != "user" {
            return Err(Error::InvalidParams(format!(
                "agent.editAndRegenerate: messageId {message_id} is not a user message (role: {})",
                messages[idx].role
            )));
        }
        Ok(idx)
    }

    /// Validate the `agent.editAndRegenerate` target: `message_id` must
    /// reference an existing **user** message in the agent's transcript.
    /// Returns the 0-based index of that message. Read-only, so the caller can
    /// reject a bad `messageId` with `-32602` BEFORE stopping an in-flight
    /// turn or mutating any state (PROTOCOL §5.5).
    pub(crate) async fn agent_validate_edit_target_op(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<usize> {
        let messages = self.store.get_agent_messages(agent_id, None).await?;
        Self::find_edit_target(&messages, message_id)
    }

    /// `agent.editAndRegenerate` truncation step: atomically truncate the
    /// transcript to just BEFORE the (already validated) user message
    /// `message_id`, dropping it and everything after it. Reuses the
    /// replaceMessages store machinery (fresh row ids / 0-based `seq`).
    /// Emits `agent:updated` with `{ truncatedCount, remainingCount }`.
    /// Returns the number of messages removed.
    ///
    /// Re-validates against the SAME transcript fetch it slices (single read;
    /// no index/slice divergence). If the target vanished between the caller's
    /// pre-stop validation and this call (concurrent `agent.replaceMessages`),
    /// this fails with `-32602` after the stop already happened — the
    /// "reject before any state change" contract covers the pre-stop check;
    /// the transcript itself is still untouched here.
    pub(crate) async fn agent_edit_truncate_op(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<usize> {
        let session = self.store.get_agent_session(agent_id).await?;
        let messages = self.store.get_agent_messages(agent_id, None).await?;
        let idx = Self::find_edit_target(&messages, message_id)?;
        let keep = &messages[..idx];
        let batch: Vec<intent_store::ReplaceMessage<'_>> = keep
            .iter()
            .map(|m| intent_store::ReplaceMessage {
                role: m.role.as_str(),
                content: &m.content,
                metadata: m.metadata.as_ref(),
                created_at: m.created_at.as_str(),
            })
            .collect();
        let inserted = self.store.replace_agent_messages(agent_id, &batch).await?;
        self.invalidate_agent_list_cache(&session.workspace_id);
        let truncated_count = messages.len() - inserted.len();
        // Question hold (PROTOCOL §5.5): truncation drops the rows the
        // pending-questions marker may name AND re-mints ids for the kept rows
        // (`replace_agent_messages`), so a surviving marker would be dangling
        // — and since the hold derivation never checks that the marked row
        // still exists, a dangling marker would wedge the hold forever. The
        // marker is therefore explicitly RE-DERIVED from the post-truncation
        // transcript (never tolerated as dangling), which also recomputes the
        // needs_attention displayStatus and kicks the drain when the
        // truncation released the hold. The dismissal marker keeps its
        // existing dangling-tolerant laxity.
        self.reconcile_pending_questions_marker(&session.workspace_id, agent_id, &inserted)
            .await;
        // Same re-mint hazard for the pending-proposals list (see
        // `agent_replace_messages_op`): remap surviving entries onto the
        // re-minted kept rows and drop entries whose carrying rows were
        // truncated away.
        self.reconcile_pending_proposals(&session.workspace_id, agent_id, &inserted)
            .await;
        self.publish_agent_mutation_event(
            &session.workspace_id,
            agent_id,
            AGENT_UPDATED,
            json!({
                "agentId": agent_id.0,
                "truncatedCount": truncated_count,
                "remainingCount": inserted.len(),
            }),
        )
        .await;
        Ok(truncated_count)
    }

    /// `agent.getModels` (PROTOCOL §5.5): auggie CLI fetch; an unavailable
    /// CLI yields an empty model list (the provider CLI owns model
    /// discovery — there is no static fallback catalog).
    pub(crate) async fn agent_get_models_op(&self) -> Result<Value> {
        let models = fetch_auggie_models(self.auggie_bin.clone())
            .await?
            .unwrap_or_default();
        Ok(json!({ "models": models }))
    }

    /// `models.list`: the rich model catalog for FE model pickers (PROTOCOL
    /// §5.30). With no `providerId` this is the backward-compatible auggie
    /// path — auggie CLI (JSON → plain-text fallback) with a success cache
    /// served fresh for [`crate::model_catalog::MODELS_STALE_AFTER`]; an
    /// aged entry is served immediately (labeled `stale`) while a refresh
    /// probe runs in the background (stale-while-revalidate,
    /// intent-hq/intent#3874), a blocking probe runs only on a true miss or
    /// `forceRefresh`, degrading
    /// to an empty list (`source: "static"`) when the CLI is unavailable;
    /// `forceRefresh` skips the cache read. With a `providerId` the request
    /// goes through the generic per-provider cache
    /// ([`crate::model_catalog`]): registered sources are probed and cached
    /// per (provider, version key); unknown providers degrade to an empty
    /// list with a `warning` — never an error.
    pub(crate) async fn models_list_op(
        &self,
        provider_id: Option<String>,
        force_refresh: bool,
    ) -> Result<Value> {
        let Some(provider_id) = provider_id else {
            return self.models_list_auggie_op(force_refresh, false).await;
        };
        if provider_id == "auggie" {
            let mut response = self.models_list_auggie_op(force_refresh, true).await?;
            response["providerId"] = Value::String(provider_id);
            return Ok(response);
        }
        let Some(source) = crate::model_catalog::source_for(&provider_id) else {
            return Ok(static_provider_response(
                &provider_id,
                &format!("no dynamic model discovery for provider '{provider_id}'"),
            ));
        };
        let version_key = (source.version_key)();
        let resolved = crate::model_catalog::resolve_with_cache(
            &self.models_catalog,
            &provider_id,
            &version_key,
            force_refresh,
            crate::model_catalog::ModelCatalogCache::now_ms(),
            source.fetch,
        )
        .await;
        match resolved.models {
            Some(models) => {
                let mut out =
                    json!({ "providerId": provider_id, "models": models, "source": provider_id });
                if resolved.stale {
                    out["stale"] = Value::Bool(true);
                }
                if let Some(w) = resolved.warning {
                    out["warning"] = Value::String(w);
                }
                Ok(out)
            }
            None => Ok(static_provider_response(
                &provider_id,
                &resolved
                    .warning
                    .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed")),
            )),
        }
    }

    /// The legacy no-`providerId` `models.list` path, routed through the same
    /// generic per-provider cache as `providerId: "auggie"` — same provider
    /// id and same registry-derived version key — so the two can never
    /// diverge: one cache, one single-flight, one negative window, one
    /// staleness threshold. Only the
    /// wire shape differs: the response omits the `providerId` field,
    /// `source` is `"auggie"` or `"static"`, and an empty list is the
    /// fallback when the probe fails with no last-good list. A failed probe
    /// with a last-good cached list serves it labeled `stale: true` +
    /// `warning` — never silently — whether or not the read was forced.
    /// The provider-scoped wire shape also keeps the resolver warning on an
    /// empty static fallback; the legacy shape omits it for compatibility.
    async fn models_list_auggie_op(
        &self,
        force_refresh: bool,
        include_fallback_warning: bool,
    ) -> Result<Value> {
        let auggie_bin = self.auggie_bin.clone();
        let mut response = self
            .models_list_auggie_with(
                force_refresh,
                crate::model_catalog::ModelCatalogCache::now_ms(),
                || Box::pin(fetch_auggie_models_rich(auggie_bin)),
            )
            .await?;
        if !include_fallback_warning && response["source"] == "static" {
            response
                .as_object_mut()
                .expect("Auggie model response must be an object")
                .remove("warning");
        }
        Ok(response)
    }

    /// [`Self::models_list_auggie_op`] with an injectable fetch and clock
    /// (the unit-test seam). Delegates all cache policy — fresh-window
    /// serving, stale-while-revalidate background refresh,
    /// negative window, single-flight, last-good fallback — to
    /// [`crate::model_catalog::resolve_with_cache`] and only maps the
    /// resolved rows onto the shared internal shape. The caller removes the
    /// empty-fallback warning only when it must preserve the legacy wire shape.
    async fn models_list_auggie_with<F>(
        &self,
        force_refresh: bool,
        now_ms: u64,
        fetch: F,
    ) -> Result<Value>
    where
        F: FnOnce() -> intent_core::BoxFuture<'static, Option<Vec<Value>>> + Send + 'static,
    {
        // Derive the version key from the registry (like the per-provider
        // path) so an auggie pin added later cannot silently split the
        // legacy and providerId caches again.
        let version_key = crate::model_catalog::source_for("auggie")
            .map(|s| (s.version_key)())
            .unwrap_or_default();
        let resolved = crate::model_catalog::resolve_with_cache(
            &self.models_catalog,
            "auggie",
            &version_key,
            force_refresh,
            now_ms,
            || {
                Box::pin(async move {
                    match fetch().await {
                        Some(models) => crate::model_catalog::ModelFetchResult {
                            models: Some(models),
                            warning: None,
                        },
                        None => crate::model_catalog::ModelFetchResult {
                            models: None,
                            warning: Some(
                                "auggie CLI unavailable or returned no models".to_string(),
                            ),
                        },
                    }
                })
            },
        )
        .await;
        if let Some(models) = resolved.models {
            let mut out = json!({ "models": models, "source": "auggie" });
            if resolved.stale {
                out["stale"] = Value::Bool(true);
                if let Some(w) = resolved.warning {
                    out["warning"] = Value::String(w);
                }
            }
            Ok(out)
        } else {
            let mut out = json!({ "models": [], "source": "static" });
            if let Some(warning) = resolved.warning {
                out["warning"] = Value::String(warning);
            }
            Ok(out)
        }
    }

    /// `agent.queueMessage` (PROTOCOL §5.5). Enqueues the message, publishes
    /// `agent:queue:updated`, and asks the runtime [`AgentManager`] (when attached)
    /// to drain the queue immediately if the agent is idle — closing the bug where
    /// a queued message would never be sent because the BE only drained the queue
    /// from a live worker loop.
    pub(crate) async fn agent_queue_message_op(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
    ) -> Result<Value> {
        // Attachment-reference validation (PROTOCOL §5.5) before any state
        // change, matching `agent.sendMessage`.
        validate_file_blocks("agent.queueMessage", file_blocks.as_ref())?;
        validate_image_blocks("agent.queueMessage", image_blocks.as_ref())?;
        // monorepo#568: reject nonexistent targets BEFORE enqueueing — a
        // truncated/mistyped id would otherwise create a queue entry that
        // never drains (same fail-closed contract as `agent.sendMessage`).
        let session = self.require_agent_session(&agent_id).await?;
        self.validate_image_block_refs("agent.queueMessage", image_blocks.as_ref())
            .await?;
        let (queued, position) = self.enqueue_message(
            &agent_id,
            content,
            image_blocks,
            file_blocks,
            None,
            None,
            false,
        );
        let result = json!({
            "success": true,
            "queuedMessage": queued.to_value(position),
            "turnId": queued.turn_id,
        });
        self.publish_queue_updated(&agent_id).await;
        if let Some(manager) = self.agent_manager() {
            manager
                .try_drain_queue(agent_id, session.workspace_id)
                .await;
        }
        Ok(result)
    }

    /// `agent.getQueue` (PROTOCOL §5.5). When `workspace_id` is supplied the
    /// callee verifies the session belongs to that workspace (defense-in-depth
    /// against a bare `agentId` probe across workspaces); a mismatch surfaces
    /// as `NotFound`.
    pub(crate) async fn agent_get_queue_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        if let Some(ws) = workspace_id.as_ref() {
            let session = self.store.get_agent_session(&agent_id).await?;
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let queue = self.queue_snapshot(&agent_id);
        Ok(json!({ "success": true, "queue": queue }))
    }

    /// `agent.editQueuedMessage` (PROTOCOL §5.5). Updates the entry's content
    /// in place (matching the reference's `handleEditQueuedMessage`) and, when
    /// the optional `editing` flag is provided, transitions the entry between
    /// "ready-to-send" (`editing = false`) and "under edit" (`editing = true`).
    /// Publishes `agent:queue:updated` with the post-edit snapshot. Returns
    /// `Internal` when the message id is unknown — only `removeQueuedMessage` is
    /// idempotent.
    ///
    /// When an entry transitions `editing: true → false` (the FE finished
    /// editing) we additionally fire `try_drain_queue` so the message
    /// self-drains as if it had just been enqueued — honouring the user's
    /// "re-queued on save, which self-drains" semantics (PROTOCOL §5.5/§6.5).
    pub(crate) async fn agent_edit_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
        editing: Option<bool>,
    ) -> Result<Value> {
        let (edited, was_editing, now_editing) = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let queue = guard
                .get_mut(&agent_id)
                .ok_or_else(|| Error::Internal("Queued message not found".to_string()))?;
            let position = queue
                .iter()
                .position(|m| m.id == message_id)
                .ok_or_else(|| Error::Internal("Queued message not found".to_string()))?;
            let was = queue[position].editing;
            queue[position].content = content;
            if let Some(flag) = editing {
                queue[position].editing = flag;
            }
            let now = queue[position].editing;
            (queue[position].to_value(position), was, now)
        };
        self.publish_queue_updated(&agent_id).await;
        // editing: true → false ⇒ the message is now ready-to-send. Self-drain.
        if was_editing && !now_editing {
            if let Some(manager) = self.agent_manager() {
                if let Ok(session) = self.store.get_agent_session(&agent_id).await {
                    manager
                        .try_drain_queue(agent_id, session.workspace_id)
                        .await;
                }
            }
        } else if !was_editing && now_editing {
            // editing: false → true ⇒ the entry left the ready-to-send queue.
            // If that emptied it while the agent is idle, an interim-skipped
            // watch (monorepo#1280) has no further completion coming — re-run
            // delivery so it still gets its wake.
            self.redeliver_completion_after_queue_mutation(&agent_id)
                .await;
        }
        Ok(json!({ "success": true, "queuedMessage": edited }))
    }

    /// `agent.removeQueuedMessage` (PROTOCOL §5.5). **Idempotent**: returns
    /// `{ success: true }` whether or not the message (or the agent's queue) was
    /// found. The FE's seeded queue can diverge from the BE's in-memory queue
    /// (especially after a daemon restart); the original "Queued message not
    /// found" error caused the FE's optimistic delete to roll back, leaving
    /// ghost messages on screen.
    pub(crate) async fn agent_remove_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        let removed = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            match guard.get_mut(&agent_id) {
                Some(queue) => {
                    let before = queue.len();
                    queue.retain(|m| m.id != message_id);
                    before != queue.len()
                }
                None => false,
            }
        };
        if removed {
            self.publish_queue_updated(&agent_id).await;
            // monorepo#1280: a retraction that empties the ready-to-send
            // queue while the agent is idle strands any watch whose
            // `agent:idle` was skipped as interim — re-run delivery.
            self.redeliver_completion_after_queue_mutation(&agent_id)
                .await;
        }
        Ok(json!({ "success": true }))
    }

    /// Ownership-checked removal for the MCP `ws.agent.removeQueuedMessage`
    /// binding (PROTOCOL §6.8). Removes the entry ONLY when its
    /// `messageMetadata.fromAgentId` equals `caller_agent_id`; an entry sent
    /// by another agent, or by the user/FE (no `fromAgentId` attribution), is
    /// rejected with `InvalidParams`. Unlike the idempotent FE op above, an
    /// unknown message id is an error — a retracting agent needs to know its
    /// target was not found. On success republishes `agent:queue:updated`
    /// (which also persists) — same path as the FE RPC.
    pub(crate) async fn agent_remove_queued_message_owned_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        caller_agent_id: AgentId,
    ) -> Result<Value> {
        {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let queue = guard
                .get_mut(&agent_id)
                .ok_or_else(|| Error::NotFound(format!("queued message {message_id}")))?;
            let position = queue
                .iter()
                .position(|m| m.id == message_id)
                .ok_or_else(|| Error::NotFound(format!("queued message {message_id}")))?;
            let from_agent_id = queue[position]
                .message_metadata
                .as_ref()
                .and_then(|md| md.get("fromAgentId"))
                .and_then(Value::as_str);
            if from_agent_id != Some(caller_agent_id.as_str()) {
                return Err(Error::InvalidParams(format!(
                    "queued message {message_id} belongs to another sender (or the user) and cannot be removed"
                )));
            }
            queue.remove(position);
        }
        self.publish_queue_updated(&agent_id).await;
        // monorepo#1280: same strand guard as the FE removal op above.
        self.redeliver_completion_after_queue_mutation(&agent_id)
            .await;
        Ok(json!({ "success": true, "messageId": message_id }))
    }

    /// Resolve the target agent session, failing closed on a nonexistent id
    /// (monorepo#564): a truncated/mistyped `agentId` must surface a
    /// client-facing `-32602` naming the id instead of silently proceeding
    /// (auto-queue / phantom watch). Only `NotFound` maps to `InvalidParams`;
    /// internal store failures propagate unchanged — mirrors the
    /// `app_agents_wait_op` target-validation loop.
    ///
    /// Soft-retire inertness: a RETIRED session (`retired_at` set) is rejected
    /// with a clear "agent is retired" `-32602`. Every caller of this helper
    /// is a mutation/interaction path (sends, queueing, watches, wakes, queue
    /// migration targets, turn starts via the manager), and none of them may
    /// touch a retired session — reads that must still serve retired rows
    /// (`agent.get`, `agent.getSession`, conversation reads) do not come
    /// through here.
    pub(crate) async fn require_agent_session(&self, agent_id: &AgentId) -> Result<AgentSession> {
        let session = self
            .store
            .get_agent_session(agent_id)
            .await
            .map_err(|e| match e {
                Error::NotFound(_) => {
                    Error::InvalidParams(format!("unknown agent id: {}", agent_id.0))
                }
                other => other,
            })?;
        if session.retired_at.is_some() {
            return Err(Error::InvalidParams(format!(
                "agent {} is retired; restore it with agent.restore before interacting",
                agent_id.0
            )));
        }
        Ok(session)
    }

    /// Validate the reference arm of an `imageBlocks` array against the
    /// attachment registry (PROTOCOL §5.5, monorepo#3338): every
    /// `attachmentId` must name a registered attachment, and the recorded
    /// sizes must fit [`IMAGE_REF_MAX_BYTES`] **in aggregate** across all
    /// references in the array (a per-reference cap alone would let a small
    /// request name many attachments whose resolved bytes expand one ACP
    /// prompt far past the transport bound the cap mirrors). Rejections are
    /// `-32602` naming the id, raised at the RPC seam BEFORE any state
    /// change; inline-data entries are untouched. Callers run the shape
    /// check ([`validate_image_blocks`]) first. The lookup is registry-wide
    /// rather than workspace-scoped by design: `workspace.create`'s
    /// `initialAgent` references attachments placed BEFORE the new workspace
    /// exists, so they necessarily live in another workspace's registry;
    /// resolution reads from the record's own workspace root either way.
    pub(crate) async fn validate_image_block_refs(
        &self,
        method: &str,
        image_blocks: Option<&Value>,
    ) -> Result<()> {
        let mut total: u64 = 0;
        for id in image_block_ref_ids(image_blocks) {
            let record = self.store.get_attachment(&id).await.map_err(|e| match e {
                Error::NotFound(_) => {
                    Error::InvalidParams(format!("{method}: unknown attachment id: {id}"))
                }
                other => other,
            })?;
            let size = u64::try_from(record.size).unwrap_or(0);
            if size > IMAGE_REF_MAX_BYTES {
                return Err(Error::InvalidParams(format!(
                    "{method}: attachment {id} is {} bytes — exceeds the {IMAGE_REF_MAX_BYTES} byte cap for image references",
                    record.size
                )));
            }
            total = total.saturating_add(size);
            if total > IMAGE_REF_MAX_BYTES {
                return Err(Error::InvalidParams(format!(
                    "{method}: image references total {total} bytes at attachment {id} — exceeds the {IMAGE_REF_MAX_BYTES} byte aggregate cap for image references"
                )));
            }
        }
        Ok(())
    }

    /// Resolve reference-arm image entries into inline `{ data, mimeType }`
    /// form for prompt assembly (monorepo#3338): the attachment's bytes are
    /// read from the record's own canonical workspace root (with the same
    /// within-root containment guard as `file.getAttachment`) and
    /// base64-encoded so the ACP receives the image exactly as an inline
    /// block. MIME resolves block `mimeType` > registry `mime_type` >
    /// extension inference. Fail-soft by design — ingress already rejected
    /// bad references, so a row/file that vanished since is skipped with a
    /// warning rather than breaking the turn (same convention as note-image
    /// resolution); the same skip re-enforces the [`IMAGE_REF_MAX_BYTES`]
    /// aggregate cap over the bytes actually read, in case files grew after
    /// ingress validated the recorded sizes. Inline entries pass through
    /// untouched; inputs without references return unchanged.
    pub(crate) async fn resolve_image_block_refs(
        &self,
        image_blocks: Option<Value>,
    ) -> Option<Value> {
        use base64::Engine as _;
        if image_block_ref_ids(image_blocks.as_ref()).is_empty() {
            return image_blocks;
        }
        let arr = match image_blocks {
            Some(Value::Array(arr)) => arr,
            other => return other,
        };
        let mut out = Vec::with_capacity(arr.len());
        let mut total: u64 = 0;
        for img in arr {
            let Some(obj) = img.as_object() else {
                out.push(img);
                continue;
            };
            let attachment_id = obj
                .get("attachmentId")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty());
            let (Some(id), None) = (attachment_id, obj.get("data").and_then(Value::as_str)) else {
                out.push(img);
                continue;
            };
            let Ok(record) = self.store.get_attachment(id).await else {
                tracing::warn!(attachment = %id, "image reference: attachment row vanished; skipping");
                continue;
            };
            let root = crate::file_ops::resolve_root(&self.store, &record.workspace_id, None).await;
            if root.is_empty() {
                tracing::warn!(attachment = %id, "image reference: attachment workspace has no resolved root; skipping");
                continue;
            }
            let Ok(path) = crate::file_ops::resolve_attachment_source(&root, &record.stored_path)
            else {
                tracing::warn!(attachment = %id, "image reference: stored path escapes the workspace; skipping");
                continue;
            };
            let bytes = match tokio::fs::read(&path).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(attachment = %id, error = %e, "image reference: read failed; skipping");
                    continue;
                }
            };
            if bytes.len() as u64 > IMAGE_REF_MAX_BYTES {
                tracing::warn!(attachment = %id, size = bytes.len(), "image reference: over the byte cap; skipping");
                continue;
            }
            if total.saturating_add(bytes.len() as u64) > IMAGE_REF_MAX_BYTES {
                tracing::warn!(attachment = %id, size = bytes.len(), total, "image reference: over the aggregate byte cap; skipping");
                continue;
            }
            total += bytes.len() as u64;
            let mime = obj
                .get("mimeType")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| record.mime_type.clone())
                .unwrap_or_else(|| crate::note_ops::mime_from_extension(&record.file_name));
            let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
            out.push(json!({ "type": "image", "data": data, "mimeType": mime }));
        }
        Some(Value::Array(out))
    }

    /// `agent.sendMessage`: persist the user message; on failure auto-queue
    /// (PROTOCOL §5.5). Fails closed on a nonexistent target (monorepo#564).
    pub(crate) async fn agent_send_message_op(
        &self,
        agent_id: AgentId,
        mut content: String,
        message_id: Option<String>,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
    ) -> Result<Value> {
        // Validate message_id length to prevent unbounded storage.
        if let Some(ref id) = message_id {
            if id.len() > MAX_MESSAGE_ID_LEN {
                return Err(Error::InvalidParams(format!(
                    "messageId exceeds maximum length of {MAX_MESSAGE_ID_LEN} bytes"
                )));
            }
        }
        // A2A sender header (intent-hq/intent#3721, monorepo#1015): the store-only front door —
        // mirrors the runtime `AgentManager::send_message` prepend so both
        // wirings persist identical agent-origin content. Idempotent: the
        // runtime path may already have annotated (delegate kickoff), and
        // the exact-header guard makes the second application a no-op.
        annotate_sender_attribution(&mut content, message_metadata.as_ref());
        // Attachment-reference validation (PROTOCOL §5.5): every file and
        // image block must carry exactly one of `data` / `attachmentId`,
        // rejected before any state change.
        validate_file_blocks("agent.sendMessage", file_blocks.as_ref())?;
        validate_image_blocks("agent.sendMessage", image_blocks.as_ref())?;
        // monorepo#564: reject nonexistent targets BEFORE any state change —
        // the auto-queue fallback below is for store-append failures on a
        // REAL agent, not a phantom queue for an id that will never drain.
        let session = self.require_agent_session(&agent_id).await?;
        // Image references must name registered attachments
        // (monorepo#3338) — rejected before any state change.
        self.validate_image_block_refs("agent.sendMessage", image_blocks.as_ref())
            .await?;
        // STAB-133: persist FE-supplied attachments alongside the text block so
        // the transcript row carries them (the conversation view renders them).
        let blocks = user_message_blocks(&content, image_blocks.as_ref(), file_blocks.as_ref());
        let created_at = now_iso();
        let message = match message_id {
            Some(id) => {
                self.store
                    .append_agent_message_with_id(
                        &agent_id,
                        &id,
                        "user",
                        &blocks,
                        message_metadata.as_ref(),
                        &created_at,
                    )
                    .await
            }
            None => {
                self.store
                    .append_agent_message_with_metadata(
                        &agent_id,
                        "user",
                        &blocks,
                        message_metadata.as_ref(),
                        &created_at,
                    )
                    .await
            }
        };
        match message {
            Ok(message) => {
                self.invalidate_agent_list_cache(&session.workspace_id);
                // Refresh agent_session.updated_at so the FE agent-card timestamp
                // reflects message activity, not just status transitions (STAB-19).
                // Reuses the session validated above; best-effort (logged on error).
                if let Err(e) = self
                    .store
                    .refresh_agent_session_timestamp(&session.workspace_id, &agent_id, &created_at)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
                }
                // Publish agent:message events using the store-returned message id.
                self.publish_agent_message_events(&session.workspace_id, &agent_id, &message, None)
                    .await;
                // Answer intake (PROTOCOL §5.5, question hold): parity with
                // the runtime `AgentManager::send_message` persist — a
                // `question_answers` tag naming the marked assistant message
                // clears the pending-questions marker, and only that clear can
                // retire the workspace's needs_attention displayStatus (§6.5
                // step 0): recompute-and-compare.
                if self
                    .resolve_pending_questions_for_answer(
                        &session.workspace_id,
                        &agent_id,
                        message_metadata.as_ref(),
                    )
                    .await
                {
                    self.maybe_emit_display_status_changed(&session.workspace_id)
                        .await;
                }
                Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
            }
            Err(append_err) => {
                // Check-then-act race guard (monorepo#564): if the session
                // vanished between the up-front validation and the append
                // (concurrent delete), fail closed like the guard rather than
                // auto-queueing a phantom message for a gone agent.
                if self.store.get_agent_session(&agent_id).await.is_err() {
                    tracing::warn!(agent = %agent_id, error = %append_err, "agent session vanished mid-send; rejecting instead of auto-queueing");
                    return Err(Error::InvalidParams(format!(
                        "unknown agent id: {}",
                        agent_id.0
                    )));
                }
                // The agent exists but the append failed (e.g. duplicate
                // client-supplied messageId). STAB-7: preserve image_blocks and
                // file_blocks when auto-queueing, matching the runtime-manager
                // path's behavior. `message_metadata` rides along too: an
                // answer auto-queued after a failed write must keep its
                // `question_answers` tag, or the drain persist can no longer
                // clear the pending-questions marker and the hold wedges.
                let (queued, position) = self.enqueue_message(
                    &agent_id,
                    content,
                    image_blocks,
                    file_blocks,
                    message_metadata,
                    None,
                    false,
                );
                let result = json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                    "turnId": queued.turn_id,
                });
                self.publish_queue_updated(&agent_id).await;
                Ok(result)
            }
        }
    }

    /// `agent.sendQueuedMessageNow` (store-only fallback when no `AgentManager`
    /// is attached): atomically remove the queued entry named by `message_id`
    /// and persist it to the transcript immediately, preserving the rest of
    /// the queue (PROTOCOL §5.5). Deliberately NOT idempotent (unlike
    /// `agent.removeQueuedMessage`): an absent entry returns `-32602`
    /// ("queued message not found") with NO side effects, so the client knows
    /// the atomic send did not happen. On a persist failure the entry is
    /// restored at the FRONT of the queue before the error surfaces — the
    /// transactional guarantee that the message is never lost.
    pub(crate) async fn agent_send_queued_message_now_op(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        // Fail closed on a nonexistent target BEFORE touching the queue
        // (monorepo#564).
        let session = self.require_agent_session(&agent_id).await?;
        let entry = self
            .take_queued_message(&agent_id, &message_id)
            .ok_or_else(|| {
                Error::InvalidParams(format!("queued message not found: {message_id}"))
            })?;
        // Publish the shrunk snapshot (write-through persist inside).
        self.publish_queue_updated(&agent_id).await;
        // A terminal-failure requeue whose user row already reached the
        // transcript must not double-append (STAB-112).
        if entry.persisted {
            return Ok(json!({ "success": true, "queued": false, "messageId": entry.id }));
        }
        // Delivery-time unblocked hints (monorepo#2044): on the no-manager
        // path this persist IS the delivery, so the section is resolved here
        // — parity with the manager's `send_queued_message_now` and the
        // store-only `deliver_parent_wake` branch. Same idempotency guard:
        // a content that already carries the section is never re-annotated.
        let mut entry = entry;
        if !entry
            .content
            .contains(ready_delta::UNBLOCKED_SECTION_PREFIX)
            && ready_delta::metadata_has_triggers(entry.message_metadata.as_ref())
        {
            if let Some(section) = self
                .unblocked_section_for_delivery(
                    &agent_id,
                    std::iter::once(entry.message_metadata.as_ref()),
                )
                .await
            {
                entry.content = format!("{}\n\n{}", entry.content, section);
            }
        }
        // STAB-133: persist the entry's attachments alongside the text block.
        let blocks = user_message_blocks(
            &entry.content,
            entry.image_blocks.as_ref(),
            entry.file_blocks.as_ref(),
        );
        let created_at = now_iso();
        let message = match self
            .store
            .append_agent_message_with_id(
                &agent_id,
                &entry.id,
                "user",
                &blocks,
                entry.message_metadata.as_ref(),
                &created_at,
            )
            .await
        {
            Ok(message) => message,
            Err(e) => {
                // Transactional guarantee: restore the entry at the front so
                // the message is never lost, then surface the failure.
                self.requeue_front(&agent_id, entry);
                self.publish_queue_updated(&agent_id).await;
                return Err(e);
            }
        };
        self.invalidate_agent_list_cache(&session.workspace_id);
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(&session.workspace_id, &agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        } else if entry.user_origin {
            // Schedule debounced lastActivity event (§10.1) for USER-origin
            // entries only — parity with the queue-drain `persist_user` gate:
            // on this store-only path no turn runs, so the user's force-sent
            // message is itself the boundary.
            self.schedule_last_activity_event(session.workspace_id.clone());
        }
        // Publish agent:message events using the store-returned message id.
        self.publish_agent_message_events(&session.workspace_id, &agent_id, &message, None)
            .await;
        // Answer intake (PROTOCOL §5.5, question hold): parity with the
        // runtime `send_queued_message_now` persist — only a matching answer
        // tag clears the marker, and only that clear can retire the
        // workspace's needs_attention displayStatus (§6.5 step 0).
        if self
            .resolve_pending_questions_for_answer(
                &session.workspace_id,
                &agent_id,
                entry.message_metadata.as_ref(),
            )
            .await
        {
            self.maybe_emit_display_status_changed(&session.workspace_id)
                .await;
        }
        Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
    }

    /// Question-hold derivation (PROTOCOL §5.5, question hold): `true` iff the
    /// session's persisted pending-questions marker
    /// ([`intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY`], written at turn end
    /// when the assistant tail bears `application/vnd.intent.question+json`
    /// resource blocks) is set AND differs from the dismissal marker
    /// ([`intent_core::DISMISSED_QUESTIONS_MESSAGE_ID_KEY`]). Stored-on-write,
    /// so this is a bounded single-row metadata read: pendingness survives
    /// later user messages, later agent turns, and daemon restarts, and only
    /// an answer (`messageMetadata.type == "question_answers"` naming the
    /// marked message), an explicit `agent.dismissQuestions`, or a NEWER
    /// question-bearing turn resolves it.
    ///
    /// Pre-upgrade sessions (marker key absent entirely — the daemon never
    /// wrote it) fall back to the legacy transcript tail walk so a hold that
    /// was live across the upgrade is not lost: walk back from the tail past
    /// any trailing `system` rows (e.g. the resume-interruption marker
    /// `resume_interrupted_agent` appends BEFORE its `Automatic`
    /// continuation) and hold when the first non-system row is an
    /// un-dismissed question-bearing assistant message. A marker written as
    /// the empty string is authoritative ("nothing pending") and does NOT
    /// fall back. A hold derived that way is immediately MATERIALIZED as a
    /// marker so it survives the very next user message: the tail walk stops
    /// seeing the question once a plain user row lands, which is exactly the
    /// disappearance this contract exists to prevent.
    ///
    /// Fails open (`false`) on store errors so a read failure can never wedge
    /// deliveries.
    pub(crate) async fn question_hold_active(&self, agent_id: &AgentId) -> bool {
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return false;
        };
        if session.pending_questions_marker_written() {
            return match session.pending_questions_message_id() {
                Some(pending) => session.dismissed_questions_message_id() != Some(pending),
                None => false,
            };
        }
        let Some(pending) = self.question_hold_active_from_tail(agent_id).await else {
            return false;
        };
        self.record_pending_questions_marker(&session.workspace_id, agent_id, &pending)
            .await;
        true
    }

    /// The number of question resource blocks still pending on the marked
    /// question message — the counting form of the question-hold derivation
    /// ([`Services::question_hold_active`] holds exactly when this is
    /// non-zero, both keyed on the persisted pending-questions marker): the
    /// block count of the marker's message when the marker is set and not
    /// dismissed, `0` otherwise. Pre-upgrade sessions (marker key never
    /// written) fall back to the tail derivation and materialize the marker,
    /// mirroring [`Services::question_hold_active`]. Backs the
    /// `numQuestionsAsked` snapshot field alongside the turn-attachment
    /// registry count ([`Services::pending_question_count`]). Bounded: one
    /// session read plus at most one single-row message read
    /// ([`Store::get_agent_message_by_id`]) per call — this runs on every
    /// turn prompt. Store errors fail open to `0`.
    pub(crate) async fn pending_question_tail_count(&self, agent_id: &AgentId) -> usize {
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return 0;
        };
        let pending = if session.pending_questions_marker_written() {
            session.pending_questions_message_id().map(str::to_string)
        } else {
            let pending = self.question_hold_active_from_tail(agent_id).await;
            if let Some(id) = pending.as_deref() {
                self.record_pending_questions_marker(&session.workspace_id, agent_id, id)
                    .await;
            }
            pending
        };
        let Some(pending) = pending else {
            return 0;
        };
        if session.dismissed_questions_message_id() == Some(pending.as_str()) {
            return 0;
        }
        let Ok(Some(msg)) = self.store.get_agent_message_by_id(agent_id, &pending).await else {
            return 0;
        };
        question_block_count(&msg.content)
    }

    /// This agent's structured questions currently pending — the
    /// `numQuestionsAsked` snapshot field: questions registered via
    /// `ws.app.question.ask` earlier in the CURRENT turn that are still
    /// waiting for the turn-end drain (turn-attachment registry, question
    /// MIME type), plus questions already presented on the marked question
    /// message that are still awaiting an answer or dismissal
    /// ([`Services::pending_question_tail_count`]). The two sources are
    /// disjoint by construction: the registry drains into the trailing
    /// message when the turn finalizes.
    pub(crate) async fn pending_question_count(&self, agent_id: &AgentId) -> usize {
        let in_turn = self.turn_attachments.pending_count_by_mime(
            agent_id,
            intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE,
        );
        in_turn + self.pending_question_tail_count(agent_id).await
    }

    /// Legacy transcript tail-walk hold derivation, retained as the
    /// pre-upgrade fallback for sessions with no persisted pending-questions
    /// marker (see [`Services::question_hold_active`]). Returns the id of the
    /// question-bearing assistant message holding the session, so the caller
    /// can materialize it as the marker.
    async fn question_hold_active_from_tail(&self, agent_id: &AgentId) -> Option<String> {
        // Trailing `system` rows (e.g. repeated interruption notices) are
        // transparent to the derivation, so the anchor is simply the newest
        // non-system row — resolved by the store in one index-backed
        // statement that decodes at most ONE message
        // ([`Store::get_last_non_system_message`]), never by paging full
        // rows back through the tail. An empty or all-system transcript has
        // no hold; store errors fail open.
        let Ok(Some(last)) = self.store.get_last_non_system_message(agent_id).await else {
            return None;
        };
        if last.role != "assistant" || !has_question_blocks(&last.content) {
            return None;
        }
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return None;
        };
        (session.dismissed_questions_message_id() != Some(last.id.as_str())).then_some(last.id)
    }

    /// Persist the pending-questions marker for `message_id` — the assistant
    /// message whose just-persisted content carries question resource blocks
    /// (called from the turn-end persist paths). Single-slot: a newer
    /// question-bearing turn overwrites an older marker, which is exactly the
    /// spec's "newest set supersedes" rule. The persisted message `seq` orders
    /// competing completions, and the per-agent mutation lock keeps the write
    /// plus its event in that same order. Atomic single-key `json_set` so
    /// sibling metadata keys (`dismissedQuestionsMessageId`,
    /// `lastSeenMessageId`) are preserved. A successful write emits
    /// `agent:updated` with the marker so clients re-read the `AgentLite`
    /// projection. Returns `true` only when this call committed the latest
    /// marker. Best-effort: a failure is logged and never fails the turn (the
    /// hold simply stays as it was).
    pub(crate) async fn record_pending_questions_marker(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        message_id: &str,
    ) -> bool {
        self.park_pending_marker_mutation("set", message_id).await;
        let lock = self.pending_question_mutation_locks.lock_for(agent_id);
        let _guard = lock.lock().await;

        let candidate = match self
            .store
            .get_agent_message_by_id(agent_id, message_id)
            .await
        {
            Ok(Some(message)) => message,
            Ok(None) => return false,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to order pending-questions marker");
                return false;
            }
        };
        let session = match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to read pending-questions marker");
                return false;
            }
        };
        if session.pending_questions_message_id() == Some(message_id) {
            return false;
        }
        if let Some(current_id) = session.pending_questions_message_id() {
            match self
                .store
                .get_agent_message_by_id(agent_id, current_id)
                .await
            {
                Ok(Some(current)) if current.seq >= candidate.seq => return false,
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(agent = %agent_id, error = %e, "failed to order pending-questions marker");
                    return false;
                }
            }
        }
        let current_value = session
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY));
        if current_value.is_some_and(|value| !value.is_string()) {
            tracing::warn!(
                agent = %agent_id,
                "pending-questions marker has a non-string value"
            );
            return false;
        }
        let current_value = current_value.and_then(Value::as_str).map(str::to_string);
        let expected = if session.pending_questions_marker_written() {
            current_value.as_deref().map(Some)
        } else {
            Some(None)
        };
        match self
            .store
            .set_pending_questions_marker_if_unanswered(
                workspace_id,
                agent_id,
                expected,
                &candidate,
                &now_iso(),
            )
            .await
        {
            Ok(true) => {
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_UPDATED,
                    json!({
                        "agentId": agent_id.0,
                        "pendingQuestionsMessageId": message_id,
                    }),
                )
                .await;
                true
            }
            Ok(false) => false,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to persist pending-questions marker");
                false
            }
        }
    }

    /// Clear the pending-questions marker (written as the empty string, which
    /// reads back as "no pending questions" while still marking the session as
    /// marker-aware so the pre-upgrade tail-walk fallback stays off). A
    /// successful write emits `agent:updated` with the written empty string so
    /// clients can distinguish the clear from a legacy-absent marker. When
    /// `expected` is set, the existing per-key CAS clears only that exact
    /// marker. Returns `true` only when the clear committed. Best-effort: a
    /// failure is logged and never fails the caller.
    pub(crate) async fn clear_pending_questions_marker(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        expected: Option<&str>,
    ) -> bool {
        let lock = self.pending_question_mutation_locks.lock_for(agent_id);
        let _guard = lock.lock().await;
        self.clear_pending_questions_marker_locked(workspace_id, agent_id, expected)
            .await
    }

    async fn clear_pending_questions_marker_locked(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        expected: Option<&str>,
    ) -> bool {
        let expected_owned = match expected {
            Some(value) => Some(Some(value.to_string())),
            None => match self.store.get_agent_session_summary(agent_id).await {
                Ok(session) => {
                    let current = session
                        .metadata
                        .as_ref()
                        .and_then(|metadata| {
                            metadata.get(intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY)
                        })
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    if current.as_deref() == Some("") {
                        return false;
                    }
                    if session.pending_questions_marker_written() {
                        current.map(Some)
                    } else {
                        Some(None)
                    }
                }
                Err(e) => {
                    tracing::warn!(agent = %agent_id, error = %e, "failed to read pending-questions marker");
                    return false;
                }
            },
        };
        let expected_guard = expected_owned.as_ref().map(|value| value.as_deref());
        match self
            .store
            .set_agent_session_metadata_key(
                workspace_id,
                agent_id,
                intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY,
                "",
                expected_guard,
                &now_iso(),
            )
            .await
        {
            Ok(true) => {
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_UPDATED,
                    json!({
                        "agentId": agent_id.0,
                        "pendingQuestionsMessageId": "",
                    }),
                )
                .await;
                true
            }
            Ok(false) => false,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to clear pending-questions marker");
                false
            }
        }
    }

    /// Merge `proposal_ids` (the proposal-resource blocks of the assistant
    /// message `message_id`, in block order) into the session's ordered
    /// pending-proposals list ([`intent_core::PENDING_PROPOSALS_KEY`],
    /// PROTOCOL §5.5): each id lands as `{ proposalId, messageId }` appended
    /// last; a re-proposed id replaces its older entry (dedupe by
    /// `proposalId`, newest wins). Called from the turn-end persist paths
    /// after the carrying message committed. Serialized per agent on the
    /// same mutation lock as the question markers so concurrent completions
    /// cannot interleave the read-modify-write. Atomic single-key `json_set`
    /// so sibling metadata keys are preserved. A committed change emits
    /// `agent:updated` with the new list so clients re-read the `AgentLite`
    /// projection; an unchanged list (same ids already recorded under the
    /// same message) writes and emits nothing. Returns `true` only when this
    /// call committed a change. Best-effort: a failure is logged and never
    /// fails the turn (the list simply stays as it was).
    pub(crate) async fn record_pending_proposals(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        message_id: &str,
        proposal_ids: &[String],
    ) -> bool {
        if proposal_ids.is_empty() {
            return false;
        }
        let lock = self.pending_question_mutation_locks.lock_for(agent_id);
        let _guard = lock.lock().await;

        let session = match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to read pending proposals");
                return false;
            }
        };
        let existing = session.pending_proposals();
        let mut merged: Vec<intent_core::PendingProposal> = existing
            .iter()
            .filter(|entry| !proposal_ids.contains(&entry.proposal_id))
            .cloned()
            .collect();
        for id in proposal_ids {
            merged.push(intent_core::PendingProposal {
                proposal_id: id.clone(),
                message_id: message_id.to_string(),
            });
        }
        if merged == existing {
            return false;
        }
        self.persist_pending_proposals(workspace_id, agent_id, &merged)
            .await
    }

    /// Rebuild the pending-proposals list after a transcript swap that
    /// re-mints row ids (`agent.editAndRegenerate` truncation,
    /// `agent.replaceMessages`), where any surviving entry's `messageId` is by
    /// construction dangling. `messages` is the post-swap transcript in order:
    /// each pending entry is REMAPPED to the newest assistant row still
    /// carrying its proposal block, and an entry whose proposal block no
    /// longer exists anywhere in the transcript is dropped (clients could not
    /// recover it). Resolution state is preserved — entries the list no
    /// longer holds are never re-added, even when their blocks survive the
    /// swap. A committed change emits `agent:updated` with the new list;
    /// best-effort like [`Services::record_pending_proposals`].
    pub(crate) async fn reconcile_pending_proposals(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        messages: &[intent_core::AgentMessage],
    ) {
        let lock = self.pending_question_mutation_locks.lock_for(agent_id);
        let _guard = lock.lock().await;

        let session = match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to read pending proposals");
                return;
            }
        };
        let existing = session.pending_proposals();
        if existing.is_empty() {
            return;
        }
        // Newest carrying assistant row per proposal id in the post-swap
        // transcript (later rows overwrite earlier ones).
        let mut carriers: std::collections::HashMap<String, &str> =
            std::collections::HashMap::new();
        for msg in messages {
            if msg.role == "assistant" {
                if let Some(blocks) = msg.content.as_array() {
                    for id in crate::tool_block::proposal_ids_in(blocks) {
                        carriers.insert(id, msg.id.as_str());
                    }
                }
            }
        }
        let reconciled: Vec<intent_core::PendingProposal> = existing
            .iter()
            .filter_map(|entry| {
                carriers
                    .get(&entry.proposal_id)
                    .map(|mid| intent_core::PendingProposal {
                        proposal_id: entry.proposal_id.clone(),
                        message_id: (*mid).to_string(),
                    })
            })
            .collect();
        if reconciled == existing {
            return;
        }
        self.persist_pending_proposals(workspace_id, agent_id, &reconciled)
            .await;
    }

    /// Shared persist+emit tail of the pending-proposals writers: atomic
    /// single-key `json_set` (sibling metadata keys preserved) and, on
    /// success, `agent:updated` carrying the new list so clients re-read the
    /// `AgentLite` projection. Callers hold the per-agent mutation lock and
    /// have already established the list changed. Returns `true` only when
    /// the write committed; a failure is logged and swallowed.
    async fn persist_pending_proposals(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        proposals: &[intent_core::PendingProposal],
    ) -> bool {
        let value = match serde_json::to_string(proposals) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to serialize pending proposals");
                return false;
            }
        };
        match self
            .store
            .set_agent_session_metadata_key_json(
                workspace_id,
                agent_id,
                intent_core::PENDING_PROPOSALS_KEY,
                &value,
                &now_iso(),
            )
            .await
        {
            Ok(()) => {
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_UPDATED,
                    json!({
                        "agentId": agent_id.0,
                        "pendingProposals": proposals,
                    }),
                )
                .await;
                true
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to persist pending proposals");
                false
            }
        }
    }

    /// Re-derive the pending-questions marker after a transcript swap that
    /// re-mints row ids (`agent.editAndRegenerate` truncation,
    /// `agent.replaceMessages`), where any surviving marker is by construction
    /// dangling. `messages` is the post-swap transcript in order: the marker
    /// becomes the id of the newest question-bearing assistant row that is not
    /// followed by a user row answering it, and clears when there is none.
    ///
    /// Answer matching cannot be pure id equality the way
    /// [`Services::resolve_pending_questions_for_answer`] does it: the swap
    /// re-mints ids, so a carried-over `answeredQuestionsMessageId` names a row
    /// that no longer exists even when it DOES answer the question directly
    /// above it. So a `question_answers`-tagged user row resolves the pending
    /// row unless its tag names a DIFFERENT row that is still present in the
    /// post-swap transcript — a live reference to another question set, which
    /// must not release this one (mirroring the exact-match rule wherever ids
    /// are meaningful). Bounded — the caller already holds the (post-swap)
    /// rows.
    ///
    /// A swap can move the hold in either direction, so this also performs the
    /// two follow-ups every other hold-transition path performs: recompute the
    /// workspace's `needs_attention` displayStatus (transition-only, §6.5 step
    /// 0) and — when the re-derivation leaves no hold — kick the queue drain
    /// for the automatic entries the hold parked (these paths start no turn of
    /// their own, so without the kick those entries sit on an idle agent).
    pub(crate) async fn reconcile_pending_questions_marker(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        messages: &[intent_core::AgentMessage],
    ) {
        let resolves = |answered: &str, marked: &str| {
            answered == marked || !messages.iter().any(|m| m.id == answered)
        };
        let mut pending: Option<&str> = None;
        for msg in messages {
            match msg.role.as_str() {
                "assistant" if has_question_blocks(&msg.content) => pending = Some(&msg.id),
                "user" => {
                    if let (Some(answered), Some(marked)) = (
                        answered_questions_message_id(msg.metadata.as_ref()),
                        pending,
                    ) {
                        if resolves(answered, marked) {
                            pending = None;
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(id) = pending {
            let id = id.to_string();
            self.record_pending_questions_marker(workspace_id, agent_id, &id)
                .await;
        } else {
            self.clear_pending_questions_marker(workspace_id, agent_id, None)
                .await;
            if let Some(manager) = self.agent_manager() {
                manager
                    .try_drain_queue(agent_id.clone(), workspace_id.clone())
                    .await;
            }
        }
        self.maybe_emit_display_status_changed(workspace_id).await;
    }

    /// Resolve a just-persisted user row against the pending-questions marker:
    /// when the row's `messageMetadata` is a `question_answers` tag naming
    /// EXACTLY the marked message, the questions are answered and the marker
    /// clears (releasing the hold). A missing/foreign/stale
    /// `answeredQuestionsMessageId` — e.g. an answer for a question set a newer
    /// turn already superseded — is a no-op, so a late answer can neither
    /// release a newer hold nor re-arm an old one. The daemon never inspects
    /// the answer TEXT (spec §Decisions 3).
    ///
    /// Returns `true` when the marker was cleared, so callers can recompute
    /// displayStatus and — on the persist-only paths that start no turn of
    /// their own — kick the queue drain for the entries the hold parked.
    pub(crate) async fn resolve_pending_questions_for_answer(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        message_metadata: Option<&Value>,
    ) -> bool {
        let Some(answered) = answered_questions_message_id(message_metadata) else {
            return false;
        };
        self.park_pending_marker_mutation("clear", answered).await;
        let lock = self.pending_question_mutation_locks.lock_for(agent_id);
        let _guard = lock.lock().await;
        let Ok(session) = self.store.get_agent_session_summary(agent_id).await else {
            return false;
        };
        if session.pending_questions_message_id() != Some(answered) {
            return false;
        }
        self.clear_pending_questions_marker_locked(workspace_id, agent_id, Some(answered))
            .await
    }

    /// `agent.dismissQuestions` (PROTOCOL §5.5): persist the dismissal marker
    /// (`message_id` — the assistant message whose trailing question resource
    /// blocks the user dismissed) on the agent session so the dismissed
    /// question set never re-surfaces (survives reload), emit `agent:updated`,
    /// deliver the questions-dismissed system notice to the agent
    /// ([`Services::notify_questions_dismissed`] — marker persists FIRST so
    /// the question hold cannot re-park the notice), and kick the queue drain
    /// so messages held by the question hold resume. Idempotent: re-dismissing
    /// the same message succeeds without a duplicate notice. Fails closed on a
    /// nonexistent target or a workspace mismatch (`NotFound`).
    pub(crate) async fn agent_dismiss_questions_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        let message_id = message_id.trim().to_string();
        if message_id.is_empty() {
            return Err(Error::InvalidParams("messageId is required".to_string()));
        }
        if message_id.len() > MAX_MESSAGE_ID_LEN {
            return Err(Error::InvalidParams(format!(
                "messageId exceeds maximum length of {MAX_MESSAGE_ID_LEN}"
            )));
        }
        // Metadata-only lookup (no transcript hydration); workspace mismatch
        // surfaces as NotFound (defense-in-depth against bare-id probes).
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        // Idempotency for the notice below: a repeat dismissal of the same
        // messageId re-persists the marker (harmless) but must NOT deliver a
        // duplicate dismissal notice. The claim is atomic — a check-and-insert
        // into the per-agent notice registry under one lock acquisition — so
        // concurrent dismissals of the same id race to a single winner, and
        // the registry remembers OLDER ids the single-slot persisted marker
        // has since been overwritten by (A -> B -> A). The persisted marker
        // still short-circuits ids dismissed before a daemon restart.
        let already_dismissed = {
            let persisted = session.dismissed_questions_message_id() == Some(message_id.as_str());
            let mut guard = self
                .dismissal_notices_sent
                .lock()
                .expect("dismissal notice registry poisoned");
            let claimed = !guard
                .entry(agent_id.clone())
                .or_default()
                .insert(message_id.clone());
            persisted || claimed
        };
        // Atomic single-key write (store-side `json_set`): sibling metadata
        // keys — e.g. a concurrently-advanced `lastSeenMessageId`
        // (`agent.markSeen`) — are preserved rather than clobbered by a
        // whole-column replacement, non-object metadata is preserved under
        // `priorNonObjectMetadata` (monorepo#751 review), and only
        // `metadata`+`updated_at` are touched so the stored `system_prompt`
        // (absent from the summary projection above) survives. Unconditional
        // (no CAS guard): the last dismissal wins, matching the single-slot
        // marker semantics.
        if let Err(e) = self
            .store
            .set_agent_session_metadata_key(
                &workspace_id,
                &agent_id,
                intent_core::DISMISSED_QUESTIONS_MESSAGE_ID_KEY,
                &message_id,
                None,
                &now_iso(),
            )
            .await
        {
            // Release the notice claim so a retried dismissal (after this
            // store failure) still delivers the notice.
            if !already_dismissed {
                let mut guard = self
                    .dismissal_notices_sent
                    .lock()
                    .expect("dismissal notice registry poisoned");
                if let Some(sent) = guard.get_mut(&agent_id) {
                    sent.remove(&message_id);
                }
            }
            return Err(e);
        }
        // Self-contained event (monorepo#3180): carry the session's
        // pending-questions marker alongside the dismissal marker so clients
        // can re-derive the hold from this one event without an extra
        // `agent.get` round-trip. Same projection rule as `AgentLite`:
        // present when the marker was ever written (the empty string is the
        // authoritative clear), omitted for legacy marker-less sessions. The
        // marker is RE-READ and the event published under the per-agent
        // mutation lock — marker set/clear paths hold that lock across their
        // write + event, so the value emitted here is coherent with the event
        // order a client observes; the top-of-op snapshot could be stale by
        // emit time and would let a client re-derive an already-cleared hold
        // (PR #1496 review).
        {
            let lock = self.pending_question_mutation_locks.lock_for(&agent_id);
            let _guard = lock.lock().await;
            let marker_session = match self.store.get_agent_session_summary(&agent_id).await {
                Ok(fresh) => fresh,
                Err(e) => {
                    // Best-effort: the dismissal marker is already persisted,
                    // so keep the event flowing with the top-of-op snapshot
                    // rather than failing the dismissal.
                    tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        "failed to re-read pending-questions marker for dismiss event"
                    );
                    session
                }
            };
            let mut event_data = json!({
                "agentId": agent_id.0,
                "dismissedQuestionsMessageId": message_id,
            });
            if marker_session.pending_questions_marker_written() {
                event_data["pendingQuestionsMessageId"] = json!(marker_session
                    .pending_questions_message_id()
                    .unwrap_or_default());
            }
            self.publish_agent_mutation_event(&workspace_id, &agent_id, AGENT_UPDATED, event_data)
                .await;
        }
        // Dismissing the questions retires the question hold, which can
        // retire the workspace's needs_attention displayStatus (§6.5 step 0):
        // recompute-and-compare.
        self.maybe_emit_display_status_changed(&workspace_id).await;
        // Deliver the questions-dismissed system notice (first dismissal of
        // this messageId only) BEFORE the drain kick, so an idle agent's next
        // turn is the notice rather than a previously held entry. The marker
        // above is already persisted, so the hold cannot re-park it.
        if !already_dismissed {
            self.notify_questions_dismissed(&workspace_id, &agent_id, &message_id)
                .await;
        }
        // The hold (if it was gating this message's questions) is now released:
        // kick the drain so held queue entries resume without waiting for the
        // next end-of-turn drain.
        if let Some(manager) = self.agent_manager() {
            manager
                .try_drain_queue(agent_id.clone(), workspace_id)
                .await;
        }
        Ok(json!({
            "success": true,
            "dismissedQuestionsMessageId": message_id,
        }))
    }

    /// `agent.markSeen` (PROTOCOL §5.5): persist the per-conversation seen
    /// marker (`message_id` — the newest transcript message the user has
    /// seen) in the session metadata (survives daemon restarts) and emit
    /// `agent:updated` so other clients converge. **Monotonic**: when both
    /// the named message and the current marker resolve to transcript
    /// positions and the named one is OLDER, the call is a no-op returning
    /// the current marker (no write, no event) — enforced against concurrent
    /// markers too: the store write is a compare-and-set on the current
    /// marker value (atomic single-key `json_set`, sibling metadata keys
    /// preserved), and a CAS miss re-reads and re-applies the gate. An id
    /// that does not resolve (unknown, or truncated away by
    /// `agent.editAndRegenerate`) is tolerated as dangling — same laxity as
    /// `agent.dismissQuestions` — and the write proceeds (a dangling CURRENT
    /// marker likewise never blocks an advance). **Idempotent**: re-marking
    /// the already-persisted id succeeds without a write or a duplicate
    /// event. Fails closed on a nonexistent agent or a workspace mismatch
    /// (`NotFound`).
    pub(crate) async fn agent_mark_seen_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        // Bounded CAS retry: each iteration re-reads the current marker,
        // re-applies the monotonicity gate, and attempts a guarded write. A
        // miss means another writer moved the marker between our read and
        // write; the loop converges because the marker only ever advances.
        // The cap is defensive — two racing debounced FE triggers settle in
        // one retry.
        const MARK_SEEN_CAS_ATTEMPTS: u32 = 4;
        let message_id = message_id.trim().to_string();
        if message_id.is_empty() {
            return Err(Error::InvalidParams("messageId is required".to_string()));
        }
        if message_id.len() > MAX_MESSAGE_ID_LEN {
            return Err(Error::InvalidParams(format!(
                "messageId exceeds maximum length of {MAX_MESSAGE_ID_LEN}"
            )));
        }
        // Pre-write unread derivation (§5.1): whether the workspace read as
        // unread BEFORE this marker advance, so the settle below only emits
        // the workspace-level clear on an actual unread→none transition. A
        // probe failure reads `false` — fail closed on emission (no spurious
        // clear), the marker write is unaffected.
        let was_unread = self
            .store
            .workspace_has_unread_top_level_session(&workspace_id)
            .await
            .unwrap_or(false);
        for _ in 0..MARK_SEEN_CAS_ATTEMPTS {
            // Metadata-only lookup (no transcript hydration); workspace
            // mismatch surfaces as NotFound (defense-in-depth against
            // bare-id probes).
            let session = self.store.get_agent_session_summary(&agent_id).await?;
            if session.workspace_id != workspace_id {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
            let current = session.last_seen_message_id().map(str::to_string);
            if current.as_deref() == Some(message_id.as_str()) {
                // Already the persisted marker: no write, no duplicate event —
                // the FE trigger is debounced but can still repeat.
                return Ok(json!({
                    "success": true,
                    "lastSeenMessageId": message_id,
                }));
            }
            // Monotonicity gate: only comparable when BOTH ids resolve to
            // transcript positions (two bounded index seeks, no hydration). A
            // dangling side — unknown id, or a marker naming a row truncated
            // by `agent.editAndRegenerate` — never blocks the advance.
            if let Some(current_id) = current.as_deref() {
                let new_idx = self
                    .store
                    .get_agent_message_index(&agent_id, &message_id)
                    .await?;
                let current_idx = self
                    .store
                    .get_agent_message_index(&agent_id, current_id)
                    .await?;
                if let (Some(new_idx), Some(current_idx)) = (new_idx, current_idx) {
                    if new_idx < current_idx {
                        return Ok(json!({
                            "success": true,
                            "lastSeenMessageId": current_id,
                        }));
                    }
                }
            }
            // Guarded atomic single-key write: `json_set` on exactly
            // `lastSeenMessageId` (sibling keys — e.g. a concurrent
            // `dismissedQuestionsMessageId` — are preserved; only
            // `metadata`+`updated_at` are touched so the stored
            // `system_prompt` survives), conditioned on the marker still
            // holding the value the gate above was computed against.
            let wrote = self
                .store
                .set_agent_session_metadata_key(
                    &workspace_id,
                    &agent_id,
                    intent_core::LAST_SEEN_MESSAGE_ID_KEY,
                    &message_id,
                    Some(current.as_deref()),
                    &now_iso(),
                )
                .await?;
            if !wrote {
                // Marker moved underneath us: re-read and re-gate.
                continue;
            }
            self.publish_agent_mutation_event(
                &workspace_id,
                &agent_id,
                AGENT_UPDATED,
                json!({
                    "agentId": agent_id.0,
                    "lastSeenMessageId": message_id,
                }),
            )
            .await;
            // Reading the last unread agent clears the derived workspace
            // `unread` (§5.1): re-derive and, on the unread→none transition,
            // clear the stored legacy flag + emit ONE
            // `workspace:attention-changed { none }`. A workspace with other
            // unread sessions — or one that was not unread to begin with —
            // stays silent.
            self.settle_workspace_unread_after_seen(&workspace_id, was_unread)
                .await;
            return Ok(json!({
                "success": true,
                "lastSeenMessageId": message_id,
            }));
        }
        Err(Error::Internal(format!(
            "agent.markSeen: seen-marker CAS did not settle after \
             {MARK_SEEN_CAS_ATTEMPTS} attempts for agent {agent_id}"
        )))
    }

    /// Number of question resource blocks on the dismissed assistant message.
    /// Bounded cost: an index seek plus a single-row page — no transcript
    /// hydration. Unknown or unreadable messages count zero, which routes the
    /// notice to its countless fallback wording.
    async fn dismissed_question_count(&self, agent_id: &AgentId, message_id: &str) -> usize {
        let Ok(Some(idx)) = self
            .store
            .get_agent_message_index(agent_id, message_id)
            .await
        else {
            return 0;
        };
        let Ok(page) = self.store.get_agent_messages_page(agent_id, idx, 1).await else {
            return 0;
        };
        page.first()
            .filter(|m| m.id == message_id)
            .map_or(0, |m| question_block_count(&m.content))
    }

    /// Deliver the questions-dismissed system notice (`agent.dismissQuestions`,
    /// PROTOCOL §5.5): a system-origin message telling the agent the user
    /// dismissed its N pending questions without answering. The notice is
    /// informative only: the agent must not re-ask and must not proceed with
    /// any work — it ends its turn and waits for the user's next message.
    /// Reuses the wake-delivery machinery ([`Services::deliver_wake_message`]):
    /// an idle agent gets the notice as an immediate turn; when it lands in
    /// the queue instead (busy turn, store-append fallback, or a NEWER pending
    /// question re-holding automatic deliveries) the entry is promoted to the
    /// FRONT of the queue so the notice is the next delivery, ahead of parked
    /// interrupts and held wakes. The promotion is a separate queue-lock
    /// acquisition from the enqueue, so a concurrent drain can pop a
    /// previously parked entry (or the notice itself) in the window between
    /// them — benign: the notice still delivers, just not strictly first, and
    /// [`Services::move_queued_message_front`] returns `false` when the entry
    /// is already gone. Fail-soft: delivery problems are logged, never
    /// surfaced to the RPC — the durable dismissal marker is the source of
    /// truth.
    async fn notify_questions_dismissed(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        dismissed_message_id: &str,
    ) {
        let count = self
            .dismissed_question_count(agent_id, dismissed_message_id)
            .await;
        let content = crate::harness::latest().questions_dismissed_notice(count);
        let metadata = json!({
            "type": QUESTIONS_DISMISSED_METADATA_TYPE,
            "source": "system",
            "dismissedQuestionsMessageId": dismissed_message_id,
        });
        match self
            .deliver_wake_message(workspace_id, agent_id, &content, Some(&metadata))
            .await
        {
            Ok(result) => {
                if result["queued"] == json!(true) {
                    if let Some(qid) = result["queuedMessage"]["id"].as_str() {
                        if self.move_queued_message_front(agent_id, qid) {
                            self.publish_queue_updated(agent_id).await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "questions-dismissed notice delivery failed"
                );
            }
        }
    }

    /// `agent.resolveProposal` (PROTOCOL §5.5): record the user's resolution
    /// of a pending proposal. Persists the `proposalId -> outcome` entry in
    /// the [`intent_core::PROPOSAL_RESOLUTIONS_KEY`] map (bounded — oldest
    /// entries evicted past [`MAX_PROPOSAL_RESOLUTIONS`]), THEN removes the
    /// entry from the session's `pendingProposals` list (two atomic
    /// single-key `json_set`s in that order, so a failure between them
    /// leaves the entry pending with its outcome recorded and a retry
    /// converges instead of losing the resolution — sibling metadata keys
    /// preserved), emits `agent:updated` carrying both, and delivers the
    /// proposal-resolved system notice
    /// ([`Services::notify_proposal_resolved`]) for BOTH outcomes. The whole
    /// read-modify-write is serialized per agent on the same mutation lock
    /// as the pending-proposals writers, so a concurrent double-resolve
    /// races to a single winner — the loser sees the entry already gone and
    /// takes the idempotent path. **Idempotent**: re-resolving an id that is
    /// no longer pending but present in the resolutions map succeeds
    /// (echoing the CURRENT persisted outcome) without a duplicate notice or
    /// event. An id that was never pending and never resolved →
    /// `NotFound`. Nonexistent agent or workspace mismatch → `NotFound`.
    pub(crate) async fn agent_resolve_proposal_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        proposal_id: String,
        outcome: String,
        detail: Option<String>,
    ) -> Result<Value> {
        // The id is matched VERBATIM against the recorded pending entries —
        // recording preserves `applyToolCallId` / `preview.title` exactly as
        // proposed (including any incidental whitespace), so normalizing here
        // would orphan such a proposal as unresolvable `NotFound`.
        if proposal_id.trim().is_empty() {
            return Err(Error::InvalidParams("proposalId is required".to_string()));
        }
        if proposal_id.len() > MAX_MESSAGE_ID_LEN {
            return Err(Error::InvalidParams(format!(
                "proposalId exceeds maximum length of {MAX_MESSAGE_ID_LEN}"
            )));
        }
        if outcome != PROPOSAL_OUTCOME_APPLIED && outcome != PROPOSAL_OUTCOME_DISMISSED {
            return Err(Error::InvalidParams(format!(
                "outcome must be \"{PROPOSAL_OUTCOME_APPLIED}\" or \
                 \"{PROPOSAL_OUTCOME_DISMISSED}\""
            )));
        }
        let detail = detail
            .map(|d| d.trim().to_string())
            .filter(|d| !d.is_empty());
        if detail.as_ref().is_some_and(|d| d.len() > MAX_DETAIL_LEN) {
            return Err(Error::InvalidParams(format!(
                "detail exceeds maximum length of {MAX_DETAIL_LEN}"
            )));
        }
        // Serialize with the pending-proposals writers (turn-end recording,
        // transcript-swap reconciliation) so the remove-and-persist below
        // cannot interleave with a concurrent list rewrite.
        let lock = self.pending_question_mutation_locks.lock_for(&agent_id);
        let _guard = lock.lock().await;

        // Metadata-only lookup (no transcript hydration); workspace mismatch
        // surfaces as NotFound (defense-in-depth against bare-id probes).
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        let pending = session.pending_proposals();
        let entry = pending
            .iter()
            .find(|p| p.proposal_id == proposal_id)
            .cloned();
        let mut resolutions = session.proposal_resolutions();
        let Some(entry) = entry else {
            // Not pending: idempotent success when already resolved (echo the
            // persisted outcome — no rewrite, no duplicate notice), NotFound
            // when the id was never tracked.
            if let Some(existing) = resolutions.get(&proposal_id).and_then(Value::as_str) {
                return Ok(json!({
                    "success": true,
                    "proposalId": proposal_id,
                    "outcome": existing,
                }));
            }
            return Err(Error::NotFound(format!("proposal {proposal_id}")));
        };
        // Resolve the human-readable title from the carrying message's
        // proposal resource block BEFORE mutating anything (best-effort —
        // the id doubles as the fallback title).
        let title = self
            .proposal_title_from_message(&agent_id, &entry.message_id, &proposal_id)
            .await
            .unwrap_or_else(|| proposal_id.clone());

        let remaining: Vec<intent_core::PendingProposal> = pending
            .iter()
            .filter(|p| p.proposal_id != proposal_id)
            .cloned()
            .collect();
        let remaining_json = serde_json::to_string(&remaining)
            .map_err(|e| Error::Internal(format!("serialize pending proposals: {e}")))?;
        resolutions.insert(proposal_id.clone(), json!(outcome));
        // Retention cap: evict the OLDEST entries (the map is
        // insertion-ordered — serde_json's `preserve_order` rides the
        // workspace dependency tree) so the persisted blob and the AgentLite
        // projection lifting it into hot list payloads stay bounded.
        while resolutions.len() > MAX_PROPOSAL_RESOLUTIONS {
            let Some(oldest) = resolutions.keys().next().cloned() else {
                break;
            };
            resolutions.shift_remove(&oldest);
        }
        let resolutions_json = serde_json::to_string(&resolutions)
            .map_err(|e| Error::Internal(format!("serialize proposal resolutions: {e}")))?;
        // Persist the resolution FIRST (it doubles as the durable idempotency
        // marker), then shrink the pending list. A failure between the two
        // writes leaves the entry pending WITH its outcome recorded: the RPC
        // errors and a retry re-runs this path — the re-insert is an
        // idempotent overwrite, and no notice was sent (the notice follows
        // both writes), so none can duplicate. The reverse order could
        // permanently lose the resolution: pending entry gone, no recorded
        // outcome, every retry NotFound.
        self.store
            .set_agent_session_metadata_key_json(
                &workspace_id,
                &agent_id,
                intent_core::PROPOSAL_RESOLUTIONS_KEY,
                &resolutions_json,
                &now_iso(),
            )
            .await?;
        self.store
            .set_agent_session_metadata_key_json(
                &workspace_id,
                &agent_id,
                intent_core::PENDING_PROPOSALS_KEY,
                &remaining_json,
                &now_iso(),
            )
            .await?;
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            AGENT_UPDATED,
            json!({
                "agentId": agent_id.0,
                "pendingProposals": remaining,
                "proposalResolutions": resolutions,
            }),
        )
        .await;
        // Deliver the proposal-resolved system notice — fail-soft, the
        // persisted resolution is the source of truth.
        self.notify_proposal_resolved(
            &workspace_id,
            &agent_id,
            &proposal_id,
            &outcome,
            &title,
            detail.as_deref(),
        )
        .await;
        Ok(json!({
            "success": true,
            "proposalId": proposal_id,
            "outcome": outcome,
        }))
    }

    /// The `preview.title` of the proposal `proposal_id` as carried by the
    /// persisted message `message_id`'s lifted proposal-resource block.
    /// Bounded: one index lookup + one single-message page, mirroring
    /// [`Services::dismissed_question_count`]. `None` when the message is
    /// gone, carries no matching block, or the proposal has no title.
    async fn proposal_title_from_message(
        &self,
        agent_id: &AgentId,
        message_id: &str,
        proposal_id: &str,
    ) -> Option<String> {
        let idx = self
            .store
            .get_agent_message_index(agent_id, message_id)
            .await
            .ok()??;
        let page = self
            .store
            .get_agent_messages_page(agent_id, idx, 1)
            .await
            .ok()?;
        let msg = page.first().filter(|m| m.id == message_id)?;
        let blocks = msg.content.as_array()?;
        crate::tool_block::proposal_title_in(blocks, proposal_id)
    }

    /// Deliver the proposal-resolved system notice (`agent.resolveProposal`,
    /// PROTOCOL §5.5): a system-origin message telling the agent the user
    /// applied or dismissed the named proposal. Mirrors
    /// [`Services::notify_questions_dismissed`]: reuses the wake-delivery
    /// machinery ([`Services::deliver_wake_message`]) — an idle agent gets
    /// the notice as an immediate turn; when it lands in the queue instead
    /// (busy turn or store-append fallback) the entry is promoted to the
    /// FRONT of the queue so the notice is the next delivery. Fail-soft:
    /// delivery problems are logged, never surfaced to the RPC — the durable
    /// resolution record is the source of truth.
    async fn notify_proposal_resolved(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        proposal_id: &str,
        outcome: &str,
        title: &str,
        detail: Option<&str>,
    ) {
        let harness = crate::harness::latest();
        let content = if outcome == PROPOSAL_OUTCOME_APPLIED {
            harness.proposal_applied_notice(title, detail)
        } else {
            harness.proposal_dismissed_notice(title)
        };
        let metadata = json!({
            "type": PROPOSAL_RESOLVED_METADATA_TYPE,
            "source": "system",
            "proposalId": proposal_id,
            "outcome": outcome,
        });
        match self
            .deliver_wake_message(workspace_id, agent_id, &content, Some(&metadata))
            .await
        {
            Ok(result) => {
                if result["queued"] == json!(true) {
                    if let Some(qid) = result["queuedMessage"]["id"].as_str() {
                        if self.move_queued_message_front(agent_id, qid) {
                            self.publish_queue_updated(agent_id).await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    proposal = %proposal_id,
                    error = %e,
                    "proposal-resolved notice delivery failed"
                );
            }
        }
    }

    /// `agent.summary`: a quick summary derived from the transcript (PROTOCOL §5.5).
    pub(crate) async fn agent_summary_op(&self, agent_id: AgentId) -> Result<Value> {
        let session = self.load_session_internal(&agent_id).await?;
        let last_response = last_assistant_text(&session.messages);
        let tool_counts = tool_call_counts(&session.messages);
        let mut out = json!({
            "agentId": agent_id,
            "agentName": session.name,
            "status": session.status,
            "messageCount": session.messages.len(),
            "toolCallCounts": tool_counts,
            "createdAt": session.created_at,
            "updatedAt": session.updated_at,
        });
        if let Some(text) = last_response {
            out["lastResponse"] = Value::String(text);
        }
        Ok(out)
    }

    /// `agent.getSessionStats`: the per-session credit/message/tool rollup as
    /// `{ stats: SessionStats }` (PROTOCOL §5.24). `NotFound` is surfaced to the
    /// router which maps it to `-32602`. Stats are sourced from the auggie CLI
    /// (`session stats <sessionId> --json`); when the CLI is unavailable the
    /// counts fall back to the transcript and `creditsUsed` stays `null`
    /// (graceful degrade — never panics). A refreshed rollup that differs from
    /// the cached snapshot pushes `agent:session-stats-changed` (§6.5).
    pub(crate) async fn agent_get_session_stats_op(
        &self,
        session_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&session_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {session_id}")));
            }
        }
        let stats =
            if let Some(cli) = fetch_session_stats(self.auggie_bin.clone(), &session_id).await {
                cli
            } else {
                let (message_count, tool_count) = transcript_counts(&session.messages);
                SessionStats {
                    credits_used: None,
                    message_count,
                    tool_count,
                }
            };
        self.cache_and_emit_session_stats(&session, &stats).await;
        Ok(json!({ "stats": stats }))
    }

    /// Cache the latest session-stats snapshot and, when it differs from the
    /// previously observed one, push the self-sufficient
    /// `agent:session-stats-changed` event (PROTOCOL §5.24 / §6.5). In this model
    /// a session id is the agent id, so the payload carries both.
    async fn cache_and_emit_session_stats(&self, session: &AgentSession, stats: &SessionStats) {
        let changed = {
            let mut cache = self
                .session_stats_cache
                .lock()
                .expect("session stats cache poisoned");
            if cache.get(&session.id) == Some(stats) {
                false
            } else {
                cache.insert(session.id.clone(), stats.clone());
                true
            }
        };
        if !changed {
            return;
        }
        crate::publish_event(
            self.event_bus.as_ref(),
            intent_store::NewEvent {
                workspace_id: session.workspace_id.clone(),
                timestamp: now_iso(),
                event_type: intent_core::events::AGENT_SESSION_STATS_CHANGED.to_string(),
                actor: crate::system_actor(),
                session_id: Some(session.id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({
                    "sessionId": session.id.0,
                    "agentId": session.id.0,
                    "stats": stats,
                }),
            },
        )
        .await;
    }

    /// `agent.reportToParent`: a delegated child reports back to its parent
    /// (PROTOCOL §5.5). Caller identity comes only from the MCP front door; the
    /// RPC dispatch path passes `None`, so it always surfaces `-32603`. When the
    /// caller has no `parentAgentId` (created directly by a user), this is also
    /// `-32603`. Otherwise the report is persisted on the child session
    /// (`metadata.completionReport` / `completionReportTimestamp`, the TS
    /// parity; P3-1.2b) — emitting `agent:updated` — and, when the caller has a
    /// linked task note whose current status is non-terminal, the note is
    /// transitioned to `review_required` (TASK-B, mirroring the reference
    /// `reportToParent` writer). For non-grouped children a progress wake is
    /// issued to the parent, gated by `agents.reportToParentDebounceSeconds`:
    /// zero delivers it immediately; a non-zero window parks it as a held
    /// queue entry that either flushes at expiry or is retracted and folded
    /// into the terminal wake when the child settles first (spec Design
    /// §2/§4). The report never consumes the completion watch — the child's
    /// `agent:idle` still delivers the terminal wake, whose formatted
    /// text/metadata includes the persisted `completionReport`. `after_all`
    /// grouping path is unchanged — the group's aggregated wake still folds
    /// this child's report in.
    pub(crate) async fn agent_report_to_parent_op(
        &self,
        workspace_id: WorkspaceId,
        report: Value,
        caller_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let not_delegated = || {
            Error::Internal("report_to_parent is only available to delegated agents".to_string())
        };
        let caller = caller_agent_id.ok_or_else(not_delegated)?;
        let mut session = self.load_session_internal(&caller).await?;
        // Copilot #104 (thread PRRT_kwDOS9Wxuc6QKTPK): scope-guard the
        // caller-supplied `workspace_id` the same way `agent_get_op` /
        // `agent_get_conversation_op` do — reject a cross-workspace mismatch
        // with `NotFound` before any state changes (report persistence,
        // `review_required` transition, subscription notification), so a
        // request targeting the wrong workspace never mutates the session's
        // actual workspace by side effect.
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {caller}")));
        }
        let parent = session.parent_agent_id.clone().ok_or_else(not_delegated)?;
        // `report` is declared as a string on the MCP surface; coerce other
        // JSON shapes to their textual form for delivery.
        let report_text = match &report {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let report_len = i64::try_from(report_text.chars().count()).expect("value fits in i64");
        // Persist the completion report on the child so `agent.get`/`agent.list`
        // (and `ws.agent.summary`) can re-serve it after restarts.
        let saved_at = now_iso();
        session.completion_report = Some(report_text.clone());
        session.completion_report_timestamp = Some(saved_at.clone());
        session.updated_at = saved_at.clone();
        let workspace_id = session.workspace_id.clone();
        let task_note_id = session.task_note_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &caller,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": caller.0, "completionReportLength": report_len }),
        )
        .await;
        // TASK-B: move the linked task note to `review_required` so the FE
        // reflects the child's completion. Terminal statuses (`complete`,
        // `cancelled`) are never overwritten; agents without a linked note are
        // a no-op. Errors from the store lookup or the status writer are
        // logged and swallowed — the report itself is already persisted, and
        // the caller's response must not depend on FE-facing task metadata.
        if let Some(note_id) = task_note_id.clone() {
            self.transition_linked_task_to_review_required(&workspace_id, note_id, caller.clone())
                .await;
        }

        // Progress wake for non-grouped children. A report does not
        // consume a terminal completion watch: idle/failure/deletion still owns
        // the final wake and durable one-shot retirement. Grouped reports remain
        // deferred to the single after_all aggregate.
        let grouped = self.child_in_undelivered_group(&parent, &caller);
        if !grouped {
            let watches = self.find_watches_for_child(&caller);
            let watch_still_armed = watches
                .iter()
                .any(|watch| watch.group_id.is_none() && watch.parent_agent_id == parent);
            // Deliver exactly ONE wake to the parent, regardless of watch count.
            // Format the wake message with the persisted report. "reported", not
            // "completed" — a report is not necessarily a completion. Passing
            // false omits the terminal retirement/re-arm suffix.
            let wake_text = crate::harness::latest().report_to_parent_wake(
                &session.name,
                &caller.0,
                &report_text,
                false,
            );
            // Build event notification metadata (mirroring deliver_completion_to_watches).
            let mut metadata = json!({
                "type": "event_notification",
                "eventCount": 1,
                "eventTypes": ["agent:reportToParent"],
                "events": [{
                    "id": uuid::Uuid::new_v4().to_string(),
                    "type": "agent:reportToParent",
                    "timestamp": saved_at,
                    "data": {
                        "agentId": caller.0,
                        "agentName": session.name.clone(),
                        // `completionReport` is canonical; `report` is kept
                        // for back-compat with older clients.
                        "completionReport": report_text.clone(),
                        "report": report_text.clone(),
                    },
                    "actor": {
                        "type": "agent",
                        "id": caller.0,
                        "name": session.name.clone(),
                    }
                }]
            });
            // Progress does not consume completion trigger facts. They remain
            // available for the terminal wake that retires the watch.
            if watch_still_armed {
                metadata["watchStillArmed"] = json!(true);
            }
            // Debounce (spec Design §2/§6): with a non-zero
            // `agents.reportToParentDebounceSeconds` (read live from the
            // settings snapshot) the wake is PARKED as a held entry on the
            // parent's durable queue instead of delivered now — the per-entry
            // timer flushes it at `holdUntil`, and a child settlement inside
            // the window retracts it and folds its event into the single
            // terminal wake (`deliver_completion_to_watches`). A repeat
            // report from the same child upserts the held entry in place and
            // resets `holdUntil`. Zero disables the debounce entirely:
            // immediate wake, identical to the legacy behavior.
            let debounce_secs =
                crate::settings::report_to_parent_debounce_seconds(&self.effective_settings());
            if debounce_secs > 0 {
                let hold_until = intent_core::iso_ms_from_now(u64::from(debounce_secs) * 1000);
                self.enqueue_held_message(
                    &parent,
                    wake_text,
                    Some(metadata),
                    REPORT_DEBOUNCE_HOLD_KIND,
                    &hold_until,
                    &caller.0,
                )
                .await;
                // monorepo#4026: stamp the report identity now; whether the
                // held wake actually reached the parent is proven at
                // settlement by the #1614 retract gate (a retracted hold
                // renders the full report, a flushed-and-drained one is
                // suppressed as already delivered).
                self.stamp_watch_delivered_report_ts(&parent, &caller, &saved_at);
            } else {
                // Deliver the wake in the parent's HOME workspace: for a
                // cross-workspace (chief) parent this differs from the child's;
                // fall back to the child's workspace when the parent session
                // cannot be resolved (matches the pre-lift behavior).
                let parent_home_ws = self
                    .store
                    .get_agent_session(&parent)
                    .await
                    .map_or_else(|_| workspace_id.clone(), |s| s.workspace_id);
                // Deliver the wake to the parent unconditionally (even if no
                // watch exists).
                if let Err(e) = self
                    .deliver_parent_wake(&parent_home_ws, parent.clone(), wake_text, Some(metadata))
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        parent = %parent.0,
                        child = %caller.0,
                        "failed to deliver reportToParent progress wake to parent"
                    );
                } else {
                    // monorepo#4026: remember which report identity this wake
                    // carried so the terminal completion wake can suppress a
                    // verbatim repeat. Whether the wake actually LEFT the
                    // parent's queue is proven at settlement by the #1614
                    // retract gate (retracts removing nothing = delivered).
                    self.stamp_watch_delivered_report_ts(&parent, &caller, &saved_at);
                }
            }
        }

        Ok(json!({
            "ok": true,
            "parentAgentId": parent,
            "reportLength": report_len,
            "savedAt": saved_at,
        }))
    }

    /// TASK-B helper: transition the caller's linked task note to
    /// `review_required` iff its current status is non-terminal (i.e. not
    /// `complete`/`cancelled`). Uses the same `task.updateNoteStatus` writer
    /// the router path uses, so it publishes `task:status-changed` +
    /// `notes:ready-tasks-changed` with the caller as `agentId`. All errors
    /// are logged and swallowed: the persisted completion report is the
    /// contract of `agent.reportToParent`, and the FE-facing status update is
    /// best-effort.
    async fn transition_linked_task_to_review_required(
        &self,
        workspace_id: &WorkspaceId,
        task_note_id: NoteId,
        caller: AgentId,
    ) {
        self.transition_linked_task_status(
            workspace_id,
            task_note_id,
            caller,
            intent_core::TaskStatus::ReviewRequired,
            "review_required",
        )
        .await;
    }

    /// Shared best-effort linked-task transition (TASK-B shape): move the
    /// caller's linked task note to `target` iff its current status is
    /// non-terminal (not `complete`/`cancelled`) and not already `target`
    /// (the writer always persists — bumping `updated_at` and `rev` — before
    /// its own no-op-when-unchanged check, so repeated calls would otherwise
    /// churn the note). Uses the same `task.updateNoteStatus` writer the
    /// router path uses, so it publishes `task:status-changed` +
    /// `notes:ready-tasks-changed` with the caller as `agentId`. All errors
    /// are logged and swallowed — the calling op's own persistence is the
    /// contract; the FE-facing status update is best-effort.
    async fn transition_linked_task_status(
        &self,
        workspace_id: &WorkspaceId,
        task_note_id: NoteId,
        caller: AgentId,
        target: intent_core::TaskStatus,
        target_word: &str,
    ) {
        let note = match crate::fetch_note(&self.store, workspace_id, &task_note_id).await {
            Ok(note) => note,
            // A missing or out-of-workspace linked note is the expected shape
            // for stale/cross-workspace session metadata — keep it a silent
            // no-op (debug-level) so normal operation isn't noisy. Real
            // internal failures still surface as warnings.
            Err(Error::NotFound(_)) => {
                tracing::debug!(
                    note = %task_note_id,
                    "linked task note not found in this workspace; skipping status transition"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    note = %task_note_id,
                    "failed to load linked task note for status transition"
                );
                return;
            }
        };
        let Some(task) = note.metadata.task.as_ref() else {
            return;
        };
        // Terminal statuses must not be downgraded (parity with the router
        // path's own no-op-when-unchanged branch).
        if matches!(
            task.status,
            intent_core::TaskStatus::Complete | intent_core::TaskStatus::Cancelled
        ) || task.status == target
        {
            return;
        }
        if let Err(e) = WorkspaceApi::task_update_note_status(
            self,
            workspace_id.clone(),
            task_note_id.clone(),
            target_word.into(),
            None,
            Some(caller),
        )
        .await
        {
            tracing::warn!(
                error = %e,
                note = %task_note_id,
                target = target_word,
                "failed to transition linked task status"
            );
        }
    }

    /// Shared services op behind `ws.agent.requestDiscussion` /
    /// `ws.agent.reportBlocker` (`kind`: `"discussion" | "blocker"`). Modeled
    /// on [`Self::agent_report_to_parent_op`], but available to ALL agents —
    /// delegated or not, with or without a linked task:
    /// 1. persists the pending attention request (kind/reason/timestamp) on
    ///    the caller's session (cleared when the agent next receives a
    ///    message);
    /// 2. surfaces the request to the user via
    ///    [`Self::surface_attention_request`] — the `agent:updated` attention
    ///    fields, the system-role transcript notice
    ///    (`meta.kind = "discussion-request"` / `"blocker-report"`), the
    ///    self-sufficient `agent:attention-requested` event
    ///    `{ workspaceId, agentId, agentName, kind, reason, parentAgentId? }`
    ///    (FE sticky toast; `parentAgentId` is present only for delegated
    ///    callers — omitted entirely, never `null`, when there is no parent),
    ///    and the displayStatus promotion. A raise from INSIDE a live turn is
    ///    NOT surfaced here: it is parked on the deferred-attention registry
    ///    and flushed at the raising agent's next turn end
    ///    ([`Services::flush_deferred_attention`]), so mid-turn raises do not
    ///    interleave the notice with the turn's own output and the workspace
    ///    stays `in_progress` until the agent is actually idle;
    /// 3. transitions the linked task to `discussion_needed` / `blocked`
    ///    (terminal statuses untouched; no linked task = skip);
    /// 4. wakes a delegated caller's parent with a kind-flavored message —
    ///    delivered IMMEDIATELY even when the child is in an undelivered
    ///    `after_all` delegation group (mirroring the STAB-160 immediate
    ///    grouped-failure wake: an attention request is an alert the parent
    ///    must hear now, not at group settlement). The group's later
    ///    aggregated wake also annotates the child's line from the persisted
    ///    session fields (the record) while the request is still pending —
    ///    the fields are cleared when the child next receives a message, so
    ///    a parent reply before settlement retires the fold. Non-delegated
    ///    callers have no parent to wake.
    ///
    /// Agent status and `stop_reason` are untouched: the turn ends normally
    /// (no retry/requeue interaction).
    pub(crate) async fn agent_request_attention_op(
        &self,
        workspace_id: WorkspaceId,
        kind: String,
        reason: String,
        caller_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let caller = caller_agent_id.ok_or_else(|| {
            Error::Internal("requestAttention is only available to agents".to_string())
        })?;
        let (meta_kind, task_target, task_target_word) = match kind.as_str() {
            "discussion" => (
                "discussion-request",
                intent_core::TaskStatus::DiscussionNeeded,
                "discussion_needed",
            ),
            "blocker" => (
                "blocker-report",
                intent_core::TaskStatus::Blocked,
                "blocked",
            ),
            other => {
                return Err(Error::InvalidParams(format!(
                    "invalid attention kind: {other} (must be \"discussion\" or \"blocker\")"
                )));
            }
        };
        let reason = reason.trim().to_string();
        if reason.is_empty() {
            return Err(Error::InvalidParams("reason is required".to_string()));
        }
        let session = self.load_session_internal(&caller).await?;
        // Scope-guard the caller-supplied `workspace_id` (same shape as
        // `agent_report_to_parent_op`): reject a cross-workspace mismatch with
        // `NotFound` before any state changes.
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {caller}")));
        }
        // 1. Persist the pending attention request on the session via the
        // narrow attention writer (with `clear_attention_request` the only
        // post-insert mutator of the attention columns — the full-row
        // `update_agent_session` excludes them so a racing persist of a stale
        // session cannot clobber this write).
        let saved_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        let task_note_id = session.task_note_id.clone();
        let parent = session.parent_agent_id.clone();
        self.store
            .set_attention_request(&workspace_id, &caller, &kind, &reason, &saved_at)
            .await?;
        // The attention payload rides both the surfacing events and the
        // (always-immediate) parent/watcher wakes below. `parentAgentId` is
        // present only for delegated callers — OMITTED entirely (never
        // `null`) when there is no parent.
        let mut attention_data = json!({
            "workspaceId": workspace_id.0,
            "agentId": caller.0,
            "agentName": session.name.clone(),
            "kind": kind,
            "reason": reason,
        });
        if let Some(parent) = &parent {
            attention_data["parentAgentId"] = json!(parent.0);
        }
        // 2+3. User-facing surfacing (the `agent:updated` attention fields,
        // the transcript notice, `agent:attention-requested`, and the
        // displayStatus promotion). A raise from INSIDE a live turn — the
        // normal tool-call path — is parked on the deferred registry and
        // flushed by the turn-end choke points once the agent goes idle, so
        // the notice lands after the turn's own output and the workspace
        // stays `in_progress` while the agent is still working. Each raise
        // parks its own payload (captured here, at raise time): the store's
        // pending columns are latest-wins, but the flush surfaces every
        // parked raise so the event history matches the per-raise
        // parent/watcher wakes. A raise with no in-flight turn (empty-wake
        // recovery, FE-less edge paths) surfaces immediately as before.
        // Everything below this branch (linked-task transition, parent wake,
        // watcher fan-out) stays immediate either way — backend coordination
        // must not wait for the child's turn to end.
        if self.agent_is_busy(caller.clone()) {
            self.mark_deferred_attention(
                &caller,
                crate::DeferredAttention {
                    meta_kind,
                    reason: reason.clone(),
                    saved_at: saved_at.clone(),
                    attention_data: attention_data.clone(),
                },
            );
        } else {
            self.surface_attention_request(
                &workspace_id,
                &caller,
                meta_kind,
                &reason,
                &saved_at,
                &attention_data,
            )
            .await;
        }
        // 4. Linked-task transition (no linked task = skip).
        if let Some(note_id) = task_note_id {
            self.transition_linked_task_status(
                &workspace_id,
                note_id,
                caller.clone(),
                task_target,
                task_target_word,
            )
            .await;
        }
        // 5. Kind-flavored parent wake for delegated callers — delivered
        // immediately even when the child is enrolled in an undelivered
        // `after_all` delegation group (mirroring the STAB-160 immediate
        // grouped-failure wake in `deliver_completion_to_watches`): the
        // request is an alert the parent must hear now, not at group
        // settlement. The group's later aggregated wake also annotates the
        // child's line from the persisted session fields (the record) while
        // the request is still pending — a parent reply before settlement
        // clears the fields and retires the fold. Non-delegated callers have
        // no parent to wake.
        if let Some(parent) = &parent {
            let parent_home_ws = self
                .store
                .get_agent_session(parent)
                .await
                .map_or_else(|_| workspace_id.clone(), |s| s.workspace_id);
            let wake_text = crate::harness::latest().attention_parent_wake(
                &session.name,
                &caller.0,
                &kind,
                &reason,
            );
            let metadata = json!({
                "type": "event_notification",
                "eventCount": 1,
                "eventTypes": [intent_core::events::AGENT_ATTENTION_REQUESTED],
                "events": [{
                    "id": uuid::Uuid::new_v4().to_string(),
                    "type": intent_core::events::AGENT_ATTENTION_REQUESTED,
                    "timestamp": saved_at,
                    // Same enriched payload as the published event (including
                    // `parentAgentId` — the wake only fires for delegated
                    // callers, so it is always present here).
                    "data": attention_data,
                    "actor": {
                        "type": "agent",
                        "id": caller.0,
                        "name": session.name.clone(),
                    }
                }]
            });
            if let Err(e) = self
                .deliver_parent_wake(&parent_home_ws, parent.clone(), wake_text, Some(metadata))
                .await
            {
                tracing::warn!(
                    error = %e,
                    parent = %parent.0,
                    child = %caller.0,
                    "failed to deliver attention-request wake to parent"
                );
            }
        }
        // 6. monorepo#1229: attention fan-out to the caller's watchers. Every
        // ordinary active completion watch is woken — auto-registered
        // (wakeOrCreate/delegate SUB-1) watches included, not just explicit
        // `agent.watch` registrations (monorepo#3443 widened the fan-out past
        // the old `wake_on_attention` filter). Completion-only Chief asks wait
        // for terminal completion. The caller's parent is
        // excluded — step 5 already woke it directly — so a parent that ALSO
        // watches its child never receives a duplicate attention wake.
        // Watches are left in place (attention is not a completion).
        for watch in self
            .find_watches_for_child(&caller)
            .into_iter()
            .filter(|w| !w.completion_only && Some(&w.parent_agent_id) != parent.as_ref())
        {
            // Attention is not a completion, so the watch is left in place —
            // say so explicitly (issue monorepo#2051) to avoid reading as
            // terminal next to the retiring completion wake. A watch adopted
            // into an `after_all` delegation group wakes at group settlement,
            // not this agent's individual completion, so state the promise
            // that actually holds.
            let wake_text = crate::harness::latest().attention_watcher_wake(
                &session.name,
                &caller.0,
                &kind,
                &reason,
                watch.group_id.is_some(),
            );
            // `watchStillArmed: true` (monorepo#2060) is the machine-readable
            // twin of the "remains armed" note above, mirroring the hook
            // wakes' `hookStillActive` flag.
            let metadata = json!({
                "type": "event_notification",
                "eventCount": 1,
                "eventTypes": [intent_core::events::AGENT_ATTENTION_REQUESTED],
                "watchStillArmed": true,
                "events": [{
                    "id": uuid::Uuid::new_v4().to_string(),
                    "type": intent_core::events::AGENT_ATTENTION_REQUESTED,
                    "timestamp": saved_at,
                    "data": attention_data,
                    "actor": {
                        "type": "agent",
                        "id": caller.0,
                        "name": session.name.clone(),
                    }
                }]
            });
            if let Err(e) = self
                .deliver_parent_wake(
                    &watch.parent_workspace_id,
                    watch.parent_agent_id.clone(),
                    wake_text,
                    Some(metadata),
                )
                .await
            {
                tracing::warn!(
                    error = %e,
                    watcher = %watch.parent_agent_id.0,
                    child = %caller.0,
                    "failed to deliver attention-request wake to watcher"
                );
            }
        }

        Ok(json!({
            "ok": true,
            "kind": kind,
            "reason": reason,
            "savedAt": saved_at,
        }))
    }

    /// The user-facing surfacing of a pending attention request — the piece
    /// of [`Self::agent_request_attention_op`] that is deferred to turn end
    /// for mid-turn raises:
    /// - `agent:updated` with the attention fields;
    /// - the system-role transcript notice with
    ///   `meta.kind = "discussion-request"` / `"blocker-report"` (emits
    ///   `agent:message`) so the conversation renders a distinct card that
    ///   survives rehydration — best-effort, the session fields are the
    ///   durable contract;
    /// - the self-sufficient `agent:attention-requested` event (FE sticky
    ///   toast);
    /// - the debounced lastActivity schedule and the displayStatus
    ///   recompute (a pending request on a top-level agent promotes the
    ///   derived displayStatus to `needs_attention`, §6.5 step 0;
    ///   child/background raises stay silent — the derivation ignores them
    ///   and the dedup cache suppresses the no-op).
    ///
    /// Callers: the immediate arm of `agent_request_attention_op` (no
    /// in-flight turn) and [`Services::flush_deferred_attention`] (turn-end
    /// flush of a mid-turn raise).
    pub(crate) async fn surface_attention_request(
        &self,
        workspace_id: &WorkspaceId,
        caller: &AgentId,
        meta_kind: &str,
        reason: &str,
        saved_at: &str,
        attention_data: &Value,
    ) {
        let kind = attention_data
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.publish_agent_mutation_event(
            workspace_id,
            caller,
            intent_core::events::AGENT_UPDATED,
            json!({
                "agentId": caller.0,
                "attentionRequestKind": kind,
                "attentionRequestTimestamp": saved_at,
            }),
        )
        .await;
        let notice_content = json!([{
            "type": "text",
            "text": reason,
            "meta": { "kind": meta_kind }
        }]);
        match self
            .store
            .append_agent_message(caller, "system", &notice_content, saved_at)
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_message_events(workspace_id, caller, &message, None)
                    .await;
            }
            Err(e) => {
                tracing::warn!(
                    agent = %caller.0,
                    error = %e,
                    "request_attention: failed to append transcript notice"
                );
            }
        }
        self.publish_agent_mutation_event(
            workspace_id,
            caller,
            intent_core::events::AGENT_ATTENTION_REQUESTED,
            attention_data.clone(),
        )
        .await;
        // Schedule debounced lastActivity event (§10.1).
        self.schedule_last_activity_event(workspace_id.clone());
        self.maybe_emit_display_status_changed(workspace_id).await;
    }

    /// Turn-end flush of the mid-turn attention raises: if `agent_id` holds
    /// parked raises AND the persisted request is still pending (not cleared
    /// by a mid-turn user delivery), surface each parked raise in order via
    /// [`Self::surface_attention_request`] using the payload captured at
    /// raise time — so two raises in one turn each get their transcript
    /// notice and `agent:attention-requested` event, matching the per-raise
    /// parent/watcher wakes. Called from every turn termination choke point
    /// — the prompt-turn settlement (clean idle and terminal error), the
    /// suspend-interrupt enrollment, the harness-wake idle, and the
    /// interrupt path — so the requests surface at the FIRST turn end after
    /// the raise regardless of how the turn ended. Consuming the queue up
    /// front makes the flush idempotent across racing choke points; a queue
    /// whose persisted request was already cleared retires silently.
    /// Best-effort: a session read failure only logs (the persisted fields
    /// still surface through ordinary list/get reads).
    pub(crate) async fn flush_deferred_attention(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) {
        let parked = self.take_deferred_attention(agent_id);
        if parked.is_empty() {
            return;
        }
        let session = match self.store.get_agent_session_summary(agent_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "deferred attention flush: failed to load session"
                );
                return;
            }
        };
        if session.attention_request_kind.is_none() {
            // Cleared before idle (user-origin delivery mid-turn) — nothing
            // to surface.
            return;
        }
        for entry in parked {
            self.surface_attention_request(
                workspace_id,
                agent_id,
                entry.meta_kind,
                &entry.reason,
                &entry.saved_at,
                &entry.attention_data,
            )
            .await;
        }
    }

    /// `agent.delegate`: create a session and (best-effort) assign it to the
    /// target task note (PROTOCOL §5.5). The batch form (`tasks` present)
    /// routes to [`Self::agent_delegate_batch_op`]; single-task calls are
    /// unchanged.
    pub(crate) async fn agent_delegate_op(
        &self,
        workspace_id: WorkspaceId,
        input: intent_core::AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        // Resolve the child's first message up front so it can be persisted as
        // `AgentSession.initial_message` on the created session (harvested from
        // the `metadata.initialMessage` create param; served by
        // `agent.getSession` only — P3-1.2b; the FE
        // stored it so a wake-up can resume). Source priority mirrors the TS
        // `DelegateTaskTool`: explicit `agentInstructions`, then `taskText`,
        // then the linked task note's content (falling back to its title).
        fn first_nonempty(s: &str) -> Option<String> {
            let trimmed = s.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        // Resolve the child agent's name to match the reference `DelegateTaskTool`
        // (agent-interaction-tools.ts): the taskText path uses `taskText`, the
        // taskNoteId path uses the note's title. Both truncate to 100 chars
        // (`len > 100 ? substring(0,97) + "..." : text`). Without this the
        // child inherits the generic `Agent xxxxxx` fallback from
        // `agent_create_op`, which then leaks into the waiting panel, agent
        // cards, and `agent:idle` wake reports (NAME-1).
        fn truncate_agent_name(name: String) -> String {
            // The reference uses JS `.length` / `.substring(0, 97)`, which
            // count UTF-16 code units — non-BMP characters (e.g. emoji)
            // count as 2. Match that to avoid off-by-one divergences on
            // emoji-heavy task titles (Copilot #84 review).
            let u16_len = name.encode_utf16().count();
            if u16_len > 100 {
                let units: Vec<u16> = name.encode_utf16().take(97).collect();
                // `substring(0, 97)` can split a surrogate pair; drop a
                // trailing lone high surrogate so the resulting Rust string
                // stays valid UTF-8 instead of embedding a U+FFFD replacement.
                let cutoff = if units
                    .last()
                    .is_some_and(|&u| (0xD800..=0xDBFF).contains(&u))
                {
                    units.len() - 1
                } else {
                    units.len()
                };
                let mut truncated = String::from_utf16_lossy(&units[..cutoff]);
                truncated.push_str("...");
                truncated
            } else {
                name
            }
        }
        // `greedy` was a batch-level conflict override; it is REMOVED. Any
        // supplied value — `true`, `false`, or an explicit `null` — rejects
        // on BOTH forms (the check sits before the batch routing so a
        // single-task call cannot silently carry it either); only a missing
        // field passes.
        if input.greedy.is_some() {
            return Err(Error::InvalidParams(
                "agent.delegate: greedy was removed; delegate a held task individually to force it past the conflict hold".to_string(),
            ));
        }
        if input.tasks.is_some() {
            return self
                .agent_delegate_batch_op(workspace_id, input, parent_agent_id)
                .await;
        }
        let wait_mode = input.wait_mode.clone();
        // Persist the task linkage + skipAutoCommit on the session so the
        // auto-commit-on-idle subscriber (LNI-1) can resolve `Linked-Note-Id:`
        // and honor the opt-out without a reverse lookup on every idle event.
        let session_task_note_id = input.task_note_id.clone().or(input.note_id.clone());
        // Harness-owned commits: the effective opt-out is the caller's explicit
        // `skipAutoCommit` OR the workspace's effective auto-commit being off,
        // so delegated children skip the idle subscriber whenever nothing
        // would auto-commit anyway. Deliberately sticky: the derived opt-out
        // is persisted on the session, so toggling the workspace back ON via
        // `workspace.setAutoCommit` never re-enables idle commits for
        // sessions created while it was OFF. New sessions created after the
        // toggle pick up the ON state. The child's prompt stays status-neutral
        // (the `## Commit Policy` clause in `rules.rs`); enforcement lives in
        // the `git_ops` gate and the idle subscriber, never in prompts.
        let skip_auto_commit = input.skip_auto_commit.unwrap_or(false)
            || !self.effective_auto_commit(&workspace_id).await;
        let task_text_msg = input.task_text.as_deref().and_then(first_nonempty);
        let mut message = input
            .agent_instructions
            .as_deref()
            .and_then(first_nonempty)
            .or_else(|| task_text_msg.clone());
        // Load the linked task note whenever the delegation names one: the
        // note's title/body feeds the message fallback, the child name
        // derivation, and the TASK-C reference preamble that prefixes the
        // child's first message when a task is linked.
        let task_note = match session_task_note_id.as_ref() {
            Some(note_id) => crate::fetch_note(&self.store, &workspace_id, note_id)
                .await
                .ok(),
            None => None,
        };
        if message.is_none() {
            if let Some(note) = task_note.as_ref() {
                message = first_nonempty(&note.content).or_else(|| first_nonempty(&note.title));
            }
        }
        // TASK-C: mirror the reference `DelegateTaskTool` preamble
        // (agent-interaction-tools.ts). When the delegation links a task note,
        // APPEND the standard "Your Task Note" block after the user message
        // with a `---` separator so the child knows its note ID/title and the
        // single-task scope contract; without a linked note the message is
        // delivered verbatim. No state-specific commit instruction is
        // appended: the child relies on the status-neutral `## Commit Policy`
        // system-prompt clause, and `skip_auto_commit` only gates the idle
        // subscriber and the `git_ops` commit gate.
        if let (Some(note), Some(note_id)) = (task_note.as_ref(), session_task_note_id.as_ref()) {
            let title = first_nonempty(&note.title).unwrap_or_default();
            message = Some(crate::harness::latest().delegation_first_message(
                message.as_deref(),
                &title,
                &note_id.0,
            ));
        }
        let child_name = task_text_msg
            .or_else(|| task_note.as_ref().and_then(|n| first_nonempty(&n.title)))
            .map(truncate_agent_name);
        // Load the delegating parent once: it feeds the child's
        // `delegationDepth` (parent depth + 1; P3-1.2b), the depth-limit guard
        // below, and the completion-watch registration.
        let parent_session = match &parent_agent_id {
            Some(parent) => self.store.get_agent_session(parent).await.ok(),
            None => None,
        };
        // Depth guard (port of `MAX_DELEGATION_DEPTH` in the reference
        // `agent-interaction-tools.ts`): a caller already at the max depth
        // cannot delegate further. Enforced only when a caller is present
        // (MCP front door); RPC-level creates stay parentless and skip it.
        if parent_agent_id.is_some() {
            let parent_depth = parent_session
                .as_ref()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0);
            if parent_depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "Cannot delegate task: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {parent_depth}. Please complete this task directly instead of delegating further."
                )));
            }
        }
        // Run the watch scope gate BEFORE any side-effectful work (child
        // creation, group enrollment) so a rejection — a non-chief parent
        // delegating outside its home workspace — is side-effect free: no
        // orphaned Pending child, no partially-initialized delegation group.
        // Gated on the same condition as the registration block below
        // (caller present and not deleted); `register_completion_watch`
        // re-checks the same gate as the shared enforcement point.
        if parent_agent_id.is_some() {
            if let Some(session) = parent_session
                .as_ref()
                .filter(|s| s.status != AgentStatus::Deleted)
            {
                crate::agent_subscriptions::check_watch_scope(
                    &session.workspace_id,
                    &workspace_id,
                )?;
            }
        }
        // Occupancy pre-gate: a task note that already has a live assigned
        // agent cannot be silently double-delegated. Runs BEFORE any
        // side-effectful work (child creation, group enrollment), alongside
        // the depth/scope gates above, so a rejection leaves no orphaned
        // child. "Occupied" reuses the same live/resumable predicate as
        // `agent_wake_or_create_op`'s newest-first scan (loadable, not
        // Deleted, not poisoned) and only applies while the task itself is
        // still workable (status not complete/cancelled). `force: true`
        // deliberately adds a second agent.
        if input.force != Some(true) {
            if let Some(task) = task_note.as_ref().and_then(|n| n.metadata.task.as_ref()) {
                if !matches!(task.status, TaskStatus::Complete | TaskStatus::Cancelled) {
                    if let Some(existing) = self
                        .scan_assigned_agents(&task.assigned_agent_ids)
                        .await?
                        .live_session
                    {
                        return Err(Error::InvalidParams(format!(
                            "Task is already being worked by agent {} (\"{}\"). \
                             Use agent.sendToTask or agent.wakeOrCreate to reach the existing agent, \
                             or pass force: true to intentionally add a second agent.",
                            existing.id, existing.name
                        )));
                    }
                }
            }
        }
        let delegation_depth = parent_agent_id.as_ref().map(|_| {
            parent_session
                .as_ref()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0)
                + 1
        });
        // D2: resolve the provider up front. Precedence (PROTOCOL §5.5):
        // explicit `provider` param > specialist frontmatter `codingAgent` >
        // settings-derived default. An explicit `provider` must be known and
        // available — it rejects with `-32602` before any side effect. When
        // nothing resolves at all, `resolve_delegate_provider` fails loudly
        // with `-32602` (monorepo#3044: no positional last resort).
        // SECURITY: derive workspace_path from the stored workspace record,
        // never a client-supplied value (same rationale as
        // `agent_create_op`'s model resolution).
        let workspace_path = self
            .store
            .get_workspace(&workspace_id)
            .await
            .ok()
            .and_then(|w| crate::git_ops::worktree_path(&w));
        // Specialist validation (monorepo#3497): reject an unknown specialist
        // id with `-32602` HERE, before provider/effort resolution — an
        // unknown id would otherwise surface as a confusing provider-
        // resolution failure (or only fail inside `agent_create_op` after the
        // resolution rungs silently skipped the specialist tiers). Runs
        // before any side effect, so a rejection leaves no orphaned child.
        // The whole resolution cluster below — specialist canonicalization,
        // provider guard, default-model and effort resolution — re-reads the
        // specialist tier directories, so it runs on the blocking pool
        // (monorepo#4148).
        let services = self.clone();
        let specialist_param = input.specialist.clone();
        let provider_param_in = input.provider.clone();
        let model_param = input.model.clone();
        let effort_param = input.reasoning_effort.clone();
        let ws_path = workspace_path.clone();
        let (delegate_provider, effective_model, reasoning_effort) = tokio::task::spawn_blocking(
            move || -> Result<(Option<String>, Option<String>, Option<String>)> {
                if let Some(spec_id) = specialist_param.as_deref() {
                    services
                        .specialists_service()
                        .canonical_id_or_err(spec_id, ws_path.as_deref())?;
                }
                let delegate_provider = if let Some(provider_param) = provider_param_in.as_deref() {
                    ensure_known_provider("agent.delegate", provider_param)?;
                    ensure_provider_available(
                        "agent.delegate",
                        provider_param,
                        &services.effective_settings().providers,
                    )?;
                    Some(provider_param.to_string())
                } else if model_param.is_none() {
                    resolve_delegate_provider(
                        &services,
                        specialist_param.as_deref(),
                        ws_path.as_deref(),
                    )?
                } else {
                    None
                };
                // Reasoning effort (PROTOCOL §5.11): param > chosen model option's
                // effort > specialist frontmatter > unset, validated against the
                // cached catalog's `effortLevels` for the effective model. The
                // effective model must be the one `agent_create_op` will actually
                // pin — the explicit `model`, else the full default-model resolution
                // (specialist frontmatter pin, then the settings chain) — so a
                // specialist whose `modelOptions` entry keys on the settings default
                // model still gets its option effort selected. Runs BEFORE the child
                // is created so a `-32602` rejection is side-effect free.
                let effective_model = model_param.clone().or_else(|| {
                    resolve_agent_default_model(
                        &services,
                        specialist_param.as_deref(),
                        ws_path.as_deref(),
                        delegate_provider.as_deref(),
                    )
                });
                let reasoning_effort = resolve_delegate_reasoning_effort(
                    &services,
                    effort_param.as_deref(),
                    specialist_param.as_deref(),
                    effective_model.as_deref(),
                    ws_path.as_deref(),
                );
                Ok((delegate_provider, effective_model, reasoning_effort))
            },
        )
        .await
        .map_err(|e| Error::Internal(format!("agent.delegate resolution task failed: {e}")))??;
        // A blank resolved value is an explicit clear (see
        // `resolve_delegate_reasoning_effort`); only a real level is validated.
        if let Some(effort) = reasoning_effort.as_deref().filter(|e| !e.trim().is_empty()) {
            ensure_effort_supported_by_model(
                "agent.delegate",
                &self.models_catalog,
                effective_model.as_deref(),
                effort,
            )?;
        }
        let mut extra_metadata = serde_json::Map::new();
        if let Some(depth) = delegation_depth {
            extra_metadata.insert("delegationDepth".to_string(), json!(depth));
        }
        if let Some(msg) = &message {
            extra_metadata.insert("initialMessage".to_string(), json!(msg));
        }
        // Delegated agents are background agents (the TS `DelegateTaskTool`
        // always sets `metadata.isBackground: true`; G-A1/P3-1.2c).
        extra_metadata.insert("isBackground".to_string(), json!(true));
        let extra = AgentCreateExtra {
            provider: delegate_provider,
            reasoning_effort,
            metadata: (!extra_metadata.is_empty()).then_some(Value::Object(extra_metadata)),
            // Delegated agents carry a task-derived name but stay renameable
            // by the child's opening-turn `ws.workspace.setAgentName`
            // (`skipIfExplicitlySet: true`) — mirror the reference which
            // does not set `nameExplicitlySet` at delegate-time creation.
            name_explicitly_set: Some(false),
            ..AgentCreateExtra::default()
        };
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                child_name,
                input.model,
                input.specialist,
                parent_agent_id.clone(),
                session_task_note_id.clone(),
                skip_auto_commit,
                extra,
            )
            .await?;
        let agent_id = created["agent"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let name = created["agent"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        // Resolved ACP provider persisted on the created session (AgentLite
        // `provider`, skip-if-none) — surfaced on the delegate result so
        // clients can render the provider immediately (PROTOCOL §5.5). Absent
        // when the session has none (provider CLI default applies).
        let provider = created["agent"]["provider"].as_str().map(str::to_string);

        // Track effective isolation mode for the result. Provisioning runs in
        // a background task (monorepo#871), so an eligible CoW request reports
        // "pending" here; the settled outcome is observable via the session's
        // sandbox fields and the `sandbox:cow:created` event.
        let mut effective_isolation: Option<&str> = None;

        // Provision sandbox if isolation=cow is requested (Task 3).
        // Check if isolation is "cow" (explicit or defaulted from workspace setting).
        // Default to "cow" if workspace.cowIsolation setting is enabled and no explicit
        // isolation parameter was provided (Task 5).
        let mut isolation = input.isolation.clone();
        if isolation.is_none() {
            // Check workspace.cowIsolation setting
            // `settings_get` returns the `{ path, value, definition }`
            // envelope (§5.9) — read the nested `value`.
            if let Ok(setting) = self
                .settings_get("workspace.cowIsolation".to_string())
                .await
            {
                if setting["value"].as_bool().unwrap_or(false) {
                    isolation = Some("cow".to_string());
                }
            }
        }
        if isolation.as_deref() == Some("cow") {
            let workspace = self.store.get_workspace(&workspace_id).await.ok();
            if let Some(ws) = workspace {
                // Sandbox-eligible: direct-mode workspaces (no worktree or
                // skip_worktree=true; sandbox sourced from the user's repo folder),
                // CoW-checkout workspaces (sourced from the workspace checkout),
                // and direct-checkout workspaces (`checkoutMode == "direct"`,
                // standalone plain repo — cache-hydrated or isNewRepo).
                // Worktree-mode workspaces keep the shared checkout (no sandbox).
                let is_direct_mode = (ws.skip_worktree || ws.worktree_path.is_none())
                    && ws.repository_path.is_some();
                let is_standalone_checkout = matches!(
                    ws.checkout_mode,
                    Some(intent_core::CheckoutMode::Cow | intent_core::CheckoutMode::Direct)
                ) && ws.worktree_path.is_some();
                if is_direct_mode || is_standalone_checkout {
                    // Drop guard: settles the gate even if provisioning
                    // panics, so the gate map never accumulates stale
                    // entries. Constructed BEFORE the spawn (and moved into
                    // the task) so cleanup is unconditional even when the
                    // runtime drops the task unpolled at shutdown. On the
                    // normal path the guard drops AFTER
                    // `provision_delegate_sandbox` returns — the session's
                    // sandbox fields and the `sandbox:cow:created` event are
                    // already published, so a released waiter observes the
                    // settled state. Dropping the held sender (also via the
                    // guard) releases every waiter.
                    struct SettleGuard {
                        services: Services,
                        aid: AgentId,
                        _release: tokio::sync::watch::Sender<()>,
                    }
                    impl Drop for SettleGuard {
                        fn drop(&mut self) {
                            self.services.settle_sandbox_provisioning(&self.aid);
                        }
                    }
                    // Same root fallback as `workspace.create` (the intentd
                    // binary configures the root via INTENTD_WORKSPACES_DIR /
                    // `workspaces.root` rather than `.with_workspaces_root`).
                    let root = self
                        .workspaces_root
                        .clone()
                        .unwrap_or_else(crate::default_workspaces_root);
                    let aid = AgentId::from(agent_id.as_str());
                    // The CoW clone runs OFF the delegate critical path
                    // (monorepo#871): on large checkouts it takes tens of
                    // seconds, which previously blew through the 30s
                    // `workspace_api` budget and the harness's own MCP client
                    // timeout. Register the settlement gate BEFORE returning
                    // so the child's turn worker (`ensure_started`) blocks its
                    // first ACP spawn until the clone settles — the child
                    // never spawns against a half-copied sandbox.
                    let settled = self.begin_sandbox_provisioning(&aid);
                    effective_isolation = Some("pending");
                    let guard = SettleGuard {
                        services: self.clone(),
                        aid,
                        _release: settled,
                    };
                    let ws_id = workspace_id.clone();
                    tokio::spawn(async move {
                        guard
                            .services
                            .provision_delegate_sandbox(&ws_id, &guard.aid, root)
                            .await;
                    });
                }
            }
        }

        if let Some(task_note_id) = input.task_note_id.clone().or(input.note_id.clone()) {
            // Occupancy was already resolved by the pre-gate above (or
            // deliberately overridden), so this internal assignment must not
            // be re-blocked by `assign_agent`'s own guard.
            let _ = self
                .assign_agent(
                    workspace_id.clone(),
                    task_note_id,
                    agent_id.clone(),
                    Some(true),
                )
                .await;
        }
        // Auto-subscribe the delegating caller to the child's completion (AS-2).
        // Only the MCP front door carries a caller (`parent_agent_id = Some`); the
        // RPC front door (`None`) registers nothing. `after_all` defers to the
        // delegation-group fan-in (AS-4); `immediate`/default registers an
        // ungrouped watch.
        if let Some(parent) = parent_agent_id {
            // Best-effort guard: skip if the parent agent is already deleted
            // (TS `selectIsAgentDeleted`).
            let parent_deleted = parent_session
                .as_ref()
                .is_some_and(|s| s.status == AgentStatus::Deleted);
            if !parent_deleted {
                // The watch/group is anchored in the parent's HOME workspace
                // (where wakes are delivered): for same-workspace delegation
                // this equals `workspace_id`; for a chief parent it is
                // `__chief__`. Fall back to the child's workspace when the
                // parent session could not be loaded.
                let parent_home_ws = parent_session
                    .as_ref()
                    .map_or_else(|| workspace_id.clone(), |s| s.workspace_id.clone());
                let parent_name = parent_session.map(|s| s.name).unwrap_or_default();
                let child = AgentId::from(agent_id.as_str());
                // The scope gate already ran up front (before child creation),
                // so `register_completion_watch`'s re-check cannot reject here
                // and the group creation below is safe from partial state.
                if wait_mode.as_deref() == Some(WAIT_MODE_AFTER_ALL) {
                    // Enroll the child in the parent's after_all delegation group
                    // and register a group watch (group_id = Some) so the
                    // delivery worker routes its completion into the group
                    // fan-in instead of waking the parent immediately (AS-4).
                    let gid = self.get_or_create_delegation_group(&parent_home_ws, &parent);
                    self.enroll_child_in_group(&gid, &child);
                    self.register_completion_watch(
                        &parent_home_ws,
                        &workspace_id,
                        parent.clone(),
                        parent_name,
                        child,
                        Some(gid),
                    )?;
                } else {
                    self.register_completion_watch(
                        &parent_home_ws,
                        &workspace_id,
                        parent.clone(),
                        parent_name,
                        child,
                        None,
                    )?;
                }
                self.publish_subscriptions_changed(&parent_home_ws, &parent)
                    .await;
            }
        }
        // Deliver the child's first message (resolved above, persisted as
        // `AgentSession.initial_message`) and start its turn (PROTOCOL §5.5).
        // Without this the child stays `Pending` and never runs. `wait_mode` is
        // already honored by the completion-watch registration above; the child
        // turn itself starts unconditionally.
        //
        // Delivery routes through the runtime `AgentManager` when attached (the
        // proven `agent.sendMessage` path: persist + spawn the turn worker, which
        // lazily spawns the child and streams `agent:stream:*` keyed by the CHILD
        // `agentId`); read-only/test wiring falls back to the store-only persist.
        if let Some(message) = message {
            let child = AgentId::from(agent_id.as_str());
            let send = match self.agent_manager() {
                Some(manager) => {
                    manager
                        .send_message(
                            child,
                            workspace_id,
                            message,
                            None,
                            crate::agent_manager::TurnOptions::default(),
                        )
                        .await
                }
                None => {
                    self.agent_send_message_op(child, message, None, None, None, None)
                        .await
                }
            };
            if let Err(e) = send {
                tracing::warn!(agent = %agent_id, error = %e, "delegate: failed to start child turn");
            }
        }

        // Include effective isolation in the result when isolation was requested
        let mut result = json!({
            "ok": true,
            "agentId": agent_id,
            "name": name,
        });
        if let Some(provider) = provider {
            result
                .as_object_mut()
                .unwrap()
                .insert("provider".to_string(), json!(provider));
        }
        if let Some(eff_iso) = effective_isolation {
            result
                .as_object_mut()
                .unwrap()
                .insert("effectiveIsolation".to_string(), json!(eff_iso));
        }
        Ok(result)
    }

    /// Snapshot every task note in `workspace_id` into the
    /// [`batch::BatchTaskSnap`] shape (+ a note-id → title map). Shared by
    /// batch `agent.delegate` classification and the delivery-time unblocked
    /// computation (intent-hq/monorepo#2044), so both consume identical
    /// readiness inputs: status, `dependsOn`/`conflictsWith` edges, live
    /// assigned agents, and effort estimates.
    pub(crate) async fn snapshot_batch_tasks(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<(
        HashMap<String, batch::BatchTaskSnap>,
        HashMap<String, String>,
    )> {
        let notes = self.store.list_notes(workspace_id).await?;
        let mut titles: HashMap<String, String> = HashMap::new();
        let mut snaps: HashMap<String, batch::BatchTaskSnap> = HashMap::new();
        for note in &notes {
            let Some(task) = &note.metadata.task else {
                continue;
            };
            let live_agent = if matches!(task.status, TaskStatus::Complete | TaskStatus::Cancelled)
                || task.assigned_agent_ids.is_empty()
            {
                None
            } else {
                self.scan_assigned_agents(&task.assigned_agent_ids)
                    .await?
                    .live_session
                    .map(|s| (s.id.0, s.name))
            };
            titles.insert(note.id.0.clone(), note.title.clone());
            snaps.insert(
                note.id.0.clone(),
                batch::BatchTaskSnap {
                    status: task.status,
                    depends_on: task.depends_on.iter().map(|d| d.0.clone()).collect(),
                    conflicts_with: task.conflicts_with.iter().map(|c| c.0.clone()).collect(),
                    live_agent,
                    effort_minutes: task
                        .estimated_effort
                        .as_deref()
                        .and_then(crate::task_effort::parse_effort_minutes),
                },
            );
        }
        Ok((snaps, titles))
    }

    /// Delivery-time "tasks now unblocked" section for a draining batch of
    /// completion wakes (intent-hq/monorepo#2044). `metadatas` are the
    /// `messageMetadata` values of EVERY entry delivering in the same model
    /// turn: their stamped trigger tasks (enqueue-time facts — the settled
    /// children's linked task-note ids) are collected and deduplicated, the
    /// named workspaces' task state is fetched FRESH, and ONE coalesced
    /// [`ready_delta::ready_set_delta`] is rendered — so the section always
    /// reflects readiness at delivery time, never a stale enqueue-time
    /// snapshot. Returns `None` when no entry carries triggers or the delta
    /// is empty. Strictly advisory and best-effort: store errors fail open
    /// (`None`) and the wake delivers unannotated.
    pub(crate) async fn unblocked_section_for_delivery<'a>(
        &self,
        agent_id: &AgentId,
        metadatas: impl Iterator<Item = Option<&'a Value>>,
    ) -> Option<String> {
        let triggers = ready_delta::collect_trigger_tasks(metadatas);
        if triggers.is_empty() {
            return None;
        }
        match self
            .store
            .get_agent_session_task_graph_enabled(agent_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => {
                tracing::warn!(
                    agent_id = %agent_id,
                    %error,
                    "taskGraph session snapshot unavailable; delivering wake without the section"
                );
                return None;
            }
        }
        // Group trigger task ids per workspace (cross-workspace parents can
        // in principle coalesce wakes from several workspaces) and compute
        // one delta per workspace against its CURRENT snapshot.
        let mut by_ws: Vec<(String, Vec<String>)> = Vec::new();
        for (ws, id) in &triggers {
            match by_ws.iter_mut().find(|(w, _)| w == ws) {
                Some((_, ids)) => ids.push(id.clone()),
                None => by_ws.push((ws.clone(), vec![id.clone()])),
            }
        }
        let mut delta = Vec::new();
        for (ws, ids) in &by_ws {
            let workspace_id = WorkspaceId::from(ws.as_str());
            let (snaps, titles) = match self.snapshot_batch_tasks(&workspace_id).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        workspace = %ws,
                        error = %e,
                        "unblocked-section snapshot failed; delivering wake without the section"
                    );
                    continue;
                }
            };
            delta.extend(ready_delta::ready_set_delta(ids, &snaps, &titles));
        }
        if delta.is_empty() {
            return None;
        }
        Some(ready_delta::render_unblocked_section(
            &delta,
            triggers.len() > 1,
        ))
    }

    /// Batch `agent.delegate` (PROTOCOL §5.5): classify every task in
    /// `input.tasks` as start / held / skipped — a PURE function of current
    /// state (task statuses, `dependsOn`/`conflictsWith` edges, live assigned
    /// agents; see [`batch::classify_batch_tasks`]) — then delegate exactly
    /// the start subset through the unchanged single-task path (per-task
    /// agent creation, group enrollment honoring `waitMode`). Entries may be
    /// bare task-note ids or objects carrying per-task
    /// `specialist`/`model`/`reasoningEffort` overrides of the top-level
    /// defaults. No scheduler state is written: held tasks are simply not
    /// started, and re-calling with the same list is idempotent
    /// (running/terminal tasks classify as `skipped`). The result enumerates
    /// EVERY supplied task with its disposition + reason, carries a top-level
    /// `summary` (started/held/skipped/errors counts) plus a top-level
    /// `warning` when zero tasks started (monorepo#3334), and projects the
    /// unlock plan; the existing group settlement wake is the resume signal —
    /// the caller re-calls delegate then, which recomputes everything. A
    /// zero-started `after_all` call with no open delegation group owes no
    /// future wake, so it additionally delivers an immediate advisory wake to
    /// the parent (best-effort) instead of silence.
    async fn agent_delegate_batch_op(
        &self,
        workspace_id: WorkspaceId,
        input: intent_core::AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        use batch::{
            classify_batch_tasks, project_unlock_plan, relations_unknown_ids, BatchDisposition,
        };
        use intent_core::BatchTaskEntry;

        let entries: Vec<BatchTaskEntry> = input.tasks.clone().unwrap_or_default();
        // Per-task option overrides, keyed by task-note id. Only OBJECT
        // entries populate the map, so for a duplicated id the last object
        // entry wins and a trailing bare-string duplicate does NOT reset an
        // earlier object's overrides (classification dedups to one row
        // anyway).
        let mut overrides: HashMap<String, intent_core::BatchTaskOptions> = HashMap::new();
        for entry in &entries {
            if let BatchTaskEntry::Options(opts) = entry {
                if opts.agent_instructions.is_some() {
                    return Err(Error::InvalidParams(
                        "agent.delegate: agentInstructions is not supported on a tasks entry — each started task's first message resolves from its own task note".to_string(),
                    ));
                }
                overrides.insert(opts.task_note_id.0.clone(), opts.clone());
            }
        }
        let requested: Vec<String> = entries
            .iter()
            .map(|entry| entry.task_note_id().0.clone())
            .collect();
        if requested.is_empty() {
            return Err(Error::InvalidParams(
                "agent.delegate: tasks must be a non-empty array of task note ids".to_string(),
            ));
        }
        // The batch form is an alternative addressing mode: mixing it with
        // the single-task addressing params is ambiguous and rejected.
        if input.task_note_id.is_some() || input.note_id.is_some() || input.task_text.is_some() {
            return Err(Error::InvalidParams(
                "agent.delegate: tasks is mutually exclusive with taskNoteId/noteId/taskText"
                    .to_string(),
            ));
        }
        // Single-task-only params are rejected rather than silently dropped:
        // `agentInstructions` addresses ONE child's first message (each batch
        // child resolves its own from its task note), and `force` overrides
        // the occupancy gate that batch mode deliberately maps to `skipped`.
        if input.agent_instructions.is_some() {
            return Err(Error::InvalidParams(
                "agent.delegate: agentInstructions is not supported with tasks — each started task's first message resolves from its own task note".to_string(),
            ));
        }
        if input.force == Some(true) {
            return Err(Error::InvalidParams(
                "agent.delegate: force is not supported with tasks — occupied tasks classify as skipped (use the single-task form to force a second agent)".to_string(),
            ));
        }
        // The top-level `provider` is the batch default shared by every entry
        // that doesn't override it, so validate it up front — before the
        // classification loop can start any task — rather than surfacing the
        // same failure as N per-row `error` dispositions after earlier rows
        // already spawned. Per-entry `provider`/`model` overrides stay
        // per-row (`error` disposition via the single-task path), consistent
        // with the other per-entry options.
        if let Some(provider_param) = input.provider.as_deref() {
            ensure_known_provider("agent.delegate", provider_param)?;
            ensure_provider_available(
                "agent.delegate",
                provider_param,
                &self.effective_settings().providers,
            )?;
        }
        // The top-level `specialist` is likewise the batch default shared by
        // every entry that doesn't override it — validate it once up front
        // (monorepo#3497) so an unknown default fails with one crisp `-32602`
        // instead of N identical per-row `error` dispositions. Per-entry
        // `specialist` overrides stay per-row via the single-task path, like
        // the other per-entry options.
        if let Some(spec_id) = input.specialist.as_deref() {
            let workspace_path = self
                .store
                .get_workspace(&workspace_id)
                .await
                .ok()
                .and_then(|w| crate::git_ops::worktree_path(&w));
            // Validation walks the specialist tier directories — blocking
            // pool (monorepo#4148).
            let services = self.clone();
            let spec_id = spec_id.to_string();
            tokio::task::spawn_blocking(move || {
                services
                    .specialists_service()
                    .canonical_id_or_err(&spec_id, workspace_path.as_deref())
                    .map(|_| ())
            })
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "agent.delegate batch specialist validation task failed: {e}"
                ))
            })??;
        }
        // Depth + watch-scope guards up front (the same checks the
        // single-task path runs before any side-effectful work) so a
        // rejection is one clear error before any child is created, not N
        // identical per-task failures.
        if let Some(parent) = &parent_agent_id {
            let parent_session = self.store.get_agent_session(parent).await.ok();
            let parent_depth = parent_session
                .as_ref()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0);
            if parent_depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "Cannot delegate task: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {parent_depth}. Please complete this task directly instead of delegating further."
                )));
            }
            if let Some(session) = parent_session
                .as_ref()
                .filter(|s| s.status != AgentStatus::Deleted)
            {
                crate::agent_subscriptions::check_watch_scope(
                    &session.workspace_id,
                    &workspace_id,
                )?;
            }
        }

        // Snapshot every task note in the workspace (not just the requested
        // ones): conflicts are symmetric and a running non-requested task
        // must still hold a requested one, and dependency statuses can name
        // any task note.
        let (snaps, titles) = self.snapshot_batch_tasks(&workspace_id).await?;
        let unknown: Vec<&String> = requested
            .iter()
            .filter(|id| !snaps.contains_key(*id))
            .collect();
        if !unknown.is_empty() {
            return Err(Error::InvalidParams(format!(
                "agent.delegate: tasks names ids that are not task notes in this workspace: {}",
                unknown
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        let classified = classify_batch_tasks(&requested, &snaps);
        // Relation-less annotation (monorepo#2457 part 3): requested tasks
        // the graph does not cover (no relations of their own, not
        // referenced by any other requested task) classify exactly as
        // before, but their rows carry `relationsUnknown: true` and the
        // summary counts the started ones.
        let relations_unknown = relations_unknown_ids(&requested, &snaps);

        // Delegate the start subset through the unchanged single-task path.
        // A per-task failure becomes an `error` disposition rather than
        // failing the batch — earlier tasks may already have started.
        let mut rows: Vec<Value> = Vec::new();
        let mut started_ids: Vec<String> = Vec::new();
        for (id, disposition) in &classified {
            let title = titles.get(id).cloned().unwrap_or_default();
            let mut row = json!({ "taskNoteId": id, "title": title });
            let obj = row.as_object_mut().unwrap();
            if relations_unknown.contains(id) {
                obj.insert("relationsUnknown".into(), json!(true));
            }
            match disposition {
                BatchDisposition::Start => {
                    // Per-entry overrides beat the top-level defaults, field
                    // by field.
                    let opts = overrides.get(id);
                    let single = intent_core::AgentDelegateInput {
                        task_note_id: Some(NoteId::from(id.as_str())),
                        specialist: opts
                            .and_then(|o| o.specialist.clone())
                            .or_else(|| input.specialist.clone()),
                        model: opts
                            .and_then(|o| o.model.clone())
                            .or_else(|| input.model.clone()),
                        provider: opts
                            .and_then(|o| o.provider.clone())
                            .or_else(|| input.provider.clone()),
                        reasoning_effort: opts
                            .and_then(|o| o.reasoning_effort.clone())
                            .or_else(|| input.reasoning_effort.clone()),
                        behavior_prompt: input.behavior_prompt.clone(),
                        wait_mode: input.wait_mode.clone(),
                        skip_auto_commit: input.skip_auto_commit,
                        isolation: input.isolation.clone(),
                        ..Default::default()
                    };
                    match Box::pin(self.agent_delegate_op(
                        workspace_id.clone(),
                        single,
                        parent_agent_id.clone(),
                    ))
                    .await
                    {
                        Ok(res) => {
                            obj.insert("disposition".into(), json!("started"));
                            obj.insert("agentId".into(), res["agentId"].clone());
                            obj.insert("agentName".into(), res["name"].clone());
                            started_ids.push(id.clone());
                        }
                        Err(e) => {
                            obj.insert("disposition".into(), json!("error"));
                            obj.insert("reason".into(), json!(e.to_string()));
                        }
                    }
                }
                BatchDisposition::HeldOnDeps {
                    unmet,
                    decision_needed,
                } => {
                    obj.insert("disposition".into(), json!("held:blocked-on-deps"));
                    obj.insert("unmetDependsOn".into(), json!(unmet));
                    let mut reason =
                        format!("waiting on incomplete dependencies: {}", unmet.join(", "));
                    if !decision_needed.is_empty() {
                        obj.insert("decisionNeeded".into(), json!(decision_needed));
                        let _ = write!(reason, "; dependencies {} are cancelled or missing and will never complete — decision needed",
                            decision_needed.join(", "));
                    }
                    obj.insert("reason".into(), json!(reason));
                }
                BatchDisposition::HeldOnConflict { conflicts_with } => {
                    obj.insert("disposition".into(), json!("held:conflict"));
                    obj.insert("conflictsWith".into(), json!(conflicts_with));
                    obj.insert(
                        "reason".into(),
                        json!(format!(
                            "conflictsWith intersects the running/starting set ({}); delegate it individually to force it",
                            conflicts_with.join(", ")
                        )),
                    );
                }
                BatchDisposition::SkippedAlreadyRunning {
                    agent_id,
                    agent_name,
                } => {
                    obj.insert("disposition".into(), json!("skipped"));
                    obj.insert("agentId".into(), json!(agent_id));
                    obj.insert("agentName".into(), json!(agent_name));
                    obj.insert(
                        "reason".into(),
                        json!(format!(
                            "already being worked by agent {agent_id} (\"{agent_name}\")"
                        )),
                    );
                }
                BatchDisposition::SkippedComplete => {
                    obj.insert("disposition".into(), json!("skipped"));
                    obj.insert("reason".into(), json!("task is complete"));
                }
                BatchDisposition::SkippedCancelled => {
                    obj.insert("disposition".into(), json!("skipped"));
                    obj.insert("reason".into(), json!("task is cancelled"));
                }
            }
            rows.push(row);
        }

        // Project the unlock plan from what ACTUALLY started (an errored
        // start never settles) plus every live-agent task in the workspace —
        // requested or not — since any of those settling can release a hold.
        let unlocked = project_unlock_plan(&classified, &snaps, &started_ids);

        let held_count = rows
            .iter()
            .filter(|r| {
                r["disposition"]
                    .as_str()
                    .is_some_and(|d| d.starts_with("held:"))
            })
            .count();
        let mut unlock_message = if unlocked.is_empty() {
            if held_count == 0 {
                "Nothing is held; no re-call needed beyond the normal completion wakes.".to_string()
            } else {
                "Held tasks are not unlocked by the started/running set settling alone (dependencies outside it that are incomplete, cancelled, or missing — possibly needing a decision — or conflicts with tasks that are not settling). Resolve those, then re-call agent.delegate.".to_string()
            }
        } else {
            format!(
                "When the started/running tasks settle, {} become startable — re-call agent.delegate then with the same list or a subset (classification is recomputed each call).",
                unlocked.join(", ")
            )
        };
        // Effort-weighted critical-path estimate (response text only, no wake
        // changes): surfaced only when at least one requested chain carries a
        // parseable estimate — pure-defaults graphs stay silent, and the
        // number reflects only estimated chains, so it can understate when
        // an unestimated chain is longer.
        let critical_path_minutes = batch::serial_remaining_minutes(&requested, &snaps);
        if let Some(minutes) = critical_path_minutes {
            let _ = write!(
                unlock_message,
                " ~{minutes} min of serial work remains on the critical path."
            );
        }
        // Count started relation-less tasks in the human-readable summary
        // (annotation only — nothing about the start decision changed).
        let started_unknown = started_ids
            .iter()
            .filter(|id| relations_unknown.contains(*id))
            .count();
        if started_unknown > 0 {
            let _ = write!(unlock_message, " {started_unknown} of {} started tasks carry no relations — the graph does not cover them.",
                started_ids.len());
        }

        let mut unlock_plan = json!({
            "unlockedBySettlement": unlocked,
            "message": unlock_message,
        });
        if let Some(minutes) = critical_path_minutes {
            unlock_plan
                .as_object_mut()
                .unwrap()
                .insert("criticalPathMinutes".into(), json!(minutes));
        }
        // Top-level disposition summary (monorepo#3334): a lazy `.ok` read
        // must still surface "started nothing" without parsing the rows.
        let count_disposition = |d: &str| {
            rows.iter()
                .filter(|r| r["disposition"].as_str() == Some(d))
                .count()
        };
        let held_deps = count_disposition("held:blocked-on-deps");
        let held_conflict = count_disposition("held:conflict");
        let skipped_count = count_disposition("skipped");
        let error_count = count_disposition("error");
        let started_count = started_ids.len();
        let mut result = json!({
            "ok": true,
            "tasks": rows,
            "startedTaskIds": started_ids,
            "summary": {
                "started": started_count,
                "held": held_count,
                "skipped": skipped_count,
                "errors": error_count,
            },
            "unlockPlan": unlock_plan,
        });
        if started_count == 0 {
            // Prominent top-level warning: a zero-started batch is the
            // silent-stall footgun (monorepo#3334) — name the hold reasons so
            // even a summary read explains what happened and what to do.
            let mut reasons: Vec<String> = Vec::new();
            if held_deps > 0 {
                reasons.push(format!("{held_deps} held on unmet dependencies"));
            }
            if held_conflict > 0 {
                reasons.push(format!("{held_conflict} held on conflicts"));
            }
            if skipped_count > 0 {
                reasons.push(format!(
                    "{skipped_count} skipped (already running or complete/cancelled)"
                ));
            }
            if error_count > 0 {
                reasons.push(format!("{error_count} failed to start"));
            }
            let breakdown = if reasons.is_empty() {
                "nothing was startable".to_string()
            } else {
                reasons.join(", ")
            };
            let warning = format!(
                "NO TASKS STARTED — {breakdown}. Nothing starts on its own and no completion wake will arrive from this call; resolve the holds, then re-call agent.delegate."
            );
            result
                .as_object_mut()
                .unwrap()
                .insert("warning".into(), json!(warning));
            // A zero-started `after_all` batch owes the caller a future
            // settlement wake it will never get: no child enrolled, so no
            // group formed (or extended). Unless an open group from earlier
            // delegations still guarantees a wake, deliver an immediate
            // advisory wake so the silence cannot become a permanent stall.
            // Best-effort: an advisory delivery failure never fails the call.
            if input.wait_mode.as_deref() == Some(WAIT_MODE_AFTER_ALL) {
                if let Some(parent) = &parent_agent_id {
                    if !self.has_open_delegation_group(parent) {
                        let parent_home_ws = self
                            .store
                            .get_agent_session(parent)
                            .await
                            .map_or_else(|_| workspace_id.clone(), |s| s.workspace_id);
                        let advisory = format!(
                            "Advisory: your batch agent.delegate (waitMode: \"after_all\") started ZERO tasks — {breakdown}. No delegation group was formed, so NO settlement wake will ever arrive from that call. Re-call agent.delegate once the holds clear, or delegate a held task individually to force it."
                        );
                        if let Err(e) = self
                            .deliver_wake_message(&parent_home_ws, parent, &advisory, None)
                            .await
                        {
                            tracing::warn!(
                                "zero-started batch delegate advisory wake failed for {}: {e}",
                                parent.0
                            );
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Background half of the delegate CoW-isolation path (monorepo#871): run
    /// [`provision_sandbox`](crate::sandbox_ops::provision_sandbox) and settle
    /// the outcome onto the child's session. Success persists the sandbox
    /// fields and emits `sandbox:cow:created` (payload unchanged, §5.5);
    /// `Unsupported`/failure falls back to shared mode exactly as before —
    /// debug/warn log only, the session keeps no sandbox fields and the child
    /// spawns in the shared checkout.
    async fn provision_delegate_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        workspaces_root: std::path::PathBuf,
    ) {
        use crate::sandbox_ops::{provision_sandbox, ProvisionConfig, ProvisionOutcome};
        let config = ProvisionConfig { workspaces_root };
        match provision_sandbox(&self.store, workspace_id, agent_id, &config).await {
            Ok(ProvisionOutcome::Supported {
                path,
                branch,
                base_commit_sha,
                snapshot_commit_sha,
            }) => {
                self.settle_provisioned_sandbox(
                    workspace_id,
                    agent_id,
                    path,
                    branch,
                    base_commit_sha,
                    snapshot_commit_sha,
                )
                .await;
            }
            Ok(ProvisionOutcome::Unsupported) => {
                // Fallback to shared mode (no action needed, session stays without sandbox fields)
                tracing::debug!(
                    workspace = %workspace_id,
                    agent = %agent_id,
                    "CoW not supported; fallback to shared mode"
                );
            }
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id,
                    agent = %agent_id,
                    error = %e,
                    "Sandbox provisioning failed; fallback to shared mode"
                );
            }
        }
    }

    /// Settle a successfully provisioned sandbox onto the child's session:
    /// persist the sandbox fields and emit `sandbox:cow:created`. Because the
    /// clone runs off the delegate critical path (monorepo#871),
    /// `agent.delete` can race it — if the session is gone or soft-deleted
    /// by settlement time, discard the just-provisioned sandbox (directory +
    /// store record) instead of stranding a multi-GB clone on disk
    /// (`gc_orphaned_sandboxes` has no runtime caller).
    async fn settle_provisioned_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        path: std::path::PathBuf,
        branch: String,
        base_commit_sha: String,
        snapshot_commit_sha: Option<String>,
    ) {
        let session = match self.store.get_agent_session(agent_id).await {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id,
                    agent = %agent_id,
                    error = %e,
                    "agent session lookup failed after sandbox provisioning"
                );
                None
            }
        };
        match session {
            Some(mut session) if session.status != AgentStatus::Deleted => {
                // Update the session with sandbox metadata
                session.sandbox_id = Some(format!(
                    "sandbox-{}-{}",
                    workspace_id.as_str(),
                    agent_id.as_str()
                ));
                session.sandbox_path = Some(path.to_string_lossy().to_string());
                session.sandbox_branch = Some(branch.clone());
                let _ = self
                    .store
                    .update_agent_session(workspace_id, &session)
                    .await;

                // Emit sandbox:cow:created event
                crate::publish_event(
                    self.event_bus.as_ref(),
                    intent_store::NewEvent {
                        workspace_id: workspace_id.clone(),
                        timestamp: crate::now_iso(),
                        event_type: "sandbox:cow:created".to_string(),
                        actor: crate::system_actor(),
                        session_id: Some(agent_id.0.clone()),
                        correlation_id: None,
                        parent_event_id: None,
                        metadata: None,
                        data: json!({
                            "workspaceId": workspace_id.as_str(),
                            "agentId": agent_id.as_str(),
                            "sandboxPath": path.to_string_lossy(),
                            "branch": branch,
                            "baseCommitSha": base_commit_sha,
                            "snapshotCommitSha": snapshot_commit_sha,
                        }),
                    },
                )
                .await;
            }
            _ => {
                tracing::warn!(
                    workspace = %workspace_id,
                    agent = %agent_id,
                    sandbox_path = %path.display(),
                    "agent session missing or deleted after sandbox provisioning (agent.delete raced the clone); discarding the sandbox"
                );
                // Remove the directory via the in-hand path: a hard
                // `agent.delete` already cascaded the sandbox row away
                // (FK ON DELETE CASCADE), so a record lookup can't be
                // relied on for the path. The directory is a full CoW
                // checkout (same size class as the clone), so removal runs
                // on the blocking pool (monorepo#954).
                if path.exists() {
                    let dir = path.clone();
                    let workspace_id = workspace_id.clone();
                    let agent_id = agent_id.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        if let Err(e) = std::fs::remove_dir_all(&dir) {
                            tracing::warn!(
                                workspace = %workspace_id,
                                agent = %agent_id,
                                error = %e,
                                "failed to remove orphaned sandbox directory after agent deletion"
                            );
                        }
                    })
                    .await;
                }
                // Best-effort: drop the record too (still present on the
                // soft-delete path; already gone after a hard delete).
                if let Err(e) = self.store.delete_sandbox(workspace_id, agent_id).await {
                    tracing::warn!(
                        workspace = %workspace_id,
                        agent = %agent_id,
                        error = %e,
                        "failed to delete orphaned sandbox record after agent deletion"
                    );
                }
            }
        }
    }

    /// Auto-subscribe `parent_agent_id` to `child_agent_id`'s completion
    /// (AS-5, the MCP `create_agent` front door): register an ungrouped
    /// watch, mirroring the immediate-mode branch of `agent_delegate_op`
    /// above — including the deleted-parent guard (TS `selectIsAgentDeleted`).
    /// Idempotent: reuses an existing watch when one already exists.
    pub(crate) async fn agent_watch_completion_op(
        &self,
        workspace_id: WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> Result<Value> {
        // monorepo#568: fail closed on a nonexistent CHILD before any watch
        // registration — a parent→child watch on an id that does not exist
        // never fires, leaving the parent in a phantom "waiting" state
        // (mirrors the SUB-1 sender-watch guard below).
        let child_session = self.require_agent_session(&child_agent_id).await?;
        let parent_session = self.store.get_agent_session(&parent_agent_id).await.ok();
        let parent_deleted = parent_session
            .as_ref()
            .is_some_and(|s| s.status == AgentStatus::Deleted);
        if parent_deleted {
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        let parent_name = parent_session.as_ref().map(|s| s.name.clone());
        // Anchor the watch in the parent's HOME workspace (falls back to the
        // call's workspace when the parent session lookup failed) and the
        // child's own workspace (already resolved by the fail-closed guard
        // above) — same-workspace pairs behave exactly as before; a chief
        // parent registers cross-workspace. `resolved_home` is Some only when
        // read from a real session row: the reuse path uses it to correct a
        // watch whose anchor was registered from the fallback.
        let resolved_home = parent_session.as_ref().map(|s| s.workspace_id.clone());
        let parent_home_ws = resolved_home
            .clone()
            .unwrap_or_else(|| workspace_id.clone());
        let child_ws = child_session.workspace_id;
        // Dedupe: reuse an existing ungrouped watch if present, otherwise create new.
        let id = match self.find_and_refresh_ungrouped_watch(
            &parent_agent_id,
            &child_agent_id,
            parent_name.clone(),
            resolved_home.as_ref(),
        ) {
            Some(existing) => existing,
            None => self.register_completion_watch(
                &parent_home_ws,
                &child_ws,
                parent_agent_id.clone(),
                parent_name.unwrap_or_default(),
                child_agent_id,
                None,
            )?,
        };
        self.publish_subscriptions_changed(&parent_home_ws, &parent_agent_id)
            .await;
        Ok(json!({ "ok": true, "subscriptionId": id }))
    }

    /// Conditionally auto-subscribe a coordination-message SENDER to the
    /// target's completion (SUB-1, the TS
    /// `maybeSubscribeCallerToAgentCompletionForCoordinationMessage`):
    /// register a caller→target watch UNLESS the caller is a
    /// delegated background task session — those often send sibling
    /// coordination messages, and passively subscribing them creates noisy
    /// wakeup cards unrelated to their own task — or the caller is a child
    /// of the target (watches are auto-registered parent→child only), or
    /// the TARGET is an independent top-level foreground agent (not a
    /// child, not background): messaging a co-equal peer must not passively
    /// subscribe the sender to its completion — watch peers explicitly with
    /// `agent.watch`.
    /// Idempotent: reuses an existing watch when one already exists.
    pub(crate) async fn agent_watch_completion_for_sender_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> Result<Value> {
        // monorepo#564: fail closed on a nonexistent TARGET before any watch
        // registration — a caller→target watch on an id that does not exist
        // never fires, leaving the sender in a phantom "waiting" state.
        let target_session = self.require_agent_session(&target_agent_id).await?;
        let caller_session = self.store.get_agent_session(&caller_agent_id).await.ok();
        let skip = caller_session
            .as_ref()
            .is_some_and(is_delegated_background_task_session);
        if skip {
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        // SUB-1 child→parent suppression: the auto-watch is one-directional
        // (parent→child only). A child sending a coordination message to its
        // own parent must never be subscribed to the parent's completion —
        // otherwise the child is woken whenever the parent goes idle. Child
        // linkage is read from the caller session's `parent_agent_id`,
        // falling back to the metadata `createdByAgentId` the create/delegate
        // writers populate.
        let is_child_of_target = caller_session.as_ref().is_some_and(|s| {
            s.parent_agent_id.as_ref() == Some(&target_agent_id)
                || s.metadata
                    .as_ref()
                    .and_then(|m| m.get("createdByAgentId"))
                    .and_then(Value::as_str)
                    == Some(target_agent_id.0.as_str())
        });
        if is_child_of_target {
            tracing::debug!(
                caller = %caller_agent_id.0,
                target = %target_agent_id.0,
                "skipping SUB-1 auto-watch — caller is a child of the target"
            );
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        // SUB-1 delegation-group conflict suppression: skip ungrouped watch
        // registration when the (caller, target) pair is already a member of
        // an undelivered after_all delegation group (mirrors the existing
        // child_in_undelivered_group suppression used for reportToParent wakes).
        // Prevents duplicate wakes when a coordinator sends coordination messages
        // (sendToTask) to children that are already covered by a grouped watch.
        if self.child_in_undelivered_group(&caller_agent_id, &target_agent_id) {
            tracing::debug!(
                caller = %caller_agent_id.0,
                target = %target_agent_id.0,
                "skipping SUB-1 auto-watch — target already in undelivered after_all group"
            );
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        // SUB-1 independent-peer suppression: a top-level
        // FOREGROUND target is a co-equal peer, not a worker — messaging it
        // must not passively subscribe the sender to its completion (peers
        // are watched explicitly with `agent.watch`). The auto-watch is
        // armed only for targets that are delegated/created children (parent
        // linkage, `createdByAgentId`, or depth >= 1) or background agents
        // (the send-and-await-result worker shape the SUB-1 watch exists
        // for). The bindings only attach the "You will be notified"
        // notification when a subscription id comes back, so this skip also
        // removes that text for depth-0 foreground targets.
        let target_is_child = target_session.parent_agent_id.is_some()
            || target_session.delegation_depth.unwrap_or(0) >= 1
            || target_session
                .metadata
                .as_ref()
                .and_then(|m| m.get("createdByAgentId"))
                .and_then(Value::as_str)
                .is_some();
        let target_is_background = target_session.is_background
            || target_session
                .metadata
                .as_ref()
                .and_then(|m| m.get("isBackground"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
        if !target_is_child && !target_is_background {
            tracing::debug!(
                caller = %caller_agent_id.0,
                target = %target_agent_id.0,
                "skipping SUB-1 auto-watch — target is an independent top-level foreground agent"
            );
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        let caller_name = caller_session.as_ref().map(|s| s.name.clone());
        // Anchor the watch in the caller's HOME workspace (falls back to the
        // call's workspace when the session lookup fails) and the target's own
        // workspace (already resolved by the fail-closed guard above),
        // mirroring `agent_watch_completion_op` — including the
        // `resolved_home` anchor-correction on the reuse path.
        let resolved_home = caller_session.as_ref().map(|s| s.workspace_id.clone());
        let caller_home_ws = resolved_home
            .clone()
            .unwrap_or_else(|| workspace_id.clone());
        let target_ws = target_session.workspace_id;
        // Dedupe: reuse an existing ungrouped watch if present, otherwise create new.
        let id = match self.find_and_refresh_ungrouped_watch(
            &caller_agent_id,
            &target_agent_id,
            caller_name.clone(),
            resolved_home.as_ref(),
        ) {
            Some(existing) => existing,
            None => self.register_completion_watch(
                &caller_home_ws,
                &target_ws,
                caller_agent_id.clone(),
                caller_name.unwrap_or_default(),
                target_agent_id,
                None,
            )?,
        };
        self.publish_subscriptions_changed(&caller_home_ws, &caller_agent_id)
            .await;
        Ok(json!({ "ok": true, "subscriptionId": id }))
    }

    /// The explicit-registration idle-target guard (monorepo#2972): a
    /// `RuntimeIdle` target with NO waiting reason — nothing pending that
    /// could ever produce a "next completion" — is rejected, because
    /// accepting it either fires an instant synthetic wake replaying the
    /// stale report (settled shape) or leaves a permanently-armed dead
    /// watch (unsettled shape). Waiting reasons that keep the target
    /// watchable: a ready-to-send queue entry or a busy worker (about to
    /// run), an unresolved attention request (runs when the user answers),
    /// pending structured questions (same parked-on-user-input shape),
    /// live outgoing completion watches (runs when a watched target
    /// settles), live event subscriptions (matching events wake it — same
    /// accept-and-defer shape as hooks), an interrupted row (the resume
    /// sweep re-runs it), active hooks, or active PR monitors (both wake
    /// it on fire — the monorepo#2532 Gap B accept-and-defer shape).
    /// Applied ONLY by the explicit registration ops (`agent.watch` /
    /// `app.agents.waitFor`) and only AFTER the `check_watch_scope` gate:
    /// out-of-scope targets must keep failing on scope alone, so the guard
    /// cannot leak a foreign agent's idle/nothing-pending state.
    /// Auto-subscribe paths pair the watch with a message that wakes the
    /// target, and boot rehydration must keep delivering missed wakes.
    /// Store probes fail open (accept): a false accept only reproduces the
    /// pre-guard behavior, a false reject would block a legitimate watch.
    async fn check_idle_target_watchable(&self, target_session: &AgentSession) -> Result<()> {
        if !matches!(target_session.status, AgentStatus::RuntimeIdle) {
            return Ok(());
        }
        let target_id = &target_session.id;
        if self.has_ready_to_send(target_id)
            || self.agent_is_busy(target_id.clone())
            || target_session.attention_request_kind.is_some()
            || self.agent_is_waiting_on_agents(target_id)
            || !self
                .list_event_subscriptions_for_agent(target_id)
                .is_empty()
            || self.pending_question_count(target_id).await > 0
        {
            return Ok(());
        }
        match self.store.get_interrupted_agent(target_id).await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    "idle-target watch guard: interrupted_agent check failed for {}: {e}",
                    target_id.0
                );
                return Ok(());
            }
        }
        match self.store.count_active_hooks_by_agent(target_id).await {
            Ok(0) => {}
            Ok(_) => return Ok(()),
            Err(e) => {
                tracing::warn!(
                    "idle-target watch guard: active-hook count failed for {}: {e}",
                    target_id.0
                );
                return Ok(());
            }
        }
        if !self
            .active_pr_monitors_for_agent(target_id)
            .await
            .is_empty()
        {
            return Ok(());
        }
        Err(Error::InvalidParams(format!(
            "agent {} is idle with nothing pending (no active hooks, no PR monitors, \
             no event subscriptions, no queued messages, no outgoing waits, no \
             unresolved attention request or pending questions, no interrupted turn) — \
             it has no future completion to watch. Wake it instead: agent.send \
             delivers a message and auto-arms a completion watch on the sender, or \
             agent.wakeOrCreate resumes its task",
            target_id.0
        )))
    }

    /// The registration-time mutual-wait guard: rejects an explicit watch
    /// registration when the target already holds a live completion watch —
    /// grouped or ungrouped — on the caller, i.e. the target is itself
    /// waiting on the caller to settle. Accepting the registration would arm
    /// an A⇄B pair where each side's settlement is deferred by the other's
    /// watch; the settlement-time `mutual_idle` break in
    /// [`Services::classify_agent_waiting`] eventually declassifies such a
    /// deadlocked 2-cycle, but only once both sides sit idle — refusing to
    /// create the pair up front is strictly better. "Live" means
    /// non-`report_delivered` ([`Services::waiting_watches_for_parent`]): a
    /// `report_delivered` reverse watch has already delivered its
    /// report-time wake and is not something the target is waiting FOR, so
    /// it does not trigger the rejection. Enforced ONLY by the explicit
    /// registration ops (`agent.watch` / `app.agents.waitFor`): the auto-arm
    /// paths (delegate, the send SUB-1 auto-watch, `wakeOrCreate`, startup
    /// rehydration) pair the watch with a message/wake that settles one side
    /// or must keep rehydrating persisted pairs, and stay covered by the
    /// settlement-time backstop. Direct pair check only — deeper cycles
    /// (A→B→C→A) are intentionally not detected here. Best-effort under
    /// concurrency (TOCTOU): two agents racing explicit registrations on
    /// each other can both pass this check before either watch lands and
    /// still form the mutual pair — that race is covered by the same
    /// settlement-time `mutual_idle` backstop.
    fn check_no_reverse_watch(
        &self,
        caller_agent_id: &AgentId,
        target_agent_id: &AgentId,
    ) -> Result<()> {
        if self
            .waiting_watches_for_parent(target_agent_id)
            .iter()
            .any(|w| &w.child_agent_id == caller_agent_id)
        {
            return Err(Error::InvalidParams(format!(
                "agent {} is already waiting on you ({}): it holds a live completion \
                 watch on your completion, so watching it back would create a mutual \
                 wait where each agent waits for the other to settle. Finish your \
                 work and settle instead — agent.reportToParent (if it delegated \
                 you) or agent.send delivers your result and wakes it",
                target_agent_id.0, caller_agent_id.0
            )));
        }
        Ok(())
    }

    /// `agent.watch` (monorepo#1229): explicit caller→target subscription to
    /// the target's harness-curated completion set — idle/completed, failed,
    /// deleted, blocker raised, discussion requested. Like the
    /// auto-registered delegation/SUB-1 watches, the attention fan-out in
    /// `agent_request_attention_op` wakes it (monorepo#3443 widened the
    /// fan-out to every active watch; the persisted `wake_on_attention` flag
    /// still records the explicit registration). The registration is durably
    /// persisted before returning. Fails closed on a nonexistent target and
    /// rejects self-watching; the shared `check_watch_scope` gate rejects
    /// cross-workspace targets for non-chief callers, the idle-target
    /// guard (monorepo#2972, [`Services::check_idle_target_watchable`])
    /// rejects a `RuntimeIdle` target with no waiting reason, and the
    /// mutual-wait guard ([`Services::check_no_reverse_watch`]) rejects a
    /// target that already holds a live watch on the caller.
    pub(crate) async fn agent_watch_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> Result<Value> {
        if caller_agent_id == target_agent_id {
            return Err(Error::InvalidParams("cannot watch yourself".to_string()));
        }
        let target_session = self.require_agent_session(&target_agent_id).await?;
        if target_session.status == AgentStatus::Deleted {
            return Err(Error::InvalidParams(format!(
                "unknown agent id: {}",
                target_agent_id.0
            )));
        }
        let caller_session = self.store.get_agent_session(&caller_agent_id).await.ok();
        let caller_name = caller_session.as_ref().map(|s| s.name.clone());
        // Anchor the watch in the caller's HOME workspace (falls back to the
        // call's workspace when the session lookup failed), mirroring
        // `agent_watch_completion_op`.
        let resolved_home = caller_session.as_ref().map(|s| s.workspace_id.clone());
        let caller_home_ws = resolved_home.unwrap_or_else(|| workspace_id.clone());
        // Scope gate BEFORE the idle-target guard: an out-of-scope target
        // must fail on scope alone, so the guard cannot leak a foreign
        // agent's idle/nothing-pending state. (`register_agent_watch_durable`
        // re-checks the same gate as the single-registration-path backstop.)
        crate::agent_subscriptions::check_watch_scope(
            &caller_home_ws,
            &target_session.workspace_id,
        )?;
        self.check_idle_target_watchable(&target_session).await?;
        // Mutual-wait guard: a target already waiting on the caller must not
        // be watched back — reject before any side-effectful registration.
        self.check_no_reverse_watch(&caller_agent_id, &target_agent_id)?;
        let target_ws = target_session.workspace_id;
        let id = self
            .register_agent_watch_durable(
                &caller_home_ws,
                &target_ws,
                caller_agent_id.clone(),
                caller_name.unwrap_or_default(),
                target_agent_id.clone(),
            )
            .await?;
        self.publish_subscriptions_changed(&caller_home_ws, &caller_agent_id)
            .await;
        // Close the registration-time race: a target that already settled
        // delivers its synthetic completion immediately (same reconciliation
        // path as `app.agents.waitFor` / startup rehydration). Registration
        // call site (monorepo#2532): a reported idle target still owning
        // active hooks/PR monitors defers instead of firing instantly.
        self.reconcile_watch_child_on_rehydration(
            &target_agent_id,
            &target_ws,
            crate::agent_subscriptions::WatchReconcileCallSite::Registration,
        )
        .await;
        Ok(json!({
            "ok": true,
            "subscriptionId": id,
            "agentId": target_agent_id.0,
        }))
    }

    /// `agent.unwatch` (monorepo#1229): remove one of the caller's own
    /// completion watches — addressed by `subscriptionId` or by the watched
    /// `agentId`. A watch owned by another agent is rejected with `-32602`
    /// (never removed); grouped watches are owned by delegation-group
    /// settlement and are rejected too. Idempotent on the agentId form: no
    /// matching watch returns `{ ok: true, removed: false }`.
    pub(crate) async fn agent_unwatch_op(
        &self,
        _workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        subscription_id: Option<String>,
        target_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let watches = self.list_watches_for_parent(&caller_agent_id);
        let watch = match (&subscription_id, &target_agent_id) {
            (Some(id), _) => {
                let Some(w) = watches.iter().find(|w| &w.id == id) else {
                    return Err(Error::InvalidParams(format!(
                        "unknown subscription id: {id} (not owned by caller)"
                    )));
                };
                Some(w)
            }
            (None, Some(target)) => watches
                .iter()
                .find(|w| &w.child_agent_id == target && w.group_id.is_none()),
            (None, None) => {
                return Err(Error::InvalidParams(
                    "subscriptionId or agentId is required".to_string(),
                ));
            }
        };
        let Some(watch) = watch else {
            return Ok(json!({ "ok": true, "removed": false }));
        };
        if watch.group_id.is_some() {
            return Err(Error::InvalidParams(
                "cannot unwatch a delegation-group watch; use \
                 agent.cancelSubscriptions with groupId instead"
                    .to_string(),
            ));
        }
        let removed = self.remove_watch(&watch.id);
        if removed {
            self.publish_subscriptions_changed(&watch.parent_workspace_id, &caller_agent_id)
                .await;
            // Agent-waiting deferral backstop (issue intent-hq/monorepo#1468):
            // the caller's own `agent:idle` may have been deferred (not fired,
            // watch armed) because it held this outgoing watch. Removing it
            // here is outside the wake path, so re-run the mutation-path
            // redelivery — a no-op unless the caller is idle with no remaining
            // waiting reason, in which case it synthesizes the caller's real
            // completion and settles its own deferred watchers.
            self.redeliver_completion_after_queue_mutation(&caller_agent_id)
                .await;
        }
        Ok(json!({ "ok": true, "removed": removed }))
    }

    /// `app.agents.waitFor`: register completion watches for the caller on a
    /// set of existing target agents — the subscription side of
    /// `agent.delegate` without creating children. Reuses the exact same
    /// registration/group helpers as the delegate call sites: `immediate`
    /// (default) registers an ungrouped watch per target; `after_all` enrolls
    /// every target in the caller's open delegation group anchored in the
    /// caller's home workspace (sealed on the caller's idle, one aggregated
    /// wake, restart-safe through the existing group persistence). Targets
    /// outside the caller's home workspace pass only for chief-workspace
    /// callers — enforced by the shared `check_watch_scope` gate, which runs
    /// for every target BEFORE any side-effectful registration so a rejection
    /// leaves no partial group or watches behind.
    ///
    /// Pair uniqueness: as an EXPLICIT registration path, a target the caller
    /// already watches (ungrouped or grouped) is rejected with
    /// `-32602` naming the target — run in the same up-front validation loop,
    /// so the rejection is side-effect free. (Auto-subscribe paths silently
    /// adopt the existing watch instead; see `register_completion_watch`.)
    /// The mutual-wait guard ([`Services::check_no_reverse_watch`]) runs in
    /// the same loop for both modes: a target already holding a live watch
    /// on the caller is rejected, naming the offending target.
    ///
    /// After registration every target is reconciled against current agent
    /// state (same [`Services::reconcile_watch_child_on_rehydration`] path the
    /// startup rehydration uses): a target that already settled — Completed /
    /// Error / genuinely idle with a completion report — delivers its
    /// synthetic completion immediately instead of leaving a watch armed for
    /// an event that fired long ago. This also closes the TOCTOU window where
    /// a target settles between the validation loop above and the watch
    /// registration (its live event would dispatch before the watch exists).
    /// Chief-only cross-workspace send behind the MCP `ws.app.agents.send`
    /// binding. The source anchor always comes from the caller's newest
    /// persisted conversation row. A model cannot supply or replace it.
    pub(crate) async fn app_agents_send_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
        message: String,
        priority: Option<String>,
    ) -> Result<Value> {
        if !workspace_id.is_chief() {
            return Err(Error::InvalidParams(
                "ws.app.* is only available in the Chief of Staff workspace".to_string(),
            ));
        }
        if caller_agent_id == target_agent_id {
            return Err(Error::InvalidParams(
                "Chief of Staff cannot send a message to itself".to_string(),
            ));
        }

        let caller = self
            .agent_get_op(caller_agent_id.clone(), Some(workspace_id.clone()))
            .await?;
        if caller.status == AgentStatus::Deleted
            || caller.retired_at.is_some()
            || !caller.workspace_id.is_chief()
        {
            return Err(Error::InvalidParams(format!(
                "caller agent {} is not an active Chief of Staff agent",
                caller_agent_id.0
            )));
        }
        let source_message_id = caller.last_message_id.ok_or_else(|| {
            Error::InvalidParams(
                "Chief conversation has no persisted source message to link".to_string(),
            )
        })?;

        let target = self.require_agent_session(&target_agent_id).await?;
        if target.status == AgentStatus::Deleted {
            return Err(Error::InvalidParams(format!(
                "agent {} is deleted",
                target_agent_id.0
            )));
        }
        if target.workspace_id.is_chief() {
            return Err(Error::InvalidParams(
                "Chief messages can only target agents outside the Chief workspace".to_string(),
            ));
        }

        let source_url = format!(
            "intent://local/{}/agent/{}/message/{}",
            workspace_id.0, caller_agent_id.0, source_message_id
        );
        let metadata = json!({
            "type": "chief_message",
            "fromAgentId": caller_agent_id.0,
            "fromAgentName": "Chief of Staff",
            "fromWorkspaceId": workspace_id.0,
            "sourceMessageId": source_message_id,
            "sourceUrl": source_url,
        });
        let outcome = WorkspaceApi::agent_send_message(
            self,
            target.workspace_id.clone(),
            target_agent_id.clone(),
            message,
            None,
            None,
            None,
            priority,
            None,
            None,
            None,
            Some(metadata),
            intent_core::MessageOrigin::Automatic,
        )
        .await?;

        let mut result = json!({
            "ok": true,
            "agentId": target_agent_id.0,
            "agentName": target.name,
            "workspaceId": target.workspace_id.0,
            "sourceMessageId": source_message_id,
            "sourceUrl": source_url,
        });
        if let (Some(result), Some(outcome)) = (result.as_object_mut(), outcome.as_object()) {
            result.extend(outcome.clone());
        }
        Ok(result)
    }

    /// Chief-only completion-bound ask. The message is an ordinary Chief send;
    /// after delivery, one durable ungrouped completion watch is armed and the
    /// target is reconciled against its post-send state. The explicit
    /// idle-target rejection is intentionally not used: this operation pairs
    /// the watch with a send that wakes a previously settled target.
    pub(crate) async fn app_agents_ask_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
        message: String,
        priority: Option<String>,
    ) -> Result<Value> {
        if !workspace_id.is_chief() {
            return Err(Error::InvalidParams(
                "ws.app.* is only available in the Chief of Staff workspace".to_string(),
            ));
        }
        let caller = self.require_agent_session(&caller_agent_id).await?;
        let sent = self
            .app_agents_send_op(
                workspace_id.clone(),
                caller_agent_id.clone(),
                target_agent_id.clone(),
                message,
                priority,
            )
            .await?;
        let target_workspace = sent
            .get("workspaceId")
            .and_then(Value::as_str)
            .map(WorkspaceId::from)
            .ok_or_else(|| Error::Internal("Chief send omitted target workspace".to_string()))?;
        let subscription_id = self
            .register_completion_watch_strict_durable(
                &workspace_id,
                &target_workspace,
                caller_agent_id.clone(),
                caller.name,
                target_agent_id.clone(),
            )
            .await?;
        self.publish_subscriptions_changed(&workspace_id, &caller_agent_id)
            .await;
        self.reconcile_watch_child_on_rehydration(
            &target_agent_id,
            &target_workspace,
            crate::agent_subscriptions::WatchReconcileCallSite::Registration,
        )
        .await;
        let target_name = sent
            .get("agentName")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Internal("Chief send omitted target name".to_string()))?
            .to_string();
        Ok(json!({
            "ok": true,
            "send": sent,
            "watch": {
                "ok": true,
                "waitMode": "immediate",
                "results": [{
                    "agentId": target_agent_id.0,
                    "agentName": target_name,
                    "workspaceId": target_workspace.0,
                    "subscriptionId": subscription_id,
                    "groupId": Value::Null,
                }],
            },
        }))
    }

    pub(crate) async fn app_agents_wait_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        agent_ids: Vec<String>,
        wait_mode: Option<String>,
    ) -> Result<Value> {
        let wait_mode = match wait_mode.as_deref() {
            None | Some("immediate") => "immediate",
            Some(WAIT_MODE_AFTER_ALL) => WAIT_MODE_AFTER_ALL,
            Some(other) => {
                return Err(Error::InvalidParams(format!(
                    "invalid waitMode `{other}` (expected \"immediate\" or \"after_all\")"
                )))
            }
        };
        // Dedupe target ids preserving order; drop empty entries.
        let mut seen: HashSet<String> = HashSet::new();
        let targets: Vec<AgentId> = agent_ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .filter(|id| seen.insert((*id).to_string()))
            .map(AgentId::from)
            .collect();
        if targets.is_empty() {
            return Err(Error::InvalidParams(
                "agentIds must contain at least one agent id".to_string(),
            ));
        }
        if targets.iter().any(|t| t == &caller_agent_id) {
            return Err(Error::InvalidParams(
                "agentIds must not include the caller agent itself".to_string(),
            ));
        }
        // Resolve the caller's home workspace (where wakes are delivered),
        // mirroring `agent_watch_completion_op`: fall back to the call's
        // workspace when the session lookup fails; a deleted caller can never
        // receive a wake, so registering for it is refused.
        let caller_session = self.store.get_agent_session(&caller_agent_id).await.ok();
        if caller_session
            .as_ref()
            .is_some_and(|s| s.status == AgentStatus::Deleted)
        {
            return Err(Error::InvalidParams(format!(
                "caller agent {caller_agent_id} is deleted"
            )));
        }
        let caller_name = caller_session.as_ref().map(|s| s.name.clone());
        let resolved_home = caller_session.as_ref().map(|s| s.workspace_id.clone());
        let caller_home_ws = resolved_home
            .clone()
            .unwrap_or_else(|| workspace_id.clone());
        // Validate every target and run the scope gate BEFORE any
        // side-effectful registration (mirrors the delegate path's up-front
        // gate): a rejection is side-effect free — no group, no watches.
        let mut resolved: Vec<(AgentId, String, WorkspaceId)> = Vec::with_capacity(targets.len());
        for target in targets {
            // Only NotFound maps to a client-facing InvalidParams; internal
            // store failures propagate unchanged.
            let session = self
                .store
                .get_agent_session(&target)
                .await
                .map_err(|e| match e {
                    Error::NotFound(_) => {
                        Error::InvalidParams(format!("unknown agent id: {}", target.0))
                    }
                    other => other,
                })?;
            if session.status == AgentStatus::Deleted {
                return Err(Error::InvalidParams(format!(
                    "agent {} is deleted",
                    target.0
                )));
            }
            // Scope gate FIRST: an out-of-scope target must fail on scope
            // alone, so the idle-target guard below cannot leak a foreign
            // agent's idle/nothing-pending state.
            crate::agent_subscriptions::check_watch_scope(&caller_home_ws, &session.workspace_id)?;
            // Idle-target guard (monorepo#2972): same up-front validation
            // loop as the scope gate, so a rejection is side-effect free.
            self.check_idle_target_watchable(&session).await?;
            // Mutual-wait guard: a target already waiting on the caller must
            // not be waited on back — same up-front loop, so a rejection
            // leaves no group or watches behind.
            self.check_no_reverse_watch(&caller_agent_id, &target)?;
            // Pair uniqueness: an explicit registration on a child the caller
            // ALREADY watches (ungrouped or grouped) is rejected
            // up front — before any side-effectful registration — instead of
            // silently adopting the existing watch like the auto-subscribe
            // paths do, so a duplicate wait can never appear on the wire.
            if self.pair_watch_exists(&caller_agent_id, &target) {
                return Err(Error::InvalidParams(format!(
                    "already waiting on agent {}: a completion watch for this \
                     (caller, target) pair is already active — at most one \
                     active watch per pair (cancel it via \
                     agent.cancelSubscriptions to re-register)",
                    target.0
                )));
            }
            resolved.push((target, session.name, session.workspace_id));
        }
        let reconcile_targets: Vec<(AgentId, WorkspaceId)> = resolved
            .iter()
            .map(|(t, _, ws)| (t.clone(), ws.clone()))
            .collect();
        let mut results = Vec::with_capacity(resolved.len());
        if wait_mode == WAIT_MODE_AFTER_ALL {
            // Enroll every target in the caller's open after_all group and
            // register a grouped watch each (group_id = Some),
            // exactly like the delegate after_all branch (AS-4).
            let gid = self.get_or_create_delegation_group(&caller_home_ws, &caller_agent_id);
            for (target, target_name, target_ws) in resolved {
                self.enroll_child_in_group(&gid, &target);
                // Durable (awaited) persist: the reconciliation below may fire
                // and delete this watch immediately for an already-settled
                // target, and a spawned upsert racing that delete could leave
                // an orphan row that re-delivers after a restart.
                let sub_id = self
                    .register_completion_watch_durable(
                        &caller_home_ws,
                        &target_ws,
                        caller_agent_id.clone(),
                        caller_name.clone().unwrap_or_default(),
                        target.clone(),
                        Some(gid.clone()),
                    )
                    .await?;
                results.push(json!({
                    "agentId": target.0,
                    "agentName": target_name,
                    "workspaceId": target_ws.0,
                    "subscriptionId": sub_id,
                    "groupId": gid,
                }));
            }
        } else {
            // Immediate: one ungrouped watch per target, deduped against a
            // live one (same reuse path as `agent.watchCompletion`).
            for (target, target_name, target_ws) in resolved {
                let sub_id = match self.find_and_refresh_ungrouped_watch(
                    &caller_agent_id,
                    &target,
                    caller_name.clone(),
                    resolved_home.as_ref(),
                ) {
                    Some(existing) => existing,
                    // Durable (awaited) persist — same orphan-row rationale
                    // as the after_all branch above.
                    None => {
                        self.register_completion_watch_durable(
                            &caller_home_ws,
                            &target_ws,
                            caller_agent_id.clone(),
                            caller_name.clone().unwrap_or_default(),
                            target.clone(),
                            None,
                        )
                        .await?
                    }
                };
                results.push(json!({
                    "agentId": target.0,
                    "agentName": target_name,
                    "workspaceId": target_ws.0,
                    "subscriptionId": sub_id,
                    "groupId": Value::Null,
                }));
            }
        }
        self.publish_subscriptions_changed(&caller_home_ws, &caller_agent_id)
            .await;
        // Reconcile already-settled targets NOW (immediate: fires the fresh
        // watch right away; after_all: records the completion in the
        // still-open group, which fires once it seals on the caller's idle).
        // Registration call site (monorepo#2532): a reported idle target
        // still owning active hooks/PR monitors defers instead.
        for (target, target_ws) in reconcile_targets {
            self.reconcile_watch_child_on_rehydration(
                &target,
                &target_ws,
                crate::agent_subscriptions::WatchReconcileCallSite::Registration,
            )
            .await;
        }
        Ok(json!({ "ok": true, "waitMode": wait_mode, "results": results }))
    }

    /// `agent.getSubscriptions`: live completion-watch payload for `agent_id`
    /// from the AS-2/AS-4 registry, in the TS camelCase wire shape with the
    /// `subscriptions`, `delegationGroups`, and `agentStatuses` fields.
    /// `awaitMode` maps the registry's `after_all` to TS's `"all"`;
    /// `agentStatuses` is best-effort, keyed off the persisted `AgentStatus` of
    /// the agents present in the payload. `eventSubscriptions` (additive,
    /// monorepo#947) lists the caller's live `event.subscribe` registrations
    /// so an agent can recover a lost `subscriptionId`.
    pub(crate) async fn agent_get_subscriptions_op(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        let watches = self.list_watches_for_parent(&agent_id);
        let groups = self.list_groups_for_parent(&agent_id);

        let event_types = [AGENT_IDLE, AGENT_FAILED, AGENT_DELETED];

        let mut present: Vec<AgentId> = vec![agent_id.clone()];
        let subscriptions: Vec<Value> = watches
            .iter()
            .map(|w| {
                if !present.contains(&w.child_agent_id) {
                    present.push(w.child_agent_id.clone());
                }
                let delegation_group = w.group_id.as_ref().and_then(|gid| {
                    groups.iter().find(|g| &g.group_id == gid).map(|g| {
                        json!({
                            "groupId": g.group_id,
                            "awaitMode": "all",
                            "expectedAgentIds": g.expected_agent_ids,
                        })
                    })
                });
                let description = describe_subscription(w, &event_types, delegation_group.as_ref());
                json!({
                    "id": w.id,
                    "agentId": w.parent_agent_id,
                    "agentName": w.parent_agent_name,
                    // The watch's own anchor (the parent's home workspace,
                    // where the wake is delivered) — identical to the call
                    // workspace for same-workspace watches, self-consistent
                    // for cross-workspace (chief) ones.
                    "workspaceId": w.parent_workspace_id,
                    "createdAt": w.created_at,
                    "actorIds": [w.child_agent_id],
                    "eventTypes": event_types,
                    "delegationGroup": delegation_group,
                    "description": description,
                })
            })
            .collect();

        let delegation_groups: Vec<Value> = groups
            .iter()
            .map(|g| {
                for id in &g.expected_agent_ids {
                    if !present.contains(id) {
                        present.push(id.clone());
                    }
                }
                json!({
                    "groupId": g.group_id,
                    "parentAgentId": g.parent_agent_id,
                    "awaitMode": "all",
                    "expectedAgentIds": g.expected_agent_ids,
                    "completedAgentIds": g.completed_agent_ids,
                    "deletedAgentIds": g.deleted_agent_ids,
                    "delivered": g.delivered,
                })
            })
            .collect();

        // Batched status projection (intent-hq/monorepo#3018): one `IN`-list
        // query for every present agent's status, instead of a per-agent
        // `get_agent_session` loop that hydrated each agent's full message
        // log just to read `status` — that made the dispatch duration scale
        // with the watched agents' transcript sizes (the monorepo#958
        // incident shape) and its statement count with the watch fan-out.
        let mut agent_statuses = serde_json::Map::new();
        if let Ok(statuses) = self.store.get_agent_statuses(&present).await {
            for (id, status) in statuses {
                if let Some(word) = agent_status_wire(status) {
                    agent_statuses.insert(id.0, json!(word));
                }
            }
        }

        let event_subscriptions: Vec<Value> = self
            .list_event_subscriptions_for_agent(&agent_id)
            .iter()
            .map(event_subscription_wire)
            .collect();

        Ok(json!({
            "subscriptions": subscriptions,
            "delegationGroups": delegation_groups,
            "agentStatuses": Value::Object(agent_statuses),
            "eventSubscriptions": event_subscriptions,
        }))
    }

    /// `agent.cancelSubscriptions`: with no scoping params, remove every
    /// completion watch registered by `agent_id`, drop any delegation groups
    /// it parents (persisted rows swept best-effort), and drop its event
    /// subscriptions (monorepo#937). Idempotent — always returns
    /// `{ "success": true }` (TS shape).
    ///
    /// Scoped cancel (additive, monorepo): an optional `subscriptionId`
    /// cancels exactly that completion watch, an optional `groupId` cancels
    /// that delegation group plus its grouped watches; each removal deletes
    /// the matching persisted `completion_watch` / `delegation_group` row(s)
    /// and publishes the same `agent:subscriptions-changed` snapshot event as
    /// the other watch-set mutation paths (§6.5), anchored in the parent's
    /// home workspace. Cancelling a GROUPED watch by `subscriptionId` also
    /// drops that child from its delegation group's expected set — group
    /// settlement is driven exclusively by the grouped watch, so leaving the
    /// child expected would stall the group (and the surviving siblings'
    /// aggregated wake) forever — and then attempts `try_fire_group`, since
    /// the shrunk group may now be sealed AND complete. The group-row delete
    /// is durable-before-observable (awaited before any in-memory removal; a
    /// failed delete errors the call with the registry untouched). An id not
    /// owned by `agent_id` is rejected with `-32602` BEFORE anything is
    /// removed (mirroring the unknown-id guards elsewhere in §5.5), so a
    /// combined call is all-or-nothing. Scoped cancel never touches event
    /// subscriptions.
    pub(crate) async fn agent_cancel_subscriptions_op(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: AgentId,
        subscription_id: Option<String>,
        group_id: Option<String>,
    ) -> Result<Value> {
        if subscription_id.is_none() && group_id.is_none() {
            // Snapshot the anchor workspaces BEFORE the sweep: dropping the
            // caller's last watch/group can demote its home workspace's
            // displayStatus, so recompute each distinct anchor afterwards
            // (transition-only, best-effort — a no-op recompute stays silent).
            let mut anchors: Vec<WorkspaceId> = Vec::new();
            for w in self.list_watches_for_parent(&agent_id) {
                if !anchors.contains(&w.parent_workspace_id) {
                    anchors.push(w.parent_workspace_id);
                }
            }
            for g in self.list_groups_for_parent(&agent_id) {
                if !anchors.contains(&g.workspace_id) {
                    anchors.push(g.workspace_id);
                }
            }
            self.remove_all_for_parent(&agent_id);
            self.remove_groups_for_parent(&agent_id);
            self.remove_event_subscriptions_for_agent(&agent_id).await;
            for anchor in &anchors {
                self.maybe_emit_display_status_changed(anchor).await;
                self.maybe_emit_waiting_changed(anchor).await;
            }
            // Agent-waiting deferral backstop (issue intent-hq/monorepo#1468):
            // dropping every outgoing watch may remove the caller's last
            // waiting reason, so re-run the mutation-path redelivery to settle
            // any watch on the caller whose `agent:idle` was deferred.
            self.redeliver_completion_after_queue_mutation(&agent_id)
                .await;
            return Ok(json!({ "success": true }));
        }

        // Resolve BOTH ids against the caller's own watches/groups before
        // removing anything, so an unknown id leaves the registry untouched.
        let watches = self.list_watches_for_parent(&agent_id);
        let target_watch =
            match &subscription_id {
                Some(sid) => Some(watches.iter().find(|w| &w.id == sid).cloned().ok_or_else(
                    || Error::InvalidParams(format!("unknown subscription id: {sid}")),
                )?),
                None => None,
            };
        let target_group = match &group_id {
            Some(gid) => Some(
                self.list_groups_for_parent(&agent_id)
                    .into_iter()
                    .find(|g| &g.group_id == gid)
                    .ok_or_else(|| {
                        Error::InvalidParams(format!("unknown delegation group id: {gid}"))
                    })?,
            ),
            None => None,
        };

        // DURABLE-BEFORE-OBSERVABLE (mirrors `take_group_if_ready`): commit
        // the persisted delegation_group delete BEFORE any in-memory removal.
        // If the delete fails, the call errors with the registry untouched —
        // no cancelled-in-memory group can rehydrate on restart. (A concurrent
        // `try_fire_group` racing this delete is benign: both deletes are
        // idempotent, and whichever removes the in-memory group first wins.)
        if let Some(group) = &target_group {
            self.store.delete_delegation_group(&group.group_id).await?;
        }

        // Parent home workspaces to publish `agent:subscriptions-changed` in
        // (deduped — a watch and its group share the same anchor). A grouped
        // watch cancelled by id must also stop gating its group's completion,
        // and the shrunk group may thereby become ready — fire it (skipped
        // when the group itself is being cancelled in the same call).
        let mut anchors: Vec<WorkspaceId> = Vec::new();
        let mut group_to_refire: Option<String> = None;
        if let Some(watch) = target_watch {
            self.remove_watch(&watch.id);
            if let Some(gid) = &watch.group_id {
                let cancelled_with_group =
                    target_group.as_ref().is_some_and(|g| &g.group_id == gid);
                if !cancelled_with_group && self.remove_child_from_group(gid, &watch.child_agent_id)
                {
                    group_to_refire = Some(gid.clone());
                }
            }
            anchors.push(watch.parent_workspace_id);
        }
        if let Some(group) = target_group {
            self.remove_group_with_watches(&agent_id, &group.group_id);
            if !anchors.contains(&group.workspace_id) {
                anchors.push(group.workspace_id);
            }
        }
        if let Some(gid) = group_to_refire {
            self.try_fire_group(&gid).await;
        }
        for anchor in &anchors {
            self.publish_subscriptions_changed(anchor, &agent_id).await;
        }
        // Agent-waiting deferral backstop (issue intent-hq/monorepo#1468):
        // cancelling a scoped outgoing watch may remove the caller's last
        // waiting reason, so re-run the mutation-path redelivery to settle any
        // watch on the caller whose `agent:idle` was deferred.
        self.redeliver_completion_after_queue_mutation(&agent_id)
            .await;
        Ok(json!({ "success": true }))
    }

    /// Build [`AgentSnapshot`] for `agent_id` — the cheap per-turn digest
    /// behind `ws.agent.snapshot()` and the turn-prompt injection line
    /// (`ws.agent.diagnostics` stays the deep-dive tool). O(this agent):
    /// every field reads a per-agent registry length or a bounded per-agent
    /// count statement — no workspace-wide scans, no transcript or blob
    /// hydration (`prs` adds the workspace row plus the workspace's
    /// registered git roots, both single bounded statements). `session` is
    /// the caller's already-fetched summary row so the op path stays at one
    /// session read.
    pub(crate) async fn build_agent_snapshot(
        &self,
        session: &AgentSession,
    ) -> Result<AgentSnapshot> {
        let agent_id = &session.id;
        // Count-only aggregate: `active_hooks_for_agent` would hydrate every
        // hook row the agent ever owned (code + lastState blobs included).
        let hooks = usize::try_from(
            self.store
                .count_active_hooks_by_agent(agent_id)
                .await
                .unwrap_or(0),
        )
        .expect("value fits in usize");
        let agent_watches = self.list_watches_for_parent(agent_id).len();
        // Length-only registry read: `queue_snapshot` materializes each
        // entry's wire JSON (image/file blocks included) just to be counted.
        let queued_messages = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .map_or(0, std::vec::Vec::len);
        let event_subscriptions = self.list_event_subscriptions_for_agent(agent_id).len();
        // One aggregate statement over `idx_agent_parent`, unscoped by
        // workspace so Chief cross-workspace delegates count too. Active is
        // the live runtime-turn subset (`is_active`); unsettled also includes
        // idle children waiting on hooks or other background work. The legacy
        // running preserves the existing in-flight-status count. Fails open.
        // intent-hq/monorepo#3906 (investigated): all three counters are
        // scoped to rows whose `parent_agent_id` IS this agent — watched
        // non-child peers (`agent.watch` targets) are NEVER included; they
        // surface only in `agent_watches` above.
        let child_counts = self
            .store
            .count_child_agents(agent_id)
            .await
            .unwrap_or_default();
        let active_sub_agents = usize::try_from(child_counts.active).expect("value fits in usize");
        let unsettled_sub_agents =
            usize::try_from(child_counts.unsettled).expect("value fits in usize");
        let running_sub_agents =
            usize::try_from(child_counts.running).expect("value fits in usize");
        let num_questions_asked = self.pending_question_count(agent_id).await;
        // Per-agent indexed read over this agent's monitor rows; labels only,
        // no snapshot hydration. Fails open to empty.
        let pr_monitors = self.active_pr_monitor_labels(agent_id).await;
        // Tracked open PRs from persisted columns only (workspace row +
        // registered git roots) — no forge calls, no per-PR statements.
        // Fails open to None.
        let prs = self.tracked_open_prs_grouped(&session.workspace_id).await;
        // Whole-second UTC timestamp — the snapshot line is injected into
        // every turn prompt, so sub-second precision only spends tokens.
        let time = {
            let iso = now_iso();
            match iso.split_once('.') {
                Some((head, _)) => format!("{head}Z"),
                None => iso,
            }
        };
        Ok(AgentSnapshot {
            time,
            hooks,
            agent_watches,
            queued_messages,
            event_subscriptions,
            active_sub_agents,
            unsettled_sub_agents,
            running_sub_agents,
            num_questions_asked,
            pr_monitors,
            prs,
            pending_attention: session.attention_request_kind.clone(),
        })
    }

    /// The snapshot's `prs` field: the workspace's tracked open PRs grouped
    /// by state (see [`AgentSnapshotPrs`]), read from persisted columns only
    /// — the workspace row's `pull_requests` under its
    /// `repository_owner`/`repository_name`, plus each registered git root's
    /// `pull_requests` under its `repo_owner`/`repo_name` (a pool whose repo
    /// identity is missing or blank is skipped: no label can be formed). No
    /// forge calls and
    /// no per-PR statements (RPC cost contract); best-effort — a store
    /// failure reads as `None` so a snapshot build never fails on it.
    async fn tracked_open_prs_grouped(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Option<AgentSnapshotPrs> {
        let workspace = match self.store.get_workspace(workspace_id).await {
            Ok(ws) => Some(ws),
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "workspace lookup failed; snapshot prs skips the workspace pool"
                );
                None
            }
        };
        let roots = match self.store.list_workspace_git_roots(workspace_id).await {
            Ok(roots) => roots,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "git-root lookup failed; snapshot prs skips the root pools"
                );
                Vec::new()
            }
        };
        let mut pools: Vec<(&str, &str, &[PullRequestInfo])> = Vec::new();
        if let Some(ws) = &workspace {
            if let (Some(owner), Some(name), Some(prs)) = (
                ws.repository_owner.as_deref(),
                ws.repository_name.as_deref(),
                ws.pull_requests.as_deref(),
            ) {
                pools.push((owner, name, prs));
            }
        }
        for root in &roots {
            if let (Some(owner), Some(name), Some(prs)) = (
                root.repo_owner.as_deref(),
                root.repo_name.as_deref(),
                root.pull_requests.as_deref(),
            ) {
                pools.push((owner, name, prs));
            }
        }
        grouped_open_prs(pools)
    }

    /// `ws.agent.snapshot()` (MCP-only, PROTOCOL §7.1): the caller's own
    /// state snapshot as a plain JSON object — zero-count and null fields
    /// omitted, `time` always present. Never gated by
    /// `agentFeatures.stateSnapshot` (the toggle governs only the turn-prompt
    /// injection). Workspace mismatch fails closed as `NotFound`
    /// (defense-in-depth against bare-id probes, same as `getSessionStats`).
    pub(crate) async fn agent_snapshot_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        let snapshot = self.build_agent_snapshot(&session).await?;
        serde_json::to_value(&snapshot)
            .map_err(|e| Error::Internal(format!("serialize agent snapshot: {e}")))
    }

    /// The per-turn snapshot injection line for `agent_id`, or `None` when
    /// the injection is suppressed: `agentFeatures.stateSnapshot` is off in
    /// the session's captured harness feature snapshot
    /// ([`Services::session_agent_features`] — like every other toggle,
    /// flipping the setting affects new sessions only; legacy NULL-snapshot
    /// rows fall back to the live settings until their first-activation
    /// freeze), the snapshot is trivial (every field other than `time`
    /// zero/absent — `time` alone never forces an injection), or the
    /// snapshot could not be built (fails open to no line so a store error
    /// never blocks a turn).
    pub(crate) async fn agent_state_snapshot_line(&self, agent_id: &AgentId) -> Option<String> {
        let session = self.store.get_agent_session_summary(agent_id).await.ok()?;
        if !self.session_agent_features(&session).state_snapshot {
            return None;
        }
        let snapshot = self.build_agent_snapshot(&session).await.ok()?;
        if snapshot.is_trivial() {
            return None;
        }
        let json = serde_json::to_string(&snapshot).ok()?;
        Some(crate::harness::latest().snapshot_line(&json))
    }

    /// `agent.diagnostics`: a sanitized snapshot of agent statuses,
    /// subscriptions, delegation groups, and stuck-risk signals (PROTOCOL §5.5).
    ///
    /// Ports the TS `buildAgentDiagnosticsSnapshot` shape over the daemon's
    /// (simpler) runtime: completion-watch records back the `subscriptions` view
    /// and the delegation-group registry backs `delegationGroups`. `queues`
    /// carries real per-agent pending-message snapshots — one entry per
    /// in-scope agent with a non-empty queue, each listing its entries in
    /// drain order via [`Services::queue_snapshot_preview`] (content truncated
    /// to [`QUEUE_PREVIEW_MAX_CHARS`] chars, sender attribution preserved in
    /// `messageMetadata`) — and `summary.queuedAgents` counts those agents.
    /// A queue whose ready-to-send entries have sat undelivered past
    /// [`STALE_QUEUE_ENTRY_AFTER_MS`] while the target agent is not actively
    /// responding raises a `stale-queue-entry` stuck-risk
    /// (intent-hq/monorepo#1897). Affirmatively-parked queues are excluded:
    /// archived workspaces park every entry, and an active question hold
    /// parks automatic (non-user-origin) entries — neither is stuck.
    /// The daemon does not track per-agent event queues, deleted-agent
    /// references, or delivery health, so `deletedAgentReferences` and
    /// `recentEvents` are empty and `deliveryStats` is zeroed — honestly
    /// reflecting what the runtime knows about. Returns
    /// `{ ok, diagnostics, text }` (`buildToolResponse`).
    pub(crate) async fn agent_diagnostics_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        stale_responding_after_ms: Option<i64>,
    ) -> Result<Value> {
        let stale_after_ms = stale_responding_after_ms.unwrap_or(DEFAULT_STALE_RESPONDING_AFTER_MS);
        let now = now_iso();
        let now_ms = iso_ms(&now);

        // Finding F1/F3: use message-free summaries + lightweight stats for diagnostics.
        // Diagnostics needs session metadata and message counts/assistant-message presence,
        // but never needs full message bodies.
        let sessions = self
            .store
            .list_agent_session_summaries(&workspace_id)
            .await?;
        let message_stats = self
            .store
            .get_agent_session_message_stats(&workspace_id)
            .await?;
        let watches = self.all_watches(&workspace_id);
        let groups = self.all_groups(&workspace_id);

        let agent_filter = agent_id.as_ref().map(|a| a.0.clone());
        // A taskNoteId filter matches the agents actually associated with the
        // task (monorepo#1150): sessions persist `task_note_id` (set by
        // `agent.delegate`) and the note side tracks `assigned_agents`
        // (`task.assignAgent`) — the scope is the union of both, mirroring
        // `agent.sendToTask`'s note-side resolution. A missing or non-task
        // note yields an empty scope (empty snapshot), never an error.
        let task_filter = task_note_id.as_ref().map(|n| n.0.clone());
        let has_filter = agent_filter.is_some() || task_filter.is_some();
        let mut task_agent_ids: HashSet<String> = HashSet::new();
        if let Some(tid) = &task_note_id {
            for s in &sessions {
                if s.task_note_id.as_ref() == Some(tid) {
                    task_agent_ids.insert(s.id.0.clone());
                }
            }
            match self.get_my_task(workspace_id.clone(), tid.clone()).await {
                Ok(task) => {
                    task_agent_ids.extend(task.assigned_agents.into_iter().map(|a| a.0));
                }
                // `get_my_task` maps a missing note / non-task note to these
                // `Internal` messages — the expected empty-note-scope shape,
                // kept silent. Anything else is a real store failure worth
                // surfacing before we fall back to session-side matches only.
                Err(Error::Internal(msg))
                    if msg == "Task note not found" || msg == "Note is not a task" => {}
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        note = %tid,
                        "agent.diagnostics: task note lookup failed; \
                         scoping to session-side matches only"
                    );
                }
            }
        }

        let session_ids: HashSet<String> = sessions.iter().map(|s| s.id.0.clone()).collect();
        let session_by_id: std::collections::HashMap<String, &AgentSession> =
            sessions.iter().map(|s| (s.id.0.clone(), s)).collect();

        let mut matching: HashSet<String> = HashSet::new();
        for s in &sessions {
            if let Some(aid) = &agent_filter {
                if &s.id.0 != aid {
                    continue;
                }
            }
            if task_filter.is_some() && !task_agent_ids.contains(&s.id.0) {
                continue;
            }
            matching.insert(s.id.0.clone());
        }
        if agent_filter.is_none() {
            // Note-side assignees without a session row still scope the
            // snapshot (union semantics). Last use — the set is moved.
            matching.extend(task_agent_ids);
        }
        if let Some(aid) = &agent_filter {
            matching.insert(aid.clone());
        }
        let in_scope = |id: &str| !has_filter || matching.contains(id);
        let intersects_scope =
            |ids: &[String]| !has_filter || ids.iter().any(|id| matching.contains(id));

        // Union of every agent id referenced anywhere in the snapshot.
        let mut all_agent_ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let push_id = |id: &str, all: &mut Vec<String>, seen: &mut HashSet<String>| {
            if seen.insert(id.to_string()) {
                all.push(id.to_string());
            }
        };
        for s in &sessions {
            push_id(&s.id.0, &mut all_agent_ids, &mut seen);
        }
        for w in &watches {
            push_id(&w.parent_agent_id.0, &mut all_agent_ids, &mut seen);
            push_id(&w.child_agent_id.0, &mut all_agent_ids, &mut seen);
        }
        for g in &groups {
            push_id(&g.parent_agent_id.0, &mut all_agent_ids, &mut seen);
            for id in &g.expected_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
            for id in &g.completed_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
            for id in &g.deleted_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
        }

        let event_types = [AGENT_IDLE, AGENT_FAILED, AGENT_DELETED];

        // subscriptions (completion watches), filtered to scope.
        let subscriptions: Vec<Value> = watches
            .iter()
            .filter(|w| {
                in_scope(&w.parent_agent_id.0)
                    || intersects_scope(std::slice::from_ref(&w.child_agent_id.0))
            })
            .map(|w| {
                json!({
                    "id": w.id,
                    "agentId": w.parent_agent_id,
                    "agentName": w.parent_agent_name,
                    "createdAt": w.created_at,
                    "eventTypes": event_types,
                    "actorIds": [w.child_agent_id.clone()],
                    "priority": "normal",
                    "delegationGroupId": w.group_id,
                    "orphaned": !session_ids.contains(&w.parent_agent_id.0),
                })
            })
            .collect();

        // eventSubscriptions (monorepo#947), filtered to scope: an event
        // subscription is in scope when its subscriber agent is (front-door
        // subscriptions have no subscriber and only appear unfiltered).
        // `orphaned` first checks this workspace's session set, then falls
        // back to a direct liveness lookup — chief-workspace agents may
        // legitimately subscribe cross-workspace (validate_event_subscriber),
        // and must not be flagged orphaned in the target workspace's view.
        let mut event_subscriptions: Vec<Value> = Vec::new();
        for r in self
            .list_event_subscriptions_for_workspace(&workspace_id)
            .iter()
            .filter(|r| match &r.subscriber_agent_id {
                Some(a) => in_scope(&a.0),
                None => !has_filter,
            })
        {
            let mut v = event_subscription_wire(r);
            let orphaned = match &r.subscriber_agent_id {
                Some(a) => !session_ids.contains(&a.0) && !self.agent_is_live(a).await,
                None => false,
            };
            v["orphaned"] = json!(orphaned);
            event_subscriptions.push(v);
        }

        // delegationGroups, filtered to scope.
        // The group-level `subscription_id` is a TS-parity legacy field the
        // daemon never populates; the real linkage is the per-child grouped
        // completion watches (each carries its group's id). Index them once
        // (group id → [(child id, watch id)]) so the per-group derivation
        // below stays O(rows returned) rather than rescanning the watch
        // table per group and per pending child (monorepo#1694).
        let mut group_watches: HashMap<&str, Vec<(&str, &str)>> = HashMap::new();
        for w in &watches {
            if let Some(gid) = w.group_id.as_deref() {
                group_watches
                    .entry(gid)
                    .or_default()
                    .push((w.child_agent_id.0.as_str(), w.id.as_str()));
            }
        }
        let delegation_groups: Vec<Value> = groups
            .iter()
            .filter(|g| {
                let mut ids = vec![g.parent_agent_id.0.clone()];
                ids.extend(g.expected_agent_ids.iter().map(|a| a.0.clone()));
                ids.extend(g.completed_agent_ids.iter().map(|a| a.0.clone()));
                ids.extend(g.deleted_agent_ids.iter().map(|a| a.0.clone()));
                intersects_scope(&ids)
            })
            .map(|g| {
                let done: HashSet<String> = g
                    .completed_agent_ids
                    .iter()
                    .chain(g.deleted_agent_ids.iter())
                    .map(|a| a.0.clone())
                    .filter(|id| g.expected_agent_ids.iter().any(|e| &e.0 == id))
                    .collect();
                let pending: Vec<String> = g
                    .expected_agent_ids
                    .iter()
                    .map(|a| a.0.clone())
                    .filter(|id| !done.contains(id))
                    .collect();
                let complete = !g.expected_agent_ids.is_empty()
                    && g.expected_agent_ids.iter().all(|id| {
                        g.completed_agent_ids.contains(id) || g.deleted_agent_ids.contains(id)
                    });
                // Derive linkage from the grouped-watch index (monorepo#1694):
                // linkage is missing only when some still-pending child has
                // no grouped watch observing it.
                let watches_for_group = group_watches
                    .get(g.group_id.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let subscription_ids: Vec<&str> =
                    watches_for_group.iter().map(|(_, wid)| *wid).collect();
                let subscription_missing = pending.iter().any(|id| {
                    !watches_for_group
                        .iter()
                        .any(|(child, _)| *child == id.as_str())
                });
                json!({
                    "groupId": g.group_id,
                    "parentAgentId": g.parent_agent_id,
                    "awaitMode": g.await_mode,
                    "expectedAgentIds": g.expected_agent_ids,
                    "completedAgentIds": g.completed_agent_ids,
                    "deletedAgentIds": g.deleted_agent_ids,
                    "pendingAgentIds": pending,
                    "subscriptionIds": subscription_ids,
                    "subscriptionMissing": subscription_missing,
                    "delivered": g.delivered,
                    "complete": complete,
                    "eventCount": g.event_summaries.len(),
                })
            })
            .collect();

        // agents rows.
        all_agent_ids.sort();
        // Idle-visibility: per-agent active-hook metadata (`waitingOnHooks`,
        // omitted when empty) from one workspace-wide hook query.
        let mut hooks_by_agent = self.active_hooks_by_agent(&workspace_id).await;
        // Idle-visibility (unified external-wait): per-agent active PR
        // monitor metadata (`waitingOnPrMonitors`, omitted when empty) from
        // one workspace-wide monitor query.
        let mut pr_monitors_by_agent = self.active_pr_monitors_by_agent(&workspace_id).await;
        // Per-agent subtree memory attribution (monorepo#2063 A2): resident
        // bytes of each agent's descendant process tree from the runtime
        // manager's tree probe, stamped as `subtreeMemoryBytes` (omitted when
        // the agent has no bucket — not spawned, no sample yet, or no runtime
        // manager attached). Diagnostics-only by design: this deliberately
        // stays off the hot `agent.list`/`agent.get` payloads.
        let agent_memory: HashMap<AgentId, u64> = self
            .agent_manager()
            .map(|m| m.agent_memory_samples())
            .unwrap_or_default();
        let mut agent_rows: Vec<Value> = Vec::new();
        for id in &all_agent_ids {
            if !in_scope(id) {
                continue;
            }
            let session = session_by_id.get(id).copied();
            let status = session
                .and_then(|s| agent_status_wire(s.status))
                .unwrap_or("unknown");
            // Use lightweight message stats instead of full hydration
            let (message_count_val, has_assistant, conversation_bytes) =
                message_stats.get(id).copied().unwrap_or((0, false, 0));
            let message_count = if message_count_val > 0 {
                Some(message_count_val)
            } else {
                None
            };
            // Liveness-aware activity stamp (monorepo#3647): the persisted
            // `updated_at` freezes at turn start, so a long healthy turn
            // would read stale off it alone. Overlay the live-turn slot's
            // stream stamp (refreshed on every chunk/tool call) and key the
            // stale-responding heuristic on the max of the two — an agent
            // actively streaming through the harness is never flagged stale.
            // Compare parsed instants (`iso_ms`) — RFC-3339 strings carry
            // variable sub-second precision, so lexicographic order is not
            // chronological. `iso_ms` truncates to milliseconds, so a
            // same-millisecond tie prefers the live stamp — it is refreshed
            // on every stream event and thus at least as fresh.
            let live_stream_activity = self.live_turn_activity_at(&AgentId(id.clone()));
            let last_activity = match (
                session.map(|s| s.updated_at.clone()),
                live_stream_activity.clone(),
            ) {
                (Some(p), Some(l)) => Some(if iso_ms(&l) >= iso_ms(&p) { l } else { p }),
                (p, l) => p.or(l),
            };
            let last_activity_age = last_activity.as_deref().map(|t| age_ms(now_ms, t));
            let stale_responding = status == "responding"
                && match last_activity_age {
                    None => true,
                    Some(age) => age > stale_after_ms,
                };
            let pending_initial_response = session.is_some()
                && status.eq_ignore_ascii_case("idle")
                && message_count == Some(1)
                && !has_assistant;
            let subscription_count = watches
                .iter()
                .filter(|w| &w.parent_agent_id.0 == id)
                .count();
            let event_subscription_count = event_subscriptions
                .iter()
                .filter(|s| s["subscriberAgentId"].as_str() == Some(id))
                .count();

            let mut row = serde_json::Map::new();
            row.insert("id".into(), json!(id));
            if let Some(s) = session {
                row.insert("name".into(), json!(s.name));
                row.insert("sessionStatus".into(), json!(s.status));
                row.insert("createdAt".into(), json!(s.created_at));
            }
            row.insert("status".into(), json!(status));
            if let Some(mc) = message_count {
                row.insert("messageCount".into(), json!(mc));
            }
            // Persisted-conversation size (intent-hq/monorepo#2669): session-
            // size pressure signal so coordinators can rotate agents before
            // turns start dying under context bloat. Omitted when zero (no
            // messages), like `messageCount`.
            if conversation_bytes > 0 {
                row.insert("conversationBytes".into(), json!(conversation_bytes));
            }
            row.insert("subscriptionCount".into(), json!(subscription_count));
            row.insert(
                "eventSubscriptionCount".into(),
                json!(event_subscription_count),
            );
            row.insert("queuedEventCount".into(), json!(0));
            row.insert("staleResponding".into(), json!(stale_responding));
            row.insert("deleted".into(), json!(false));
            row.insert("presentInBackend".into(), json!(session.is_some()));
            row.insert(
                "pendingInitialResponse".into(),
                json!(pending_initial_response),
            );
            if let Some(la) = &last_activity {
                row.insert("lastActivity".into(), json!(la));
            }
            // Additive (monorepo#3647): the raw live-turn stream stamp, so
            // diagnostics consumers can distinguish harness liveness from
            // persisted-row churn. Omitted when no turn is streaming.
            if let Some(ls) = &live_stream_activity {
                row.insert("lastStreamActivityAt".into(), json!(ls));
            }
            if let Some(hooks) = hooks_by_agent.remove(id.as_str()) {
                if !hooks.is_empty() {
                    row.insert("waitingOnHooks".into(), Value::Array(hooks));
                }
            }
            if let Some(monitors) = pr_monitors_by_agent.remove(id.as_str()) {
                if !monitors.is_empty() {
                    row.insert("waitingOnPrMonitors".into(), Value::Array(monitors));
                }
            }
            if let Some(bytes) = agent_memory.get(&AgentId(id.clone())) {
                row.insert("subtreeMemoryBytes".into(), json!(bytes));
            }
            // Silent tail of the most recently ended turn
            // (intent-hq/monorepo#2669): ms of stream silence before the
            // prompt resolved, recorded in-memory at turn end. Omitted when
            // no turn ended this daemon lifetime. Diagnostics-only by design
            // (like `subtreeMemoryBytes`): never on the hot
            // `agent.list`/`agent.get` payloads.
            if let Some(tail_ms) = self.last_turn_silent_tail(&AgentId(id.clone())) {
                row.insert("lastTurnSilentTailMs".into(), json!(tail_ms));
            }
            agent_rows.push(Value::Object(row));
        }

        // Real per-agent pending-message queue snapshots (drain order, content
        // truncated) for every in-scope agent with a non-empty queue.
        let mut queues: Vec<Value> = Vec::new();
        for id in &all_agent_ids {
            if !in_scope(id) {
                continue;
            }
            let entries = self.queue_snapshot_preview(&AgentId(id.clone()));
            if entries.is_empty() {
                continue;
            }
            let mut q = serde_json::Map::new();
            q.insert("agentId".into(), json!(id));
            if let Some(s) = session_by_id.get(id) {
                q.insert("agentName".into(), json!(s.name));
            }
            q.insert("queueLength".into(), json!(entries.len()));
            q.insert("entries".into(), Value::Array(entries));
            queues.push(Value::Object(q));
        }

        // stuck-risk signals.
        let mut stuck_risks: Vec<Value> = Vec::new();
        for row in &agent_rows {
            let aid = row["id"].as_str().unwrap_or_default();
            if row["staleResponding"].as_bool() == Some(true) {
                stuck_risks.push(json!({
                    "type": "stale-responding-status",
                    "severity": "warning",
                    "message": format!("Agent {aid} is marked responding without recent activity"),
                    "agentId": aid,
                }));
            }
            if row["pendingInitialResponse"].as_bool() == Some(true) {
                let present = row["presentInBackend"].as_bool() == Some(true);
                let age = row["lastActivity"].as_str().map(|t| age_ms(now_ms, t));
                let severity = match age {
                    Some(a) if a <= stale_after_ms => "info",
                    _ => "warning",
                };
                let message = if present {
                    format!("Agent {aid} has an initial user message but no assistant response")
                } else {
                    format!("Agent {aid} has an initial user message but no active backend session or assistant response")
                };
                let mut risk = serde_json::Map::new();
                risk.insert("type".into(), json!("initial-prompt-not-running"));
                risk.insert("severity".into(), json!(severity));
                risk.insert("message".into(), json!(message));
                risk.insert("agentId".into(), json!(aid));
                if let Some(a) = age {
                    risk.insert("ageMs".into(), json!(a));
                }
                stuck_risks.push(Value::Object(risk));
            }
            // Large persisted conversation (intent-hq/monorepo#2669): past
            // [`LARGE_CONVERSATION_WARN_BYTES`] the session is at risk of
            // silently-truncated turns — surface it so coordinators rotate to
            // a fresh agent before turns start dying.
            if let Some(bytes) = row["conversationBytes"].as_u64() {
                if bytes > LARGE_CONVERSATION_WARN_BYTES {
                    stuck_risks.push(json!({
                        "type": "large-conversation",
                        "severity": "warning",
                        "message": format!(
                            "Agent {aid} has a large persisted conversation ({bytes} bytes > {LARGE_CONVERSATION_WARN_BYTES}); turns may start silently truncating under session bloat — consider rotating to a fresh agent"
                        ),
                        "agentId": aid,
                        "conversationBytes": bytes,
                        "thresholdBytes": LARGE_CONVERSATION_WARN_BYTES,
                    }));
                }
            }
            // Long silent tail on the last ended turn
            // (intent-hq/monorepo#2669): the incident signature of a
            // silently-truncated turn is a clean `end_turn` after 11-13 min
            // of stream silence — surface a tail past the suspect threshold
            // so coordinators see the turn likely died rather than finished.
            // The tail is recorded for every resolution (completed,
            // cancelled, or timed out — only invisible pre-output redrives
            // are excluded), so the wording covers all of them: a deliberate
            // interrupt aimed at a stalled agent legitimately lands here,
            // and its follow-up turn overwrites the record anyway.
            if let Some(tail_ms) = row["lastTurnSilentTailMs"].as_u64() {
                let threshold_ms = crate::agent_session::silent_tail_suspect_ms();
                if tail_ms >= threshold_ms {
                    stuck_risks.push(json!({
                        "type": "long-silent-tail",
                        "severity": "warning",
                        "message": format!(
                            "Agent {aid}'s last turn ended (completed, cancelled, or timed out) after {tail_ms}ms of stream silence (>= {threshold_ms}ms); if it completed, it may have been silently truncated under session bloat rather than finishing"
                        ),
                        "agentId": aid,
                        "silentTailMs": tail_ms,
                        "thresholdMs": threshold_ms,
                    }));
                }
            }
        }
        // Stale undelivered queue entries (intent-hq/monorepo#1897): a
        // ready-to-send entry older than [`STALE_QUEUE_ENTRY_AFTER_MS`] whose
        // target agent is not actively responding should have drained long
        // ago — surface it instead of leaving the wake invisible. Entries
        // under edit are excluded (the drain skips them by design), as are
        // entries whose `queuedAt` fails to parse. An actively-responding,
        // non-stale agent legitimately holds its queue until the turn ends.
        //
        // Affirmatively-parked queues are expected, not stuck: an archived
        // workspace parks every queue until unarchive (the drain kick then
        // delivers), and an active question hold (PROTOCOL §5.5) parks
        // automatic entries until the user answers or dismisses — but never
        // user-origin entries, which drain through the hold, so a stale
        // user-origin entry under a hold is still a genuine risk. Both checks
        // are lazy — one workspace read per call and one bounded
        // [`Services::question_hold_active`] session read per agent, paid
        // only when a stale candidate actually exists.
        let mut workspace_archived: Option<bool> = None;
        for q in &queues {
            let aid = q["agentId"].as_str().unwrap_or_default();
            // Liveness-aware, like `staleResponding` above (monorepo#3647):
            // a mid-turn agent's `updated_at` is frozen, so also accept the
            // live-turn stream stamp as proof of active draining.
            let actively_responding = session_by_id.get(aid).is_some_and(|s| {
                agent_status_wire(s.status) == Some("responding")
                    && (age_ms(now_ms, &s.updated_at) <= stale_after_ms
                        || self
                            .live_turn_activity_at(&AgentId(aid.to_string()))
                            .is_some_and(|t| age_ms(now_ms, &t) <= stale_after_ms))
            });
            if actively_responding {
                continue;
            }
            let mut stale: Vec<(&str, i64)> = q["entries"]
                .as_array()
                .map(|entries| {
                    entries
                        .iter()
                        .filter(|e| e["editing"].as_bool() != Some(true))
                        .filter_map(|e| {
                            let queued_at = e["queuedAt"].as_str()?;
                            let queued_ms = i64::try_from(
                                parse_iso(queued_at)?.unix_timestamp_nanos() / 1_000_000,
                            )
                            .unwrap_or(0);
                            let age = (now_ms - queued_ms).max(0);
                            if age > STALE_QUEUE_ENTRY_AFTER_MS {
                                Some((e["id"].as_str().unwrap_or_default(), age))
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            if stale.is_empty() {
                continue;
            }
            let archived = if let Some(v) = workspace_archived {
                v
            } else {
                let v = !workspace_id.is_chief()
                    && self
                        .store
                        .get_workspace(&workspace_id)
                        .await
                        .is_ok_and(|w| w.archived);
                workspace_archived = Some(v);
                v
            };
            if archived {
                break;
            }
            let agent = AgentId(aid.to_string());
            if self.question_hold_active(&agent).await {
                let user_origin_ids: HashSet<String> = {
                    let guard = self
                        .agent_queues
                        .lock()
                        .expect("agent queue registry poisoned");
                    guard
                        .get(&agent)
                        .map(|entries| {
                            entries
                                .iter()
                                .filter(|m| m.user_origin)
                                .map(|m| m.id.clone())
                                .collect()
                        })
                        .unwrap_or_default()
                };
                stale.retain(|(id, _)| user_origin_ids.contains(*id));
            }
            let Some((oldest_id, oldest_age)) = stale.iter().max_by_key(|(_, age)| *age) else {
                continue;
            };
            let status = session_by_id
                .get(aid)
                .and_then(|s| agent_status_wire(s.status))
                .unwrap_or("unknown");
            stuck_risks.push(json!({
                "type": "stale-queue-entry",
                "severity": "warning",
                "message": format!(
                    "Agent {aid} ({status}) has {} undelivered queued message(s); oldest entry {oldest_id} queued {oldest_age}ms ago",
                    stale.len(),
                ),
                "agentId": aid,
                "entryId": oldest_id,
                "ageMs": oldest_age,
                "count": stale.len(),
            }));
        }
        for sub in &subscriptions {
            if sub["orphaned"].as_bool() == Some(true) {
                let sid = sub["id"].as_str().unwrap_or_default();
                let aid = sub["agentId"].as_str().unwrap_or_default();
                stuck_risks.push(json!({
                    "type": "orphaned-subscription",
                    "severity": "warning",
                    "message": format!("Subscription {sid} targets missing or deleted owner {aid}"),
                    "agentId": aid,
                    "subscriptionId": sid,
                }));
            }
        }
        for sub in &event_subscriptions {
            if sub["orphaned"].as_bool() == Some(true) {
                let sid = sub["id"].as_str().unwrap_or_default();
                let aid = sub["subscriberAgentId"].as_str().unwrap_or_default();
                stuck_risks.push(json!({
                    "type": "orphaned-event-subscription",
                    "severity": "warning",
                    "message": format!(
                        "Event subscription {sid} targets missing or deleted subscriber {aid}"
                    ),
                    "agentId": aid,
                    "subscriptionId": sid,
                }));
            }
        }
        for g in &delegation_groups {
            let complete = g["complete"].as_bool() == Some(true);
            let delivered = g["delivered"].as_bool() == Some(true);
            if !complete && !delivered {
                let gid = g["groupId"].as_str().unwrap_or_default();
                let pending_ids: Vec<&str> = g["pendingAgentIds"]
                    .as_array()
                    .map(|a| a.iter().filter_map(Value::as_str).collect())
                    .unwrap_or_default();
                let pending = pending_ids.len();
                // Severity ladder (monorepo#1694): missing subscription
                // linkage means the group can never observe a pending
                // child's completion — critical. Otherwise a group whose
                // pending children are all actively responding (non-stale)
                // is normal in-progress fan-in — informational; anything
                // else (idle/stale/missing pending child) is worth a look.
                let all_pending_active = !pending_ids.is_empty()
                    && pending_ids.iter().all(|id| {
                        session_by_id.get(*id).is_some_and(|s| {
                            agent_status_wire(s.status) == Some("responding")
                                && (age_ms(now_ms, &s.updated_at) <= stale_after_ms
                                    || self
                                        .live_turn_activity_at(&AgentId((*id).to_string()))
                                        .is_some_and(|t| age_ms(now_ms, &t) <= stale_after_ms))
                        })
                    });
                let severity = if g["subscriptionMissing"].as_bool() == Some(true) {
                    "critical"
                } else if all_pending_active {
                    "info"
                } else {
                    "warning"
                };
                stuck_risks.push(json!({
                    "type": "incomplete-delegation-group",
                    "severity": severity,
                    "message": format!("Delegation group {gid} is waiting for {pending} agent(s)"),
                    "groupId": gid,
                    "count": pending,
                }));
            }
        }

        let mut filters = serde_json::Map::new();
        if let Some(aid) = &agent_filter {
            filters.insert("agentId".into(), json!(aid));
        }
        if let Some(tid) = &task_filter {
            filters.insert("taskNoteId".into(), json!(tid));
        }

        let summary = json!({
            "agents": agent_rows.len(),
            "subscriptions": subscriptions.len(),
            "eventSubscriptions": event_subscriptions.len(),
            "queuedAgents": queues.len(),
            "queuedEvents": 0,
            "delegationGroups": delegation_groups.len(),
            "deletedAgents": 0,
            "stuckRisks": stuck_risks.len(),
        });

        let delivery_stats = json!({
            "totalDeliveries": 0,
            "successfulDeliveries": 0,
            "failedDeliveries": 0,
            "timeoutDeliveries": 0,
            "droppedEvents": 0,
            "lastDeliveryTime": Value::Null,
            "lastFailureTime": Value::Null,
        });

        let diagnostics = json!({
            "workspaceId": workspace_id,
            "generatedAt": now,
            "filters": Value::Object(filters),
            "summary": summary,
            "agents": agent_rows,
            "subscriptions": subscriptions,
            "eventSubscriptions": event_subscriptions,
            "queues": queues,
            "delegationGroups": delegation_groups,
            "deliveryStats": delivery_stats,
            "deletedAgentReferences": [],
            "recentEvents": [],
            "stuckRisks": stuck_risks,
        });

        // Human-readable `text` (mirrors `GetAgentDiagnosticsTool`).
        let mut lines = vec![
            format!("Agent diagnostics for workspace {}", workspace_id.0),
            format!("Agents: {}", diagnostics["summary"]["agents"]),
            format!("Subscriptions: {}", diagnostics["summary"]["subscriptions"]),
            format!(
                "Event subscriptions: {}",
                diagnostics["summary"]["eventSubscriptions"]
            ),
            format!("Queued agents: {}", diagnostics["summary"]["queuedAgents"]),
            format!("Queued events: {}", diagnostics["summary"]["queuedEvents"]),
            format!(
                "Delegation groups: {}",
                diagnostics["summary"]["delegationGroups"]
            ),
            format!("Stuck risks: {}", diagnostics["summary"]["stuckRisks"]),
        ];
        if let Some(qs) = diagnostics["queues"].as_array() {
            if !qs.is_empty() {
                lines.push(String::new());
                lines.push("Pending message queues:".to_string());
                for q in qs.iter().take(10) {
                    let aid = q["agentId"].as_str().unwrap_or_default();
                    let name = q["agentName"].as_str().unwrap_or(aid);
                    let len = q["queueLength"].as_u64().unwrap_or(0);
                    lines.push(format!("- {name} ({aid}): {len} queued message(s)"));
                }
            }
        }
        if let Some(risks) = diagnostics["stuckRisks"].as_array() {
            if !risks.is_empty() {
                lines.push(String::new());
                lines.push("Stuck-risk signals:".to_string());
                for risk in risks.iter().take(10) {
                    let target = risk["agentId"]
                        .as_str()
                        .or_else(|| risk["groupId"].as_str())
                        .or_else(|| risk["subscriptionId"].as_str())
                        .unwrap_or("workspace");
                    let severity = risk["severity"].as_str().unwrap_or_default();
                    let rtype = risk["type"].as_str().unwrap_or_default();
                    lines.push(format!("- [{severity}] {rtype}: {target}"));
                }
            }
        }

        Ok(json!({
            "ok": true,
            "diagnostics": diagnostics,
            "text": lines.join("\n"),
        }))
    }

    /// `agent.sendToTask`: deliver to the agent assigned to a task note (PROTOCOL §5.5).
    /// `priority: "interrupt"` preempts the assignee's in-flight turn keep-alive
    /// (never killing the child) and delivers immediately when the runtime
    /// manager is attached; other priorities keep the existing delivery.
    /// The target resolution (`task.assigned_agents.first()`) is mirrored by
    /// the MCP `ws.agent.sendToTask` single-pending guard in
    /// `intent-acp/src/mcp_server/bindings/agent.rs` — if this resolution
    /// changes, that guard site must change with it.
    pub(crate) async fn agent_send_to_task_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        priority: Option<String>,
        message_metadata: Option<Value>,
    ) -> Result<Value> {
        let task = self.get_my_task(workspace_id.clone(), task_note_id).await?;
        let Some(agent) = task.assigned_agents.first().cloned() else {
            return Ok(
                json!({ "ok": false, "delivered": false, "error": "No agent assigned to task" }),
            );
        };
        // DELIV-1: non-interrupt priority MUST also drive a real turn when
        // the runtime is attached — the store-only `agent_send_message_op`
        // fallback would persist the message without ever prompting the
        // assignee (the "coordinator send silently lost" bug). Mirror the
        // `agent_send_message` (WorkspaceApi) routing: the manager path
        // spawns the turn worker; only the read-only wiring with no
        // manager falls back to the store-only op.
        let options = crate::agent_manager::TurnOptions {
            message_metadata,
            ..crate::agent_manager::TurnOptions::default()
        };
        let result = match (
            self.agent_manager(),
            is_interrupt_priority(priority.as_deref()),
        ) {
            (Some(manager), true) => {
                manager
                    .interrupt_send_message(agent.clone(), workspace_id, message, None, options)
                    .await?
            }
            (Some(manager), false) => {
                manager
                    .send_message(agent.clone(), workspace_id, message, None, options)
                    .await?
            }
            (None, interrupt) => {
                // Read-only fallback (no `agent_manager` wired): mirrors
                // `agent_send_message` — plumb the metadata through the
                // store-only append so attribution is consistent across
                // deployments with and without a runtime manager. Question
                // hold (PROTOCOL §5.5): sendToTask is automatic by
                // definition, so an active hold parks the message instead
                // of persisting a user row that buries the pending Q&A.
                if self.question_hold_active(&agent).await {
                    // A2A sender header (intent-hq/intent#3721, monorepo#1015): this hold-park
                    // bypasses `agent_send_message_op`'s prepend, so the
                    // queued entry is annotated here — the drain persist
                    // then inherits it.
                    let mut message = message;
                    annotate_sender_attribution(&mut message, options.message_metadata.as_ref());
                    let (queued, position) = self.enqueue_message(
                        &agent,
                        message,
                        None,
                        None,
                        options.message_metadata,
                        None,
                        interrupt,
                    );
                    let held = json!({
                        "success": true,
                        "queued": true,
                        "heldForQuestions": true,
                        "queuedMessage": queued.to_value(position),
                        "turnId": queued.turn_id,
                    });
                    self.publish_queue_updated(&agent).await;
                    // Race close (hold-check → enqueue vs a concurrent
                    // `dismissQuestions`/answer): this `(None, _)` arm only
                    // runs with no `AgentManager` attached, so there is no
                    // drain to kick here — same as the other store-only
                    // fallbacks above.
                    held
                } else {
                    self.agent_send_message_op(
                        agent.clone(),
                        message,
                        None,
                        None,
                        None,
                        options.message_metadata,
                    )
                    .await?
                }
            }
        };
        Ok(json!({ "ok": true, "agentId": agent, "result": result }))
    }

    /// Newest-first probe over a task's `assignedAgentIds` (B1 + B2;
    /// `Vec::push` append-order means newest is the tail). Shared by
    /// `agent_wake_or_create_op`'s live/resumable scan and the occupancy
    /// guards in `agent_delegate_op` / `assign_agent`. Probe each session:
    ///   * `NotFound` / Deleted → stale, queue for cleanup.
    ///   * Poisoned (monorepo#840: Error + session-fatal provider block or
    ///     an identical-failure streak) → NOT resumable: waking it would
    ///     replay the provider-blocked turn ("start a new session" means a
    ///     fresh session). Queue for cleanup so a fresh agent is created,
    ///     keeping it as the inheritance source for specialist/model.
    ///     Poisoned ids are ALSO tracked separately: their parked queues
    ///     are migrated onto the wake/create target and the dead session
    ///     is GC'd (monorepo#847). `NotFound` / soft-Deleted ids keep the
    ///     cleanup-only behavior.
    ///   * Otherwise → treat as resumable; the newest live session wins.
    ///
    /// Once the newest live session is found, older candidates are left
    /// untouched EXCEPT poisoned ones: a failed queue migration keeps the
    /// poisoned assignment in place (now older than the live winner), so
    /// the scan keeps probing for poisoned ids to retry the migration + GC
    /// on this wake (monorepo#847).
    /// `inheritance_source` captures the newest **known** previous session
    /// (live, poisoned, or deleted) so wakeOrCreate's create branch can still
    /// inherit specialist/model when no live agent is available.
    pub(crate) async fn scan_assigned_agents(
        &self,
        assigned: &[AgentId],
    ) -> Result<AssignedAgentScan> {
        let mut scan = AssignedAgentScan::default();
        for candidate in assigned.iter().rev().cloned() {
            if scan.live_session.is_some() {
                match self.store.get_agent_session(&candidate).await {
                    // Retired sessions are never GC'd (the poisoned path
                    // hard-deletes) — leave them untouched here.
                    Ok(session)
                        if session.retired_at.is_none()
                            && session.status != AgentStatus::Deleted
                            && self.session_poisoned(&session) =>
                    {
                        scan.poisoned.push(candidate.clone());
                        scan.cleaned_up.push(candidate);
                    }
                    Ok(_) | Err(Error::NotFound(_)) => {}
                    Err(e) => return Err(e),
                }
                continue;
            }
            match self.store.get_agent_session(&candidate).await {
                // Soft-retired: inert — never resumable and never GC'd (the
                // poisoned path below hard-deletes, which must not touch a
                // retired session). Treated like a stale assignment (cleaned
                // up) while still serving as the specialist/model
                // inheritance source, mirroring the Deleted case.
                Ok(session) if session.retired_at.is_some() => {
                    if scan.inheritance_source.is_none() {
                        scan.inheritance_source = Some(session);
                    }
                    scan.cleaned_up.push(candidate);
                }
                Ok(session)
                    if session.status != AgentStatus::Deleted
                        && !self.session_poisoned(&session) =>
                {
                    if scan.inheritance_source.is_none() {
                        scan.inheritance_source = Some(session.clone());
                    }
                    scan.live_session = Some(session);
                }
                Ok(unusable_session) => {
                    if unusable_session.status != AgentStatus::Deleted {
                        tracing::warn!(
                            agent = %candidate,
                            stop_reason = unusable_session.stop_reason.as_deref().unwrap_or(""),
                            "assigned-agent scan skipping poisoned session; not resumable (monorepo#840)"
                        );
                        scan.poisoned.push(candidate.clone());
                    }
                    if scan.inheritance_source.is_none() {
                        scan.inheritance_source = Some(unusable_session);
                    }
                    scan.cleaned_up.push(candidate);
                }
                Err(Error::NotFound(_)) => scan.cleaned_up.push(candidate),
                Err(e) => return Err(e),
            }
        }
        Ok(scan)
    }

    /// `agent.wakeOrCreate` (PROTOCOL §5.5, widened by C1d-10a): resume the
    /// newest live/resumable agent assigned to the task, or — when none is
    /// found — create a new one with specialist/model inheritance from the
    /// most-recent previous session and the FE `WakeOrCreateTaskAgentTool`
    /// create payload (name, contextReferences, metadata, skipAutoCommit),
    /// then deliver the context message (optionally tagged with
    /// `messageMetadata`). Prunes stale assignments (`cleanedUpAgentIds`) and
    /// enforces `MAX_DELEGATION_DEPTH` when the caller provides
    /// `callerAgentId`/`delegationDepth`.
    pub(crate) async fn agent_wake_or_create_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        input: AgentWakeOrCreateInput,
    ) -> Result<Value> {
        // B3: delegation-depth guard. `parent_depth` mirrors the FE constant
        // (`MAX_DELEGATION_DEPTH = 2`, "error if parent >= 2" per the C1d-10
        // fence report). When neither `callerAgentId` nor `delegationDepth` is
        // provided the guard is a no-op (backward-compatible with the
        // pre-widening 3-param callers).
        let parent_depth = self.resolve_parent_delegation_depth(&input).await?;
        if let Some(depth) = parent_depth {
            if depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "agent.wakeOrCreate: delegation depth {depth} exceeds \
                     MAX_DELEGATION_DEPTH ({MAX_DELEGATION_DEPTH})"
                )));
            }
        }

        // monorepo#932: run the SUB-1 watch scope gate BEFORE any
        // side-effectful work (agent create/wake, task assignment, poisoned
        // queue migration, context-message delivery), mirroring
        // `agent_delegate_op`'s pre-gate. Without this, the shared gate inside
        // `register_completion_watch` would reject only AFTER those side
        // effects, hiding the freshly created/woken `agentId` behind the
        // error. The caller's session is resolved ONCE here (lookup via
        // `.ok()`; a failed lookup falls back to the call's workspace — a
        // trivial pass — at registration time) and threaded into the SUB-1
        // registration blocks on all three branches below, so the gating
        // decision cannot drift between two lookups and a pass here
        // guarantees the later shared gate cannot reject.
        let caller_session = match input.caller_agent_id.as_ref() {
            Some(caller) => self.store.get_agent_session(caller).await.ok(),
            None => None,
        };
        // monorepo#994: a Deleted caller can never receive the wake (the
        // `agent_watch_completion_op` deleted-parent rationale), so it gets
        // neither the pre-gate nor a SUB-1 watch — mirroring
        // `agent_delegate_op`'s deleted-parent guard. A failed lookup (`None`)
        // keeps the fallback-anchor behavior and still registers a watch.
        let caller_deleted = caller_session
            .as_ref()
            .is_some_and(|s| s.status == AgentStatus::Deleted);
        if let Some(session) = caller_session
            .as_ref()
            .filter(|s| s.status != AgentStatus::Deleted)
        {
            crate::agent_subscriptions::check_watch_scope(&session.workspace_id, &workspace_id)?;
        }
        // monorepo#3442: the caller becomes the created agent's parent ONLY
        // when its session actually resolved and is not Deleted. Derived from
        // `caller_session` (not the raw wire `callerAgentId`) so an unknown
        // client-supplied ID can never be persisted as a dangling parent —
        // a dangling parent would enable `agent.reportToParent` against a
        // nonexistent recipient and emit an unresolvable `parentAgentId`.
        let caller_parent = caller_session
            .as_ref()
            .filter(|s| s.status != AgentStatus::Deleted)
            .map(|s| s.id.clone());

        let task = self
            .get_my_task(workspace_id.clone(), task_note_id.clone())
            .await?;
        let task_title = task.title.clone();

        // B1 + B2: the newest-first live/resumable probe over the task's
        // assignments (see `scan_assigned_agents` for the full contract —
        // stale/poisoned tracking, inheritance source, newest live winner).
        let AssignedAgentScan {
            live_session,
            inheritance_source,
            mut cleaned_up,
            poisoned,
        } = self.scan_assigned_agents(&task.assigned_agents).await?;

        // B7: `messageMetadata` is applied to the delivered context message on
        // BOTH branches via `deliver_wake_message`.
        if let Some(session) = live_session {
            let agent_id = session.id.clone();
            let agent_name = session.name.clone();
            let agent_status = serde_json::to_value(session.status)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            // monorepo#847: migrate poisoned siblings' parked queues onto the
            // live target BEFORE the wake delivery below can claim a turn
            // (its turn-end drain consumes the target queue) and before the
            // explicit `try_drain_queue` kick. Migrating first means this
            // call starts no consumer that could dequeue from the target
            // mid-migration, sidestepping the helper's theoretical
            // failure-path duplicate (the rollback restore racing a
            // concurrent dequeue of an already-migrated entry).
            let failed = self
                .migrate_poisoned_queues_to(&poisoned, &agent_id, &workspace_id)
                .await;
            // Failed migrations stay assigned (and out of the response's
            // `cleanedUpAgentIds`) so the next wakeOrCreate retries them.
            cleaned_up.retain(|id| !failed.contains(id));
            let result = self
                .deliver_wake_message(
                    &workspace_id,
                    &agent_id,
                    &context_message,
                    input.message_metadata.as_ref(),
                )
                .await?;
            self.remove_agent_ids_from_workspace_tasks(&workspace_id, &cleaned_up)
                .await?;
            // B8: `action` distinguishes queued-to-active-agent from woke-existing
            // via the delivery's `queued` flag.
            let queued = result
                .get("queued")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            // Wave B: when the wake message was directly delivered (not queued),
            // force a queue drain so any previously-queued normal-priority messages
            // send immediately without waiting for an interrupt-priority nudge.
            // Post-restart recovery: a woken agent must resume pending work.
            if !queued {
                if let Some(mgr) = self.agent_manager() {
                    tokio::spawn({
                        let mgr = mgr.clone();
                        let agent_id = agent_id.clone();
                        let workspace_id = workspace_id.clone();
                        async move {
                            mgr.try_drain_queue(agent_id, workspace_id).await;
                        }
                    });
                }
            }
            let action = if queued {
                "message_queued_to_active_agent"
            } else {
                "woke_existing"
            };
            let mut response = build_wake_response(
                &agent_id.clone(),
                &agent_name,
                false,
                action,
                &task_title.clone(),
                &result,
                &cleaned_up,
            );
            // SUB-1: auto-subscribe the waking caller to the target's
            // completion (TS `WakeOrCreateTaskAgentTool`). Response text
            // mirrors the reference tool, including the notification line.
            //
            // SUB-2: repeated `wakeOrCreate` calls for the same caller/target
            // pair must not stack duplicate watches (which would multiply the
            // parent wakes on the next `agent:idle`). Reuse any existing live
            // ungrouped watch for this pair.
            //
            // monorepo#994: skipped entirely for a Deleted caller (see
            // `caller_deleted` at the pre-gate) — the response then keeps the
            // caller-less shape (no `subscriptionId` / `message`), mirroring
            // `agent_delegate_op`'s deleted-parent guard.
            if let Some(caller) = input.caller_agent_id.clone().filter(|_| !caller_deleted) {
                // Resolve the caller's current display name up front so a
                // fresh watch is registered with it, and a reused watch has
                // its stored `parent_agent_name` refreshed against the same
                // source (SUB-2 Copilot #104): agents can rename via
                // `agent.rename` / `agent.update`, and `describe_subscription`
                // formats using `watch.parent_agent_name`, so a long-lived
                // reused watch would otherwise report a stale `agentName` /
                // `description` from `agent.getSubscriptions`.
                // Copilot #104 thread PRRT_kwDOS9Wxuc6QKWuU: keep the
                // resolved name as `Option<String>` so a failed session
                // lookup does not overwrite an existing watch's stored
                // `parent_agent_name` with an empty placeholder on the
                // reuse path. Only the fresh-register branch has to
                // materialize a name, and there `""` matches the pre-fix
                // behaviour for a brand-new watch.
                // monorepo#932: `caller_session` is the single lookup the
                // pre-gate ran at the top of this op — reusing it keeps the
                // gating decision and this anchor resolution consistent (no
                // TOCTOU between two lookups) and saves a duplicate read.
                let caller_name = caller_session.as_ref().map(|s| s.name.clone());
                // The watch is anchored in the caller's HOME workspace (falls
                // back to the call's workspace when the session lookup fails)
                // so a chief caller's wake lands in `__chief__`. `resolved_home`
                // is Some only when read from a real session row: the reuse
                // path uses it to correct a fallback-registered anchor.
                let resolved_home = caller_session.map(|s| s.workspace_id);
                let caller_home_ws = resolved_home
                    .clone()
                    .unwrap_or_else(|| workspace_id.clone());
                // SUB-2 (Copilot #104 follow-up, thread
                // PRRT_kwDOS9Wxuc6QKPyt): resolve reuse atomically. If a live
                // ungrouped watch is found, its `parent_agent_name` is
                // refreshed under the same lock when a fresh name was
                // resolved; otherwise we fall through to registering a fresh
                // watch. This closes the race where a concurrent delivery
                // removed the watch between a prior find and refresh, which
                // would otherwise leave the caller subscribed to a dead id.
                let (subscription_id, reused) = if let Some(existing_id) = self
                    .find_and_refresh_ungrouped_watch(
                        &caller,
                        &agent_id,
                        caller_name.clone(),
                        resolved_home.as_ref(),
                    ) {
                    (existing_id, true)
                } else {
                    let new_id = self.register_completion_watch(
                        &caller_home_ws,
                        &workspace_id,
                        caller.clone(),
                        caller_name.unwrap_or_default(),
                        agent_id.clone(),
                        None,
                    )?;
                    (new_id, false)
                };
                if !reused {
                    self.publish_subscriptions_changed(&caller_home_ws, &caller)
                        .await;
                }
                let message = if queued {
                    format!(
                        "Agent \"{}\" is already actively working on task \"{task_title}\".\n\
                         Context message has been queued and will be delivered when the agent finishes its current response.\n\
                         You will be notified when the agent responds.",
                        agent_id.0
                    )
                } else {
                    format!(
                        "Woke existing agent \"{}\" for task \"{task_title}\".\n\
                         Agent status: {agent_status}\n\
                         Context message delivered.\nYou will be notified when the agent responds.",
                        agent_id.0
                    )
                };
                response["subscriptionId"] = json!(subscription_id);
                response["message"] = json!(message);
            }
            return Ok(response);
        }

        // Create branch: no live session.
        let create_opts = input.create.clone().unwrap_or_default();

        // SECURITY: the project tier comes from the stored workspace record.
        let workspace_path = self
            .store
            .get_workspace(&workspace_id)
            .await
            .ok()
            .and_then(|w| crate::git_ops::worktree_path(&w));
        // Strict validation (monorepo#3497): the client-supplied
        // `create.specialist` is validated even when an inherited specialist
        // wins the B4 precedence below (client input never bypasses the
        // `-32602`), and BEFORE the stale-assignment purge so a rejected
        // wake leaves task state untouched.
        if let Some(spec_id) = create_opts.specialist.as_deref() {
            // Validation walks the specialist tier directories — blocking
            // pool (monorepo#4148).
            let services = self.clone();
            let spec_id = spec_id.to_string();
            let wp = workspace_path.clone();
            tokio::task::spawn_blocking(move || {
                services
                    .specialists_service()
                    .canonical_id_or_err(&spec_id, wp.as_deref())
                    .map(|_| ())
            })
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "agent.wakeOrCreate specialist validation task failed: {e}"
                ))
            })??;
        }

        // Purge stale (NotFound / soft-deleted) assignments first so the
        // subsequent `assign_agent` starts from a clean list, then build the
        // rich create payload. Poisoned ids are deliberately NOT purged yet:
        // their assignment must survive a failed queue migration below so the
        // next wakeOrCreate can retry it; they are purged after a successful
        // migration instead.
        let stale_now: Vec<AgentId> = cleaned_up
            .iter()
            .filter(|id| !poisoned.contains(id))
            .cloned()
            .collect();
        self.remove_agent_ids_from_workspace_tasks(&workspace_id, &stale_now)
            .await?;
        // B4: specialist/model inheritance — the previous session's specialist
        // wins; the wake-level `model` override wins over both the previous
        // session's model and the `create.model` fallback. An INHERITED
        // specialist that no longer resolves (a stale id persisted before the
        // monorepo#3497 strict validation, or a since-deleted user/project
        // specialist file) is dropped with a warn instead of failing the wake
        // — the strict `-32602` above applies to the client-supplied
        // `create.specialist`, never to legacy stored state. Dropping means
        // the `.or()` below falls through to the (already-validated)
        // `create.specialist` when one was supplied, else no specialist.
        let inherited_specialist = match inheritance_source
            .as_ref()
            .and_then(|s| s.specialist.clone())
        {
            Some(spec_id) => {
                // The resolvability probe walks the specialist tiers —
                // blocking pool (monorepo#4148); a JoinError counts as
                // unresolved (the warn + fallback below), never failing the
                // wake.
                let services = self.clone();
                let wp = workspace_path.clone();
                let sid = spec_id.clone();
                let known = tokio::task::spawn_blocking(move || {
                    services
                        .specialists_service()
                        .canonical_id(&sid, wp.as_deref())
                        .is_some()
                })
                .await
                .unwrap_or(false);
                if !known {
                    tracing::warn!(
                        specialist = %spec_id,
                        fallback = create_opts.specialist.as_deref().unwrap_or("none"),
                        "agent.wakeOrCreate: dropping inherited specialist that no longer resolves; falling back to create.specialist"
                    );
                }
                known.then_some(spec_id)
            }
            None => None,
        };
        let specialist = inherited_specialist.or(create_opts.specialist.clone());
        let model = input
            .model
            .clone()
            .or_else(|| inheritance_source.as_ref().and_then(|s| s.model.clone()))
            .or(create_opts.model.clone());
        let provider = create_opts.provider.clone();
        let agent_type = create_opts.agent_type.clone();
        // Reasoning effort (PROTOCOL §5.11), create branch only: the
        // wake-level param wins over `create.reasoningEffort`, then the chosen
        // model option's effort, then the specialist frontmatter. Validated
        // against the cached catalog's `effortLevels` for the resolved model
        // before the child is created, so a `-32602` leaves no orphan.
        // Same effective-model rule as `agent.delegate`: fall through to the
        // full default-model resolution (specialist pin, then the settings
        // chain) so a `modelOptions` entry keyed on the settings default
        // model is still matched.
        // Same tier-walking resolvers as `agent.delegate` — blocking pool
        // (monorepo#4148).
        let (effort_model, reasoning_effort) = {
            let services = self.clone();
            let model = model.clone();
            let specialist = specialist.clone();
            let workspace_path = workspace_path.clone();
            let provider = provider.clone();
            let effort_param = input
                .reasoning_effort
                .clone()
                .or_else(|| create_opts.reasoning_effort.clone());
            tokio::task::spawn_blocking(move || {
                let effort_model = model.or_else(|| {
                    resolve_agent_default_model(
                        &services,
                        specialist.as_deref(),
                        workspace_path.as_deref(),
                        provider.as_deref(),
                    )
                });
                let reasoning_effort = resolve_delegate_reasoning_effort(
                    &services,
                    effort_param.as_deref(),
                    specialist.as_deref(),
                    effort_model.as_deref(),
                    workspace_path.as_deref(),
                );
                (effort_model, reasoning_effort)
            })
            .await
            .map_err(|e| {
                Error::Internal(format!("agent.wakeOrCreate resolution task failed: {e}"))
            })?
        };
        // A blank resolved value is an explicit clear (see
        // `resolve_delegate_reasoning_effort`); only a real level is validated.
        if let Some(effort) = reasoning_effort.as_deref().filter(|e| !e.trim().is_empty()) {
            ensure_effort_supported_by_model(
                "agent.wakeOrCreate",
                &self.models_catalog,
                effort_model.as_deref(),
                effort,
            )?;
        }

        // B5: rich create payload (`name` default `Task: {title}`,
        // `contextReferences` + provenance metadata folded into the persisted
        // metadata blob so the daemon-side session read-back retains them).
        let name = Some(
            create_opts
                .name
                .clone()
                .unwrap_or_else(|| format!("Task: {task_title}")),
        );
        // B6: honor `create.skipAutoCommit` from the request; like
        // `agent.delegate`, the effective opt-out is the caller's explicit
        // flag OR the workspace's effective auto-commit being off, so
        // wakeOrCreate-created task agents carry the same persisted opt-out
        // (and OFF first-message instruction) as delegated children.
        let skip_auto_commit = create_opts.skip_auto_commit.unwrap_or(false)
            || !self.effective_auto_commit(&workspace_id).await;
        let metadata = build_create_metadata(
            &create_opts,
            &input,
            &task_note_id,
            parent_depth,
            agent_type.clone(),
        );
        let extra = AgentCreateExtra {
            provider,
            reasoning_effort,
            agent_type,
            metadata,
            workspace_path: None,
            workspace_context: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: None,
            name_explicitly_set: None,
        };
        // monorepo#3442: the resolved live caller (`caller_parent`, derived
        // from the single `caller_session` lookup — never the raw wire
        // `callerAgentId`) becomes the created agent's `parent_agent_id`, so
        // a wakeOrCreate-created agent is a delegated child
        // (`agent.reportToParent` works, attention/failure events carry the
        // parent). A Deleted caller and an unknown/unresolvable `callerAgentId`
        // both stay parentless. This is deliberately STRICTER than
        // `agent_delegate_op`, which passes its parent unfiltered and only
        // skips watch registration for a Deleted parent — here a parent that
        // can never receive the report wake is not recorded at all. Note
        // `build_create_metadata` above still records the raw wire caller as
        // `createdByAgentId` (creation provenance) even when the parent is
        // filtered out, so the two fields can intentionally diverge on the
        // deleted-caller path. Depth guards: the B3 pre-check reads the wire
        // `delegationDepth` (else the caller's `metadata.delegationDepth`),
        // while `agent_create_op`'s LC-1 guard reads the parent's persisted
        // `delegation_depth` column — so a caller whose column is at
        // `MAX_DELEGATION_DEPTH` passing an explicit lower wire depth clears
        // B3 but is rejected by LC-1. That pass-then-reject is intentional
        // fail-closed behavior: the wire value cannot bypass the stored cap.
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                name,
                model,
                specialist,
                caller_parent,
                Some(task_note_id.clone()),
                skip_auto_commit,
                extra,
            )
            .await?;
        let agent_lite = &created["agent"];
        let agent_id_str = agent_lite
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let agent_name = agent_lite
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let agent = AgentId::from(agent_id_str.as_str());
        // The scan above already established there is no live assigned agent
        // (create branch), so this internal assignment bypasses
        // `assign_agent`'s occupancy guard.
        let _ = self
            .assign_agent(workspace_id.clone(), task_note_id, agent_id_str, Some(true))
            .await;
        // monorepo#847: same ordering contract as the wake branch — migrate
        // the poisoned siblings' parked queues BEFORE `deliver_wake_message`
        // spawns the fresh agent's first turn, so no drain races the
        // migration and the migrated messages are picked up at turn end.
        // Specialist/model inheritance was resolved from `inheritance_source`
        // (an owned clone captured in the candidate loop) above, before this
        // GC hard-deletes the poisoned session row.
        let failed = self
            .migrate_poisoned_queues_to(&poisoned, &agent, &workspace_id)
            .await;
        // Successfully migrated poisoned ids can now be purged from the
        // task's assignments; failed ones stay assigned (and out of the
        // response's `cleanedUpAgentIds`) so the next wakeOrCreate retries.
        cleaned_up.retain(|id| !failed.contains(id));
        let migrated: Vec<AgentId> = poisoned
            .iter()
            .filter(|id| !failed.contains(id))
            .cloned()
            .collect();
        self.remove_agent_ids_from_workspace_tasks(&workspace_id, &migrated)
            .await?;
        let result = self
            .deliver_wake_message(
                &workspace_id,
                &agent,
                &context_message,
                input.message_metadata.as_ref(),
            )
            .await?;
        let mut response = build_wake_response(
            &agent.clone(),
            &agent_name,
            true,
            "created_new",
            &task_title.clone(),
            &result,
            &cleaned_up,
        );
        // SUB-1 parity (monorepo#926): auto-subscribe the waking caller to the
        // created agent's completion, mirroring the woke-existing branch. The
        // child id was freshly minted this call, so no live watch can exist
        // for the pair — always a fresh registration (no SUB-2 reuse: a
        // brand-new agent has no in-flight turn for the context message to
        // queue behind).
        //
        // monorepo#994: skipped entirely for a Deleted caller (see
        // `caller_deleted` at the pre-gate), mirroring `agent_delegate_op`'s
        // deleted-parent guard.
        if let Some(caller) = input.caller_agent_id.clone().filter(|_| !caller_deleted) {
            // monorepo#932: reuse the single pre-gate session lookup so the
            // gating decision and this anchor resolution cannot diverge.
            let caller_name = caller_session
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            // The watch is anchored in the caller's HOME workspace (falls
            // back to the call's workspace when the session lookup fails)
            // so a chief caller's wake lands in `__chief__`.
            let caller_home_ws =
                caller_session.map_or_else(|| workspace_id.clone(), |s| s.workspace_id);
            let subscription_id = self.register_completion_watch(
                &caller_home_ws,
                &workspace_id,
                caller.clone(),
                caller_name,
                agent.clone(),
                None,
            )?;
            self.publish_subscriptions_changed(&caller_home_ws, &caller)
                .await;
            response["subscriptionId"] = json!(subscription_id);
            response["message"] = json!(format!(
                "Created new agent \"{}\" for task \"{task_title}\".\n\
                 Context message delivered.\nYou will be notified when the agent responds.",
                agent.0
            ));
        }
        Ok(response)
    }

    /// Resolve the effective **parent** delegation depth for the
    /// `agent.wakeOrCreate` guard. `delegation_depth` on the wire wins when
    /// present (the FE surfaces it explicitly). Otherwise, when
    /// `caller_agent_id` is provided, read the caller's persisted
    /// `session.metadata.delegationDepth` (default `0`). Missing caller
    /// context → `None` (no guard).
    async fn resolve_parent_delegation_depth(
        &self,
        input: &AgentWakeOrCreateInput,
    ) -> Result<Option<i64>> {
        if let Some(depth) = input.delegation_depth {
            return Ok(Some(depth));
        }
        let Some(caller) = input.caller_agent_id.as_ref() else {
            return Ok(None);
        };
        match self.store.get_agent_session(caller).await {
            Ok(session) => Ok(Some(
                session
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("delegationDepth"))
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `agent.wakeOrCreate` context-message delivery (both branches). When
    /// `message_metadata` is `Some`, it is folded onto the persisted text
    /// block as `messageMetadata` so subscribers/`agent.getConversation`
    /// consumers see the FE tag (`{type:'task_wake', source, taskNoteId,
    /// callerAgentId}`) verbatim; when `None`, the block matches the plain
    /// `agent.sendMessage` shape.
    ///
    /// **Runtime drive (DELIV-1).** When the [`AgentManager`] is attached,
    /// the context message is delivered as a real turn (the proven
    /// `agent.sendMessage` shape: try to claim the in-flight slot, persist
    /// the wake-tagged user block, spawn the turn worker) so the newly
    /// woken/created agent actually processes the message. A busy assignee
    /// gets the wake enqueued behind the running turn — its drain loop
    /// picks the message up at turn end, and the queue entry carries the
    /// `messageMetadata` so the drained re-persist keeps the FE tag on the
    /// user message row. Read-only/test wiring with no manager
    /// falls back to the pre-DELIV-1 store-only persist so hermetic tests
    /// keep working. Auto-queue-on-store-failure mirrors
    /// [`Services::agent_send_message_op`].
    pub(crate) async fn deliver_wake_message(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        content: &str,
        message_metadata: Option<&Value>,
    ) -> Result<Value> {
        // Up-front vanished-session gate (intent-hq/monorepo#2762): reject
        // nonexistent targets BEFORE any state change (the monorepo#564
        // contract). This covers the enqueue-only routes below
        // (archived-workspace park, question hold, busy-agent fast enqueue)
        // that return queued success without ever touching `agent_message` —
        // without it a wake racing an `agent.delete` parks a phantom entry no
        // drain can ever deliver. The append-failure arms keep their own
        // NotFound re-check as the check-then-act race guard.
        if matches!(
            self.store.get_agent_session_status(agent_id).await,
            Err(Error::NotFound(_))
        ) {
            self.drop_queue(agent_id);
            return Err(Error::InvalidParams(format!(
                "unknown agent id: {}",
                agent_id.0
            )));
        }
        // A2A sender header (intent-hq/intent#3721, monorepo#1015): the wake front door — the
        // `agent.wakeOrCreate` context message carries the daemon-stamped
        // attribution, and this path persists/enqueues directly (it never
        // routes through `send_message`/`agent_send_message_op`), so the
        // prepend happens here, before every branch below.
        let annotated = {
            let mut c = content.to_string();
            annotate_sender_attribution(&mut c, message_metadata);
            c
        };
        let content = annotated.as_str();
        let build_block = || match message_metadata {
            Some(md) => json!({ "type": "text", "text": content, "messageMetadata": md }),
            None => json!({ "type": "text", "text": content }),
        };
        let Some(manager) = self.agent_manager() else {
            return self
                .deliver_wake_message_store_only(
                    workspace_id,
                    agent_id,
                    content,
                    message_metadata,
                    build_block,
                )
                .await;
        };
        // Archived-workspace gate (mirrors `try_drain_queue`'s): a wake must
        // not start a turn while the workspace is archived — it parks in the
        // queue until unarchive, whose drain kick delivers it (see
        // `unarchive_workspace`). Chief is virtual and never archived, so
        // skip the row read. Fail open on a lookup error: the gate only
        // parks affirmatively-archived workspaces; a transient store error
        // must not swallow a wake.
        if !workspace_id.is_chief() {
            match self.store.get_workspace(workspace_id).await {
                Ok(ws) if ws.archived => {
                    // Test seam (intent-hq/monorepo#2739): park in the
                    // archived-check → enqueue window so a test can land a
                    // concurrent `workspace.unarchive` between the gate's
                    // read above and the enqueue below.
                    if let Some(park) = &self.wake_archived_park {
                        park.entered.notify_one();
                        park.release.notified().await;
                    }
                    let (queued, position) = self.enqueue_message(
                        agent_id,
                        content.to_string(),
                        None,
                        None,
                        message_metadata.cloned(),
                        None,
                        false,
                    );
                    let result = json!({
                        "success": true,
                        "queued": true,
                        "archivedParked": true,
                        "queuedMessage": queued.to_value(position),
                    });
                    self.publish_queue_updated(agent_id).await;
                    // Race close (archived-check → enqueue vs a concurrent
                    // `workspace.unarchive`): the unarchive's own drain kick
                    // may have fired against a still-empty queue before this
                    // enqueue landed, stranding the wake. Re-check and kick
                    // the drain if the workspace is no longer archived; the
                    // archived gate in `try_drain_queue` makes this a no-op
                    // while still archived.
                    if let Ok(current) = self.store.get_workspace(workspace_id).await {
                        if !current.archived {
                            manager
                                .clone()
                                .try_drain_queue(agent_id.clone(), workspace_id.clone())
                                .await;
                        }
                    }
                    return Ok(result);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        agent = %agent_id.0,
                        workspace = %workspace_id.as_str(),
                        error = %e,
                        "wake delivery: workspace archived-state lookup failed; proceeding"
                    );
                }
            }
        }
        // Soft-retire gate (mirrors the archived-workspace gate above): a
        // wake must not start a turn on a retired session — hook dispatches,
        // PR-monitor wakes, and batch-delegate advisory wakes can all still
        // target one, since retiring does not cancel those sources. Park the
        // wake in the queue; `agent.restore` returns the session to service
        // and its drain kick delivers it. Fail open on a lookup error — the
        // gate only parks affirmatively-retired sessions.
        match self.store.get_agent_session_retired_at(agent_id).await {
            Ok(Some(_)) => {
                let (queued, position) = self.enqueue_message(
                    agent_id,
                    content.to_string(),
                    None,
                    None,
                    message_metadata.cloned(),
                    None,
                    false,
                );
                let result = json!({
                    "success": true,
                    "queued": true,
                    "retiredParked": true,
                    "queuedMessage": queued.to_value(position),
                });
                self.publish_queue_updated(agent_id).await;
                // Race close (retired-check → enqueue vs a concurrent
                // `agent.restore`): the restore's drain kick may have fired
                // against a still-empty queue before this enqueue landed,
                // stranding the wake. Re-check and kick the drain if the
                // session is no longer retired; the retired gate in
                // `try_drain_queue` makes this a no-op while still retired.
                if let Ok(None) = self.store.get_agent_session_retired_at(agent_id).await {
                    manager
                        .clone()
                        .try_drain_queue(agent_id.clone(), workspace_id.clone())
                        .await;
                }
                return Ok(result);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "wake delivery: retired-state lookup failed; proceeding"
                );
            }
        }
        // Runtime path (DELIV-1): two-step claim/persist/spawn so the
        // user-message row is on disk BEFORE the turn worker starts, and no
        // worker is ever spawned for a row that failed to persist:
        //   1. Claim the in-flight slot (busy assignee → enqueue branch).
        //   2. Persist the wake-tagged user block. Slot released on failure
        //      and the message enqueued, then a best-effort
        //      `AgentManager::try_drain_queue` kick so the queued wake is
        //      picked up while the agent is idle (mirrors the
        //      `AgentManager::send_message` auto-queue fallback).
        //   3. Spawn the worker with the same content in-memory (the worker
        //      path does not re-persist).
        let content_owned = content.to_string();
        // Question hold (PROTOCOL §5.5): wakes are automatic by definition
        // (`agent.wakeOrCreate` context messages, reportToParent /
        // completion-watch wakes) — while the target's hold is active they
        // park in the queue instead of claiming the slot, so the pending Q&A
        // is never superseded. Checked BEFORE `try_begin_turn` so even an
        // idle asking agent holds the wake.
        if self.question_hold_active(agent_id).await {
            let (queued, position) = self.enqueue_message(
                agent_id,
                content_owned,
                None,
                None,
                message_metadata.cloned(),
                None,
                false,
            );
            self.publish_queue_updated(agent_id).await;
            // Race close (hold-check → enqueue vs a concurrent
            // `dismissQuestions`/answer): re-check and kick the drain if the
            // hold cleared while the enqueue above was in flight, mirroring
            // `AgentManager::send_message`'s hold-gate re-check — otherwise
            // this entry could be stranded with no future drain trigger.
            if !self.question_hold_active(agent_id).await {
                manager
                    .clone()
                    .try_drain_queue(agent_id.clone(), workspace_id.clone())
                    .await;
            }
            return Ok(json!({
                "success": true,
                "queued": true,
                "heldForQuestions": true,
                "queuedMessage": queued.to_value(position),
            }));
        }
        if !manager.try_begin_turn(agent_id, workspace_id).await {
            // Fast enqueue branch: the manager is already draining a turn. The
            // metadata rides along on the queue entry so the drain re-persist
            // keeps the wake tag.
            let (queued, position) = self.enqueue_message(
                agent_id,
                content_owned,
                None,
                None,
                message_metadata.cloned(),
                None,
                false,
            );
            self.publish_queue_updated(agent_id).await;
            return Ok(json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
            }));
        }
        let blocks = json!([build_block()]);
        let created_at = now_iso();
        // Row-level metadata rides along with the in-block fold (monorepo#1217)
        // so wake deliveries match the direct-send and queue-drain persists —
        // the FE attribution chip reads the row's `metadata` column.
        let message = match self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "user",
                &blocks,
                message_metadata,
                &created_at,
            )
            .await
        {
            Ok(msg) => {
                self.invalidate_agent_list_cache(workspace_id);
                msg
            }
            Err(append_err) => {
                manager.release_slot(agent_id).await;
                // Fail closed on a vanished session (intent-hq/monorepo#2762):
                // the only FK on `agent_message` is `agent_id →
                // agent_session(id)`, so an append failure against a gone row
                // means the agent was deleted. Auto-queueing here would park a
                // phantom entry no drain can ever deliver (every re-append
                // re-fails 787) — reject with the same `unknown agent id`
                // contract as `agent.sendMessage`'s monorepo#564 guard and
                // drop any stale queue entries for the gone agent.
                if matches!(
                    self.store.get_agent_session_status(agent_id).await,
                    Err(Error::NotFound(_))
                ) {
                    tracing::warn!(
                        agent = %agent_id.0,
                        error = %append_err,
                        "agent session vanished mid-wake; rejecting instead of queueing (monorepo#2762)"
                    );
                    self.drop_queue(agent_id);
                    return Err(Error::InvalidParams(format!(
                        "unknown agent id: {}",
                        agent_id.0
                    )));
                }
                let (queued, position) = self.enqueue_message(
                    agent_id,
                    content_owned,
                    None,
                    None,
                    message_metadata.cloned(),
                    None,
                    false,
                );
                self.publish_queue_updated(agent_id).await;
                manager
                    .clone()
                    .try_drain_queue(agent_id.clone(), workspace_id.clone())
                    .await;
                return Ok(json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                }));
            }
        };
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        }
        // Publish agent:message events using the store-returned message id.
        // Wake deliveries carry no user retry record, so no turnId (spec
        // non-goal — the worker still mints one internally at spawn).
        self.publish_agent_message_events(workspace_id, agent_id, &message, None)
            .await;
        manager.clone().finish_prepersisted_turn_spawn(
            agent_id.clone(),
            workspace_id.clone(),
            content_owned,
            crate::agent_manager::TurnOptions::default(),
        );
        Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
    }

    /// Pre-DELIV-1 store-only delivery for the read-only/test path (no
    /// [`AgentManager`] attached): persist the wake-tagged block, and on
    /// store failure fall back to an in-memory enqueue with `queued: true`
    /// (the enqueue keeps `message_metadata` so the wake tag survives a
    /// later drain).
    async fn deliver_wake_message_store_only<F>(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        content: &str,
        message_metadata: Option<&Value>,
        build_block: F,
    ) -> Result<Value>
    where
        F: Fn() -> Value,
    {
        // Question hold (PROTOCOL §5.5): same automatic-delivery gate as the
        // runtime path above — the store-only persist would append the user
        // row whose turn would bury the pending Q&A, so park the wake in the queue
        // instead (hermetic wiring keeps the hold contract testable).
        if self.question_hold_active(agent_id).await {
            let (queued, position) = self.enqueue_message(
                agent_id,
                content.to_string(),
                None,
                None,
                message_metadata.cloned(),
                None,
                false,
            );
            let result = json!({
                "success": true,
                "queued": true,
                "heldForQuestions": true,
                "queuedMessage": queued.to_value(position),
            });
            self.publish_queue_updated(agent_id).await;
            // Race close (hold-check → enqueue vs a concurrent
            // `dismissQuestions`/answer), same shape as the runtime path
            // above. This wiring has no attached `AgentManager` by
            // definition (that is why we are in the store-only fallback),
            // so there is nothing to kick — the re-check only matters if a
            // manager is (or becomes) attached, which `try_drain_queue`
            // itself would then handle on its own next trigger.
            if !self.question_hold_active(agent_id).await {
                if let Some(manager) = self.agent_manager() {
                    manager
                        .try_drain_queue(agent_id.clone(), workspace_id.clone())
                        .await;
                }
            }
            return Ok(result);
        }
        let blocks = json!([build_block()]);
        let created_at = now_iso();
        // Row-level metadata parity with the runtime branch (monorepo#1217).
        match self
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "user",
                &blocks,
                message_metadata,
                &created_at,
            )
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                // Refresh agent_session.updated_at so the FE agent-card timestamp
                // reflects message activity, not just status transitions (STAB-19).
                if let Err(e) = self
                    .store
                    .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
                }
                // Publish agent:message events using the store-returned message id.
                self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                    .await;
                Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
            }
            Err(append_err) => {
                // Fail closed on a vanished session (intent-hq/monorepo#2762):
                // same contract as the runtime branch above — never park a
                // phantom entry for a deleted agent.
                if matches!(
                    self.store.get_agent_session_status(agent_id).await,
                    Err(Error::NotFound(_))
                ) {
                    tracing::warn!(
                        agent = %agent_id.0,
                        error = %append_err,
                        "agent session vanished mid-wake; rejecting instead of queueing (monorepo#2762)"
                    );
                    self.drop_queue(agent_id);
                    return Err(Error::InvalidParams(format!(
                        "unknown agent id: {}",
                        agent_id.0
                    )));
                }
                let (queued, position) = self.enqueue_message(
                    agent_id,
                    content.to_string(),
                    None,
                    None,
                    message_metadata.cloned(),
                    None,
                    false,
                );
                let result = json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                });
                self.publish_queue_updated(agent_id).await;
                Ok(result)
            }
        }
    }

    /// Daemon-side equivalent of the FE `task.removeAgentFromAllTasks`: strip
    /// the given agent ids from every task note in the workspace. Silent on
    /// notes/tasks that never referenced the ids so it is safe to call with an
    /// empty or partially-stale list.
    async fn remove_agent_ids_from_workspace_tasks(
        &self,
        workspace_id: &WorkspaceId,
        stale: &[AgentId],
    ) -> Result<()> {
        if stale.is_empty() {
            return Ok(());
        }
        let notes = self.store.list_notes(workspace_id).await?;
        for mut note in notes {
            let Some(mut task) = note.metadata.task.clone() else {
                continue;
            };
            let before = task.assigned_agent_ids.len();
            task.assigned_agent_ids.retain(|a| !stale.contains(a));
            if task.assigned_agent_ids.len() == before {
                continue;
            }
            let now = now_iso();
            note.metadata.task = Some(task);
            note.updated_at = now;
            self.store.update_note(&note).await?;
        }
        Ok(())
    }

    /// Load a session, mapping `NotFound` to `-32603` for the methods whose TS
    /// peers surface a generic failure (rename/setModel/summary).
    async fn load_session_internal(&self, agent_id: &AgentId) -> Result<AgentSession> {
        match self.store.get_agent_session(agent_id).await {
            Ok(s) => Ok(s),
            Err(Error::NotFound(_)) => {
                Err(Error::Internal(format!("Agent \"{agent_id}\" not found")))
            }
            Err(e) => Err(e),
        }
    }

    /// Push a message onto an agent's in-memory queue and return it together with
    /// its 0-based `position` in the queue (the index just appended). New messages
    /// are always ready-to-send (`editing = false`) — the FE may transition an
    /// entry to `editing = true` later via `agent.editQueuedMessage`.
    /// `message_metadata` carries an internal wake's `messageMetadata` (e.g.
    /// `event_notification`) so the drain can persist it on the user message
    /// row; user-typed enqueues pass `None`. `prepend` threads the caller's
    /// combined-delivery `prepend_*` fields onto the entry (monorepo#1034) so
    /// a queue-fallback interrupt still delivers the preempted message ahead
    /// of the interrupt message on drain; sends without prepend content pass
    /// `None`.
    ///
    /// `interrupt` marks a `priority: "interrupt"` enqueue (question hold /
    /// PROTOCOL §5.5): the entry is inserted AFTER the queue's leading
    /// interrupt entries but AHEAD of all normal entries — arrival order is
    /// preserved among interrupts, and every fallback path that parks an
    /// interrupt (hold gate, busy race, quarantine park, append-failure
    /// auto-queue) shares this ordering. Normal enqueues append at the tail.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn enqueue_message(
        &self,
        agent_id: &AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
        prepend: Option<QueuedPrepend>,
        interrupt: bool,
    ) -> (QueuedMessage, usize) {
        self.enqueue_message_with_origin(
            agent_id,
            content,
            image_blocks,
            file_blocks,
            message_metadata,
            prepend,
            interrupt,
            false,
        )
    }

    /// [`Services::enqueue_message`] with an explicit `user_origin` marker:
    /// `true` records that the entry carries a USER-originated
    /// `agent.sendMessage` parked by a queue-fallback path (busy race,
    /// quarantine, append-failure). The question-hold drain gates deliver
    /// user-origin entries instead of suspending (PROTOCOL §5.5: a user
    /// answer must never deadlock against the hold its answer tag releases).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn enqueue_message_with_origin(
        &self,
        agent_id: &AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
        prepend: Option<QueuedPrepend>,
        interrupt: bool,
        user_origin: bool,
    ) -> (QueuedMessage, usize) {
        self.enqueue_message_with_id_and_origin(
            agent_id,
            None,
            content,
            image_blocks,
            file_blocks,
            message_metadata,
            prepend,
            interrupt,
            user_origin,
        )
    }

    /// Queue a message under a caller-selected durable id. Completion-watch
    /// delivery uses this so a restart retry adopts the already-persisted queue
    /// entry instead of creating a duplicate terminal wake.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn enqueue_message_with_id_and_origin(
        &self,
        agent_id: &AgentId,
        message_id: Option<String>,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
        prepend: Option<QueuedPrepend>,
        interrupt: bool,
        user_origin: bool,
    ) -> (QueuedMessage, usize) {
        let prepend = prepend.unwrap_or_default();
        let id = message_id.unwrap_or_else(new_message_id);
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.entry(agent_id.clone()).or_default();
        if let Some((position, existing)) =
            queue.iter().enumerate().find(|(_, queued)| queued.id == id)
        {
            return (existing.clone(), position);
        }
        let queued = QueuedMessage {
            turn_id: id.clone(),
            id,
            content,
            image_blocks,
            file_blocks,
            queued_at: now_iso(),
            editing: false,
            persisted: false,
            requeued_after_failure: false,
            message_metadata,
            prepend_content: prepend.content,
            prepend_image_blocks: prepend.image_blocks,
            prepend_file_blocks: prepend.file_blocks,
            interrupt_priority: interrupt,
            user_origin,
            hold_kind: None,
            hold_until: None,
            child_agent_id: None,
        };
        let position = if interrupt {
            // Behind earlier interrupts, ahead of every normal entry.
            let idx = queue.iter().take_while(|m| m.interrupt_priority).count();
            queue.insert(idx, queued.clone());
            idx
        } else {
            queue.push(queued.clone());
            queue.len() - 1
        };
        (queued, position)
    }

    /// Enqueue (or refresh) a **held** entry on an agent's queue: the entry
    /// carries a `(hold_kind, hold_until, child_agent_id)` debounce-hold
    /// marker, is excluded from every drain path until released, persists
    /// through the write-through snapshot, and gets a per-entry release timer
    /// that flushes the hold at `hold_until` and kicks delivery. Upsert by
    /// `(child_agent_id, hold_kind)`: when a held entry for the same key
    /// already exists, its content/metadata/deadline are replaced in place
    /// (same entry id, same queue position) and the timer is re-armed —
    /// repeated debounce extensions never accumulate duplicate entries.
    pub(crate) async fn enqueue_held_message(
        &self,
        agent_id: &AgentId,
        content: String,
        message_metadata: Option<Value>,
        hold_kind: &str,
        hold_until: &str,
        child_agent_id: &str,
    ) -> (QueuedMessage, usize) {
        let (queued, position) = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let queue = guard.entry(agent_id.clone()).or_default();
            let existing = queue.iter_mut().enumerate().find(|(_, m)| {
                m.hold_kind.as_deref() == Some(hold_kind)
                    && m.child_agent_id.as_deref() == Some(child_agent_id)
            });
            if let Some((position, entry)) = existing {
                entry.content = content;
                entry.message_metadata = message_metadata;
                entry.hold_until = Some(hold_until.to_string());
                entry.queued_at = now_iso();
                (entry.clone(), position)
            } else {
                let id = new_message_id();
                let queued = QueuedMessage {
                    turn_id: id.clone(),
                    id,
                    content,
                    image_blocks: None,
                    file_blocks: None,
                    queued_at: now_iso(),
                    editing: false,
                    persisted: false,
                    requeued_after_failure: false,
                    message_metadata,
                    prepend_content: None,
                    prepend_image_blocks: None,
                    prepend_file_blocks: None,
                    interrupt_priority: false,
                    user_origin: false,
                    hold_kind: Some(hold_kind.to_string()),
                    hold_until: Some(hold_until.to_string()),
                    child_agent_id: Some(child_agent_id.to_string()),
                };
                queue.push(queued.clone());
                (queued, queue.len() - 1)
            }
        };
        self.arm_hold_release_timer(agent_id, &queued.id, hold_until);
        self.publish_queue_updated(agent_id).await;
        (queued, position)
    }

    /// Release the held entry keyed by `(child_agent_id, hold_kind)`: clear
    /// its hold marker so it becomes ready-to-send, cancel its release timer,
    /// persist/publish the updated queue, and kick delivery (wakes an idle
    /// agent; a busy agent picks the entry up at its next drain). Returns
    /// `true` iff a held entry existed for the key.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn release_held_message(
        &self,
        agent_id: &AgentId,
        child_agent_id: &str,
        hold_kind: &str,
    ) -> bool {
        let released = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            guard.get_mut(agent_id).and_then(|queue| {
                queue
                    .iter_mut()
                    .find(|m| {
                        m.hold_kind.as_deref() == Some(hold_kind)
                            && m.child_agent_id.as_deref() == Some(child_agent_id)
                    })
                    .map(|m| {
                        m.hold_kind = None;
                        m.hold_until = None;
                        m.id.clone()
                    })
            })
        };
        let Some(message_id) = released else {
            return false;
        };
        self.cancel_hold_release_timer(&message_id);
        self.publish_queue_updated(agent_id).await;
        self.kick_queue_drain(agent_id).await;
        true
    }

    /// Retract (remove) the held entry keyed by `(child_agent_id, hold_kind)`
    /// without delivering it, cancelling its release timer and persisting the
    /// shrunken queue. Returns the removed entry when one existed for the key
    /// (so the caller can fold its metadata into a combined wake) — `None`
    /// when the hold already flushed, was released, or never existed.
    pub(crate) async fn retract_held_message(
        &self,
        agent_id: &AgentId,
        child_agent_id: &str,
        hold_kind: &str,
    ) -> Option<QueuedMessage> {
        let removed = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            guard.get_mut(agent_id).and_then(|queue| {
                queue
                    .iter()
                    .position(|m| {
                        m.hold_kind.as_deref() == Some(hold_kind)
                            && m.child_agent_id.as_deref() == Some(child_agent_id)
                    })
                    .map(|idx| queue.remove(idx))
            })
        };
        let entry = removed?;
        self.cancel_hold_release_timer(&entry.id);
        self.publish_queue_updated(agent_id).await;
        Some(entry)
    }

    /// Restore a retracted held entry after the combined wake it was folded
    /// into failed to commit (the settlement-combine failure path in
    /// `deliver_completion_to_watches`): re-insert the entry verbatim —
    /// same id, hold marker, and deadline — re-arm its release timer, and
    /// persist/publish, so the stable-id retry can retract and fold it again
    /// instead of losing the report event. No-op when a held entry for the
    /// same `(child_agent_id, hold_kind)` key already exists: a fresh report
    /// enqueued in the failure window supersedes the retracted one, and the
    /// restore must not resurrect a stale duplicate beside it. An already
    /// expired `hold_until` is fine — the re-armed timer fires immediately
    /// and flushes the entry (fail open, nothing is stranded).
    pub(crate) async fn restore_held_message(&self, agent_id: &AgentId, entry: QueuedMessage) {
        let hold_until = entry.hold_until.clone().unwrap_or_default();
        let message_id = entry.id.clone();
        let restored = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let queue = guard.entry(agent_id.clone()).or_default();
            let superseded = queue.iter().any(|m| {
                m.hold_kind.is_some()
                    && m.hold_kind == entry.hold_kind
                    && m.child_agent_id == entry.child_agent_id
            });
            if superseded {
                false
            } else {
                queue.push(entry);
                true
            }
        };
        if !restored {
            return;
        }
        self.arm_hold_release_timer(agent_id, &message_id, &hold_until);
        self.publish_queue_updated(agent_id).await;
    }

    /// `true` when a queued entry is an undelivered `agent.reportToParent`
    /// progress wake from `child_agent_id`: every event row of its
    /// `event_notification` metadata is an `agent:reportToParent` event whose
    /// `data.agentId` is the child. Matching on metadata rather than the
    /// `child_agent_id` hold stamp also identifies the immediate
    /// (debounce = 0) wake path, which enqueues via the generic busy-parent
    /// fallback without the stamp. The all-rows guard keeps combined wakes
    /// (report + terminal events) out of the match.
    fn is_undelivered_report_wake(entry: &QueuedMessage, child_agent_id: &str) -> bool {
        let Some(events) = entry
            .message_metadata
            .as_ref()
            .and_then(|m| m.get("events"))
            .and_then(|e| e.as_array())
        else {
            return false;
        };
        !events.is_empty()
            && events.iter().all(|e| {
                e.get("type").and_then(|t| t.as_str()) == Some("agent:reportToParent")
                    && e.get("data")
                        .and_then(|d| d.get("agentId"))
                        .and_then(|a| a.as_str())
                        == Some(child_agent_id)
            })
    }

    /// Retract (remove) every report wake from `child_agent_id` that already
    /// FLUSHED out of its debounce hold (or was enqueued directly by the
    /// immediate debounce = 0 path) but is still sitting undelivered on the
    /// parent's queue — the flushed mirror of
    /// [`Services::retract_held_message`]: the terminal settlement supersedes
    /// these wakes just the same, so the caller folds their metadata into the
    /// combined wake instead of letting them drain as stale standalone report
    /// wakes beside it. Held entries are excluded (they are the
    /// `(child_agent_id, hold_kind)` key's domain), as are entries under
    /// edit. Returns the removed entries in queue order — oldest first — so
    /// the caller can fold them chronologically and restore them verbatim on
    /// a failed send; empty when nothing matched.
    ///
    /// Crash window (shared with [`Services::retract_held_message`]): the
    /// shrunken queue persists before the combined wake commits, so a crash
    /// in between drops the queued metadata row. Bounded loss by design: the
    /// report CONTENT is persisted on the child's session
    /// (`completion_report`), and boot reconciliation replays the settlement
    /// whose wake text renders that persisted report — only the
    /// machine-readable `agent:reportToParent` metadata row of the lost
    /// entry is not re-folded. Retracting only after the wake commits would
    /// trade this for the worse inverse: a crash after the commit but before
    /// the retract leaves the stale report wake to drain beside the durable
    /// terminal wake — the exact duplicate this retract exists to prevent.
    pub(crate) async fn retract_flushed_report_messages(
        &self,
        agent_id: &AgentId,
        child_agent_id: &str,
    ) -> Vec<QueuedMessage> {
        let removed: Vec<QueuedMessage> = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            guard.get_mut(agent_id).map_or_else(Vec::new, |queue| {
                let mut removed = Vec::new();
                let mut idx = 0;
                while idx < queue.len() {
                    let m = &queue[idx];
                    if m.hold_kind.is_none()
                        && !m.editing
                        && Self::is_undelivered_report_wake(m, child_agent_id)
                    {
                        removed.push(queue.remove(idx));
                    } else {
                        idx += 1;
                    }
                }
                removed
            })
        };
        if removed.is_empty() {
            return removed;
        }
        for entry in &removed {
            // A just-flushed entry has no timer left, but cancel defensively:
            // retract can race a stale sleeper still waiting on the lock.
            self.cancel_hold_release_timer(&entry.id);
        }
        self.publish_queue_updated(agent_id).await;
        removed
    }

    /// Restore a retracted FLUSHED report wake after the combined wake it was
    /// folded into failed to commit — the flushed mirror of
    /// [`Services::restore_held_message`]: re-insert the entry verbatim (same
    /// id, no hold marker, ready-to-send) and persist/publish, so the
    /// stable-id retry can retract and fold it again instead of losing the
    /// report event. Unlike the held restore (a held entry drains by its own
    /// timer, so tail placement is fine), a flushed entry is ready-to-send:
    /// it re-enters at its FIFO position by `queued_at` among the normal
    /// (non-interrupt) entries, so entries queued after it are not delivered
    /// ahead of it if the parent drains before the retry re-retracts. No
    /// release timer is armed — the entry's hold already expired. No-op when
    /// an entry with the same id is already back on the queue (a concurrent
    /// restore path won the race).
    pub(crate) async fn restore_flushed_report_message(
        &self,
        agent_id: &AgentId,
        entry: QueuedMessage,
    ) {
        let restored = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let queue = guard.entry(agent_id.clone()).or_default();
            if queue.iter().any(|m| m.id == entry.id) {
                false
            } else {
                let idx = queue
                    .iter()
                    .position(|m| !m.interrupt_priority && m.queued_at > entry.queued_at)
                    .unwrap_or(queue.len());
                queue.insert(idx, entry);
                true
            }
        };
        if restored {
            self.publish_queue_updated(agent_id).await;
        }
    }

    /// Arm (or re-arm) the per-entry release timer for a held queue entry: a
    /// spawned sleeper that fires at `hold_until` and flushes the hold via
    /// [`Services::flush_expired_hold`]. A missing/unparseable/past deadline
    /// fires immediately (fail open — a hold must never strand a message).
    /// Re-arming replaces (aborts) any previous timer for the same entry id,
    /// so debounce extensions keep exactly one live sleeper per entry.
    pub(crate) fn arm_hold_release_timer(
        &self,
        agent_id: &AgentId,
        message_id: &str,
        hold_until: &str,
    ) {
        let delay_ms = parse_iso(hold_until).map_or(0, |t| {
            let remaining = t - time::OffsetDateTime::now_utc();
            u64::try_from(remaining.whole_milliseconds()).unwrap_or(0)
        });
        let services = self.clone();
        let agent = agent_id.clone();
        let mid = message_id.to_string();
        let task = tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            services.flush_expired_hold(&agent, &mid).await;
        });
        let previous = self
            .hold_release_timers
            .lock()
            .expect("hold timer registry poisoned")
            .insert(message_id.to_string(), task.abort_handle());
        if let Some(old) = previous {
            old.abort();
        }
    }

    /// Abort and forget the release timer for a held entry (release/retract
    /// path). A missing entry is a no-op — the timer may already have fired.
    fn cancel_hold_release_timer(&self, message_id: &str) {
        let removed = self
            .hold_release_timers
            .lock()
            .expect("hold timer registry poisoned")
            .remove(message_id);
        if let Some(handle) = removed {
            handle.abort();
        }
    }

    /// Timer-expiry flush for a held entry: clear its hold marker so it
    /// becomes ready-to-send, persist/publish the updated queue, and kick
    /// delivery. Idempotent — an entry already released, retracted, or
    /// missing leaves the queue untouched (no publish, no kick). An entry
    /// whose hold deadline sits clearly in the FUTURE is not flushed either:
    /// a stale timer (already fired, waiting on the lock) can race a
    /// same-key upsert that extended `hold_until` and re-armed a fresh timer
    /// — the extended deadline must win, so the stale fire re-arms for the
    /// remaining time instead (harmless duplicate of the fresh timer;
    /// arming replaces any previous handle for the entry). A small grace
    /// margin absorbs timer/clock rounding so an on-time fire always
    /// flushes.
    async fn flush_expired_hold(&self, agent_id: &AgentId, message_id: &str) {
        const EXPIRY_GRACE_MS: i128 = 250;
        enum Disposition {
            Cleared,
            Rearm(String),
            Gone,
        }
        self.hold_release_timers
            .lock()
            .expect("hold timer registry poisoned")
            .remove(message_id);
        let disposition = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            guard
                .get_mut(agent_id)
                .and_then(|queue| {
                    queue
                        .iter_mut()
                        .find(|m| m.id == message_id && m.hold_kind.is_some())
                })
                .map_or(Disposition::Gone, |m| {
                    let remaining_ms = m
                        .hold_until
                        .as_deref()
                        .and_then(parse_iso)
                        .map(|t| (t - time::OffsetDateTime::now_utc()).whole_milliseconds());
                    match (remaining_ms, &m.hold_until) {
                        (Some(ms), Some(until)) if ms > EXPIRY_GRACE_MS => {
                            Disposition::Rearm(until.clone())
                        }
                        _ => {
                            m.hold_kind = None;
                            m.hold_until = None;
                            Disposition::Cleared
                        }
                    }
                })
        };
        match disposition {
            Disposition::Gone => {}
            Disposition::Rearm(until) => {
                self.arm_hold_release_timer(agent_id, message_id, &until);
            }
            Disposition::Cleared => {
                self.publish_queue_updated(agent_id).await;
                self.kick_queue_drain(agent_id).await;
            }
        }
    }

    /// Kick the runtime drain for an agent (hold release/flush path): look up
    /// the owning workspace and defer to the manager's `try_drain_queue` —
    /// which no-ops when the agent is busy (the released entry then rides the
    /// normal end-of-turn drain) and starts a turn when idle. Quiet no-op
    /// when no manager is attached (read-only/test wiring) or the session row
    /// is gone.
    async fn kick_queue_drain(&self, agent_id: &AgentId) {
        let Some(manager) = self.agent_manager() else {
            return;
        };
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return;
        };
        manager
            .try_drain_queue(agent_id.clone(), session.workspace_id)
            .await;
    }

    /// Move an already-enqueued entry to position 0 (the head of the queue,
    /// ahead of every other entry — including leading interrupts, even
    /// user-origin ones: the dismissal context must precede whatever drains
    /// next) so it is the next delivery, and mark it `interrupt_priority` so
    /// a later interrupt enqueue (which inserts after the leading interrupt
    /// run) cannot slot ahead of it. That flag surfaces as
    /// `interruptPriority: true` on the `agent.getQueue` wire shape even for
    /// a solitary promoted entry that never was an interrupt enqueue —
    /// deliberate: it encodes drain precedence, not enqueue provenance. Used
    /// by the questions-dismissed notice
    /// ([`Services::notify_questions_dismissed`]) when the notice falls back
    /// to the queue: the dismissal context must reach the agent before any
    /// previously parked entry. Returns `true` iff the entry was found and
    /// the queue changed (callers republish the queue snapshot on `true`).
    pub(crate) fn move_queued_message_front(&self, agent_id: &AgentId, message_id: &str) -> bool {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let Some(queue) = guard.get_mut(agent_id) else {
            return false;
        };
        let Some(idx) = queue.iter().position(|m| m.id == message_id) else {
            return false;
        };
        if idx == 0 && queue[0].interrupt_priority {
            return false;
        }
        let mut entry = queue.remove(idx);
        entry.interrupt_priority = true;
        queue.insert(0, entry);
        true
    }

    /// Pop the oldest **ready-to-send** queued message for an agent, if any. Used
    /// by the runtime turn loop to flip a queued message to in-flight when the
    /// current turn ends. Entries with `editing = true` — and entries under an
    /// unexpired debounce hold — are skipped (left in place) so the agent stays
    /// idle only when *every* remaining entry is under edit or held
    /// (PROTOCOL §5.5/§6.5 invariant).
    pub(crate) fn dequeue_message(&self, agent_id: &AgentId) -> Option<QueuedMessage> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let idx = queue.iter().position(QueuedMessage::ready_to_send)?;
        Some(queue.remove(idx))
    }

    /// Pop the oldest ready-to-send **user-origin** queued message, if any
    /// (question hold, PROTOCOL §5.5). While the hold is active the drain
    /// paths deliver ONLY user-origin entries — a user answer parked by the
    /// turn-end busy race must reach the transcript instead of deadlocking
    /// behind the hold its answer tag releases; automatic entries stay
    /// parked.
    pub(crate) fn dequeue_user_origin_message(&self, agent_id: &AgentId) -> Option<QueuedMessage> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let idx = queue
            .iter()
            .position(|m| m.ready_to_send() && m.user_origin)?;
        Some(queue.remove(idx))
    }

    /// Batch-flush dequeue (`agents.flushQueuedMessages`, PROTOCOL §5.5): pop
    /// EVERY ready-to-send entry in stored order — which IS the drain order
    /// (interrupt-priority first, then FIFO); `editing: true` entries stay
    /// queued — so the drain can deliver them as one combined provider turn.
    /// `require_user_origin` mirrors the question-hold contract
    /// (monorepo#1791): while the hold is active the flush fires ONLY when a
    /// ready user-origin entry is present — a user delivery is starting a
    /// turn anyway, so the parked automatic entries ride along FIFO instead
    /// of being bypassed by the newer user message; with NO user-origin
    /// entry ready the whole batch is a no-op and automatic entries stay
    /// parked (an automatic delivery must never start a turn over the
    /// pending Q&A). Returns `None` — leaving the queue untouched — unless
    /// at least `min_ready` entries are ready, so callers keep the
    /// single-entry drain path byte-for-byte when too few messages are
    /// waiting.
    pub(crate) fn dequeue_ready_batch(
        &self,
        agent_id: &AgentId,
        require_user_origin: bool,
        min_ready: usize,
    ) -> Option<Vec<QueuedMessage>> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let ready = QueuedMessage::ready_to_send;
        if require_user_origin && !queue.iter().any(|m| ready(m) && m.user_origin) {
            return None;
        }
        if queue.iter().filter(|m| ready(m)).count() < min_ready {
            return None;
        }
        let mut drained = Vec::new();
        let mut i = 0;
        while i < queue.len() {
            if ready(&queue[i]) {
                drained.push(queue.remove(i));
            } else {
                i += 1;
            }
        }
        Some(drained)
    }

    /// System-only batch dequeue (`agents.flushQueuedMessages = "systemOnly"`):
    /// scan the WHOLE queue — regardless of interleaving with user-origin
    /// entries — for ready-to-send (`!editing`) SYSTEM-origin entries
    /// (`user_origin == false`) and, when at least `min_ready` are found,
    /// remove ALL of them, preserving their relative order; user-origin
    /// entries are left untouched in their original queue positions. System
    /// entries may thus be delivered ahead of earlier-queued, interleaved
    /// user entries. Returns `None` — leaving the queue untouched — when
    /// fewer than `min_ready` system entries are ready, so the caller falls
    /// through to the single-entry drain path.
    pub(crate) fn dequeue_system_only_batch(
        &self,
        agent_id: &AgentId,
        min_ready: usize,
    ) -> Option<Vec<QueuedMessage>> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let eligible = |m: &QueuedMessage| m.ready_to_send() && !m.user_origin;
        if queue.iter().filter(|m| eligible(m)).count() < min_ready {
            return None;
        }
        let mut drained = Vec::new();
        let mut i = 0;
        while i < queue.len() {
            if eligible(&queue[i]) {
                drained.push(queue.remove(i));
            } else {
                i += 1;
            }
        }
        Some(drained)
    }

    /// Mode-dispatching batch dequeue for `agents.flushQueuedMessages`: `All`
    /// defers to [`Services::dequeue_ready_batch`] (every ready entry;
    /// `require_user_origin` under an active hold — the flush fires only
    /// when a user-origin entry is ready, carrying the parked automatic
    /// entries along FIFO, monorepo#1791); `SystemOnly` defers to
    /// [`Services::dequeue_system_only_batch`] (system-origin entries
    /// anywhere in the queue) but NEVER batches while a hold is active
    /// (`require_user_origin`) — the hold's release is a user-origin entry,
    /// which `SystemOnly` by definition excludes; `Off` always returns `None`
    /// so every caller falls through to the single-entry FIFO path.
    pub(crate) fn dequeue_flush_batch(
        &self,
        agent_id: &AgentId,
        mode: intent_core::FlushQueuedMessagesMode,
        require_user_origin: bool,
        min_ready: usize,
    ) -> Option<Vec<QueuedMessage>> {
        match mode {
            intent_core::FlushQueuedMessagesMode::All => {
                self.dequeue_ready_batch(agent_id, require_user_origin, min_ready)
            }
            intent_core::FlushQueuedMessagesMode::SystemOnly => {
                if require_user_origin {
                    None
                } else {
                    self.dequeue_system_only_batch(agent_id, min_ready)
                }
            }
            intent_core::FlushQueuedMessagesMode::Off => None,
        }
    }

    /// Re-insert a batch of messages at the front of an agent's queue,
    /// preserving their order (`messages[0]` becomes the queue head). Used by
    /// the batch-flush persist-failure path to hand back the undelivered
    /// remainder in original order (never-lost).
    pub(crate) fn requeue_front_batch(&self, agent_id: &AgentId, messages: Vec<QueuedMessage>) {
        if messages.is_empty() {
            return;
        }
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.entry(agent_id.clone()).or_default();
        for (i, m) in messages.into_iter().enumerate() {
            queue.insert(i, m);
        }
    }

    /// Atomically remove and return the queued entry with id `message_id`
    /// (any position, including entries under edit), or `None` when the agent
    /// has no such entry. Backs `agent.sendQueuedMessageNow` (PROTOCOL §5.5):
    /// the removal happens under the queue lock so no concurrent drain can
    /// deliver the same entry twice.
    pub(crate) fn take_queued_message(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Option<QueuedMessage> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let idx = queue.iter().position(|m| m.id == message_id)?;
        Some(queue.remove(idx))
    }

    /// Re-insert a message at the front of an agent's queue (used when a
    /// concurrent turn won the in-flight slot during a drain race, and by
    /// `agent.sendQueuedMessageNow`'s persist-failure restore).
    pub(crate) fn requeue_front(&self, agent_id: &AgentId, message: QueuedMessage) {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .entry(agent_id.clone())
            .or_default()
            .insert(0, message);
    }

    /// Drop an agent's ENTIRE in-memory queue (intent-hq/monorepo#2762): the
    /// fail-closed contract for a vanished session. The durable `agent_queue`
    /// rows already cascaded with the deleted `agent_session` row, so this
    /// only clears the in-memory registry a raced delivery may have
    /// repopulated — no write-through, no `agent:queue:updated` (the agent no
    /// longer exists to subscribe on).
    pub(crate) fn drop_queue(&self, agent_id: &AgentId) {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .remove(agent_id);
    }

    /// `true` iff the agent has at least one queued message that is **not**
    /// under edit and **not** under an unexpired debounce hold (i.e. the
    /// "ready-to-send" queue is non-empty). Drives the self-drain trigger and
    /// gates `agent:idle` emission so the agent never reports idle while
    /// ready-to-send work remains (PROTOCOL §5.5/§6.5).
    pub(crate) fn has_ready_to_send(&self, agent_id: &AgentId) -> bool {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .is_some_and(|q| q.iter().any(QueuedMessage::ready_to_send))
    }

    /// `true` iff at least one ready-to-send queued entry is user-origin
    /// (question hold, PROTOCOL §5.5): the hold-gated drain paths use this
    /// to decide whether a drain may proceed for the user entry alone.
    pub(crate) fn has_user_origin_ready(&self, agent_id: &AgentId) -> bool {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .is_some_and(|q| q.iter().any(|m| m.ready_to_send() && m.user_origin))
    }

    /// `true` iff at least one ready-to-send user-origin entry was queued at
    /// or after `since` (RFC-3339). The archived-drain exemption uses this so
    /// only a user send made INTO the archived workspace — the explicit
    /// resurrection signal — releases the park (intent-hq/intent#3883): a
    /// user entry parked by a busy race BEFORE archival must stay parked with
    /// everything else, or the interrupted worker's end-of-turn re-kick would
    /// auto-unarchive a freshly archived workspace with no post-archive user
    /// action. An entry with an unparseable `queued_at` never matches; an
    /// unparseable `since` falls back to [`Self::has_user_origin_ready`]
    /// (fail open — a row without a usable `archivedAt` cannot be compared).
    pub(crate) fn has_user_origin_ready_since(&self, agent_id: &AgentId, since: &str) -> bool {
        let Some(cutoff) = parse_iso(since) else {
            return self.has_user_origin_ready(agent_id);
        };
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .is_some_and(|q| {
                q.iter().any(|m| {
                    m.ready_to_send()
                        && m.user_origin
                        && parse_iso(&m.queued_at).is_some_and(|t| t >= cutoff)
                })
            })
    }

    /// The `turn_id` of the oldest **ready-to-send** queued message, without
    /// removing it (the same entry [`Services::dequeue_message`] would pop).
    /// `agent.retry` reads it before kicking the drain so its RPC response
    /// carries the redriven turn's correlation id (monorepo#1022). `None`
    /// when no ready-to-send entry exists or the entry has no turn id.
    pub(crate) fn peek_ready_turn_id(&self, agent_id: &AgentId) -> Option<String> {
        let guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get(agent_id)?;
        let entry = queue.iter().find(|m| m.ready_to_send())?;
        (!entry.turn_id.is_empty()).then(|| entry.turn_id.clone())
    }

    /// Drop all queued messages for an agent (used by `agent.editAndRegenerate`,
    /// which supersedes the queue with the regenerated message). Returns `true`
    /// iff the queue previously held at least one message — the caller uses this
    /// to decide whether to publish `agent:queue:updated`.
    pub(crate) fn clear_queue(&self, agent_id: &AgentId) -> bool {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let had = guard.get(agent_id).is_some_and(|q| !q.is_empty());
        guard.remove(agent_id);
        had
    }

    /// Snapshot the current queue contents as wire-shape `QueuedMessage` JSON
    /// (the §5.5 `{id, content, queuedAt, position, imageBlocks?, fileBlocks?}` shape) for
    /// `agent.getQueue` and the `agent:queue:updated` payload (§6).
    pub(crate) fn queue_snapshot(&self, agent_id: &AgentId) -> Vec<Value> {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .map(|q| q.iter().enumerate().map(|(i, m)| m.to_value(i)).collect())
            .unwrap_or_default()
    }

    /// Like [`Services::queue_snapshot`] but with each entry's `content`
    /// truncated to [`QUEUE_PREVIEW_MAX_CHARS`] chars (with a trailing `…`
    /// marker, matching the MCP-side presentation) and the bulky
    /// `imageBlocks` / `fileBlocks` payloads dropped (potentially large
    /// base64 blobs that would defeat the size cap) — the shared preview
    /// projection for surfaces that embed *other* agents' queues
    /// (`agent.diagnostics`). Sender attribution stays available via the
    /// retained `messageMetadata`. Entries keep the stored queue order,
    /// which IS the drain order (next delivery first: interrupt-priority
    /// entries in arrival order, then normal FIFO); entries the drain skips
    /// surface flagged `editing: true` at their stored position rather than
    /// being excluded.
    pub(crate) fn queue_snapshot_preview(&self, agent_id: &AgentId) -> Vec<Value> {
        let mut entries = self.queue_snapshot(agent_id);
        for entry in &mut entries {
            let truncated = entry["content"]
                .as_str()
                .filter(|c| c.chars().count() > QUEUE_PREVIEW_MAX_CHARS)
                .map(|c| {
                    let mut t: String = c.chars().take(QUEUE_PREVIEW_MAX_CHARS).collect();
                    t.push('…');
                    t
                });
            if let Some(t) = truncated {
                entry["content"] = Value::String(t);
            }
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("imageBlocks");
                obj.remove("fileBlocks");
            }
        }
        entries
    }

    /// Write-through persistence of an agent's queue: snapshot the in-memory
    /// queue (brief lock, dropped before the await) and replace the agent's
    /// `agent_queue` rows with it. Best-effort — a store failure is logged at
    /// WARN and never fails the calling RPC; the in-memory queue remains the
    /// live source of truth and the next mutation re-snapshots.
    ///
    /// Persists are serialized through `agent_queue_persist_gate`, and the
    /// snapshot is taken *inside* that gate: concurrent mutations cannot
    /// commit snapshots out of mutation order, because whichever persist runs
    /// later re-reads the live queue (which already includes the earlier
    /// mutation). Once this returns, the DB holds this mutation's state or a
    /// newer superset — never an older snapshot.
    pub(crate) async fn persist_queue_snapshot(&self, agent_id: &AgentId) {
        let _gate = self.agent_queue_persist_gate.lock().await;
        let rows = self.queue_rows(agent_id);
        if let Err(e) = self.store.replace_agent_queue(agent_id, &rows).await {
            tracing::warn!(agent = %agent_id, error = %e, "agent queue write-through failed");
        }
    }

    /// Snapshot an agent's in-memory queue as `agent_queue` rows (brief lock,
    /// dropped on return). Callers that persist the result must hold
    /// `agent_queue_persist_gate` across snapshot + store write.
    fn queue_rows(&self, agent_id: &AgentId) -> Vec<intent_store::AgentQueueRow> {
        let guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        guard
            .get(agent_id)
            .map(|q| {
                q.iter()
                    .enumerate()
                    .map(|(i, m)| intent_store::AgentQueueRow {
                        id: m.id.clone(),
                        agent_id: agent_id.clone(),
                        position: i64::try_from(i).expect("value fits in i64"),
                        payload: serde_json::to_value(m).unwrap_or(Value::Null),
                        created_at: m.queued_at.clone(),
                        turn_id: m.turn_id.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Rehydrate every persisted agent queue into the in-memory map at daemon
    /// startup (before RPCs are served). Entries left `editing: true` at
    /// shutdown come back ready-to-send (`editing: false`) — the editing
    /// client's hold is gone; `persisted` / `requeuedAfterFailure` flags are
    /// preserved so a later drain does not double-append transcript rows
    /// (STAB-112/STAB-52). Rehydration never kicks `try_drain_queue`: messages
    /// sit until an explicit kick (resume, sendMessage, queueMessage, retry) —
    /// with ONE exception: entries carrying a debounce-hold marker get their
    /// release timer re-armed for the remaining time, and holds whose
    /// `holdUntil` already passed while the daemon was down are flushed
    /// immediately (the flush clears the marker, persists, and kicks
    /// delivery).
    /// Legacy payloads without a `turnId` (pre-monorepo#1022) rehydrate with
    /// `turn_id = id` so every in-memory entry carries a correlation id.
    /// Returns the number of messages actually inserted into the in-memory
    /// map (agents that already hold a live queue are skipped, not counted).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if loading the persisted agent queues fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn rehydrate_agent_queues(&self) -> Result<usize> {
        let rows = self.store.load_all_agent_queues().await?;
        let mut map: HashMap<AgentId, Vec<QueuedMessage>> = HashMap::new();
        for row in rows {
            match serde_json::from_value::<QueuedMessage>(row.payload) {
                Ok(mut message) => {
                    message.editing = false;
                    if message.turn_id.is_empty() {
                        message.turn_id.clone_from(&message.id);
                    }
                    map.entry(row.agent_id).or_default().push(message);
                }
                Err(e) => {
                    tracing::warn!(
                        agent = %row.agent_id,
                        message_id = %row.id,
                        error = %e,
                        "skipping undecodable persisted queue entry"
                    );
                }
            }
        }
        let mut count = 0;
        let mut held: Vec<(AgentId, String, String)> = Vec::new();
        {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            for (agent_id, queue) in map {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    guard.entry(agent_id.clone())
                {
                    count += queue.len();
                    for m in &queue {
                        if m.hold_kind.is_some() {
                            held.push((
                                agent_id.clone(),
                                m.id.clone(),
                                m.hold_until.clone().unwrap_or_default(),
                            ));
                        }
                    }
                    entry.insert(queue);
                }
            }
        }
        // Re-arm outside the queue lock: an expired hold's timer fires
        // immediately and the flush relocks the registry.
        for (agent_id, message_id, hold_until) in held {
            self.arm_hold_release_timer(&agent_id, &message_id, &hold_until);
        }
        Ok(count)
    }

    /// Publish `agent:queue:updated` with the **current** queue snapshot.
    /// Looks up the owning workspace from the agent session — when the session
    /// row is missing (e.g. an idempotent remove on an unknown agent) or no bus
    /// is wired, the call is a quiet no-op rather than an error: the durable
    /// mutation is the source of truth and a missing event is not fatal.
    ///
    /// The queue snapshot is taken **outside** the mutex it lives behind, but
    /// since this method only reads (under a brief lock that is dropped before
    /// the await) it never holds the queue lock across an `await` point. The
    /// snapshot is taken after the session lookup so the event payload and the
    /// write-through snapshot in [`publish_queue_updated_for`] reflect queue
    /// state from (nearly) the same moment.
    pub(crate) async fn publish_queue_updated(&self, agent_id: &AgentId) {
        let workspace_id = match self.store.get_agent_session(agent_id).await {
            Ok(s) => s.workspace_id,
            Err(_) => return,
        };
        let queue = self.queue_snapshot(agent_id);
        self.publish_queue_updated_for(agent_id, &workspace_id, queue)
            .await;
    }

    /// Like [`publish_queue_updated`] but takes the workspace id directly —
    /// used by call sites (the turn worker, `edit_and_regenerate`) that already
    /// hold it, avoiding a redundant `get_agent_session` round-trip per drain step.
    ///
    /// Every queue mutation flows through here (or through
    /// [`publish_queue_updated`], which delegates here), so this is also the
    /// single write-through choke point: the durable `agent_queue` snapshot is
    /// refreshed before the event is published.
    pub(crate) async fn publish_queue_updated_for(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        queue: Vec<Value>,
    ) {
        self.persist_queue_snapshot(agent_id).await;
        self.publish_queue_event(agent_id, workspace_id, queue)
            .await;
    }

    /// Publish `agent:queue:updated` WITHOUT the write-through persist — for
    /// the one caller ([`Services::migrate_queue_and_gc_poisoned_session`])
    /// whose durable snapshot was already committed by an atomic store op.
    /// Everything else goes through [`Services::publish_queue_updated_for`].
    async fn publish_queue_event(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        queue: Vec<Value>,
    ) {
        let event = intent_store::NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: AGENT_QUEUE_UPDATED.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "queue": queue,
            }),
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    /// Publish `agent:queue:processing` for a queue entry the drain loop just
    /// flipped to in-flight (PROTOCOL §6.5): the drain-start signal that
    /// covers redrives which skip the duplicate user-row append — the FE
    /// keys the turn start off `turnId` here (monorepo#1022). Payload:
    /// `{ agentId, messageId, content, turnId }` (`turnId` omitted only for
    /// legacy entries without one; every enqueue path mints one today).
    pub(crate) async fn publish_queue_processing(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        message: &QueuedMessage,
    ) {
        let mut data = json!({
            "agentId": agent_id.0,
            "messageId": message.id,
            "content": message.content,
        });
        if !message.turn_id.is_empty() {
            data["turnId"] = Value::String(message.turn_id.clone());
        }
        let event = intent_store::NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: AGENT_QUEUE_PROCESSING.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    /// `agent.wakeOrCreate` wiring for
    /// [`Services::migrate_queue_and_gc_poisoned_session`] (monorepo#847):
    /// migrate each poisoned sibling's parked queue onto the wake/create
    /// target and GC the dead session. Failures are non-fatal to the wake —
    /// the helper's error path already rolled back the in-memory drain and
    /// skipped GC, leaving the messages durable on exactly one queue (the
    /// poisoned one); log WARN and continue. Returns the ids whose migration
    /// FAILED: callers must keep those out of the `cleanedUpAgentIds`
    /// task-assignment purge (and out of the response field) so the poisoned
    /// session stays assigned and the next `agent.wakeOrCreate` retries the
    /// migration — otherwise the parked messages would be durable but
    /// permanently stranded on a session no wake can discover.
    async fn migrate_poisoned_queues_to(
        &self,
        poisoned: &[AgentId],
        target: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Vec<AgentId> {
        let mut failed = Vec::new();
        for poisoned_id in poisoned {
            if let Err(e) = self
                .migrate_queue_and_gc_poisoned_session(poisoned_id, target, workspace_id)
                .await
            {
                tracing::warn!(
                    poisoned = %poisoned_id,
                    target = %target,
                    error = %e,
                    "wakeOrCreate: poisoned-session queue migration/GC failed; \
                     continuing with the wake (monorepo#847)"
                );
                failed.push(poisoned_id.clone());
            }
        }
        failed
    }

    /// Migrate a poisoned session's parked queue onto a replacement agent,
    /// then GC the poisoned session via the hard-delete path (monorepo#847).
    ///
    /// The in-memory `agent_queues` entry is authoritative — startup
    /// rehydration populates the map before RPCs are served, so persisted
    /// rows never hold entries the map lacks. Entries move in order onto the
    /// TAIL of the target's queue with per-entry flags reset: `editing =
    /// false` (the editing client's hold dies with the old session),
    /// `persisted = false` (the old transcript row dies with the old session
    /// — the drain must re-persist into the NEW agent's transcript), and
    /// `requeued_after_failure = false` (the failure belonged to the old
    /// session). `id`, `content`, `queued_at`, `image_blocks`, `file_blocks`
    /// and `message_metadata` are preserved.
    ///
    /// When entries moved, the durable hand-off is committed ATOMICALLY via
    /// [`Store::move_agent_queue`]: one write transaction deletes both
    /// agents' persisted rows and inserts the target snapshot, so a crash at
    /// any point leaves the messages durable on exactly one queue — never
    /// neither. Unlike the best-effort write-throughs, a failed move is an
    /// ERROR: the in-memory drain is rolled back, GC is skipped (the
    /// messages stay durable on the poisoned queue), and the caller can
    /// retry. On success `agent:queue:updated` is published for the target
    /// and the poisoned session is hard-deleted through
    /// [`Services::agent_delete_op`], which emits `agent:deleted`, drops the
    /// (now empty) in-memory queue entry, and clears the failure streak +
    /// failure-wake dedup records; any persisted `agent_queue` rows cascade
    /// with the `agent_session` row (`ON DELETE CASCADE`, migration 0046) so
    /// nothing rehydrates at the next startup. No-op safe: an empty (or
    /// absent) queue still GCs the session, and a missing poisoned session
    /// stays idempotent (the delete succeeds quietly). Returns the number of
    /// migrated messages.
    ///
    /// [`Store::move_agent_queue`]: intent_store::Store::move_agent_queue
    pub(crate) async fn migrate_queue_and_gc_poisoned_session(
        &self,
        poisoned_id: &AgentId,
        target_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<usize> {
        // Fail closed BEFORE draining: a bad replacement target (or a
        // cross-workspace id) must not receive messages no live session in
        // this workspace will ever drain.
        let target = self.require_agent_session(target_id).await?;
        if &target.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {target_id}")));
        }
        match self.store.get_agent_session(poisoned_id).await {
            Ok(poisoned) => {
                if &poisoned.workspace_id != workspace_id {
                    return Err(Error::NotFound(format!("agent session {poisoned_id}")));
                }
            }
            // A missing poisoned session is the idempotent no-op case; any
            // other store error fails closed BEFORE draining, mirroring the
            // target guard above.
            Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        // Drain in-memory, keeping the original entries for rollback should
        // the durable move fail below. The append onto the target happens
        // OUTSIDE the persist gate: a concurrent enqueue to the target in the
        // window before the move commits can write-through a snapshot that
        // already carries the migrated ids while the poisoned rows still
        // exist — a global-PK conflict that fails that best-effort
        // write-through with a benign WARN (the move right below commits the
        // correct snapshot).
        let drained: Vec<QueuedMessage> = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            let drained = guard.remove(poisoned_id).unwrap_or_default();
            if !drained.is_empty() {
                let queue = guard.entry(target_id.clone()).or_default();
                for message in &drained {
                    let mut message = message.clone();
                    message.editing = false;
                    message.persisted = false;
                    message.requeued_after_failure = false;
                    queue.push(message);
                }
            }
            drained
        };
        let migrated = drained.len();
        if migrated > 0 {
            // Atomic durable hand-off: one transaction clears BOTH agents'
            // persisted rows and inserts the target snapshot. (The migrated
            // entries keep their ids and `agent_queue.id` is a GLOBAL
            // primary key, so a non-atomic clear-then-replace pair risks a
            // crash window with the messages on NEITHER queue.) The snapshot
            // is taken inside the persist gate, same ordering contract as
            // `persist_queue_snapshot`.
            let moved = {
                let _gate = self.agent_queue_persist_gate.lock().await;
                let rows = self.queue_rows(target_id);
                self.store
                    .move_agent_queue(poisoned_id, target_id, &rows)
                    .await
            };
            if let Err(e) = moved {
                // Roll back the in-memory drain and skip GC: the messages
                // remain durable on the poisoned queue (rows untouched) and
                // in its in-memory queue, so the caller can retry. The
                // restore SPLICES the drained entries back at the front of
                // any existing entry rather than overwriting it — a
                // quarantined send racing this window may have re-created
                // the poisoned entry with new messages, which must survive.
                let migrated_ids: HashSet<&str> = drained.iter().map(|m| m.id.as_str()).collect();
                let mut guard = self
                    .agent_queues
                    .lock()
                    .expect("agent queue registry poisoned");
                if let Some(queue) = guard.get_mut(target_id) {
                    queue.retain(|m| !migrated_ids.contains(m.id.as_str()));
                }
                guard
                    .entry(poisoned_id.clone())
                    .or_default()
                    .splice(0..0, drained);
                return Err(e);
            }
            // Re-arm release timers for migrated held entries against the
            // TARGET agent (mirrors the boot-rehydration re-arm): the timer
            // armed at enqueue time captured the poisoned agent id, so left
            // alone it would fire against a queue that no longer holds the
            // entry and the migrated hold would sit parked until an
            // unrelated queue event. Entries keep their ids, and arming
            // replaces (aborts) any previous timer for the same id, so the
            // stale poisoned-agent sleeper dies here too. An already expired
            // `hold_until` flushes immediately (fail open).
            for message in &drained {
                if message.hold_kind.is_some() {
                    self.arm_hold_release_timer(
                        target_id,
                        &message.id,
                        message.hold_until.as_deref().unwrap_or_default(),
                    );
                }
            }
            let queue = self.queue_snapshot(target_id);
            self.publish_queue_event(target_id, workspace_id, queue)
                .await;
        }
        self.agent_delete_op(poisoned_id.clone(), Some(workspace_id.clone()))
            .await?;
        Ok(migrated)
    }
}

/// Join all text blocks of the most-recent assistant message (the summary's
/// `lastResponse`).
fn last_assistant_text(messages: &[AgentMessage]) -> Option<String> {
    for msg in messages.iter().rev() {
        if msg.role != "assistant" {
            continue;
        }
        let joined = text_blocks(&msg.content).join(" ");
        let trimmed = joined.trim();
        return if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }
    None
}

/// Map a persisted [`AgentStatus`] to the TS runtime status word used in the
/// `agent.getSubscriptions` `agentStatuses` map. Best-effort: statuses without a
/// runtime equivalent (e.g. `deleted`) are omitted so the caller drops the key.
/// Parse an RFC-3339 timestamp into epoch milliseconds, or `0` when malformed.
fn iso_ms(ts: &str) -> i64 {
    parse_iso(ts).map_or(0, |dt| {
        i64::try_from(dt.unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
    })
}

/// Non-negative age in milliseconds of `ts` relative to `now_ms`.
fn age_ms(now_ms: i64, ts: &str) -> i64 {
    (now_ms - iso_ms(ts)).max(0)
}

fn agent_status_wire(status: AgentStatus) -> Option<&'static str> {
    match status {
        AgentStatus::Pending | AgentStatus::Waiting => Some("waiting"),
        AgentStatus::Active | AgentStatus::Processing => Some("responding"),
        AgentStatus::RuntimeIdle | AgentStatus::Idle => Some("idle"),
        AgentStatus::Completed => Some("completed"),
        AgentStatus::Error => Some("failed"),
        AgentStatus::Deleted => None,
    }
}

/// Best-effort human description mirroring TS `describeAgentSubscription`:
/// `"<parent>: <n event types>, from <child>[, delegation group <id> (await all,
/// k expected)]"`. Exact wording is not asserted.
fn describe_subscription(
    watch: &CompletionWatch,
    event_types: &[&str],
    delegation_group: Option<&Value>,
) -> String {
    let mut desc = format!(
        "{}: {} event types, from {}",
        watch.parent_agent_name,
        event_types.len(),
        watch.child_agent_id.0
    );
    if let Some(group) = delegation_group {
        let group_id = group["groupId"].as_str().unwrap_or_default();
        let expected = group["expectedAgentIds"].as_array().map_or(0, Vec::len);
        let _ = write!(
            desc,
            ", delegation group {group_id} (await all, {expected} expected)"
        );
    }
    desc
}

/// Wire shape for one live event subscription (monorepo#947): the camelCase
/// registration fields, shared by `agent.getSubscriptions`
/// (`eventSubscriptions`) and `agent.diagnostics`
/// (`diagnostics.eventSubscriptions`).
fn event_subscription_wire(record: &crate::event_subscriptions::EventSubscriptionRecord) -> Value {
    json!({
        "id": record.id,
        "workspaceId": record.workspace_id,
        "subscriberAgentId": record.subscriber_agent_id,
        "eventTypes": record.event_types,
        "excludeSelf": record.exclude_self,
        "batchWindow": record.batch_window_ms,
        "createdAt": record.created_at,
    })
}

/// Count `tool_use` content blocks by tool name across all messages.
fn tool_call_counts(messages: &[AgentMessage]) -> Value {
    let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for msg in messages {
        let Some(blocks) = msg.content.as_array() else {
            continue;
        };
        for block in blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            let name = block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            *counts.entry(name).or_insert(0) += 1;
        }
    }
    json!(counts)
}

/// Parse an optional string field in an `agent.update` `changes` object. A JSON
/// `null` clears the underlying `Option<String>`; a JSON string sets it to
/// `Some(_)`; any other value type is `-32602` (matching the trait's
/// [`Error::InvalidParams`] contract). Reused by every optional-string field in
/// [`Services::agent_update_op`] so the diagnostic wording stays uniform.
fn update_optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.clone())),
        _ => Err(Error::InvalidParams(format!(
            "agent.update: `{field}` must be a string or null"
        ))),
    }
}

/// Reject transcript entries whose `role` is not one of the four wire values
/// (`user` | `assistant` | `tool` | `system`). Shared by `agent.appendMessage`
/// and `agent.replaceMessages` so callers cannot smuggle bogus roles that would
/// break the message-log invariant.
fn validate_message_role(role: &str) -> Result<()> {
    match role {
        "user" | "assistant" | "tool" | "system" => Ok(()),
        _ => Err(Error::InvalidParams(format!(
            "invalid message role `{role}` (expected one of user|assistant|tool|system)"
        ))),
    }
}

/// Per-segment char cap for the restart-resume tail recap (middle-truncated):
/// generous enough to replay a real request/partial response verbatim, small
/// enough that a pathological message cannot blow up the continuation prompt.
const RESUME_RECAP_SEGMENT_MAX_CHARS: usize = 8_000;

/// Cap on replayed tail segments: every restart cycle can stack one more
/// flushed partial row onto the incomplete tail, so a restart loop must not
/// grow the recap without bound. When over, the oldest segments AFTER the
/// original lost request are elided (the head segment is the request the
/// whole recap exists to preserve; the freshest partials matter most).
const RESUME_RECAP_MAX_SEGMENTS: usize = 8;

/// `metadata.type` stamped on every restart-resume continuation message
/// (`resume_interrupted_agent`), riding `messageMetadata` onto the persisted
/// user row. A fresh continuation is re-sent on every resume, so persisted
/// copies in the transcript tail are never replayed by the recap — the walk
/// identifies them by THIS tag, never by their text: a text match (even a
/// prefix one) would also swallow an ordinary user request that happens to
/// start with the continuation wording, and if that request was interrupted
/// the next restart's recap would omit it even though it is absent from the
/// provider checkpoint. Rows persisted before the tag existed carry the
/// legacy wording and are skipped by exact equality with
/// [`LEGACY_RESUME_CONTINUATION_TEXT`].
pub(crate) const RESUME_CONTINUATION_METADATA_TYPE: &str = "resume_continuation";

/// The retired pre-duration continuation wording, kept ONLY as the
/// exact-equality recap skip for legacy transcript rows persisted before
/// [`RESUME_CONTINUATION_METADATA_TYPE`] tagging existed. Never sent.
pub(crate) const LEGACY_RESUME_CONTINUATION_TEXT: &str = "You were interrupted because the \
     harness shut down. You now have a chance to continue the work — review your last steps and \
     pick up where you left off.";

/// Fallback restart-resume continuation, sent unchanged when the claimed
/// row's `interrupted_at` cannot be parsed or the delta is negative — the
/// resume never fails on the timestamp.
pub(crate) const RESUME_CONTINUATION_FALLBACK_TEXT: &str = "You were interrupted due to a \
     harness shutdown and restart. You can now continue your work and pick up where you left \
     off.";

/// Humanize an outage duration coarsely for the resume continuation: a
/// single unit — seconds under 2 minutes, minutes under 2 hours, hours under
/// 2 days, else days — matching the wording's "for about {duration}" framing.
fn humanize_outage_duration(seconds: i64) -> String {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;
    let (value, unit) = if seconds < 2 * MINUTE {
        (seconds, "second")
    } else if seconds < 2 * HOUR {
        (seconds / MINUTE, "minute")
    } else if seconds < 2 * DAY {
        (seconds / HOUR, "hour")
    } else {
        (seconds / DAY, "day")
    };
    let plural = if value == 1 { "" } else { "s" };
    format!("{value} {unit}{plural}")
}

/// Build the restart-resume continuation message: the approved wording with
/// the humanized outage duration (`now` − `interrupted_at`), or
/// [`RESUME_CONTINUATION_FALLBACK_TEXT`] when the timestamp is unparseable
/// or in the future. Either variant is delivered under
/// [`RESUME_CONTINUATION_METADATA_TYPE`] metadata, which the recap skip
/// keys off.
fn resume_continuation_text(interrupted_at: &str, now: time::OffsetDateTime) -> String {
    let Some(at) = parse_iso(interrupted_at) else {
        return RESUME_CONTINUATION_FALLBACK_TEXT.to_string();
    };
    let seconds = (now - at).whole_seconds();
    if seconds < 0 {
        return RESUME_CONTINUATION_FALLBACK_TEXT.to_string();
    }
    format!(
        "You were interrupted for about {} due to a harness shutdown and restart. You can now \
         continue your work and pick up where you left off.",
        humanize_outage_duration(seconds)
    )
}

/// The rebuilt interrupted-turn tail for a restart resume: the prompt-only
/// recap text plus any attachment blocks the replayed user rows carried.
/// Persisted user rows keep their image/file blocks (STAB-133), and dropping
/// them here would resume an interrupted attachment-bearing request with
/// only its text — the blocks ride `TurnOptions::prepend_image_blocks` /
/// `prepend_file_blocks`, whose prompt assembly (`append_attachment_blocks`)
/// emits them on the recreate branch too (the history XML is text-only).
pub(crate) struct ResumeTailRecap {
    pub(crate) text: String,
    /// Replayed user rows' `image` blocks (persisted shape carries the same
    /// `data`/`mimeType` keys prompt assembly reads; the extra `type` key is
    /// ignored). `None` when the replayed rows had none.
    pub(crate) image_blocks: Option<Value>,
    /// Replayed user rows' `file` blocks (inline `data`/`mimeType`/`fileName`
    /// or attachment-reference `attachmentId`/`fileName` — both shapes are
    /// what prompt assembly reads). `None` when the replayed rows had none.
    pub(crate) file_blocks: Option<Value>,
}

/// One replayed tail row, in transcript order.
enum TailSegment {
    /// A user message the provider never committed.
    User(String),
    /// Partial assistant output flushed by an interruption.
    Partial(String),
}

/// Build the mid-turn tail recap for a restart resume (monorepo#2539).
///
/// A `session/load` resume replays the PROVIDER's session checkpoint, which
/// only contains completed turns — a turn is committed provider-side only
/// when its `session/prompt` resolves, so every row after the last COMPLETED
/// assistant turn is absent from the resumed context even though the daemon
/// transcript has it. This recap replays that daemon-side tail (prompt-only,
/// via `TurnOptions::prepend_content`) ahead of the continuation message,
/// with an explicit cut-off disclosure.
///
/// The backward walk collects user rows and `metadata.interrupted`-tagged
/// assistant rows (skipping system rows) until the completed-turn boundary.
/// A single restart leaves `user + partial`; repeated restarts stack further
/// partials, and the previous resumes' persisted continuation rows are
/// skipped by their [`RESUME_CONTINUATION_METADATA_TYPE`] metadata tag —
/// legacy pre-tag rows by exact equality with
/// [`LEGACY_RESUME_CONTINUATION_TEXT`] — never by a text prefix, so an
/// ordinary user request that happens to open with the continuation
/// wording is still replayed (each resume re-sends a fresh continuation).
/// Replayed text is XML-escaped like `format_history_as_xml`, so a message
/// containing closing tags cannot break out of its quoting element. Returns
/// `None` when the tail has no incomplete rows (the provider checkpoint is
/// current) or when an unexpected row shape makes the tail unreadable.
fn build_resume_tail_recap(messages: &[AgentMessage]) -> Option<ResumeTailRecap> {
    let mut segments: Vec<TailSegment> = Vec::new();
    // Grouped per row so reversing the walk order can't flip the block order
    // WITHIN a multi-attachment row.
    let mut image_rows: Vec<Vec<Value>> = Vec::new();
    let mut file_rows: Vec<Vec<Value>> = Vec::new();
    for m in messages.iter().rev() {
        match m.role.as_str() {
            "system" => {}
            "assistant" => {
                let interrupted = m
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("interrupted"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if !interrupted {
                    // Completed turn — the provider's checkpoint covers this
                    // row and everything before it.
                    break;
                }
                let blocks: &[Value] = m.content.as_array().map_or(&[], Vec::as_slice);
                let text = crate::agent_session::text_block_strings(blocks).join("");
                if !text.is_empty() {
                    segments.push(TailSegment::Partial(text));
                }
            }
            "user" => {
                let tagged_continuation = m
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("type"))
                    .and_then(Value::as_str)
                    == Some(RESUME_CONTINUATION_METADATA_TYPE);
                let blocks: &[Value] = m.content.as_array().map_or(&[], Vec::as_slice);
                let text = crate::agent_session::text_block_strings(blocks).join("\n");
                if tagged_continuation || text == LEGACY_RESUME_CONTINUATION_TEXT {
                    // A previous resume's continuation row (metadata-tagged,
                    // or a legacy pre-tag row matched by its exact wording)
                    // — re-sent fresh by every resume, never replayed.
                    continue;
                }
                let mut row_images: Vec<Value> = Vec::new();
                let mut row_files: Vec<Value> = Vec::new();
                for b in blocks {
                    match b.get("type").and_then(Value::as_str) {
                        Some("image") => row_images.push(b.clone()),
                        Some("file") => row_files.push(b.clone()),
                        _ => {}
                    }
                }
                if !row_images.is_empty() {
                    image_rows.push(row_images);
                }
                if !row_files.is_empty() {
                    file_rows.push(row_files);
                }
                if !text.is_empty() {
                    segments.push(TailSegment::User(text));
                }
            }
            // Any other tail shape (e.g. a bare `tool` row) is not the
            // interrupted-turn pattern this recap understands — fail safe.
            _ => return None,
        }
    }
    if segments.is_empty() {
        return None;
    }
    segments.reverse();
    image_rows.reverse();
    file_rows.reverse();
    let image_blocks: Vec<Value> = image_rows.into_iter().flatten().collect();
    let file_blocks: Vec<Value> = file_rows.into_iter().flatten().collect();
    let elided = segments.len().saturating_sub(RESUME_RECAP_MAX_SEGMENTS);
    if elided > 0 {
        segments.drain(1..=elided);
    }
    let has_partial = segments
        .iter()
        .any(|s| matches!(s, TailSegment::Partial(_)));
    let mut recap = String::from(
        "<supervisor>\nRestart recovery: the harness restarted while you were \
         responding, and your restored session may predate the exchange below \
         — it is repeated here (parts of it may or may not already be in your \
         context).\n\n",
    );
    if elided > 0 {
        let _ = write!(recap, "({elided} older interrupted segment(s) elided.)\n\n");
    }
    for segment in &segments {
        let (label, tag, text) = match segment {
            TailSegment::User(text) => (
                "The user's message, delivered before the interruption:",
                "interrupted_user_message",
                text,
            ),
            TailSegment::Partial(text) => (
                "Your partial response before the cut-off (it did NOT \
                 complete; anything after this point was lost):",
                "interrupted_partial_response",
                text,
            ),
        };
        let _ = write!(
            recap,
            "{label}\n<{tag}>\n{}\n</{tag}>\n\n",
            crate::history_xml::escape_xml(&crate::history_xml::truncate_middle_content(
                text,
                RESUME_RECAP_SEGMENT_MAX_CHARS,
            ))
        );
    }
    if !has_partial {
        recap.push_str(
            "Your response was cut off before any output was produced — \
             treat that request as not yet acted on.\n",
        );
    }
    recap.push_str("</supervisor>");
    Some(ResumeTailRecap {
        text: recap,
        image_blocks: (!image_blocks.is_empty()).then(|| Value::Array(image_blocks)),
        file_blocks: (!file_blocks.is_empty()).then(|| Value::Array(file_blocks)),
    })
}

impl Services {
    /// Wake-triggered auto-resume sweep (sleep-resume Task D): on a host wake the
    /// daemon's resume orchestrator calls this to resume every turn Task C
    /// enrolled as `system_suspend`-interrupted. It enumerates ONLY pending
    /// `interrupted_agent` rows tagged [`InterruptReason::SystemSuspend`] — rows a
    /// user left pending for any other reason (daemon restart, agent stop, manual
    /// interrupt) are never touched — and drives each through the existing
    /// [`Services::resume_interrupted_agent`] path, whose atomic claim dedupes
    /// against a concurrent `agent.resolveInterrupted` / `--resume-all`.
    ///
    /// Guard: an agent whose persisted session has no `acpSessionId` cannot be
    /// reloaded via `session/load`, so it is skipped and left pending for today's
    /// manual retry (the cheap static half of the `supports_load_session` gate;
    /// the capability half is re-checked inside the resume path). A per-agent
    /// resume failure is logged and never aborts the sweep — the row is reset to
    /// pending by `resume_interrupted_agent` and retried on the next wake or
    /// manually.
    ///
    /// Returns the number of agents successfully resumed by this sweep.
    pub async fn resume_suspend_interrupted_agents(&self) -> usize {
        let rows = match self.store.list_interrupted_agents().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(error = %e, "wake-resume: failed to list interrupted agents");
                return 0;
            }
        };
        let suspend_reason = crate::agent_session::InterruptReason::SystemSuspend.as_str();
        let mut resumed = 0usize;
        for interrupted in rows {
            // Never blanket-resume rows a user left pending for another reason.
            if interrupted.reason.as_deref() != Some(suspend_reason) {
                continue;
            }
            let agent_id = interrupted.agent_id.clone();
            // Guard: `session/load` needs a persisted acpSessionId; without one
            // the provider cannot reload, so leave the row pending for manual
            // retry rather than re-running the whole turn unattended.
            match self.store.get_agent_session(&agent_id).await {
                Ok(session) if session.acp_session_id.is_none() => {
                    tracing::info!(
                        agent_id = %agent_id,
                        "wake-resume: no resumable acpSessionId; leaving pending for manual retry"
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        agent_id = %agent_id,
                        error = %e,
                        "wake-resume: session lookup failed; leaving row pending"
                    );
                    continue;
                }
            }
            match self.resume_interrupted_agent(&agent_id).await {
                Ok(()) => {
                    tracing::info!(
                        agent_id = %agent_id,
                        workspace = %interrupted.workspace_id,
                        "wake-resume: resumed suspend-interrupted agent"
                    );
                    resumed += 1;
                }
                // A concurrent resolveInterrupted / --resume-all may have claimed
                // the row first (atomic claim → "already resolved"): that is the
                // expected dedupe outcome, not an error, so log it quietly.
                Err(e) => {
                    tracing::debug!(
                        agent_id = %agent_id,
                        error = %e,
                        "wake-resume: resume skipped (already resolved or transient failure)"
                    );
                }
            }
        }
        if resumed > 0 {
            tracing::info!(resumed, "wake-resume: sweep complete");
        }
        resumed
    }

    /// Resume an interrupted agent (INT-41 phase 2): atomically claim the row, re-register
    /// parent completion watch when delegated, then deliver a continuation message.
    ///
    /// Claim-then-act semantics prevent concurrent resume races (exactly-one-winner).
    /// If any post-claim step fails (`agent_send_message`, session lookup), the row is
    /// reset to pending (resolution=NULL) to restore retryability, and the error is
    /// returned loudly.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` if the agent is not in pending interrupted state (or was already resolved); `Error::Internal` if a store operation fails.
    pub async fn resume_interrupted_agent(&self, agent_id: &AgentId) -> Result<()> {
        // Verify the agent is in pending interrupted state (O(1) query)
        let interrupted = self
            .store
            .get_interrupted_agent(agent_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "Agent {agent_id} is not in pending interrupted state"
                ))
            })?;

        let workspace_id = interrupted.workspace_id.clone();

        // ATOMIC CLAIM: mark the row as resumed FIRST. If another process already claimed
        // it, set_interrupted_resolution returns false and we bail loudly. This prevents
        // concurrent resume races (e.g., --resume-all vs client resolveInterrupted).
        let updated = self
            .store
            .set_interrupted_resolution(agent_id, "resumed", &now_iso())
            .await?;
        if !updated {
            return Err(Error::InvalidParams(format!(
                "Agent {agent_id} is not in pending interrupted state (already resolved)"
            )));
        }

        // Helper to reset the row to pending if post-claim steps fail
        let reset_to_pending = || async {
            if let Err(e) = self.store.reset_interrupted_resolution(agent_id).await {
                tracing::warn!(
                    "Failed to reset interrupted_agent row to pending for {}: {}",
                    agent_id,
                    e
                );
            }
        };

        // Soft-retire gate: a session interrupted mid-turn and then retired
        // must not be redriven — retiring means nothing may start a turn on
        // it. Probed AFTER the atomic claim so the check inherits its race
        // protection; the row resets to pending so the interruption stays
        // resolvable once `agent.restore` returns the session to service.
        match self.store.get_agent_session_retired_at(agent_id).await {
            Ok(Some(_)) => {
                reset_to_pending().await;
                return Err(Error::InvalidParams(format!(
                    "agent {agent_id} is retired; restore it with agent.restore before resuming"
                )));
            }
            Ok(None) => {}
            Err(e) => {
                // Fail open, matching the drain/wake gates: the gate only
                // parks affirmatively-retired sessions.
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "resume: retired-state lookup failed; proceeding"
                );
            }
        }

        // Rehydrate delegation groups for this workspace (idempotent, best-effort).
        // Groups are sealed on rehydration since the original parent turn is gone.
        // Corrupted rows are logged at warn but do NOT fail the resume: the agent can
        // still proceed (group wake degrades to per-child) rather than failing entirely.
        if let Err(e) = self.rehydrate_delegation_groups(&workspace_id).await {
            tracing::warn!(
                "rehydrate_delegation_groups failed for workspace {}: {}. \
                 Agent will resume but after_all fan-in may degrade to per-child wakes.",
                workspace_id.0,
                e
            );
        }

        // Load the session to check for delegation metadata
        let session = match self.store.get_agent_session(agent_id).await {
            Ok(s) => s,
            Err(e) => {
                reset_to_pending().await;
                return Err(e);
            }
        };

        // Re-register parent completion watch if the agent was delegated
        if let Some(parent_id) = &session.parent_agent_id {
            // Extract createdByAgentId from metadata if present
            let created_by = session
                .metadata
                .as_ref()
                .and_then(|m| m.get("createdByAgentId"))
                .and_then(|v| v.as_str())
                .map(AgentId::from);

            if let Some(parent) = created_by.or_else(|| Some(parent_id.clone())) {
                // Fetch parent session for name + home workspace. The watch
                // (and any group) is anchored in the parent's HOME workspace;
                // fall back to the child's workspace when the parent session
                // cannot be loaded (matches the pre-lift behavior).
                let parent_session = self.store.get_agent_session(&parent).await.ok();
                let parent_name = parent_session
                    .as_ref()
                    .map(|s| s.name.clone())
                    .unwrap_or_default();
                let resolved_home = parent_session.map(|s| s.workspace_id);
                let parent_home_ws = resolved_home
                    .clone()
                    .unwrap_or_else(|| workspace_id.clone());

                // A cross-workspace (chief) parent's group is persisted under
                // the parent's home workspace, which the child-workspace
                // rehydration above did not load — rehydrate it too so the
                // grouped child re-enrolls (idempotent, best-effort).
                if parent_home_ws != workspace_id {
                    let _ = self.rehydrate_delegation_groups(&parent_home_ws).await;
                }

                // Check if this agent is in a rehydrated delegation group
                let groups = self.list_groups_for_parent(&parent);
                let group_id = groups
                    .iter()
                    .find(|g| g.expected_agent_ids.contains(agent_id))
                    .map(|g| g.group_id.clone());

                let registered = if let Some(gid) = group_id {
                    // Register grouped completion watch for after_all fan-in
                    self.register_completion_watch(
                        &parent_home_ws,
                        &workspace_id,
                        parent.clone(),
                        parent_name,
                        agent_id.clone(),
                        Some(gid),
                    )
                    .map(|_| ())
                } else {
                    // Register ungrouped completion watch (dedupe via find_and_refresh)
                    match self.find_and_refresh_ungrouped_watch(
                        &parent,
                        agent_id,
                        Some(parent_name.clone()),
                        resolved_home.as_ref(),
                    ) {
                        Some(_) => Ok(()),
                        None => self
                            .register_completion_watch(
                                &parent_home_ws,
                                &workspace_id,
                                parent.clone(),
                                parent_name,
                                agent_id.clone(),
                                None,
                            )
                            .map(|_| ()),
                    }
                };
                match registered {
                    // The re-armed watch changed (or refreshed) the parent's
                    // watch set: publish the subscriptions snapshot like every
                    // other watch-lifecycle site (monorepo#1449) — the publish
                    // also recomputes the anchor workspace's displayStatus.
                    Ok(()) => {
                        self.publish_subscriptions_changed(&parent_home_ws, &parent)
                            .await;
                    }
                    Err(e) => {
                        // Scope-gate rejection is non-fatal on resume: the agent
                        // still continues; only the parent wake path is lost.
                        tracing::warn!(
                            agent = %agent_id.0,
                            error = %e,
                            "resume: completion-watch re-registration rejected"
                        );
                    }
                }
            }
        }

        // Append a system interruption marker BEFORE the continuation so the
        // transcript shows the interruption boundary (same shape as the abandon
        // path; the FE InterruptionNotice keys off `meta.kind == "interruption"`).
        // Idempotent on retry: if a prior resume attempt appended the marker but
        // failed delivering the continuation (row reset to pending), the marker
        // is already the transcript tail — skip the duplicate append. A failed
        // append is treated like a failed continuation delivery: reset the row
        // to pending and surface the error.
        let marker_text =
            "The previous turn was interrupted because the harness shut down. Continuing below.";
        let marker_content = json!([{
            "type": "text",
            "text": marker_text,
            "meta": { "kind": "interruption" }
        }]);
        let already_marked = session
            .messages
            .last()
            .is_some_and(|m| m.role == "system" && m.content == marker_content);
        if !already_marked {
            let marker = match self
                .store
                .append_agent_message(agent_id, "system", &marker_content, &now_iso())
                .await
            {
                Ok(message) => message,
                Err(e) => {
                    reset_to_pending().await;
                    return Err(e);
                }
            };
            self.invalidate_agent_list_cache(&workspace_id);

            // Emit agent:message + agent:updated so live UIs render the marker.
            self.publish_agent_message_events(&workspace_id, agent_id, &marker, None)
                .await;
            self.publish_agent_mutation_event(
                &workspace_id,
                agent_id,
                AGENT_UPDATED,
                json!({ "agentId": agent_id.0 }),
            )
            .await;
        }

        // Deliver continuation message (with the humanized outage duration
        // when the claimed row's `interrupted_at` yields one). The metadata
        // tag persists on the user row so a later restart's recap can
        // identify the continuation without matching its text (an ordinary
        // user request opening with the same wording must still be replayed).
        let continuation =
            resume_continuation_text(&interrupted.interrupted_at, time::OffsetDateTime::now_utc());
        let continuation_metadata = json!({
            "type": RESUME_CONTINUATION_METADATA_TYPE,
            "source": "system",
        });

        // Mid-turn tail recap (monorepo#2539): a `session/load` resume replays
        // the PROVIDER's session checkpoint, which contains only completed
        // turns — the interrupted `session/prompt` never resolved
        // provider-side, so the interrupting user message and the partial
        // assistant output (both durable in the daemon transcript; the UI
        // renders them) would silently vanish from the model-visible context
        // and the agent would resume from a stale point. Rebuild that tail
        // from the transcript and deliver it prompt-only
        // (`TurnOptions::prepend_content`, attachments via the prepend block
        // fields) ahead of the continuation. `build_turn_prompt` drops the
        // prepend TEXT when the session is recreated instead of resumed
        // (`history_covers_prepend`: the `<supervisor>` history replay
        // already carries the whole transcript) while the prepend
        // ATTACHMENTS still ride (the history XML is text-only), so the tail
        // reaches the agent exactly once on either branch. The recap is
        // built from the pre-marker `session` snapshot, so the just-appended
        // interruption marker never leaks into it.
        let recap = build_resume_tail_recap(&session.messages);

        // Use the send-message machinery to deliver the continuation (lazily
        // respawns the provider and resumes via ACP `session/load`).
        // Automatic origin: a resume continuation must not bury a Q&A the
        // agent had pending when the harness shut down (question hold — the
        // marker is persisted, so the hold survives the restart). The manager
        // path is called directly so the recap can ride
        // `TurnOptions::prepend_content`; the store-only fallback (no manager
        // attached) keeps the plain trait call — it drives no outbound
        // prompt, so there is no context to repair.
        let send_result = match self.agent_manager() {
            Some(manager) => {
                let options = match recap {
                    Some(recap) => crate::agent_manager::TurnOptions {
                        prepend_content: Some(recap.text),
                        prepend_image_blocks: recap.image_blocks,
                        prepend_file_blocks: recap.file_blocks,
                        message_metadata: Some(continuation_metadata.clone()),
                        origin: intent_core::MessageOrigin::Automatic,
                        ..crate::agent_manager::TurnOptions::default()
                    },
                    None => crate::agent_manager::TurnOptions {
                        message_metadata: Some(continuation_metadata.clone()),
                        origin: intent_core::MessageOrigin::Automatic,
                        ..crate::agent_manager::TurnOptions::default()
                    },
                };
                manager
                    .send_message(
                        agent_id.clone(),
                        workspace_id.clone(),
                        continuation.clone(),
                        None,
                        options,
                    )
                    .await
            }
            None => {
                self.agent_send_message(
                    workspace_id.clone(),
                    agent_id.clone(),
                    continuation,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(continuation_metadata),
                    intent_core::MessageOrigin::Automatic,
                )
                .await
            }
        };
        if let Err(e) = send_result {
            reset_to_pending().await;
            return Err(e);
        }

        Ok(())
    }

    /// Abandon an interrupted agent (INT-41 phase 2): mark row `abandoned`, append
    /// a system interruption message to the log, emit chat/agent events for live UIs.
    pub(crate) async fn abandon_interrupted_agent(&self, agent_id: &AgentId) -> Result<()> {
        // Verify the agent is in pending interrupted state (O(1) lookup)
        let interrupted = self
            .store
            .get_interrupted_agent(agent_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "Agent {agent_id} is not in pending interrupted state"
                ))
            })?;

        let workspace_id = interrupted.workspace_id.clone();

        // Rehydrate delegation groups for this workspace (idempotent, best-effort).
        let _ = self.rehydrate_delegation_groups(&workspace_id).await;

        // Load the session to check for delegation group membership
        let session = self.store.get_agent_session(agent_id).await.ok();
        if let Some(session) = &session {
            if let Some(parent_id) = &session.parent_agent_id {
                // A cross-workspace (chief) parent's group is persisted under
                // the parent's home workspace — rehydrate it too so this
                // abandoned child is recorded there (idempotent, best-effort).
                if let Ok(parent_session) = self.store.get_agent_session(parent_id).await {
                    if parent_session.workspace_id != workspace_id {
                        let _ = self
                            .rehydrate_delegation_groups(&parent_session.workspace_id)
                            .await;
                    }
                }
                // Check if this agent is in a delegation group
                let groups = self.list_groups_for_parent(parent_id);
                for group in groups {
                    if group.expected_agent_ids.contains(agent_id) {
                        // Record this child as deleted in the group (AS-4 abandoned child path).
                        // Build a minimal deleted-agent event for group completeness tracking.
                        let deleted_event = Event {
                            id: format!("abandon-{}", agent_id.0),
                            workspace_id: workspace_id.clone(),
                            timestamp: now_iso(),
                            event_type: AGENT_DELETED.to_string(),
                            actor: EventActor {
                                actor_type: ActorType::System,
                                id: Some("intentd".to_string()),
                                name: Some("intentd".to_string()),
                                ..Default::default()
                            },
                            session_id: Some(agent_id.0.clone()),
                            correlation_id: None,
                            parent_event_id: None,
                            metadata: None,
                            data: json!({
                                "agentId": agent_id.0,
                                "agentName": session.name.clone(),
                            }),
                        };
                        self.record_group_child_completion(
                            &group.group_id,
                            agent_id,
                            true,
                            format!("Agent {} abandoned during restart", agent_id.0),
                            deleted_event,
                        )
                        .await;
                    }
                }
            }
        }

        // Build the system interruption message
        let text = "This conversation was interrupted because intentd restarted. The agent's in-flight work was terminated.";
        let content = json!([{
            "type": "text",
            "text": text,
            "meta": { "kind": "interruption" }
        }]);

        // Append the system message
        let message = self
            .store
            .append_agent_message(agent_id, "system", &content, &now_iso())
            .await?;
        self.invalidate_agent_list_cache(&workspace_id);

        // Mark the interrupted_agent row as resolved
        let updated = self
            .store
            .set_interrupted_resolution(agent_id, "abandoned", &now_iso())
            .await?;
        if !updated {
            return Err(Error::InvalidParams(format!(
                "Agent {agent_id} is not in pending interrupted state"
            )));
        }

        // Emit agent:message events so live UIs see the new message
        self.publish_agent_message_events(&workspace_id, agent_id, &message, None)
            .await;

        Ok(())
    }
}

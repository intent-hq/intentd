//! Wire-facing domain structs (§9.1). Every struct uses
//! `#[serde(rename_all = "camelCase")]` so JSON matches the existing TS types
//! and PROTOCOL.md §5.1/§5.2. Enums serialize to their lowercase / snake_case
//! string forms, which are also their stored DB representations.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{
    AgentId, ClientId, HookId, NoteId, PrMonitorId, WorkspaceGitRootId, WorkspaceId,
    CHIEF_WORKSPACE_ID,
};

/// Workspace lifecycle (§9.1; TS `WorkspaceStatus` in `src/shared/types.ts`).
/// Wire values are the PascalCase variant names (`Active`/`Inactive`/`Archived`/
/// `Deleted`), matching the TS string enum exactly; these are also the stored DB
/// words (the column DEFAULT is unused — inserts always bind explicitly).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum WorkspaceStatus {
    #[default]
    Active,
    Inactive,
    Archived,
    Deleted,
}

/// Derived in-flight agent state (green dot; read-only, not persisted) (§9.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceActivity {
    #[default]
    Idle,
    AgentRunning,
}

/// Dismissible attention flag (blue dot; server-owned) (§9.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAttention {
    #[default]
    None,
    Unread,
    ReviewRequired,
}

/// Pull-request lifecycle status (§9.1; TS `PullRequestStatus` in
/// `src/shared/types.ts`). Wire values are the PascalCase variant names
/// (`Open`/`Closed`/`Merged`/`Draft`), matching the TS string enum exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullRequestStatus {
    Open,
    Closed,
    Merged,
    Draft,
}

/// Pull-request metadata persisted on a [`Workspace`] as `activePullRequest`
/// (§7.6; TS `PullRequestInfo` in `src/shared/types.ts`). A focused subset of
/// the TS shape populated from the host-agnostic forge `PullRequest`; required
/// fields match the TS required set, and absent optionals are omitted from the
/// wire to mirror `JSON.stringify`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestInfo {
    pub id: String,
    pub number: u64,
    pub url: String,
    pub title: String,
    pub status: PullRequestStatus,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mergeable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mergeable_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_draft: Option<bool>,
}

/// Note body content type (§9.1). `PlainText` serializes as `plain_text` to
/// match the TS `ContentType` enum (`src/shared/types.ts`); the others are their
/// lowercase names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    #[default]
    Markdown,
    #[serde(rename = "plain_text")]
    PlainText,
    Json,
    Code,
}

/// Note visibility (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoteVisibility {
    Private,
    #[default]
    Workspace,
    Shared,
    Public,
}

/// Derived `Workspace.displayStatus` (TS `WorkspaceDisplayStatus` union):
/// the BE-owned canonical status rollup over the active/latest PR,
/// `taskStats`, live agent activity, and the per-workspace attention axes.
/// Wire values are the snake_case variant names, matching the FE union
/// exactly. Canonical precedence (§6.5): `Failed` (a top-level agent parked
/// in `error`) > `Blocked` (a top-level pending `blocker` attention
/// request) > `NeedsAttention` (discussion requests, pending structured
/// questions, or the `review_required` attention flag) > `InProgress`
/// (running agent) > the PR/task rollup. The dismissible `unread` attention
/// flag (`Workspace.attention`, §9.9) never feeds the derivation — unread
/// is the flag's own contract, not a display status. Without a running
/// agent, a task-stage rollup (`InProgress`/`NotStarted`) demotes to `Idle`
/// — so `NotStarted` and the task-derived `InProgress` never reach the wire
/// on their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceDisplayStatus {
    NotStarted,
    InProgress,
    NeedsAttention,
    Failed,
    Blocked,
    Idle,
    Complete,
    PrReady,
    PrOpen,
    PrMerged,
}

/// Workspace entity (§9.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub id: WorkspaceId,
    pub title: String,
    pub branch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_commit_sha: Option<String>,
    pub status: WorkspaceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// Asset id of the agent-authored workspace status screenshot
    /// (intent-hq/monorepo#997). Points at a content-addressed asset stored
    /// via the `note.saveAsset` machinery; clients render it with
    /// `note.readAsset` / `workspace-asset://<workspaceId>/<assetId>`.
    /// Omitted (not `null`) until an agent sets one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_image_asset_id: Option<String>,
    /// Derived, read-only; never persisted (§9.9).
    pub activity: WorkspaceActivity,
    pub attention: WorkspaceAttention,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub skip_worktree: bool,
    /// Durable worktree setup script (§5.25): the persisted `SetupScript` record
    /// read/written via `workspace.getSetupScript`/`saveSetupScript`. Omitted (not
    /// `null`) until a script has been saved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_script: Option<SetupScript>,
    pub is_remote: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    /// Persisted PR lifecycle status for the linked PR (§7.6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_status: Option<PullRequestStatus>,
    /// Persisted snapshot of the linked PR (§7.6); refreshed in the background.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_pull_request: Option<PullRequestInfo>,
    /// Persisted list of PR snapshots discovered for the workspace's baseRef
    /// (§7.6). Distinct from `activePullRequest` (the currently-linked PR); the
    /// FE reconciles stale PR links against this collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_requests: Option<Vec<PullRequestInfo>>,
    pub archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<String>,
    /// Card-aggregate rollups (§9.1). Populated only on the `workspace.list` /
    /// `workspace.get` emit paths (omitted elsewhere, e.g. `create`/`update`).
    /// The iOS coverflow cards read `taskStats`, `agentSummary`, and
    /// `diffSummary`; each is omitted (not `null`) when not computable so absent
    /// simply yields a sparser card.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_stats: Option<WorkspaceTaskStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_summary: Option<WorkspaceAgentSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<WorkspaceDiffSummary>,
    /// Derived "current cycle" display status. Computed on the same
    /// `workspace.list` / `workspace.get` emit path as the card aggregates
    /// (never persisted) from the active/latest PR and `taskStats`; omitted
    /// elsewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_status: Option<WorkspaceDisplayStatus>,
    /// Daemon-owned orthogonal wait flag (PROTOCOL §5.1): `true` when the workspace
    /// has any of ACTIVE background hooks, ACTIVE PR monitors, or waiting
    /// agent subscriptions (undelivered child completion watches held by
    /// top-level foreground agents, anchored in the parent's home
    /// workspace; `event.subscribe` registrations deliberately do not
    /// count) — the workspace is watching an external condition without a
    /// running agent turn. Orthogonal to `displayStatus` (a workspace can
    /// be `complete` or `pr_ready` and still waiting). Served from the
    /// last-observed cache on the same `workspace.list` / `workspace.get` /
    /// subscription emit path as `displayStatus` (never persisted; the
    /// hook/monitor/watch mutation choke points keep the cache current, and
    /// only a first-touch miss probes the store); omitted (not `false`)
    /// when no wait signal is live.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub waiting: bool,
    /// Durable token/credit usage accounting (§5.23 / §19.1), materialized by the
    /// daemon-internal periodic scan job and surfaced by `workspace.getTokenUsage`.
    /// Omitted (not `null`) until the first scan writes a snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<TokenUsage>,
    /// Whether CoW isolation is supported on this machine. Computed as:
    /// cow_probe(workspacesRoot, workspacesRoot) Supported — a machine
    /// capability of the workspaces root's filesystem, independent of the
    /// workspace or checkout mode. Used by the FE to gate the Copy-on-Write
    /// opt-in toggle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cow_supported: Option<bool>,
    /// How the workspace checkout was provisioned by `workspace.create` (§5.1):
    /// `worktree` (linked git worktree), `cow` (standalone copy-on-write
    /// clone), or `direct` (standalone plain repository — a local git clone
    /// hydrated from the repo cache, or an `isNewRepo` initialization working
    /// directly in the repository folder). Omitted for rows without a
    /// daemon-provisioned checkout (`skipWorktree`, remote, caller-supplied
    /// `worktreePath`, non-git repo paths, pre-existing rows).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkout_mode: Option<CheckoutMode>,
    /// Disk footprint of the daemon-managed workspace directory
    /// (`<workspaces_root>/<workspaceId>`: repo checkout, tool-outputs, agent
    /// sandboxes, everything). Never populated on `workspace.list` /
    /// `workspace.get` rows (monorepo#1396) — clients fetch it on demand via
    /// the dedicated `workspace.diskUsage` method (§5.1), which serves a
    /// cached background walk; never persisted. Omitted (not `null`) until
    /// the first walk completes and for rows without a daemon-managed
    /// directory (remote / skip-isolation / chief).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_usage: Option<WorkspaceDiskUsage>,
    /// ISO deadline of an in-memory pending deletion (PROTOCOL §5.1): set on
    /// `workspace.list` / `workspace.get` rows while a `workspace.delete`
    /// grace window (`undoDelayMs > 0`) is running, so clients can render or
    /// hide the row as they choose. Never persisted — a daemon restart drops
    /// the pending deletion and the field disappears. Omitted (not `null`)
    /// when no deletion is pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_delete_at: Option<String>,
}

/// Provisioning mode of a workspace checkout (`Workspace.checkoutMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckoutMode {
    /// Linked git worktree of the source repository.
    Worktree,
    /// Standalone copy-on-write clone of the source repository directory.
    Cow,
    /// Standalone plain git repository: a local clone hydrated from the repo
    /// cache (non-CoW filesystems), or an `isNewRepo` initialization working
    /// directly in the repository folder (no worktree provisioned).
    Direct,
}

/// Disk footprint of a workspace's daemon-managed directory
/// (`Workspace.diskUsage`). Reports **physical (allocated) bytes** — sparse
/// regions excluded, hard links deduped within one walk — so the number is an
/// upper bound for CoW-clone checkouts (clone-shared extents count at full
/// size in every workspace that references them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiskUsage {
    /// Physical (allocated) bytes for the whole workspace folder.
    pub bytes: u64,
    /// Regular files that contributed bytes (hard-link duplicates once).
    pub file_count: u64,
    /// RFC-3339 wall-clock time the walk completed.
    pub computed_at: String,
    /// Per top-level entry of the workspace folder, sorted by bytes desc
    /// (loose top-level files grouped under `"other"`).
    pub breakdown: Vec<DiskUsageBreakdownEntry>,
}

/// One top-level entry of a workspace folder's disk-usage breakdown
/// (directory name, or `"other"` for loose files).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskUsageBreakdownEntry {
    pub name: String,
    pub bytes: u64,
    pub file_count: u64,
}

/// Fixed timestamp for the synthetic Chief workspace (TS
/// `CHIEF_WORKSPACE_TIMESTAMP` in `workspace.repository.ts`). Chief is not a
/// real workspace on disk, so its `createdAt` / `updatedAt` / `lastActivity`
/// are pinned to a stable epoch rather than the daemon's clock.
pub const CHIEF_WORKSPACE_TIMESTAMP: &str = "2026-01-01T00:00:00.000Z";

/// Return the synthesized "Chief of Staff" [`Workspace`] (TS
/// `getChiefWorkspace` in `workspace.repository.ts`). Chief has no
/// repository, worktree, branch, or card aggregates: it is a daemon-known
/// virtual scope for Chief-of-Staff agents that never appears in
/// `workspace.list` and is never persisted, but `workspace.get` returns
/// this shape and `agent.create` accepts its id as the workspace scope.
pub fn chief_workspace() -> Workspace {
    Workspace {
        id: WorkspaceId::chief(),
        title: "Chief of Staff".to_string(),
        branch: String::new(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: CHIEF_WORKSPACE_TIMESTAMP.to_string(),
        updated_at: CHIEF_WORKSPACE_TIMESTAMP.to_string(),
        last_activity: Some(CHIEF_WORKSPACE_TIMESTAMP.to_string()),
        tags: Vec::new(),
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        display_status: None,
        waiting: false,
        token_usage: None,
        cow_supported: None,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Whether the given workspace id is the reserved [`CHIEF_WORKSPACE_ID`].
#[inline]
pub fn is_chief_workspace(id: &WorkspaceId) -> bool {
    id.0 == CHIEF_WORKSPACE_ID
}

/// A monetary cost figure attached to a token tally (PROTOCOL §5.23), sourced
/// from the ACP `usage_update` session notification's `cost` object. `amount`
/// is the cumulative spend and `currency` an ISO 4217 code (e.g. `"USD"`).
/// Only providers that report cost populate it — absence is never coerced to
/// zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageCost {
    pub amount: f64,
    pub currency: String,
}

impl UsageCost {
    /// Pairwise cost merge used where at most two figures combine (the
    /// recreate baseline fold and the `baseline + snapshot` per-agent tally):
    /// matching currencies sum, a (pathological) mismatch keeps the larger
    /// amount (ties keep `lhs`, the banked baseline). Absent operands
    /// contribute nothing.
    ///
    /// The mismatch arm is deliberately lossy and, unlike the bucket roll-up
    /// (which sums per currency before picking the dominant one), it is
    /// applied iteratively: each ACP session recreate re-merges into the
    /// banked baseline, so an agent that switches to a cheaper-per-session
    /// currency discards the new currency's spend one fold at a time. That is
    /// an accepted consequence of storing a single figure — a cross-currency
    /// numeric comparison is semantically meaningless and the daemon never
    /// invents a conversion rate.
    pub fn merge(lhs: Option<&UsageCost>, rhs: Option<&UsageCost>) -> Option<UsageCost> {
        match (lhs, rhs) {
            (None, None) => None,
            (Some(c), None) | (None, Some(c)) => Some(c.clone()),
            (Some(a), Some(b)) => Some(if a.currency == b.currency {
                UsageCost {
                    amount: a.amount + b.amount,
                    currency: a.currency.clone(),
                }
            } else if b.amount > a.amount {
                b.clone()
            } else {
                a.clone()
            }),
        }
    }
}

/// The consumption counters of a token tally (PROTOCOL §5.23 / §19.1), plus
/// the optional provider-reported cost. Reused for the per-agent, per-model,
/// and workspace-wide rollups.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    /// Cumulative reasoning ("thought") tokens, reported by providers that
    /// break them out of `outputTokens` via ACP `usage_update.thoughtTokens`.
    /// Omitted (not `0`) when nothing reported any, so clients that predate
    /// the field see the previous shape byte-for-byte.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub thought_tokens: u64,
    /// Cumulative cost, present only when at least one contributing session
    /// reported one via ACP `usage_update`. Omitted (not `null`) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<UsageCost>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl TokenUsageTotals {
    /// Whether every consumption counter is zero, i.e. this tally carries
    /// no token report at all. A persisted session snapshot in that state is a
    /// cost-only report (§5.23) and MUST NOT suppress the per-message token
    /// fallback for a provider that never sends an end-of-turn token report.
    pub fn counters_are_zero(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.cache_read_tokens == 0
            && self.cache_creation_tokens == 0
            && self.thought_tokens == 0
    }
}

/// Whether a session's persisted recreate `baseline` / current-session
/// `snapshot` pair carries an actual token report, i.e. whether the per-agent
/// tally uses it instead of falling back to summing per-message usage
/// metadata (§5.23). An all-zero-counter part counts as "no report": a
/// cost-only `usage_update` persist writes exactly that shape, and treating it
/// as a report would silently zero the counters of a provider that never sends
/// the end-of-turn token report. Shared by the store's usage-row fetch (which
/// hydrates message contents only for fallback sessions, monorepo#738) and the
/// tally itself, so the two decisions can never drift apart.
pub fn token_usage_reported(
    baseline: Option<&TokenUsageTotals>,
    snapshot: Option<&TokenUsageTotals>,
) -> bool {
    let reported = |t: Option<&TokenUsageTotals>| t.is_some_and(|t| !t.counters_are_zero());
    reported(baseline) || reported(snapshot)
}

/// Durable token-usage snapshot returned by `workspace.getTokenUsage` and pushed
/// via `workspace:tokenUsage-changed` (PROTOCOL §5.23 / §6.5). `byAgentId` keys
/// are `agent-{uuid}`; `byModel` keys are the effective model name (`"unknown"`
/// fallback); `lastScanAt` is the RFC-3339 timestamp of the last internal scan
/// (`null` before the first scan).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub by_agent_id: BTreeMap<String, TokenUsageTotals>,
    pub totals: TokenUsageTotals,
    pub by_model: BTreeMap<String, TokenUsageTotals>,
    pub last_scan_at: Option<String>,
}

/// Coarse project classification for worktree setup (PROTOCOL §5.25), detected
/// from a manifest file. The source detector additionally distinguishes package
/// managers (npm/yarn/pnpm, pip/poetry); the BE collapses to this coarse enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectType {
    Node,
    Python,
    Go,
    Rust,
    Ruby,
}

/// Who authored a [`SetupScript`] body (PROTOCOL §5.25): `user` for a hand-saved
/// script (`saveSetupScript`) or `agent` for an AI-assisted draft
/// (`generateSetupScript`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SetupScriptGeneratedBy {
    User,
    Agent,
}

/// Durable per-workspace worktree setup script (PROTOCOL §5.25). Persisted on the
/// `setupScript` field of the `Workspace`; returned by `workspace.getSetupScript`,
/// `saveSetupScript`, and `generateSetupScript`. `updatedAt` is the last-write
/// epoch-ms; `generatedBy` records whether the body was hand-written or drafted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetupScript {
    pub script: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_type: Option<ProjectType>,
    pub updated_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_by: Option<SetupScriptGeneratedBy>,
}

/// Script mode for repo scripts (service = long-running, command = run-once).
/// Matches `RepoScript.mode` in `cloudlands-fe/src/shared/types/repo-config.types.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepoScriptMode {
    Service,
    Command,
}

/// Script category for repo scripts. Matches `RepoScript.category` in
/// `cloudlands-fe/src/shared/types/repo-config.types.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepoScriptCategory {
    Dev,
    Build,
    Test,
    Lint,
    Typecheck,
    Format,
    Storybook,
    Other,
}

/// Per-repository script definition (FE-parity with `RepoScript` in
/// `cloudlands-fe/src/shared/types/repo-config.types.ts`). Part of the
/// committable `.intent/config.json` file. Scripts can be seeded into
/// workspace script storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoScript {
    pub name: String,
    pub command: String,
    pub mode: RepoScriptMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<RepoScriptCategory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_start: Option<bool>,
}

/// Per-repository configuration stored in `.intent/config.json` in a repo root.
/// FE-parity with `RepoConfig` in `cloudlands-fe/src/shared/types/repo-config.types.ts`.
/// All fields are optional; missing fields fall back to global app settings or none.
/// Unknown JSON keys are preserved on read→write round-trip.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RepoConfig {
    /// Branch prefix for new workspaces created from this repo (e.g. "feature/").
    /// Overrides the global branch prefix setting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_prefix: Option<String>,

    /// Default setup script to run after creating a git worktree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_script: Option<String>,

    /// General instructions for AI agents working in this repo, appended to system prompt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// Script to run the project in development mode (for agents to start dev servers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_script: Option<String>,

    /// Script to run when archiving/cleaning up a workspace (runs before worktree removal).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_script: Option<String>,

    /// Shared script definitions for this repo (bootstrap workspace scripts from config).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scripts: Option<Vec<RepoScript>>,

    /// Repo-root-relative directory prefixes excluded from CoW checkout
    /// provisioning (`workspace.create`/`workspace.duplicate`): matching
    /// directories are not cloned into the checkout (e.g. huge caches that
    /// slow the clone down). `.git` and the repo root itself cannot be
    /// excluded (such entries are ignored with a warning). Absent ⇒ clone
    /// everything (today's behavior).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cow_clone_exclude: Option<Vec<String>>,

    /// Unknown/extra keys preserved on round-trip to avoid dropping fields other tools add.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// One chat-context attachment for a workspace (PROTOCOL §5.1 —
/// `workspace.getContext` / `updateContext`). The daemon treats the item as an
/// opaque JSON blob authored by the FE (`ContextItem` union in
/// `packages/cloudlands-fe/src/features/context/types.ts` — notes, linear /
/// github / sentry issues, browser URLs) and only pulls `id` out for keying
/// and ordering. All other fields round-trip verbatim via
/// `#[serde(flatten)]`, so provider-specific extras (`identifier`, `number`,
/// `favicon`, …) reach the FE without the daemon needing a matching Rust
/// union.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextItem {
    pub id: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Persisted task↔agent linkage (PROTOCOL §5.4 — `task.linkAgent` /
/// `unlinkAgent` / `listAgentLinks`, §6.5 `task:agent-linked` /
/// `task:agent-unlinked`). Migrates the renderer-only
/// `localStorage["task-agent-associations:{workspaceId}"]` store into
/// daemon-owned rows. `taskKey` is the FE's association key
/// (`association.taskKey ?? association.taskText`); `taskText` records the
/// human-readable checkbox text at link time; `createdAt` is epoch-ms (FE
/// parity with `TaskAgentAssociation.createdAt: number`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentLink {
    pub workspace_id: WorkspaceId,
    pub note_id: NoteId,
    pub task_key: String,
    pub task_text: String,
    pub agent_id: String,
    pub created_at: i64,
}

/// `Workspace.taskStats` card aggregate (§9.1; TS `WorkspaceTaskStats`). Ports
/// the canonical `computeTaskStats` (`task-stats.ts`): `total` excludes
/// `cancelled`, `completed` counts `complete`, and `inProgress` counts
/// `in_progress` + `review_required`. The optional renderer-only per-task
/// `tasks` array is omitted (the server source-of-truth emits only the counts).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceTaskStats {
    pub total: usize,
    pub completed: usize,
    pub in_progress: usize,
}

/// Extract the spec-linked task-note ids from a spec note's markdown body
/// (`[text](intent://local/task/{id})`), mirroring the TS `extractSpecTaskIds`
/// (`TASK_LINK_REGEX_FLEXIBLE`). Shared by the enriched `taskStats` path
/// (intent-services `compute_task_stats`) and the cheap store-level counting
/// query (`Store::count_task_stats`) so the two stay in lock-step.
pub fn extract_spec_task_ids(content: &str) -> std::collections::HashSet<String> {
    const MARKER: &str = "(intent://local/task/";
    let mut ids = std::collections::HashSet::new();
    let mut rest = content;
    while let Some(pos) = rest.find(MARKER) {
        let after = &rest[pos + MARKER.len()..];
        match after.find(')') {
            Some(end) => {
                let id = &after[..end];
                if !id.is_empty() {
                    ids.insert(id.to_string());
                }
                rest = &after[end + 1..];
            }
            None => break,
        }
    }
    ids
}

/// One entry of [`WorkspaceAgentSummary::agents`] (§5.5 card; TS
/// `WorkspaceAgentInfo`). The live iOS `WorkspaceStore.parseWorkspace` decodes
/// `id`/`name`/`status`/`isStreaming`/`isResponding` as non-optional and
/// `specialist`/`lastActivity` as optional. `status` carries the same wire
/// strings as `agent.list`; `isStreaming`/`isResponding` are always `false`
/// (the headless backend has no live stream state — `status` carries liveness,
/// matching the `AgentLite` iOS wire-shape parity decision). `parentAgentId`
/// (v2.9, additive) is the delegating/spawning agent — the same session value
/// surfaced as `metadata.createdByAgentId` on full `agent.get` loads — omitted
/// for root agents so cards can draw the delegation tree from the summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAgentInfo {
    pub id: AgentId,
    pub name: String,
    pub status: AgentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub is_streaming: bool,
    pub is_responding: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
}

/// `Workspace.agentSummary` card aggregate. The iOS coverflow reads the richer
/// `{ count, agents }` object (the live `WorkspaceStore.parseWorkspace`
/// consumer); `agentIds` is additionally emitted alongside it for forward-compat
/// with the TS `WorkspaceAgentIdSummary { agentIds }` (a future
/// desktop-on-intentd reads `agentSummary?.agentIds ?? []`). `agentIds` lists
/// the same agents used to build `agents` in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAgentSummary {
    pub count: usize,
    pub agents: Vec<WorkspaceAgentInfo>,
    pub agent_ids: Vec<AgentId>,
}

/// One entry of [`WorkspaceDiffSummary::files`] (TS `WorkspaceDiffSummaryFile`).
/// The on-demand workspace card summary emits an empty `files` array (per-file
/// detail is fetched via the dedicated diff endpoints), but the type matches the
/// TS wire shape for completeness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiffSummaryFile {
    pub path: String,
    /// `create` | `modify` | `delete` | `rename` (TS `DiffSummaryFileAction`).
    pub action: String,
    pub additions: usize,
    pub deletions: usize,
}

/// `Workspace.diffSummary` card aggregate (§9.1; TS `WorkspaceDiffSummary`).
/// Ports the on-demand `computeWorkspaceDiffSummary` (`workspace-summaries.ts`):
/// `totalFiles` counts changed-vs-`HEAD` (staged+unstaged) plus untracked files;
/// `totalAdditions`/`totalDeletions` sum line stats over the tracked changes.
/// iOS reads `totalFiles`; `files` mirrors the on-demand source (empty array).
/// Omitted from a workspace when there are no changes (or no git worktree).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiffSummary {
    pub schema_version: u32,
    pub updated_at: String,
    pub total_files: usize,
    pub total_additions: usize,
    pub total_deletions: usize,
    pub files: Vec<WorkspaceDiffSummaryFile>,
}

/// Wire input for `workspace.create` (PROTOCOL §5.1). All fields are optional;
/// the service fills ids/timestamps/defaults. `initialAgent` carries the full
/// agent payload for daemon-owned initial-agent orchestration; its `prompt`
/// also seeds the auto-generated branch slug (TS `generateLocalSlug` parity).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkspaceCreate {
    pub title: Option<String>,
    pub status_message: Option<String>,
    pub branch: Option<String>,
    pub base_ref: Option<String>,
    pub base_commit_sha: Option<String>,
    pub tags: Option<Vec<String>>,
    pub path: Option<String>,
    pub repository_path: Option<String>,
    pub repository_owner: Option<String>,
    pub repository_name: Option<String>,
    pub worktree_path: Option<String>,
    pub scope: Option<String>,
    /// Opt out of the isolated checkout (worktree or CoW clone) and work
    /// directly in the repository folder. Canonical wire name is
    /// `skipIsolation`; `skipWorktree` is the deprecated pre-CoW alias
    /// (either set ⇒ direct mode). The persisted column keeps its historical
    /// `skip_worktree` name (`Workspace.skipWorktree`) — no DB migration.
    #[serde(alias = "skipWorktree")]
    pub skip_isolation: Option<bool>,
    pub setup_script: Option<String>,
    pub is_remote: Option<bool>,
    pub default_model: Option<String>,
    /// Git remote used to resolve `baseRef` when provisioning the worktree
    /// (default `origin`; e.g. `upstream` for forks). Not persisted.
    pub remote: Option<String>,
    /// GitHub-style clone URL (https or ssh). When set and `repositoryPath` is
    /// not already an existing local git repo, the daemon clones the URL into
    /// `clonePath` (or a default derived from the workspaces root) *before*
    /// worktree provisioning, and the resulting checkout becomes the
    /// workspace's `repositoryPath` (PROTOCOL §5.1).
    pub github_url: Option<String>,
    /// Optional clone target directory used when `githubUrl` is set. Defaults
    /// to `<workspaces_root>/clones/<repo-name>` when omitted.
    pub clone_path: Option<String>,
    /// New-project flow (PROTOCOL §5.1, intent-hq/monorepo#962): when `true`,
    /// `repositoryPath` is set, no `githubUrl` clone is in play, and the path
    /// is not already a local git repository, the daemon initializes the
    /// directory as a git repository (`git init -b main` + seeded initial
    /// commit) before branch naming and worktree provisioning. Absent/false
    /// keeps the legacy behavior (non-git paths skip provisioning); `true` on
    /// an existing git repo is a no-op.
    pub is_new_repo: Option<bool>,
    /// Client-supplied correlation id (PROTOCOL §5.1): when present, every
    /// `git:clone:progress` / `git:clone:done` frame this create emits echoes
    /// it as `data.progressId`, and provisioning paths that stream nothing
    /// today (worktree / CoW / direct) emit milestone frames. Absent keeps
    /// the legacy event behavior exactly. Never persisted.
    pub progress_id: Option<String>,
    /// Initial agent payload (full shape; `prompt` also seeds the branch slug).
    pub initial_agent: Option<WorkspaceCreateInitialAgent>,
}

/// The `initialAgent` sub-object of `workspace.create` (PROTOCOL §5.1). Full
/// agent payload mirroring `agent.create` (§5.5) so the daemon can own
/// initial-agent creation and prompt delivery inside the create op; agent ids
/// are server-assigned (a client-supplied `agentId` is rejected `-32602` at
/// the transport boundary), and `prompt` doubles as the branch-slug seed (TS
/// `generateLocalSlug` parity).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkspaceCreateInitialAgent {
    pub prompt: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub specialist: Option<String>,
    pub provider: Option<String>,
    pub behavior_prompt: Option<String>,
    pub agent_type: Option<String>,
    pub context_references: Option<serde_json::Value>,
    pub image_blocks: Option<serde_json::Value>,
    pub file_blocks: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}

/// Result of `workspace.create` (PROTOCOL §5.1): the created workspace plus,
/// when the request carried an `initialAgent`, the created agent's `AgentLite`
/// wire projection (the same shape as the `agent.create` result's `agent`).
/// The agent row is persisted whenever `initialAgent` is present; the first
/// turn only starts when a non-empty prompt was supplied. Serialized verbatim
/// as the RPC result object.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCreateResult {
    pub workspace: Workspace,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_agent: Option<serde_json::Value>,
}

/// Wire input for `workspace.update` (PROTOCOL §5.1). Every field is optional;
/// an absent field leaves the stored value unchanged (`workspaceId` is supplied
/// separately by the router).
///
/// Serializes with `skip_serializing_if = "Option::is_none"` so the
/// `workspace:updated` change event only carries the fields the caller
/// actually asked to mutate.
///
/// Clearable optional fields (`pr_url`, `pr_number`, `pr_status`,
/// `active_pull_request`, `pull_requests`) use `Option<Option<T>>` so a wire
/// `null` deserializes to `Some(None)` (explicit clear) and can be distinguished
/// from a missing field (`None`, no change). Callers pass `null` to drop the
/// stored value; the response `Workspace` still omits cleared optionals (§9.1
/// `skip_serializing_if`) rather than echoing `null`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkspaceUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// Clearable status-screenshot asset id (intent-hq/monorepo#997): a wire
    /// `null` (`Some(None)`) clears the stored value, `Some(Some(id))` sets
    /// it, missing (`None`) leaves it untouched.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub status_image_asset_id: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkspaceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Toggle direct mode (no isolated checkout). Canonical wire name is
    /// `skipIsolation`; `skipWorktree` is the deprecated pre-CoW alias —
    /// matches [`WorkspaceCreate::skip_isolation`]. Serializes as
    /// `skipIsolation` in the `workspace:updated { changes }` delta. The
    /// persisted column keeps its historical `skip_worktree` name
    /// (`Workspace.skipWorktree`) — no DB migration.
    #[serde(skip_serializing_if = "Option::is_none", alias = "skipWorktree")]
    pub skip_isolation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup_script: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_remote: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub pr_number: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub pr_url: Option<Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub pr_status: Option<Option<PullRequestStatus>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub active_pull_request: Option<Option<PullRequestInfo>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_field"
    )]
    pub pull_requests: Option<Option<Vec<PullRequestInfo>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attention: Option<WorkspaceAttention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
}

/// Deserialize a JSON `null` as `Some(None)` (explicit clear) and a missing
/// field as `None` (no change), so `Option<Option<T>>` on [`WorkspaceUpdate`]
/// can distinguish the two. A present non-null value maps to `Some(Some(v))`.
fn deserialize_optional_field<'de, T, D>(
    deserializer: D,
) -> std::result::Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

/// Additional per-note metadata that ships nested under `metadata` on the wire
/// (§9.1). Mirrors the TS `NoteMetadata` shape; today only carries the optional
/// [`TaskMetadata`] for task notes. Kept as its own struct so future fields
/// (author, session, etc.) land under `metadata.*` rather than at the top level.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskMetadata>,
}

impl NoteMetadata {
    /// True when no metadata field is populated; used by [`Note`]'s
    /// `skip_serializing_if` so plain notes omit the `metadata` key entirely.
    pub fn is_empty(&self) -> bool {
        self.task.is_none()
    }
}

/// Note entity (§9.1). `metadata.task` carries serialized task metadata when
/// the note is a task; this slice treats it opaquely (stored as `task_json`
/// TEXT).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Note {
    pub id: NoteId,
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub content: String,
    pub content_type: ContentType,
    pub tags: Vec<String>,
    pub is_pinned: bool,
    pub is_archived: bool,
    pub is_default: bool,
    pub parent_id: Option<NoteId>,
    pub visibility: NoteVisibility,
    #[serde(default, skip_serializing_if = "NoteMetadata::is_empty")]
    pub metadata: NoteMetadata,
    pub created_at: String,
    /// Monotonic version counter (§8.3). Bumped on every write; used as the
    /// `expectedVersion` optimistic-concurrency token. Existing rows default `0`.
    pub rev: i64,
    pub updated_at: String,
}

/// Task workflow status (§9.1). Serializes to the `snake_case` strings the TS
/// app uses (`not_started`, `in_progress`, …); these are also the stored forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    NotStarted,
    Waiting,
    DiscussionNeeded,
    /// Agent reported a blocker it cannot resolve (`ws.agent.reportBlocker`).
    /// Non-terminal; excluded from `inProgress` in task-stats rollups, like
    /// `discussion_needed`.
    Blocked,
    InProgress,
    ReviewRequired,
    Complete,
    Cancelled,
}

/// Task-note metadata (§9.1). Present iff a [`Note`] is a task; serialized into
/// the note's `task_json` column. Field names/optionality match the TS
/// `TaskMetadata` so the wire object is identical.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetadata {
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assigned_agent_ids: Vec<AgentId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance_criteria: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_order: Option<i64>,
    /// Task-note ids this task depends on (hard ordering edges). Written via
    /// `task.setRelations` / `task.markAsTask`; cycle-checked at write time.
    /// Omitted on the wire when empty so pre-existing notes are unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<NoteId>,
    /// Task-note ids this task conflicts with (advisory, symmetric by
    /// convention — no cycle check). Omitted on the wire when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts_with: Vec<NoteId>,
    /// **Computed, never persisted** (monorepo#1979): the `depends_on` subset
    /// whose task note is not `complete` (missing and cancelled deps count as
    /// unmet — same rule as the `task.list` projection). Projected onto
    /// note-shaped reads/pushes at the service layer; stripped from
    /// `task_json` at store encode time. Omitted on the wire when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmet_depends_on: Vec<NoteId>,
}

/// Comment discriminant (§9.1). Serializes to the TS wire form (e.g.
/// `change-request`) and is stored verbatim in the `comment.kind` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommentType {
    #[default]
    Comment,
    Suggestion,
    ChangeRequest,
    Question,
    Session,
}

/// Comment lifecycle status (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommentStatus {
    #[default]
    Open,
    Resolved,
    Accepted,
    Rejected,
    Pending,
}

/// Comment author kind (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorType {
    #[default]
    User,
    Agent,
}

/// Anchor positioning kind for [`CommentAnchor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommentAnchorType {
    #[default]
    Range,
    Point,
}

/// Where a comment attaches in a note (§9.1). Matches the TS `CommentAnchor`
/// shape (`type` + optional `startId`/`endId`/`pointId`); stored as
/// `comment.anchor_json`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentAnchor {
    #[serde(rename = "type")]
    pub kind: CommentAnchorType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
}

/// Comment entity (§9.1; the TS `CommentV2` union flattened). The Rust field
/// `kind` serializes as `type` to match the TS wire (Rust reserves `type`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note_id: Option<NoteId>,
    #[serde(rename = "type")]
    pub kind: CommentType,
    pub content: String,
    pub author: String,
    pub author_type: AuthorType,
    pub status: CommentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Replies carry `None` — they anchor via their thread/parent
    /// (`thread_id`/`parent_id`), never independently (monorepo#729). Roots
    /// created by `comment.add` always carry an anchor, but legacy data can
    /// deviate either way: legacy-imported roots without a `markId` have no
    /// anchor, and replies stored before this contract change may still carry
    /// a (non-authoritative) clone of the parent's anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<CommentAnchor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_original: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_proposed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// True when a subsequent note edit destroyed the anchor markers and
    /// recovery from the stored surrounding context (`anchor_before` /
    /// `anchor_after`) failed (reference
    /// `commentsRepository.update({ isOrphaned: true })` in
    /// `notes.service.ts` `applyEditEvent`). Serialized as camelCase
    /// `isOrphaned`; omitted when `None` (untouched rows). `Some(false)` is
    /// serialized explicitly so a healed comment can be signalled to clients.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_orphaned: Option<bool>,
    pub created_at: String,
    pub updated_at: String,
}

/// A comment thread: the comments sharing one `thread_id`, ordered by creation
/// time. Mirrors the TS `comment.getThread` result (`{ threadId, comments }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentThread {
    pub thread_id: String,
    pub comments: Vec<Comment>,
}

/// Wire input for `note.create` (PROTOCOL §5.2). `title` is required; the
/// service fills ids/timestamps/defaults. Built by the router from request
/// params.
#[derive(Debug, Clone, Default)]
pub struct NoteCreate {
    pub title: String,
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub parent_id: Option<String>,
}

/// Wire input for the CRUD `note.update` path (PROTOCOL §5.2). `content`
/// present → raw full-content set; otherwise `title`/`tags` metadata update.
#[derive(Debug, Clone, Default)]
pub struct NoteUpdateInput {
    pub content: Option<String>,
    pub title: Option<String>,
    pub tags: Option<Vec<String>>,
    /// Optimistic-concurrency gate: when `Some`, the write only succeeds if the
    /// note's current `rev` matches; absent → last-writer-wins (PROTOCOL §5.6).
    pub expected_version: Option<i64>,
}

/// Wire input for `note.add` (PROTOCOL §5.2).
#[derive(Debug, Clone, Default)]
pub struct NoteAddInput {
    pub content: String,
    pub heading: Option<String>,
    pub position: Option<String>,
}

/// Wire input for `note.edit` (PROTOCOL §5.2).
#[derive(Debug, Clone, Default)]
pub struct NoteEditInput {
    pub old: String,
    pub new: String,
}

/// Wire input for `note.editLines` (PROTOCOL §5.2); 1-based inclusive lines.
#[derive(Debug, Clone, Default)]
pub struct NoteEditLinesInput {
    pub start: i64,
    pub end: i64,
    pub content: String,
}

/// Result of `note.create` — the created (post-auto-convert) note plus the
/// `@@@task` conversion outcome, matching the four content-write results.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteCreateResult {
    pub note: Note,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
    /// One entry per task note created by the auto-conversion, in block
    /// order (parallel to `created_task_note_ids`).
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal auto-conversion warnings (see
    /// [`TaskConvertBlocksResult::warnings`]).
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl<'de> Deserialize<'de> for NoteCreateResult {
    /// Legacy-tolerant decode: `note.create` idempotency records persisted
    /// before the conversion fields existed contain a serialized bare `Note`,
    /// so a replay of a pre-upgrade key must still decode. A bare note maps
    /// to a zeroed conversion outcome; the current `{ note, ... }` shape
    /// decodes as derived (the count/id fields tolerate absence like the
    /// `#[serde(default)]` vecs, for symmetry with older-daemon responses).
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Current {
            note: Note,
            #[serde(default)]
            converted_count: i64,
            #[serde(default)]
            created_task_note_ids: Vec<String>,
            #[serde(default)]
            created_tasks: Vec<CreatedTaskEntry>,
            #[serde(default)]
            warnings: Vec<String>,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Compat {
            Current(Current),
            LegacyBareNote(Note),
        }
        match Compat::deserialize(deserializer)? {
            Compat::Current(c) => Ok(NoteCreateResult {
                note: c.note,
                converted_count: c.converted_count,
                created_task_note_ids: c.created_task_note_ids,
                created_tasks: c.created_tasks,
                warnings: c.warnings,
            }),
            Compat::LegacyBareNote(note) => Ok(NoteCreateResult {
                note,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
                created_tasks: Vec::new(),
                warnings: Vec::new(),
            }),
        }
    }
}

/// Result of `note.add` — mirrors the TS `ws.note.add` peer return shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteAddResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub added_length: usize,
    pub total_length: usize,
    pub position: String,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
    /// One entry per task note created by the auto-conversion, in block
    /// order (parallel to `created_task_note_ids`).
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal auto-conversion warnings (see
    /// [`TaskConvertBlocksResult::warnings`]).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of `note.edit` — first exact-match replacement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteEditResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub old_text_length: usize,
    pub new_text_length: usize,
    /// Scalar (char) offset of the match, or `-1` when the note was empty.
    pub match_position: i64,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
    /// One entry per task note created by the auto-conversion, in block
    /// order (parallel to `created_task_note_ids`).
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal auto-conversion warnings (see
    /// [`TaskConvertBlocksResult::warnings`]).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of `note.editLines` — 1-based inclusive replace/delete/insert.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteEditLinesResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub start_line: i64,
    pub end_line: i64,
    pub total_lines_before: usize,
    pub total_lines_after: usize,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
    /// One entry per task note created by the auto-conversion, in block
    /// order (parallel to `created_task_note_ids`).
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal auto-conversion warnings (see
    /// [`TaskConvertBlocksResult::warnings`]).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of `note.setContent` — full replace with the reduction guard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteSetContentResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_title: Option<String>,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_content: Option<String>,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
    /// One entry per task note created by the auto-conversion, in block
    /// order (parallel to `created_task_note_ids`).
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal auto-conversion warnings (see
    /// [`TaskConvertBlocksResult::warnings`]).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of `note.updateMetadata`. Either a normal title/tags update or a
/// `skipped` response (spec title cannot be modified).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteUpdateMetadataResult {
    pub ok: bool,
    pub note_id: NoteId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Result of `note.delete`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub deleted: bool,
}

/// One parsed checkbox row returned by `note.listTasks`. `taskNoteId` is
/// serialized as `null` when the row has no `intent://local/task/<id>` link.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteTaskRow {
    pub line_number: usize,
    pub text: String,
    pub status: String,
    pub task_note_id: Option<String>,
    pub linked_task_note_id: Option<String>,
    /// Relations mirrored from the linked task note's metadata (empty and
    /// omitted for rows without a linked task note).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<NoteId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts_with: Vec<NoteId>,
    /// Computed: `dependsOn` ids whose task is not yet `complete`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmet_depends_on: Vec<NoteId>,
}

/// Result of `note.readAsset` (PROTOCOL §5.2). `data` is base64; `sizeKb` is
/// rounded from the base64 string length to match the TS peer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadAssetResult {
    pub asset_id: String,
    pub mime_type: String,
    pub data: String,
    pub size_kb: i64,
}

/// Result of `note.saveAsset` (PROTOCOL §5.2 — additive asset write). `path`
/// is the absolute on-disk location under the workspace assets root; `url` is
/// the `workspace-asset://<workspaceId>/<assetId>` form the FE embeds in note
/// markdown and feeds back to `note.readAsset`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAssetResult {
    pub asset_id: String,
    pub path: String,
    pub url: String,
}

/// Author stamp on a stored note version (PROTOCOL §5.2 version-history
/// extensions). Mirrors the FE `VersionAuthor` shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoteVersionAuthor {
    pub id: String,
    pub name: String,
    /// `user` | `agent` | `system`.
    #[serde(rename = "type")]
    pub author_type: String,
}

/// One version-list entry returned by `note.listVersions` — the FE
/// `VersionEntry` shape without the content blob (`contentLength` instead).
/// `entry_type` is always `"snapshot"` (full-snapshot model).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteVersionSummary {
    #[serde(rename = "type")]
    pub entry_type: String,
    pub v: i64,
    pub date: String,
    pub author: NoteVersionAuthor,
    pub title: String,
    pub content_length: i64,
}

/// One full stored note version returned by `note.getVersion`. `entry_type`
/// is always `"snapshot"` (full-snapshot model).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteVersion {
    #[serde(rename = "type")]
    pub entry_type: String,
    pub v: i64,
    pub date: String,
    pub author: NoteVersionAuthor,
    pub title: String,
    pub content: String,
}

/// Result of `note.restoreVersion` — the note's content is reset to version
/// `restoredFrom` and a new version `v` capturing the restored state is
/// appended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteRestoreVersionResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub restored_from: i64,
    pub v: i64,
    pub note: Note,
}

// ---------------------------------------------------------------------------
// Line-attribution wire types (new in intentd; PROTOCOL §5.2.1). Ported from
// the FE `LineAttributionData` / `LineAttributionInfo` / `LineAuthor` shapes
// so the `line-attribution:load` + `line-attribution:updated` payloads stay
// drop-in-compatible with what `LineAttributionGutter.svelte` consumes.
// ---------------------------------------------------------------------------

/// Author info stamped on an attributed line (FE `LineAuthor`).
/// `author_type` is one of `user` / `agent` / `system`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineAttributionAuthor {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub author_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_number: Option<i64>,
}

/// Attribution info for a single line (FE `LineAttributionInfo`).
/// `timestamp` is milliseconds since Unix epoch (JS `Date.now()`-compatible)
/// so the FE gutter’s age math works unchanged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineAttributionInfo {
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<LineAttributionAuthor>,
}

/// Serialized attribution payload for a note (FE `LineAttributionData`).
/// Keys of `attributions` are stringified 1-based line numbers so the JSON
/// shape matches the FE `Record<number, AttributionInfo>` decoder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineAttributionData {
    pub note_id: NoteId,
    pub workspace_id: WorkspaceId,
    pub computed_at: String,
    pub attributions: std::collections::BTreeMap<String, LineAttributionInfo>,
}

/// Result of `note.lineAttribution.computeNow`. Mirrors the FE IPC handler’s
/// inner `{ success: true }` — `success` is IPC-transport-level and dropped
/// on the wire; the RPC result carries only `ok`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LineAttributionComputeResult {
    pub ok: bool,
}

// ---------------------------------------------------------------------------
// task.* result DTOs (PROTOCOL §5.4). Field names/optionality match the TS
// `ws.task.*` peer returns so the iOS client is unchanged.
// ---------------------------------------------------------------------------

/// Result of `task.updateStatus` (checkbox edit by task text).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateStatusResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub task_text: String,
    /// Checkbox status string: `todo` / `in-progress` / `done`.
    pub status: String,
}

/// Result of `task.updateNoteStatus` (task-note metadata status).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateNoteStatusResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub status: TaskStatus,
    pub note: Note,
}

/// Result of `task.update` (atomic single-line edit).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub line_number: i64,
    pub previous_text: String,
    pub new_text: String,
    /// Checkbox status string: `todo` / `in-progress` / `done`.
    pub status: String,
}

/// One subtask row in [`TaskGetMyTaskResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSubtask {
    pub id: NoteId,
    pub title: String,
    /// Child task status string, or `unknown` if the child lost its metadata.
    pub status: String,
}

/// Result of `task.getMyTask`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskGetMyTaskResult {
    pub note_id: NoteId,
    pub title: String,
    pub content: String,
    pub status: TaskStatus,
    pub task_metadata: TaskMetadata,
    pub parent_id: Option<NoteId>,
    pub subtasks: Vec<TaskSubtask>,
    pub assigned_agents: Vec<AgentId>,
    /// Monotonic version counter echoed from the backing note (§8.3/§8.4).
    pub rev: i64,
    /// Computed: `dependsOn` ids whose task is not yet `complete` (missing
    /// and cancelled deps count as unmet). Omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmet_depends_on: Vec<NoteId>,
}

/// Canonical task facts for a workspace (TS `WorkspaceTask`, `shared/types.ts`).
/// Returned by `task.list`/`task.get` so the FE can drop its `note.list`
/// metadata derivation of the workspace task set. The renderer selectors derive
/// counts/progress/groupings from this `{ id, title, status, updatedAt }` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceTask {
    pub id: NoteId,
    pub title: String,
    pub status: TaskStatus,
    pub updated_at: String,
    /// True iff this task's id appears in the spec note body as an
    /// `intent://local/task/{id}` link. Additive field, always serialized
    /// (`false` for every row when the spec has no links); not conditioned
    /// on `parent_id`.
    #[serde(default)]
    pub spec_linked: bool,
    /// Backing note's parent pointer, so clients can distinguish subtasks
    /// (parent is another task) from top-level tasks (parent is the spec or
    /// absent) and reconstruct the hierarchy from `task.list` alone.
    /// Additive; omitted when the note has no parent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<NoteId>,
    /// Task relations (empty and omitted for tasks without them).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<NoteId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts_with: Vec<NoteId>,
    /// Computed: `dependsOn` ids whose task is not yet `complete`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unmet_depends_on: Vec<NoteId>,
}

/// `task.list` result envelope: the projected `WorkspaceTask` list (honouring
/// the optional `status` filter) **and** the workspace-wide `taskStats`
/// aggregate. `tasks` membership is workspace-wide — every task note except
/// the spec itself, each flagged with `specLinked` — while `stats` stays the
/// spec-linked direct-child rollup (mirrors the canonical FE
/// `computeTaskStats` in `task-stats.ts`). Lets the FE render the progress
/// rollup verbatim instead of re-deriving it from `note.list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskListResult {
    pub tasks: Vec<WorkspaceTask>,
    pub stats: WorkspaceTaskStats,
}

/// Result of `task.markAsTask`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMarkAsTaskResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub status: TaskStatus,
}

/// Result of `task.setRelations` (PROTOCOL §5.4). Echoes the stored relations
/// after the write so callers see the normalized (deduped) lists — always
/// present (a cleared list echoes `[]`), unlike the omitted-when-empty
/// projections on the read paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSetRelationsResult {
    pub ok: bool,
    pub note_id: NoteId,
    #[serde(default)]
    pub depends_on: Vec<NoteId>,
    #[serde(default)]
    pub conflicts_with: Vec<NoteId>,
}

/// One task note created by `@@@task` block conversion: the block's `key=`
/// header attribute (when authored), its title, and the created note's id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedTaskEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub title: String,
    pub note_id: String,
}

/// Result of `task.convertBlocks`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskConvertBlocksResult {
    pub ok: bool,
    pub converted_count: i64,
    pub created_note_ids: Vec<String>,
    /// One entry per task note created by this conversion, in block order
    /// (parallel to `created_note_ids`); reused existing children are not
    /// listed.
    #[serde(default)]
    pub created_tasks: Vec<CreatedTaskEntry>,
    /// Non-fatal conversion problems: header parse issues, unresolvable or
    /// ambiguous `dependsOn`/`conflictsWith` references, and validator-
    /// rejected edges. Conversion never fails on these — the blocks still
    /// convert and each skipped edge/attribute adds one entry here.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of `task.createPrerequisite`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCreatePrerequisiteResult {
    pub ok: bool,
    pub prerequisite_note_id: NoteId,
    pub dependent_note_id: NoteId,
    pub title: String,
}

/// Result of `task.assignAgent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskAssignAgentResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub agent_id: AgentId,
}

/// Result of `task.removeAgentFromAllTasks` (§5.4 extension). Bulk-sweep helper
/// called during agent teardown: strips the given agent from every task-note's
/// `assignedAgentIds` in the workspace and reports how many task-notes were
/// mutated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRemoveAgentFromAllTasksResult {
    pub ok: bool,
    pub updated_count: u32,
}

// ---------------------------------------------------------------------------
// comment.* wire DTOs (PROTOCOL §5.3). The stored [`Comment`] keeps anchor and
// suggestion fields flat; on the wire they nest into `anchorContext` /
// `suggestionDiff` to match the TS `CommentV2` shape the iOS client expects.
// ---------------------------------------------------------------------------

/// Nested `anchorContext` on the wire (`{ before, after }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorContext {
    pub before: String,
    pub after: String,
}

/// Nested `suggestionDiff` on the wire (`{ original, proposed }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuggestionDiff {
    pub original: String,
    pub proposed: String,
}

/// Wire-facing comment (the TS `CommentV2`). Built from the flat [`Comment`]
/// via [`CommentWire::from_comment`]; nests `anchorContext`/`suggestionDiff`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentWire {
    pub id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note_id: Option<NoteId>,
    #[serde(rename = "type")]
    pub kind: CommentType,
    pub content: String,
    pub author: String,
    pub author_type: AuthorType,
    pub status: CommentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Omitted for replies: they anchor via `threadId`/`parentId`
    /// (monorepo#729). Present on roots created by `comment.add`; may be
    /// absent on legacy-imported roots that had no `markId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<CommentAnchor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_context: Option<AnchorContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_diff: Option<SuggestionDiff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_orphaned: Option<bool>,
    pub created_at: String,
    pub updated_at: String,
}

impl CommentWire {
    /// Map a stored [`Comment`] to its nested wire shape.
    pub fn from_comment(c: &Comment) -> Self {
        let anchor_context = match (&c.anchor_before, &c.anchor_after) {
            (None, None) => None,
            (before, after) => Some(AnchorContext {
                before: before.clone().unwrap_or_default(),
                after: after.clone().unwrap_or_default(),
            }),
        };
        let suggestion_diff = match (&c.suggestion_original, &c.suggestion_proposed) {
            (Some(original), Some(proposed)) => Some(SuggestionDiff {
                original: original.clone(),
                proposed: proposed.clone(),
            }),
            _ => None,
        };
        Self {
            id: c.id.clone(),
            thread_id: c.thread_id.clone(),
            note_id: c.note_id.clone(),
            kind: c.kind,
            content: c.content.clone(),
            author: c.author.clone(),
            author_type: c.author_type,
            status: c.status,
            parent_id: c.parent_id.clone(),
            anchor: c.anchor.clone(),
            anchor_text: c.anchor_text.clone(),
            anchor_context,
            suggestion_diff,
            agent_id: c.agent_id.clone(),
            is_orphaned: c.is_orphaned,
            created_at: c.created_at.clone(),
            updated_at: c.updated_at.clone(),
        }
    }
}

/// Anchor location echoed by `comment.add`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentLocation {
    pub line: usize,
    pub anchored_text: String,
}

/// Result of `comment.add`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentAddResult {
    pub success: bool,
    pub message: String,
    pub comment_id: String,
    pub anchored: bool,
    /// The note's post-rewrite revision: `comment.add` embeds anchor markers
    /// into the note markdown (an `update_note` that bumps `rev`), so the
    /// result echoes the authoritative new `rev` instead of leaving clients
    /// to assume "exactly one bump per add" (monorepo#638).
    pub note_rev: i64,
    pub location: CommentLocation,
}

/// One thread summary in `comment.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentThreadSummary {
    pub thread_id: String,
    pub note_id: NoteId,
    pub targeted_text: Option<String>,
    pub anchor_id: Option<String>,
    pub status: String,
    pub created_at: String,
    pub last_activity: String,
    pub latest_comment_author: String,
    pub latest_comment_author_type: AuthorType,
    pub latest_comment_at: String,
    pub comment_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comments: Option<Vec<CommentWire>>,
}

/// Result of `comment.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentListResult {
    pub threads: Vec<CommentThreadSummary>,
    pub total_threads: usize,
    pub total_comments: usize,
}

/// Result of `comment.getThread`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentGetThreadResult {
    pub thread_id: String,
    pub note_id: NoteId,
    pub root_comment: CommentWire,
    pub replies: Vec<CommentWire>,
    pub total_comments: usize,
    pub status: String,
}

/// The `thread` summary echoed by `comment.respond`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRespondThread {
    pub thread_id: String,
    pub total_comments: usize,
}

/// Result of `comment.respond`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRespondResult {
    pub success: bool,
    pub message: String,
    pub comment: CommentWire,
    pub thread: CommentRespondThread,
}

/// Result of `comment.delete`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentDeleteResult {
    pub success: bool,
    pub message: String,
}

/// Result of `comment.resolveThread`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentResolveThreadResult {
    pub success: bool,
    pub thread_id: String,
    pub note_id: NoteId,
    pub resolved: bool,
    pub status: String,
    pub comment_count: usize,
}

/// Event actor kind (§9.1, `events/types.ts` `ActorType`). Serializes to its
/// lowercase string form, matching the TS wire values used by the iOS client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorType {
    #[default]
    User,
    Agent,
    System,
    External,
    Tool,
}

/// Who originated an event (§9.1; `events/types.ts` `EventActor`). The `type`
/// field is required; the rest are optional and omitted from the wire when
/// absent, matching the TS shape. Stored as the `event.actor` JSON column.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventActor {
    #[serde(rename = "type")]
    pub actor_type: ActorType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Append-only workspace event (§9.1 / §10; `events/types.ts`
/// `WorkspaceEventBase`). `event_type` serializes as `type` and the
/// type-specific payload lives in `data`, matching the TS/iOS wire shape.
/// Persisted to the insert-only `event` table; never updated or deleted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub timestamp: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: EventActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub data: serde_json::Value,
}

/// One file-change activity row (§5.10; `agent-event-tools.ts` `FileActivity`).
/// Returned by `event.agentActivity` (per-agent variant) and embedded in
/// `event.workspaceSummary`. `actor` is `"type:name"` in the summary and the bare
/// actor name for the per-agent variant; absent optionals are omitted from the
/// wire to match the TS `JSON.stringify` shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileActivity {
    pub path: String,
    pub relative_path: String,
    pub action: String,
    pub timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additions: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<serde_json::Value>,
}

/// Aggregated per-agent activity (§5.10; `agent-event-tools.ts` `AgentActivity`).
/// Returned by `event.agentActivity` (no `agentId`) and embedded in
/// `event.workspaceSummary.activeAgents`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentActivity {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub event_count: i64,
    pub tool_calls: i64,
    pub files_modified: Vec<String>,
    pub last_active: String,
}

/// One entry of `event.workspaceSummary.topChangedFiles` (§5.10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopChangedFile {
    pub path: String,
    pub change_count: i64,
}

/// `event.workspaceSummary` result (§5.10; `WorkspaceActivity` in
/// `agent-event-tools.ts`, renamed here to avoid colliding with the
/// lifecycle [`WorkspaceActivity`] enum).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceEventSummary {
    pub recent_files: Vec<FileActivity>,
    pub active_agents: Vec<AgentActivity>,
    pub event_rate: f64,
    pub top_changed_files: Vec<TopChangedFile>,
}

/// `event.subscribe` (deprecated alias) service result (§5.10 / §6). Mirrors the
/// `ws.event.subscribe` peer return `{ subscriptionId, eventTypes }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSubscribeResult {
    pub subscription_id: String,
    pub event_types: Vec<String>,
}

/// `event.unsubscribe` (deprecated alias) service result (§5.10 / §6). Mirrors
/// the `ws.event.unsubscribe` peer return `{ ok: true, subscriptionId }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventUnsubscribeResult {
    pub ok: bool,
    pub subscription_id: String,
}

/// Filter inputs for `event.query` (§5.10). Built by the transport router from
/// request params and consumed by the service layer; not serialized on the wire.
#[derive(Debug, Clone, Default)]
pub struct EventQueryParams {
    pub event_type: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub path: Option<String>,
    pub minutes_ago: Option<i64>,
    pub limit: Option<i64>,
    /// Opt-in pagination (TA-2 / §5.5): when set true (or when `page_token` is
    /// present), `event.query` returns the `{ items, nextToken }` envelope
    /// (newest→oldest, limit clamped to [1,200] default 50) instead of the
    /// legacy bare array. Absent/false preserves the legacy shape.
    pub paginate: Option<bool>,
    /// Opaque continuation token from a previous paginated `event.query`.
    pub page_token: Option<String>,
}

/// Agent runtime status (§9.1; `AgentStatus` in `agent.types.ts`). The modern
/// values are lowercase (`pending`/`active`/`idle`/`error`/`deleted`); the
/// capitalized variants are legacy values kept so persisted sessions round-trip
/// without rewriting. Mixed casing means each variant carries an explicit
/// `rename` rather than a blanket `rename_all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AgentStatus {
    #[default]
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "active")]
    Active,
    /// Lowercase `idle` persisted by app-level runtime events (including Chief).
    #[serde(rename = "idle")]
    RuntimeIdle,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "deleted")]
    Deleted,
    /// Legacy capitalized values kept for backward-compatible round-trips.
    #[serde(rename = "Idle")]
    Idle,
    #[serde(rename = "Waiting")]
    Waiting,
    #[serde(rename = "Completed")]
    Completed,
    #[serde(rename = "Processing")]
    Processing,
}

/// Per-session credit/message/tool stats (§9.1 / §19.2). A derived snapshot
/// populated from `auggie session stats --json`; it is **not** persisted in the
/// `agent_session` table (the `stats` field is recomputed on demand). Field
/// names match the TS `SessionStats`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    #[serde(default)]
    pub credits_used: Option<f64>,
    pub message_count: u64,
    pub tool_count: u64,
}

/// One row of the append-only agent conversation log (§9.2 `agent_message`).
/// `seq` is monotonic per agent (enforced by `UNIQUE(agent_id, seq)`); `content`
/// holds the message's JSON content blocks. Names use camelCase to match the
/// wire shape returned by `agent.getConversation` (PROTOCOL §5.5). The `content`
/// and `created_at` fields serialize as `contentBlocks`/`timestamp` to match the
/// TS `AgentMessage` (`src/shared/types/agent-message.ts`); the Rust identifiers
/// stay `content`/`created_at` so DB code and call sites are unchanged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMessage {
    pub id: String,
    pub agent_id: AgentId,
    pub seq: i64,
    /// `user` | `assistant` | `tool` | `system`.
    pub role: String,
    #[serde(rename = "contentBlocks")]
    pub content: serde_json::Value,
    /// Opaque per-message payload the FE attaches to `agent.sendMessage` as
    /// `messageMetadata` (PROTOCOL §5.5): e.g.
    /// `{ source: "system" }` for daemon-initiated turns. Persisted verbatim
    /// on the user message row and round-tripped on transcript reads; `None`
    /// for messages without caller-supplied metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// Client-minted logical message identity (`userAppMessageId`, PROTOCOL
    /// §5.5): the FE mints an `app-msg-*` id for its optimistic user message
    /// and the daemon round-trips it so clients can match the canonical row
    /// against the optimistic insert (dedup guard). Persisted inside the row
    /// `metadata` JSON under `userAppMessageId` (no schema migration) and
    /// lifted to this top-level wire field (`appMessageId`, matching the TS
    /// `AgentMessage`) on reads. `None` for messages without a client id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_message_id: Option<String>,
    #[serde(rename = "timestamp")]
    pub created_at: String,
}

/// Metadata key under which the client-supplied `userAppMessageId` is
/// persisted on the `agent_message.metadata` JSON (PROTOCOL §5.5). Shared by
/// the router (which folds the top-level param into `messageMetadata`) and
/// the store (which lifts it back out as [`AgentMessage::app_message_id`]).
pub const USER_APP_MESSAGE_ID_KEY: &str = "userAppMessageId";

/// Lift the client-supplied app-message id out of a persisted `metadata`
/// payload: `Some` only when the metadata is an object carrying a non-empty,
/// non-whitespace string under [`USER_APP_MESSAGE_ID_KEY`]. Trimmed to match
/// the router's fold-side normalization, so a padded or whitespace-only value
/// smuggled directly through `messageMetadata` never surfaces as a
/// meaningless `appMessageId` on reads/events.
pub fn lift_app_message_id(metadata: Option<&serde_json::Value>) -> Option<String> {
    metadata
        .and_then(|m| m.get(USER_APP_MESSAGE_ID_KEY))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Metadata key under which the question-dismissal marker is persisted on the
/// `agent_session.metadata` JSON (PROTOCOL §5.5, question hold): the id of the
/// assistant message whose trailing question resource blocks the user
/// dismissed via `agent.dismissQuestions`. No schema migration — the marker
/// rides the existing free-form `metadata` column and survives daemon
/// restarts. Read back by [`AgentSession::dismissed_questions_message_id`].
pub const DISMISSED_QUESTIONS_MESSAGE_ID_KEY: &str = "dismissedQuestionsMessageId";

/// Metadata key under which the pending-questions marker is persisted on the
/// `agent_session.metadata` JSON (PROTOCOL §5.5, question hold): the id of the
/// assistant message whose trailing question resource blocks are still
/// awaiting an answer. Written at turn end when the persisted assistant tail
/// bears question blocks (a newer question-bearing turn overwrites it —
/// single-slot), and cleared (written as the empty string, which reads back as
/// absent) when the answer for that exact message id persists or the
/// transcript is truncated by `agent.editAndRegenerate`. Stored-on-write so
/// the hold derivation stays a bounded metadata read and pendingness survives
/// later user messages, agent turns, and daemon restarts. No schema migration
/// — the marker rides the existing free-form `metadata` column. Read back by
/// [`AgentSession::pending_questions_message_id`] /
/// [`AgentSession::pending_questions_marker_written`].
pub const PENDING_QUESTIONS_MESSAGE_ID_KEY: &str = "pendingQuestionsMessageId";

/// Metadata key under which the per-conversation seen marker is persisted on
/// the `agent_session.metadata` JSON (PROTOCOL §5.5): the id of the newest
/// transcript message the user has seen, advanced monotonically by
/// `agent.markSeen`. No schema migration — the marker rides the existing
/// free-form `metadata` column and survives daemon restarts. Read back by
/// [`AgentSession::last_seen_message_id`].
pub const LAST_SEEN_MESSAGE_ID_KEY: &str = "lastSeenMessageId";

/// Metadata key under which the initial-agent flag is persisted on the
/// `agent_session.metadata` JSON (PROTOCOL §5.1/§5.5): stamped `true` by the
/// daemon's `workspace.create` initial-agent orchestration so clients can
/// classify the workspace's coordinator. No schema migration — the flag rides
/// the existing free-form `metadata` column and survives daemon restarts.
/// Read back by [`AgentSession::is_initial_agent`].
pub const IS_INITIAL_AGENT_KEY: &str = "isInitialAgent";

/// Who originated an `agent.sendMessage`-shaped delivery (PROTOCOL §5.5,
/// question hold). `User` marks the FE `agent.sendMessage` RPC — the ONLY
/// user-originated entry point — which always delivers immediately; it
/// bypasses the hold but does NOT release it (only an answer-tagged row or
/// `agent.dismissQuestions` does). Everything else (MCP front-door sends,
/// reportToParent / completion-watch / event-subscription wakes,
/// `agent.sendToTask`, `agent.wakeOrCreate`, internal continuations) is
/// `Automatic` and is held in the queue while the target agent's question
/// hold is active. `Automatic` is the `Default` so unmarked internal paths
/// fail closed (held) rather than burying a pending Q&A.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageOrigin {
    /// FE-originated `agent.sendMessage` (typed message or wizard answers):
    /// never held by the question hold.
    User,
    /// System/agent-originated delivery: held while the question hold is
    /// active.
    #[default]
    Automatic,
}

impl MessageOrigin {
    /// `true` for [`MessageOrigin::User`].
    pub fn is_user(self) -> bool {
        matches!(self, MessageOrigin::User)
    }
}

/// Maximum delegation depth to prevent unbounded recursive agent creation
/// (port of the TS `MAX_DELEGATION_DEPTH` in `agent-interaction-tools.ts`).
/// Depth 0 = user-created agents, depth 1 = their children, depth 2 =
/// grandchildren (max). A parent already at this depth cannot spawn further
/// delegates. Lives in `intent-core` so both the service impl and the MCP tool
/// dispatcher (which cannot depend on `intent-services`) share one policy value.
pub const MAX_DELEGATION_DEPTH: i64 = 2;

/// Maximum length (chars) of a workspace `statusMessage` (port of the TS
/// `WORKSPACE_STATUS_MESSAGE_MAX_LENGTH` in `src/shared/types.ts`). The MCP
/// `set_workspace_status_message` tool enforces this cap before calling
/// `update_workspace`, matching the reference `ws.workspace.setStatusMessage`.
pub const WORKSPACE_STATUS_MESSAGE_MAX_LENGTH: usize = 500;

/// Agent runtime session (§9.1). Field names/casing match the TS `AgentSession`
/// (`agent-session.ts`): `backendSessionId`, `acpSessionId` (write-once after
/// the provider's `session:created`), `nameExplicitlySet`, `systemPrompt`, etc.
/// `messages` is the append-only conversation log; `stats` is a derived snapshot
/// (not persisted, §19.2). `provider` is immutable once set on first real use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSession {
    pub id: AgentId,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_session_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub name_explicitly_set: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Reasoning-effort level requested for this session (PROTOCOL §5.5,
    /// Option B). Stored as-is (providers interpret the vocabulary; the
    /// daemon never normalizes it) and applied on the next prompt send.
    /// `None` = provider default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Effort levels the provider's `thought_level` config option advertised
    /// at the most recent session open (PROTOCOL §5.5, Option C), minus the
    /// adapter's `"default"` sentinel. Replaced wholesale at every session
    /// open; `None` when the provider advertised no such option (the FE falls
    /// back to catalog metadata). Never `Some(empty)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_levels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Specialist id this agent was created with (`agent.create`'s `specialistId`),
    /// surfaced as `metadata.specialist` in the `AgentLite` projection. `None` for
    /// plain (non-specialist) agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    pub status: AgentStatus,
    #[serde(default)]
    pub is_active: bool,
    #[serde(default)]
    pub messages: Vec<AgentMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SessionStats>,
    /// Task note this agent is linked to (set on `agent.delegate` when
    /// `taskNoteId`/`noteId` is provided). Drives the `Linked-Note-Id:` trailer
    /// resolution for the daemon-side auto-commit-on-idle subscriber (LNI-1).
    /// `None` for non-task agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_note_id: Option<NoteId>,
    /// Opt-out flag forwarded from `agent.delegate`'s `skipAutoCommit`. When
    /// `true`, the auto-commit-on-idle subscriber skips this session even if it
    /// is task-linked (LNI-1). Omitted from the wire when `false` to preserve
    /// the TS `AgentSession` shape for non-opted-out sessions.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub skip_auto_commit: bool,
    /// The child's final report persisted by `agent.reportToParent` (P3-1.2b;
    /// FE `metadata.completionReport`). Surfaced as `metadata.completionReport`
    /// in the [`AgentLite`] projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_report: Option<String>,
    /// ISO timestamp the completion report was saved at (FE
    /// `metadata.completionReportTimestamp`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_report_timestamp: Option<String>,
    /// Pending attention request raised by `ws.agent.requestDiscussion` /
    /// `ws.agent.reportBlocker`: `"discussion"` or `"blocker"`. Cleared when
    /// the agent next receives a message. Omitted when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_kind: Option<String>,
    /// The reason supplied with the pending attention request. Omitted when
    /// absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_reason: Option<String>,
    /// ISO timestamp the pending attention request was raised at. Omitted
    /// when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_timestamp: Option<String>,
    /// Delegation-chain depth (FE `metadata.delegationDepth`): 0/absent for
    /// user-created agents, parent depth + 1 for delegated children. Gates
    /// runaway delegation loops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<i64>,
    /// The first message a delegated agent was started with (FE
    /// `metadata.initialMessage`), persisted so a wake-up can resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_message: Option<String>,
    /// Session-level context references captured at spawn (FE top-level
    /// `contextReferences`); an opaque JSON array persisted verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_references: Option<serde_json::Value>,
    /// Session-level image blocks captured at spawn (FE top-level
    /// `imageBlocks`); an opaque JSON array persisted verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_blocks: Option<serde_json::Value>,
    /// Session-level file blocks captured at spawn (FE top-level
    /// `fileBlocks`); an opaque JSON array persisted verbatim. Entries carry
    /// EITHER inline `data` or an attachment-registry `attachmentId`
    /// reference (PROTOCOL §5.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_blocks: Option<serde_json::Value>,
    /// Sandbox ID when this agent runs in a CoW-isolated sandbox (direct-mode
    /// workspaces with CoW support). `None` for shared-mode agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    /// Sandbox path when this agent runs in a CoW-isolated sandbox. The full path
    /// to the CoW clone of the workspace directory that serves as this agent's
    /// working root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_path: Option<String>,
    /// Sandbox branch name (e.g., "sb/<agentId>") when this agent runs in a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_branch: Option<String>,
    /// Whether the agent runs in the background (FE `metadata.isBackground`;
    /// G-A1/P3-1.2c). Persisted at `agent.create`/`agent.delegate` and served
    /// as `metadata.isBackground` in the [`AgentLite`] projection — the FE
    /// branches rehydration/list-placement/retry behavior on it. Omitted from
    /// the session wire form when `false` to preserve the TS `AgentSession`
    /// shape for foreground sessions.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_background: bool,
    /// Free-form `metadata` object persisted with the session (C1d-10a). Closes
    /// the metadata half of the P2-12a deferral for `agent_create_op` so the
    /// widened `agent.wakeOrCreate` composite can read back
    /// `delegationDepth` / `createdByAgentId` / `taskNoteId` / `isBackground`
    /// / `source` / `skipAutoCommit` from a parent's session without a
    /// follow-up round-trip. `None` for pre-existing rows and for creates
    /// that omit the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// Canonical stop/finish reason from the latest terminal stream/status event
    /// (Phase 2 — daemon-side persistence of agent failure text). Surfaced as
    /// top-level `stopReason` on both `AgentSession` and `AgentLite` serialization,
    /// omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    /// ISO timestamp recorded when `stop_reason` was persisted (terminal agent
    /// failures). Set alongside `stop_reason`, cleared wherever `stop_reason`
    /// clears (turn begin, `agent.retry`), so clients can render how long ago
    /// a parked-in-error session failed. Serialized as `stopReasonTimestamp`
    /// on both `AgentSession` and `AgentLite`; omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason_timestamp: Option<String>,
    /// Derived-on-emit corrupted/poisoned-session flag (monorepo#940): `true`
    /// when the session is parked in `error` AND the failure classifies as
    /// session-fatal (provider block or deterministic prompt rejection) or the
    /// identical-failure streak hit the poisoned threshold — retry will
    /// recreate the provider session instead of resuming. NOT persisted: the
    /// service layer overlays it on read (`agent.getSession`); omitted from
    /// the wire when `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub session_corrupted: bool,
    /// ISO deadline of an in-memory pending deletion (PROTOCOL §5.5): set on
    /// `agent.getSession` reads while an `agent.delete` grace window
    /// (`undoDelayMs > 0`) is running for this session. NOT persisted — the
    /// service layer overlays it on read from the in-memory registry, and a
    /// daemon restart drops the pending deletion (the session survives and
    /// the field disappears). Omitted (not `null`) when no deletion is
    /// pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_delete_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl AgentSession {
    /// The question-dismissal marker persisted under
    /// [`DISMISSED_QUESTIONS_MESSAGE_ID_KEY`] in the session's free-form
    /// `metadata`: `Some` only when the metadata is an object carrying a
    /// non-empty string under that key. The question-hold derivation compares
    /// this against the last assistant message id — a match means the user
    /// dismissed that message's questions and automatic deliveries resume.
    pub fn dismissed_questions_message_id(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(DISMISSED_QUESTIONS_MESSAGE_ID_KEY))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
    }

    /// The pending-questions marker persisted under
    /// [`PENDING_QUESTIONS_MESSAGE_ID_KEY`] in the session's free-form
    /// `metadata`: `Some` only when the metadata is an object carrying a
    /// non-empty string under that key. The question-hold derivation reads it
    /// directly (no transcript walk) — a set marker that differs from
    /// [`AgentSession::dismissed_questions_message_id`] means questions are
    /// still pending. Cleared markers are written as the empty string, which
    /// reads back as `None` here while
    /// [`AgentSession::pending_questions_marker_written`] stays `true`.
    pub fn pending_questions_message_id(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(PENDING_QUESTIONS_MESSAGE_ID_KEY))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
    }

    /// `true` when the pending-questions marker key is PRESENT in the
    /// session's metadata at all (set or cleared-to-empty). Distinguishes a
    /// session the marker-based derivation has already written (an empty
    /// marker authoritatively means "nothing pending") from a pre-upgrade
    /// session that never saw a marker write, where the hold derivation must
    /// fall back to the transcript tail walk so a live hold is not lost
    /// across the upgrade.
    pub fn pending_questions_marker_written(&self) -> bool {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(PENDING_QUESTIONS_MESSAGE_ID_KEY))
            .is_some_and(serde_json::Value::is_string)
    }

    /// The per-conversation seen marker persisted under
    /// [`LAST_SEEN_MESSAGE_ID_KEY`] in the session's free-form `metadata`:
    /// `Some` only when the metadata is an object carrying a non-empty string
    /// under that key. Clients position the "New messages" divider right
    /// after this message on conversation entry.
    pub fn last_seen_message_id(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(LAST_SEEN_MESSAGE_ID_KEY))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
    }

    /// The initial-agent flag persisted under [`IS_INITIAL_AGENT_KEY`] in the
    /// session's free-form `metadata`: `true` only when the metadata is an
    /// object carrying the JSON boolean `true` under that key (any other
    /// value — absent, `false`, or non-boolean — reads as `false`). Stamped
    /// by the daemon's `workspace.create` initial-agent orchestration.
    pub fn is_initial_agent(&self) -> bool {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(IS_INITIAL_AGENT_KEY))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }
}

/// Nested `metadata` object on [`AgentLite`] (PROTOCOL §5.5). Mirrors the subset
/// of the TS `AgentMetadata` the iOS `AgentSession.parseAgent` reads:
/// `isBackground`, `specialist`, `createdByAgentId` (the parent/spawning agent),
/// and `taskNoteId` — plus the persistence-gap fields the FE writer stored
/// under `metadata` (`completionReport`, `completionReportTimestamp`,
/// `delegationDepth`, `initialMessage`; P3-1.2b). `isBackground` is always
/// emitted (iOS reads it with a `false` default) and carries the persisted
/// session value (G-A1/P3-1.2c); the rest are omitted when absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMetadata {
    pub is_background: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_note_id: Option<NoteId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_report: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_report_timestamp: Option<String>,
    /// Pending attention request (`"discussion"` / `"blocker"`); omitted when
    /// absent. Mirrors [`AgentSession::attention_request_kind`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_request_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_message: Option<String>,
    /// Sandbox ID when this agent runs in a CoW-isolated sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<String>,
    /// Sandbox path when this agent runs in a CoW-isolated sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_path: Option<String>,
    /// Sandbox branch name when this agent runs in a sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_branch: Option<String>,
    /// Question-dismissal marker (PROTOCOL §5.5, question hold): the id of the
    /// assistant message whose trailing question resource blocks the user
    /// dismissed via `agent.dismissQuestions`. Clients gate the Q&A wizard on
    /// it so a dismissed question set never re-surfaces (including after
    /// reload). Omitted when nothing was dismissed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dismissed_questions_message_id: Option<String>,
    /// Per-conversation seen marker (PROTOCOL §5.5): the id of the newest
    /// transcript message the user has seen, advanced monotonically by
    /// `agent.markSeen`. Clients position the "New messages" divider right
    /// after it on conversation entry. Omitted when nothing was marked seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_message_id: Option<String>,
    /// Initial-agent flag (PROTOCOL §5.1/§5.5): present as `true` only when
    /// the raw session metadata carries the daemon-stamped
    /// [`IS_INITIAL_AGENT_KEY`] boolean `true` (the `workspace.create`
    /// initial-agent orchestration). Omitted otherwise — never `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_initial_agent: Option<bool>,
}

/// Lightweight `agent.list` / `agent.get` projection (PROTOCOL §5.5). Mirrors
/// the TS `AgentLite`: the full [`AgentSession`] with `messages` and
/// `systemPrompt` stripped (clients fetch the transcript via
/// `agent.getConversation`), plus a derived `messageCount`, the
/// `lastAgentResponse` / `digest` / `lastUserMessage` computed from the
/// transcript, a nested `metadata` object, and the runtime activity flags the
/// iOS coverflow reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentLite {
    pub id: AgentId,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_session_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub name_explicitly_set: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Reasoning-effort level for the session (PROTOCOL §5.5, Option B);
    /// mirrors [`AgentSession::reasoning_effort`]. Omitted when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Session-discovered effort levels (PROTOCOL §5.5, Option C); mirrors
    /// [`AgentSession::effort_levels`]. Omitted when the provider advertised
    /// no `thought_level` option at session open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_levels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub status: AgentStatus,
    #[serde(default)]
    pub is_active: bool,
    /// Runtime activity flags (iOS `isStreaming`/`isProcessing`/`isResponding`).
    /// `isStreaming`/`isProcessing` stay `false` (the projection has no separate
    /// stream/process distinction); `isResponding` is the daemon-owned in-flight
    /// signal and is overlaid by the service projection (it stays `false` here in
    /// [`AgentLite::from_session`], which has no runtime context).
    #[serde(default)]
    pub is_streaming: bool,
    #[serde(default)]
    pub is_processing: bool,
    #[serde(default)]
    pub is_responding: bool,
    /// Daemon-owned waiting flags (PROTOCOL §5.5/§7.1): the BE-authoritative port
    /// of the FE agent-state selectors so clients render verbatim. `isWaitingOnTool`
    /// is true when the in-flight turn has an unresolved `tool_use` (a tool call
    /// awaiting its result); `isWaitingForOtherAgents` is true when the agent
    /// parents one or more pending completion watches. Both stay `false` in
    /// [`AgentLite::from_session`] (no runtime context) and are overlaid by the
    /// service projection.
    #[serde(default)]
    pub is_waiting_on_tool: bool,
    #[serde(default)]
    pub is_waiting_for_other_agents: bool,
    /// The specific child agent-ids this agent is waiting on (the distinct
    /// `child_agent_id`s of its pending completion watches; PROTOCOL §5.5/§7.1).
    /// Consistent with `isWaitingForOtherAgents`: non-empty iff that flag is
    /// `true`, empty array otherwise. Always serialized (never null/omitted) so
    /// clients consume verbatim without healing. Stays empty in
    /// [`AgentLite::from_session`] (no runtime context) and is overlaid by the
    /// service projection.
    #[serde(default)]
    pub waiting_for_agent_ids: Vec<AgentId>,
    /// Idle-visibility: light metadata for the agent's active
    /// (`scheduled`/`running`) background hooks —
    /// `[{ hookId, name, nextRunAt?, expiresAt? }]` — so a parent/client can
    /// tell a hook-waiting idle agent from a stalled one. Omitted when the
    /// agent owns no active hook. Stays empty in
    /// [`AgentLite::from_session`] (no runtime context) and is overlaid by
    /// the service projection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_on_hooks: Vec<serde_json::Value>,
    /// Idle-visibility (unified external-wait, mirrors `waitingOnHooks`):
    /// light metadata for the agent's active PR monitors —
    /// `[{ monitorId, repo, prNumber, title? }]` — so a parent/client can
    /// tell a PR-monitor-waiting idle agent from a stalled one. Omitted when
    /// the agent owns no active monitor. Stays empty in
    /// [`AgentLite::from_session`] (no runtime context) and is overlaid by
    /// the service projection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waiting_on_pr_monitors: Vec<serde_json::Value>,
    /// Turn-liveness (STAB-125): `turnInFlight` is `true` while a
    /// `session/prompt` turn's live-turn slot is open for this agent, and
    /// `lastStreamActivityAt` is the RFC-3339 timestamp of the most recent
    /// stream event observed for that turn — so a poller can tell a
    /// long-but-alive turn (timestamp advancing) from a wedged agent
    /// (timestamp pinned) before anything persists. Both are additive wire
    /// fields: `turnInFlight` stays `false` and `lastStreamActivityAt` is
    /// omitted in [`AgentLite::from_session`] (no runtime context); the
    /// service projection overlays them.
    #[serde(default)]
    pub turn_in_flight: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_stream_activity_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SessionStats>,
    pub created_at: String,
    pub updated_at: String,
    /// Most-recent activity timestamp; derived from `updated_at` (iOS falls back
    /// to this after `updatedAt`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub message_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_agent_response: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_user_message: Option<String>,
    /// Role (`"user"` / `"assistant"`) of the session's newest
    /// user/assistant transcript message — system (and any other) rows are
    /// transparent. Additive wire field; omitted when the session has no
    /// user/assistant message. Mid-turn, the service projection overlays
    /// `"assistant"` once the in-flight turn has derivable streamed text
    /// (the same gate as the live `lastAgentResponse` overlay).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_role: Option<String>,
    /// Row id of the session's newest user/assistant transcript message —
    /// system (and any other) rows are transparent, matching
    /// `lastMessageRole`. Additive wire field; omitted when the session has
    /// no user/assistant message. Serves per-agent unread computation
    /// against `metadata.lastSeenMessageId` without transcript reads
    /// (intent-hq/monorepo#1597). No live-turn overlay: mid-turn it stays on
    /// the last persisted message until the assistant row persists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Session-level context references persisted at spawn (P3-1.2b); omitted
    /// when absent so pre-gap wire shapes are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_references: Option<serde_json::Value>,
    /// Session-level image blocks persisted at spawn (P3-1.2b); omitted when
    /// absent so pre-gap wire shapes are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_blocks: Option<serde_json::Value>,
    /// Session-level file blocks persisted at spawn (PROTOCOL §5.5); omitted
    /// when absent so pre-existing wire shapes are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_blocks: Option<serde_json::Value>,
    /// Canonical stop/finish reason from the latest terminal stream/status event
    /// (Phase 2). Top-level `stopReason`, matching the FE shared type; omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    /// When `stopReason` was recorded; see [`AgentSession::stop_reason_timestamp`].
    /// Top-level `stopReasonTimestamp`; omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason_timestamp: Option<String>,
    /// Derived-on-emit corrupted/poisoned-session flag (monorepo#940); see
    /// [`AgentSession::session_corrupted`]. Overlaid by the service projection
    /// (`agent.list`/`agent.get`); omitted from the wire when `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub session_corrupted: bool,
    /// ISO deadline of an in-memory pending deletion (PROTOCOL §5.5): set on
    /// `agent.list` / `agent.get` rows while an `agent.delete` grace window
    /// (`undoDelayMs > 0`) is running for this session, so clients can render
    /// or hide the row as they choose. Never persisted — overlaid by the
    /// service projection from the in-memory registry (stays `None` in
    /// [`AgentLite::from_session`], which has no runtime context); a daemon
    /// restart drops the pending deletion and the field disappears. Omitted
    /// (not `null`) when no deletion is pending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_delete_at: Option<String>,
    pub metadata: AgentMetadata,
}

impl AgentLite {
    /// Project an [`AgentSession`] into its `agent.list`/`agent.get` form,
    /// stripping `messages`/`systemPrompt` and attaching the derived fields.
    pub fn from_session(
        session: AgentSession,
        message_count: u64,
        last_agent_response: Option<String>,
        last_user_message: Option<String>,
        digest: Option<String>,
        last_message_role: Option<String>,
        last_message_id: Option<String>,
    ) -> Self {
        let dismissed_questions_message_id =
            session.dismissed_questions_message_id().map(str::to_string);
        let last_seen_message_id = session.last_seen_message_id().map(str::to_string);
        let is_initial_agent = session.is_initial_agent().then_some(true);
        let metadata = AgentMetadata {
            is_background: session.is_background,
            specialist: session.specialist,
            created_by_agent_id: session.parent_agent_id.clone(),
            task_note_id: session.task_note_id.clone(),
            completion_report: session.completion_report,
            completion_report_timestamp: session.completion_report_timestamp,
            attention_request_kind: session.attention_request_kind,
            attention_request_reason: session.attention_request_reason,
            attention_request_timestamp: session.attention_request_timestamp,
            delegation_depth: session.delegation_depth,
            initial_message: session.initial_message,
            sandbox_id: session.sandbox_id.clone(),
            sandbox_path: session.sandbox_path.clone(),
            sandbox_branch: session.sandbox_branch.clone(),
            dismissed_questions_message_id,
            last_seen_message_id,
            is_initial_agent,
        };
        Self {
            id: session.id,
            workspace_id: session.workspace_id,
            parent_agent_id: session.parent_agent_id,
            backend_session_id: session.backend_session_id,
            acp_session_id: session.acp_session_id,
            name: session.name,
            name_explicitly_set: session.name_explicitly_set,
            model: session.model,
            reasoning_effort: session.reasoning_effort,
            effort_levels: session.effort_levels,
            provider: session.provider,
            status: session.status,
            is_active: session.is_active,
            is_streaming: false,
            is_processing: false,
            is_responding: false,
            is_waiting_on_tool: false,
            is_waiting_for_other_agents: false,
            waiting_for_agent_ids: Vec::new(),
            waiting_on_hooks: Vec::new(),
            waiting_on_pr_monitors: Vec::new(),
            turn_in_flight: false,
            last_stream_activity_at: None,
            stats: session.stats,
            last_activity: Some(session.updated_at.clone()),
            created_at: session.created_at,
            updated_at: session.updated_at,
            message_count,
            last_agent_response,
            last_user_message,
            last_message_role,
            last_message_id,
            digest,
            context_references: session.context_references,
            image_blocks: session.image_blocks,
            file_blocks: session.file_blocks,
            stop_reason: session.stop_reason,
            stop_reason_timestamp: session.stop_reason_timestamp,
            session_corrupted: session.session_corrupted,
            pending_delete_at: session.pending_delete_at,
            metadata,
        }
    }
}

/// Optional wire fields on `agent.create` beyond the ported core
/// (`workspaceId`/`name`/`model`/`specialistId`/`idempotencyKey`/`agentId`)
/// — carried in a struct to keep the trait signature manageable. All fields are
/// optional; the FE fills them in when routing an agent-spawn through the daemon
/// so the seam does not need to make a follow-up RPC for provider/context.
///
/// `provider` is persisted on the created [`AgentSession`] (existing column).
/// `metadata` is harvested for the persistence-gap fields (`delegationDepth`,
/// `initialMessage`, `contextReferences`; P3-1.2b); `contextReferences` /
/// `imageBlocks` / `isBackground` also land as session-level fields (top-level
/// params win over the `metadata` fallback). `agentType`, `workspacePath`, and
/// `workspaceContext` are accepted and currently forwarded no further
/// (deferred per the P2-12a audit).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AgentCreateExtra {
    pub provider: Option<String>,
    /// Reasoning-effort level persisted on the created session (PROTOCOL
    /// §5.5, Option B). Stored as-is when a non-empty string; empty /
    /// whitespace-only values collapse to `None` at the boundary.
    pub reasoning_effort: Option<String>,
    pub agent_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub workspace_path: Option<String>,
    pub workspace_context: Option<serde_json::Value>,
    pub context_references: Option<serde_json::Value>,
    pub image_blocks: Option<serde_json::Value>,
    /// Session-level file blocks captured at spawn (PROTOCOL §5.5): entries
    /// carry EITHER inline `data` or an attachment-registry `attachmentId`
    /// reference; validated at the create seam like send/queue.
    pub file_blocks: Option<serde_json::Value>,
    pub is_background: Option<bool>,
    /// Internal override for the created session's `nameExplicitlySet` flag.
    /// Not accepted from the wire (`#[serde(skip)]`): `agent_delegate_op`
    /// sets `Some(false)` so a delegated child keeps its task-derived name
    /// while remaining renameable by the `ws.workspace.setAgentName`
    /// (`skipIfExplicitlySet: true`) opening-turn self-rename. `None`
    /// preserves the default `name.is_some()` behavior.
    #[serde(skip)]
    pub name_explicitly_set: Option<bool>,
}

/// Wire input for `agent.delegate` (PROTOCOL §5.5). `workspaceId` is passed
/// separately; these are the delegation options. Built by the router/MCP
/// surface; the runtime wiring lands in a later milestone.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AgentDelegateInput {
    pub task_note_id: Option<NoteId>,
    pub note_id: Option<NoteId>,
    pub task_text: Option<String>,
    pub agent_instructions: Option<String>,
    pub specialist: Option<String>,
    pub model: Option<String>,
    /// Reasoning-effort level for the delegated child (PROTOCOL §5.5/§5.11).
    /// Wins over the chosen model option's `reasoningEffort` and the
    /// specialist's frontmatter scalar; validated against the cached model
    /// catalog's `effortLevels` evidence when the resolved model has any.
    pub reasoning_effort: Option<String>,
    pub behavior_prompt: Option<String>,
    pub wait_mode: Option<String>,
    pub skip_auto_commit: Option<bool>,
    /// Sandbox isolation mode: "cow" (copy-on-write sandbox) or "shared" (default).
    /// When "cow" and CoW is supported, the agent runs in an isolated CoW clone of
    /// the workspace directory. Falls back to shared mode if CoW is unsupported.
    pub isolation: Option<String>,
    /// Occupancy override: a task that already has a live assigned agent
    /// rejects a second delegation unless `force: true` is passed to
    /// intentionally add another agent.
    pub force: Option<bool>,
    /// Batch form (PROTOCOL §5.5): a list of task-note ids to classify and
    /// start together. Mutually exclusive with `taskNoteId`/`noteId`/
    /// `taskText`, and the single-task-only `agentInstructions`/`force` are
    /// rejected alongside it; when present the result enumerates every listed
    /// task with its disposition (`started` / `held:*` / `skipped`) plus the
    /// unlock plan. Single-task calls (this field absent) behave exactly as
    /// before.
    pub tasks: Option<Vec<NoteId>>,
    /// Batch-only conflict policy (default false): `false` holds a task whose
    /// `conflictsWith` intersects the running/starting set, admitting
    /// startable tasks in effort-weighted critical-path priority order;
    /// `true` starts it anyway and names the conflict pairs in the result.
    pub greedy: Option<bool>,
}

/// Optional `create.*` payload on [`AgentWakeOrCreateInput`] — the fields the
/// widened `agent.wakeOrCreate` (C1d-10a) forwards into `agent.create` when the
/// task has no live/resumable assigned agent. Mirrors the FE
/// `WakeOrCreateTaskAgentTool` create payload: `name`, `specialist`,
/// `provider`, `agentType`, `model`, `contextReferences`, `metadata`, and
/// `skipAutoCommit`. All optional so the pre-widening wire shape stays valid;
/// specialist/model inheritance from a previous assigned session wins over
/// these when both are present.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AgentWakeCreateOptions {
    pub name: Option<String>,
    pub specialist: Option<String>,
    pub provider: Option<String>,
    pub agent_type: Option<String>,
    pub model: Option<String>,
    /// Reasoning-effort level for the created child (PROTOCOL §5.5/§5.11),
    /// used only on the create branch. Overridden by the wake-level
    /// [`AgentWakeOrCreateInput::reasoning_effort`] when both are present.
    pub reasoning_effort: Option<String>,
    pub context_references: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub skip_auto_commit: Option<bool>,
}

/// Wire input for `agent.wakeOrCreate` (PROTOCOL §5.5, widened by C1d-10a).
/// `workspaceId`, `taskNoteId`, `contextMessage` are passed separately (the
/// existing 3-required-params shape). Everything here is optional so the
/// pre-widening callers stay green: `model` is the wake-branch model override;
/// `callerAgentId`/`delegationDepth` drive the delegation-depth guard;
/// `messageMetadata` is threaded onto the delivered context message on both
/// branches; `create` carries the rich `agent.create` payload used when no
/// live/resumable assigned agent is found.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AgentWakeOrCreateInput {
    pub model: Option<String>,
    /// Reasoning-effort override for the create branch (PROTOCOL §5.5/§5.11);
    /// wins over `create.reasoningEffort` and the specialist frontmatter.
    pub reasoning_effort: Option<String>,
    pub caller_agent_id: Option<AgentId>,
    pub delegation_depth: Option<i64>,
    pub message_metadata: Option<serde_json::Value>,
    pub create: Option<AgentWakeCreateOptions>,
}

/// A single file's status line, mirroring the TS `GitFileStatus` enum
/// (`src/shared/types.ts`). Serializes to the porcelain status character so the
/// wire shape matches `git.status` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitFileStatus {
    #[serde(rename = "M")]
    Modified,
    #[serde(rename = "A")]
    Added,
    #[serde(rename = "D")]
    Deleted,
    #[serde(rename = "R")]
    Renamed,
    #[serde(rename = "C")]
    Copied,
    #[serde(rename = "?")]
    Untracked,
    #[serde(rename = "!")]
    Ignored,
}

/// One entry in [`GitStatus::files`] (`{ path, status, staged }`), mirroring the
/// TS `FileStatus`. A file with both staged and unstaged changes yields two
/// entries (matching the TS `parseStatusOutput`).
///
/// Submodule (gitlink) entries additionally carry `mode: "160000"` plus the
/// old/new pin SHAs (monorepo#1739) so a client can route them to a dedicated
/// presentation without probing `git.showFile`. All three fields are omitted
/// for regular file entries (additive, backward-compatible).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStatus {
    pub path: String,
    pub status: GitFileStatus,
    pub staged: bool,
    /// Octal tree-entry mode string, present only for gitlinks (`"160000"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Pre-change submodule pin SHA (`None` for a newly added submodule).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_sha: Option<String>,
    /// Post-change submodule pin SHA (`None` for a deleted submodule).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_sha: Option<String>,
}

/// `git.status` result (`GitStatus` in `src/shared/types.ts`). `diverged` is true
/// only when the branch is both ahead and behind its upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitStatus {
    pub branch: String,
    pub ahead: i64,
    pub behind: i64,
    pub diverged: bool,
    pub files: Vec<FileStatus>,
    pub has_uncommitted_changes: bool,
    pub has_untracked_files: bool,
}

/// `git.getBranches` result (`{ branches, remoteBranches, currentBranch,
/// defaultBranch }`), matching the TS `git.getBranches` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranches {
    pub branches: Vec<String>,
    pub remote_branches: Vec<String>,
    pub current_branch: String,
    pub default_branch: String,
}

/// `git.branchStatus` result — ahead/behind of the queried branch's upstream
/// (`origin/<branchName>`), the worktree's currently-checked-out branch (with a
/// derived `isCurrentBranch` flag against the queried name), and whether the
/// working tree has any uncommitted changes (staged + unstaged + untracked,
/// matching the legacy `git status --porcelain` semantics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranchStatus {
    pub branch: String,
    pub current_branch: String,
    pub is_current_branch: bool,
    pub ahead: i64,
    pub behind: i64,
    pub has_uncommitted_changes: bool,
}

/// `git.pull` result (`{ ok, error? }`), mirroring the legacy `git:pullBranch`
/// IPC's `{ success, error? }` payload: ordinary pull failures (conflicts,
/// unreachable remote, stash-recovery problems) are a structured `ok: false` +
/// `error` rather than a JSON-RPC error, so the workspace-create flow can show
/// its pull-conflict dialog. `error` is omitted on success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitPullResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `git.commit` service result (the `ok` flag is added by the transport). Mirrors
/// the TS `ws.git.commit` payload `{ hash?, files? }`; on success both are
/// present (`hash` is the new commit SHA, `files` the files it changed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCommitResult {
    pub hash: String,
    pub files: Vec<String>,
}

/// `git.agentCommit` service result (the `ok` flag is added by the transport).
/// Mirrors the TS `ws.git.agentCommit` payload `{ hash, files, fileCount }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitAgentCommitResult {
    pub hash: String,
    pub files: Vec<String>,
    pub file_count: i64,
}

/// `git.checkMergeConflicts` result, mirroring the TS `ws.git.checkMergeConflicts`
/// payload `{ hasConflicts, conflictedFiles, cannotDetermine?, targetBranch,
/// currentBranch }`. `cannotDetermine` is omitted unless the merge base could not
/// be resolved (the TS legacy fallback's only producer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitMergeConflicts {
    pub has_conflicts: bool,
    pub conflicted_files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cannot_determine: Option<bool>,
    pub target_branch: String,
    pub current_branch: String,
}

/// Script execution mode (`script.*`, PROTOCOL §5.8; ported from the TS
/// `ScriptMode`). `service` is a long-running, auto-restartable process (dev
/// server / watcher); `command` runs once to completion (build / test / lint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptMode {
    #[default]
    Service,
    Command,
}

/// Runtime status of a script process (ported from the TS `ScriptStatus`,
/// plus `restarting` — new in intentd, monorepo#1318). `restarting` covers the
/// restart-in-flight window (the auto-restart backoff and the `script.restart`
/// stop→start gap) so clients can distinguish it from a final exit; the
/// respawn flips it back to `running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptStatus {
    #[default]
    Idle,
    Running,
    Restarting,
    Exited,
}

/// In-memory runtime state of a script process — the `script.status` result and
/// the `runtime` field of a `script.list` entry (ported from the TS
/// `ScriptRuntimeState`). Not persisted, except the `was_running` marker
/// behind `previously_running` (stored-on-write on the `script` row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptRuntimeState {
    pub status: ScriptStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<String>,
    pub restart_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_url: Option<String>,
    /// `Some(true)` on a hydrated `idle` state whose service-mode script was
    /// running when the previous daemon process died (the persisted
    /// `was_running` marker), so clients can re-render its tab after a
    /// restart. Omitted otherwise (presence-detected additive convention,
    /// PROTOCOL §5.8); cleared once the script is started or stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previously_running: Option<bool>,
}

impl Default for ScriptRuntimeState {
    fn default() -> Self {
        Self {
            status: ScriptStatus::Idle,
            pid: None,
            exit_code: None,
            started_at: None,
            stopped_at: None,
            restart_count: 0,
            error: None,
            detected_url: None,
            previously_running: None,
        }
    }
}

/// A workspace script definition — the `script.create` result and the base of a
/// `script.list` entry (ported from the TS `WorkspaceScript`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Script {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    pub mode: ScriptMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_start: Option<bool>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Wire input for `script.create` (PROTOCOL §5.8); built by the router from
/// request params. `workspaceId` is passed separately.
#[derive(Debug, Clone, Default)]
pub struct ScriptCreateParams {
    pub name: String,
    pub command: String,
    pub mode: ScriptMode,
    pub cwd: Option<String>,
    pub env: Option<BTreeMap<String, String>>,
    pub category: Option<String>,
    pub auto_start: Option<bool>,
    pub script_id: Option<String>,
}

/// Lifecycle state of a background hook. `scheduled` and `running` are the
/// active states (rehydrated into the scheduler at boot); `dispatched`,
/// `evicted`, `cancelled`, and `expired` are terminal. Wire/DB words are the
/// lowercase variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HookState {
    /// Waiting for its next run (sleeping `delayMs`).
    Scheduled,
    /// A run is currently executing.
    Running,
    /// A run signalled dispatch; the owner was woken and the hook terminated.
    Dispatched,
    /// Evicted after a throw/timeout; the owner was woken with the reason.
    Evicted,
    /// Cancelled by the owner or from the FE.
    Cancelled,
    /// TTL elapsed (`expiresAt` passed); the owner was woken so it can
    /// reschedule if the condition is still worth watching.
    Expired,
}

/// A background hook: a small agent-owned script the daemon runs periodically
/// (fixed `delayMs` between runs) until it signals a dispatch, fails, is
/// cancelled, or its TTL expires. Persisted to the `hook` table so schedules
/// survive a daemon restart; the name length cap (≤50 chars) is enforced at
/// the service layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hook {
    pub hook_id: HookId,
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub name: String,
    pub code: String,
    pub delay_ms: i64,
    pub state: HookState,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<String>,
    pub run_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Captured `console.*` output from the most recent completed run
    /// (overwritten each run; capped/head-truncated at the service layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_logs: Option<String>,
    /// JSON-serialized state returned by the most recent completed run and
    /// injected into the next run as the `hookState` global (overwritten
    /// each run; size-capped at the service layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_state: Option<String>,
    /// TTL deadline (`createdAt` + clamped `ttlMs`, ≤ 60 minutes): the hook
    /// expires when this passes. `None` only on pre-TTL legacy rows, which
    /// never expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Perpetual hooks stay active after a dispatch (the owner is woken and
    /// the hook returns to `scheduled`), running on their cadence until TTL
    /// expiry, cancel, or eviction. `false` is the one-shot default.
    #[serde(default)]
    pub perpetual: bool,
    /// How many runs signalled a dispatch. Always ≤ 1 for a one-shot hook;
    /// perpetual hooks accumulate across fires.
    #[serde(default)]
    pub dispatch_count: i64,
}

/// Lifecycle state of a PR monitor. `active` is the only live state
/// (rehydrated into the poll loop at boot); `completed` (the PR merged or
/// closed) and `cancelled` are terminal. Completed rows are RETAINED and stay
/// visible in list surfaces; cancelled rows are excluded. Wire/DB words are
/// the lowercase variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrMonitorState {
    /// Being polled; changes accumulate and wake the owner after the
    /// debounce window.
    Active,
    /// The PR reached a terminal lifecycle (merged or closed); the owner was
    /// woken immediately and monitoring stopped.
    Completed,
    /// Cancelled by the owning agent (`ws.pr.unmonitor`) or from the app
    /// (`prMonitor.cancel`).
    Cancelled,
}

/// A PR monitor: an agent-owned watch on one pull request, polled centrally
/// by the daemon. Changes to the PR's merge-requirements checklist are
/// accumulated (`pendingChanges`) and delivered as ONE consolidated wake once
/// the PR has been quiet for the debounce window. Persisted to the
/// `pr_monitor` table so monitors survive a daemon restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrMonitor {
    pub monitor_id: PrMonitorId,
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub repo_owner: String,
    pub repo_name: String,
    pub pr_number: i64,
    pub state: PrMonitorState,
    /// JSON-serialized merge-requirements baseline the next poll diffs
    /// against. `None` until the first successful poll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_snapshot: Option<String>,
    /// JSON-serialized snapshot as of the last delivered wake (or
    /// registration) — the emit baseline pending changes are coalesced
    /// against. Backfilled from `last_snapshot` for pre-existing rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_snapshot: Option<String>,
    /// Change lines accumulated since the last emit, awaiting the debounce
    /// window to close. Empty when nothing is pending.
    #[serde(default)]
    pub pending_changes: Vec<String>,
    /// When the oldest un-emitted change was detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_since: Option<String>,
    /// When the most recent change was detected — the quiet-window anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_change_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_polled_at: Option<String>,
    /// Most recent forge-poll error (cleared by a successful poll); a failing
    /// poll never kills the loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// How a [`WorkspaceGitRoot`] came to be tracked. Wire/DB words are the
/// lowercase variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceGitRootSource {
    /// Registered explicitly by an agent (`ws.git.registerRoot`).
    Agent,
    /// Auto-detected by the daemon (the workspace worktree's git submodules).
    Auto,
}

/// A secondary local git repository tracked for a workspace (multi git root
/// tracking, intent-hq/monorepo#2053) — an agent-created subtree checkout, a
/// submodule, or a sibling clone anywhere on the host. Persisted to the
/// `workspace_git_root` table (rows cascade with their workspace); the daemon
/// runs the same background PR discovery on each root as on the primary
/// workspace root, so the PR fields mirror the [`Workspace`] PR fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceGitRoot {
    pub id: WorkspaceGitRootId,
    pub workspace_id: WorkspaceId,
    /// Canonicalized absolute path of the git repository root. Registration
    /// is idempotent by `(workspaceId, path)`.
    pub path: String,
    pub source: WorkspaceGitRootSource,
    /// Repository owner detected from the root's `origin` remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_owner: Option<String>,
    /// Repository name detected from the root's `origin` remote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_name: Option<String>,
    /// Agents that registered this root, in registration order (deduped).
    /// Empty for auto-detected roots with no explicit registrations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub registered_by_agent_ids: Vec<AgentId>,
    /// The root's HEAD commit SHA captured when the root was first registered
    /// (agent registration or sweep auto-detect); immutable once set — merges
    /// never touch it. `None` when HEAD was unreadable at registration or the
    /// row predates the field; the background sweep best-effort-backfills
    /// such rows with the root's current HEAD (a going-forward boundary).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    /// Persisted PR lifecycle status for the root's linked PR (§7.6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_status: Option<PullRequestStatus>,
    /// Persisted list of PR snapshots discovered for the root's current
    /// branch (§7.6). `None` = never populated by the daemon; `Some(vec![])`
    /// = explicitly no discovered PRs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_requests: Option<Vec<PullRequestInfo>>,
    pub created_at: String,
    pub updated_at: String,
}

/// Logical client record (§9.2, §16). The stable, client-supplied identity that
/// survives reconnects; persisted to the `client` table with `name`,
/// `capabilities`, `first_seen`, and `last_seen`. The ephemeral per-connection
/// id is transport-only and never stored here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub id: ClientId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub capabilities: serde_json::Value,
    pub first_seen: String,
    pub last_seen: String,
}

/// Per-client chat draft (§9.10, §15), keyed by `(workspaceId, agentId,
/// clientId)` so concurrent clients never clobber one another. Persisted to the
/// `draft` table; an empty draft is represented by the row's absence (a
/// `drafts.set` with empty text and no attachments clears it). `attachments`
/// is an opaque, FE-authored JSON array stored verbatim (like workspace
/// context items); `None` when the draft has none.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Draft {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub client_id: ClientId,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<serde_json::Value>,
    pub updated_at: String,
}

/// A persistently-registered repository (parity with the TS `KnownRepo`). Backs
/// the `repo.list` method that populates the Create-Workspace picker. Timestamps
/// are ISO-8601 strings; `owner` is omitted from the wire when absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownRepo {
    /// Absolute path to the repository.
    pub path: String,
    /// Repository name (typically the folder name).
    pub name: String,
    /// GitHub organization or user who owns this repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// ISO timestamp of when this repo was first added.
    pub added_at: String,
    /// ISO timestamp of when this repo was last used (workspace created).
    pub last_used_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn event_wire_shape_matches_ts() {
        // Mirrors `WorkspaceEventBase` + `EventActor` in events/types.ts: the
        // discriminant is `type`, ids/timestamps are camelCase, and absent
        // optionals are omitted.
        let event = Event {
            id: "01900000-0000-7000-8000-000000000000".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: "file:changed".to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some("agent-7".to_string()),
                model: Some("opus".to_string()),
                ..Default::default()
            },
            session_id: Some("sess-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "path": "src/a.rs", "action": "modify" }),
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({
                "id": "01900000-0000-7000-8000-000000000000",
                "workspaceId": "ws-1",
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "file:changed",
                "actor": { "type": "agent", "id": "agent-7", "model": "opus" },
                "sessionId": "sess-1",
                "data": { "path": "src/a.rs", "action": "modify" }
            })
        );
        // Round-trips back to an equal value.
        let back: Event = serde_json::from_value(serde_json::to_value(&event).unwrap()).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn event_metadata_serializes_camel_case() {
        // Mirrors `WorkspaceEventBase.metadata` in events/types.ts: an optional
        // free-form object emitted under the camelCase `metadata` key.
        let event = Event {
            id: "01900000-0000-7000-8000-000000000001".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: "agent:message".to_string(),
            actor: EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: Some(json!({ "source": "test", "retryCount": 2 })),
            data: json!({}),
        };
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(
            wire["metadata"],
            json!({ "source": "test", "retryCount": 2 })
        );
        let back: Event = serde_json::from_value(wire).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn script_status_serializes_restarting_lowercase() {
        // The restart-in-flight window (monorepo#1318) rides the same
        // lowercase wire encoding as the ported statuses.
        assert_eq!(
            serde_json::to_value(ScriptStatus::Restarting).unwrap(),
            json!("restarting")
        );
        let back: ScriptStatus = serde_json::from_value(json!("restarting")).unwrap();
        assert_eq!(back, ScriptStatus::Restarting);
    }

    #[test]
    fn checkout_mode_wire_values_round_trip() {
        // `Workspace.checkoutMode` wire values are lowercase strings:
        // `worktree`, `cow`, and `direct` (standalone plain repo — cache
        // hydration fallback or isNewRepo initialization).
        for (mode, wire) in [
            (CheckoutMode::Worktree, "worktree"),
            (CheckoutMode::Cow, "cow"),
            (CheckoutMode::Direct, "direct"),
        ] {
            assert_eq!(serde_json::to_value(mode).unwrap(), json!(wire));
            let back: CheckoutMode = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(back, mode);
        }
    }

    #[test]
    fn workspace_create_parses_is_new_repo_wire_name() {
        // `isNewRepo` (camelCase) is the wire name for the new-project flow
        // flag (intent-hq/monorepo#962); absent keeps `None` so legacy
        // callers are unaffected.
        let with_flag: WorkspaceCreate =
            serde_json::from_value(json!({ "repositoryPath": "/tmp/new", "isNewRepo": true }))
                .unwrap();
        assert_eq!(with_flag.is_new_repo, Some(true));
        assert_eq!(with_flag.repository_path.as_deref(), Some("/tmp/new"));

        let explicit_false: WorkspaceCreate =
            serde_json::from_value(json!({ "isNewRepo": false })).unwrap();
        assert_eq!(explicit_false.is_new_repo, Some(false));

        let absent: WorkspaceCreate = serde_json::from_value(json!({})).unwrap();
        assert_eq!(absent.is_new_repo, None);

        // snake_case is not a wire name — the unknown key is ignored and the
        // field stays `None` (camelCase only).
        let snake: WorkspaceCreate =
            serde_json::from_value(json!({ "is_new_repo": true })).unwrap();
        assert_eq!(snake.is_new_repo, None);
    }

    #[test]
    fn known_repo_wire_shape_matches_ts() {
        // Mirrors `KnownRepo` in src/shared/types/known-repo.ts: camelCase
        // `addedAt`/`lastUsedAt`, and `owner` omitted when absent.
        let with_owner = KnownRepo {
            path: "/Users/me/src/intent".to_string(),
            name: "intent".to_string(),
            owner: Some("intent-hq".to_string()),
            added_at: "2026-01-01T00:00:00Z".to_string(),
            last_used_at: "2026-01-02T00:00:00Z".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&with_owner).unwrap(),
            json!({
                "path": "/Users/me/src/intent",
                "name": "intent",
                "owner": "intent-hq",
                "addedAt": "2026-01-01T00:00:00Z",
                "lastUsedAt": "2026-01-02T00:00:00Z"
            })
        );
        // `owner: None` omits the key entirely (no `null`).
        let no_owner = KnownRepo {
            owner: None,
            ..with_owner.clone()
        };
        let wire = serde_json::to_value(&no_owner).unwrap();
        assert!(wire.get("owner").is_none());
        assert_eq!(serde_json::from_value::<KnownRepo>(wire).unwrap(), no_owner);
    }

    #[test]
    fn content_type_wire_forms_match_ts() {
        // Mirrors `ContentType` in src/shared/types.ts: plain_text (not plaintext).
        for (variant, wire) in [
            (ContentType::Markdown, "\"markdown\""),
            (ContentType::PlainText, "\"plain_text\""),
            (ContentType::Json, "\"json\""),
            (ContentType::Code, "\"code\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(serde_json::from_str::<ContentType>(wire).unwrap(), variant);
        }
    }

    /// A regular comment with `anchorContext` and no suggestion fields must
    /// serialize to the camelCase `CommentV2` wire shape (`type`, `authorType`,
    /// `createdAt`, nested `anchorContext`) the iOS client expects (§5.3).
    #[test]
    fn comment_wire_regular_camel_case_parity() {
        let comment = Comment {
            id: "c1".to_string(),
            thread_id: "t1".to_string(),
            note_id: Some(NoteId::from("note-1")),
            kind: CommentType::Comment,
            content: "hello".to_string(),
            author: "Agent".to_string(),
            author_type: AuthorType::Agent,
            status: CommentStatus::Open,
            parent_id: None,
            anchor: Some(CommentAnchor {
                kind: CommentAnchorType::Range,
                start_id: Some("c1".to_string()),
                end_id: Some("c1".to_string()),
                point_id: None,
            }),
            anchor_text: Some("Seed".to_string()),
            anchor_before: Some("be".to_string()),
            anchor_after: Some("af".to_string()),
            suggestion_original: None,
            suggestion_proposed: None,
            agent_id: None,
            is_orphaned: None,
            created_at: "t0".to_string(),
            updated_at: "t0".to_string(),
        };
        let value = serde_json::to_value(CommentWire::from_comment(&comment)).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "c1",
                "threadId": "t1",
                "noteId": "note-1",
                "type": "comment",
                "content": "hello",
                "author": "Agent",
                "authorType": "agent",
                "status": "open",
                "anchor": { "type": "range", "startId": "c1", "endId": "c1" },
                "anchorText": "Seed",
                "anchorContext": { "before": "be", "after": "af" },
                "createdAt": "t0",
                "updatedAt": "t0"
            })
        );
    }

    /// A suggestion comment nests `suggestionDiff` and omits `anchorContext`
    /// when no anchor context is present (matches `comment-loader.ts`). As a
    /// reply it carries no `anchor`/`anchorText` of its own — both keys are
    /// absent on the wire (monorepo#729).
    #[test]
    fn comment_wire_suggestion_nests_suggestion_diff() {
        let comment = Comment {
            id: "c2".to_string(),
            thread_id: "t1".to_string(),
            note_id: Some(NoteId::from("note-1")),
            kind: CommentType::Suggestion,
            content: "try this".to_string(),
            author: "Agent".to_string(),
            author_type: AuthorType::Agent,
            status: CommentStatus::Open,
            parent_id: Some("c1".to_string()),
            anchor: None,
            anchor_text: None,
            anchor_before: None,
            anchor_after: None,
            suggestion_original: Some("Seed".to_string()),
            suggestion_proposed: Some("Sprout".to_string()),
            agent_id: None,
            is_orphaned: None,
            created_at: "t0".to_string(),
            updated_at: "t0".to_string(),
        };
        let value = serde_json::to_value(CommentWire::from_comment(&comment)).unwrap();
        assert_eq!(value["type"], json!("suggestion"));
        assert_eq!(value["parentId"], json!("c1"));
        assert_eq!(
            value["suggestionDiff"],
            json!({ "original": "Seed", "proposed": "Sprout" })
        );
        assert!(value.get("anchorContext").is_none());
        assert!(value.get("suggestion_original").is_none());
        // Reply-anchoring contract (monorepo#729): no anchor/anchorText keys.
        assert!(value.get("anchor").is_none());
        assert!(value.get("anchorText").is_none());
    }

    /// `comment.add` echoes a camelCase `commentId` + nested `location`
    /// (`anchoredText`), matching the TS `ws.comment.add` return (§5.3),
    /// plus the post-rewrite `noteRev` (monorepo#638).
    #[test]
    fn comment_add_result_camel_case_parity() {
        let result = CommentAddResult {
            success: true,
            message: "Comment successfully anchored to \"Seed\"".to_string(),
            comment_id: "c1".to_string(),
            anchored: true,
            note_rev: 3,
            location: CommentLocation {
                line: 1,
                anchored_text: "Seed".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "success": true,
                "message": "Comment successfully anchored to \"Seed\"",
                "commentId": "c1",
                "anchored": true,
                "noteRev": 3,
                "location": { "line": 1, "anchoredText": "Seed" }
            })
        );
    }

    /// `task.update` returns `lineNumber` (camelCase) + a checkbox status word.
    #[test]
    fn task_update_result_camel_case_parity() {
        let result = TaskUpdateResult {
            ok: true,
            note_id: NoteId::from("task-1"),
            line_number: 3,
            previous_text: "old".to_string(),
            new_text: "new".to_string(),
            status: "done".to_string(),
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "ok": true,
                "noteId": "task-1",
                "lineNumber": 3,
                "previousText": "old",
                "newText": "new",
                "status": "done"
            })
        );
    }

    /// `AgentStatus` keeps the modern lowercase values and the legacy
    /// capitalized ones distinct, so persisted sessions round-trip unchanged
    /// (`agent.types.ts`).
    #[test]
    fn agent_status_wire_forms_match_ts() {
        for (variant, wire) in [
            (AgentStatus::Pending, "\"pending\""),
            (AgentStatus::Active, "\"active\""),
            (AgentStatus::RuntimeIdle, "\"idle\""),
            (AgentStatus::Error, "\"error\""),
            (AgentStatus::Deleted, "\"deleted\""),
            (AgentStatus::Idle, "\"Idle\""),
            (AgentStatus::Waiting, "\"Waiting\""),
            (AgentStatus::Completed, "\"Completed\""),
            (AgentStatus::Processing, "\"Processing\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(serde_json::from_str::<AgentStatus>(wire).unwrap(), variant);
        }
    }

    /// `WorkspaceStatus` serializes to the PascalCase TS `WorkspaceStatus` string
    /// enum (`src/shared/types.ts`): `Active`/`Inactive`/`Archived`/`Deleted`.
    #[test]
    fn workspace_status_wire_forms_match_ts() {
        for (variant, wire) in [
            (WorkspaceStatus::Active, "\"Active\""),
            (WorkspaceStatus::Inactive, "\"Inactive\""),
            (WorkspaceStatus::Archived, "\"Archived\""),
            (WorkspaceStatus::Deleted, "\"Deleted\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(
                serde_json::from_str::<WorkspaceStatus>(wire).unwrap(),
                variant
            );
        }
    }

    /// `Workspace` emits PascalCase `status` and omits absent optionals
    /// (`skip_serializing_if`) so the iOS decoder sees the documented field set
    /// without nulls.
    #[test]
    fn workspace_status_pascal_and_optionals_absent() {
        let ts = "2026-01-01T00:00:00Z".to_string();
        let ws = Workspace {
            id: WorkspaceId::from("ws-1"),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            display_status: None,
            waiting: false,
            token_usage: None,
            cow_supported: None,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        let v = serde_json::to_value(&ws).unwrap();
        assert_eq!(v["status"], "Active");
        // Absent optionals are omitted, not serialized as null.
        for key in [
            "statusMessage",
            "baseRef",
            "prNumber",
            "prStatus",
            "activePullRequest",
            "pullRequests",
            "repositoryOwner",
            "lastActivity",
            "archivedAt",
            "diskUsage",
        ] {
            assert!(v.get(key).is_none(), "expected `{key}` to be omitted");
        }
        // Presence-detected `waiting`: omitted when false, emitted when true.
        assert!(v.get("waiting").is_none(), "waiting omitted when false");
        // Round-trips back with optionals defaulted to None.
        let back: Workspace = serde_json::from_value(v).unwrap();
        assert_eq!(back, ws);

        let mut waiting_ws = ws.clone();
        waiting_ws.waiting = true;
        let v = serde_json::to_value(&waiting_ws).unwrap();
        assert_eq!(v["waiting"], serde_json::json!(true));
        let back: Workspace = serde_json::from_value(v).unwrap();
        assert_eq!(back, waiting_ws);
    }

    /// The card aggregates serialize with the exact nested field names + casing
    /// the iOS `WorkspaceStore.parseWorkspace` reads (`taskStats.{total,
    /// completed,inProgress}`, `agentSummary.{count,agents[]}` with
    /// `WorkspaceAgentInfo`, `diffSummary.{totalFiles,...}`).
    #[test]
    fn workspace_card_aggregates_nested_wire_shape() {
        let ts = "2026-01-01T00:00:00Z".to_string();
        let agent = WorkspaceAgentInfo {
            id: AgentId::from("agent-1"),
            name: "Builder".to_string(),
            status: AgentStatus::Active,
            specialist: Some("implementor".to_string()),
            last_activity: Some(ts.clone()),
            is_streaming: false,
            is_responding: false,
            parent_agent_id: Some(AgentId::from("agent-root")),
        };
        let summary = WorkspaceAgentSummary {
            count: 1,
            agents: vec![agent],
            agent_ids: vec![AgentId::from("agent-1")],
        };
        let task_stats = WorkspaceTaskStats {
            total: 3,
            completed: 1,
            in_progress: 1,
        };
        let diff = WorkspaceDiffSummary {
            schema_version: 1,
            updated_at: ts.clone(),
            total_files: 2,
            total_additions: 10,
            total_deletions: 4,
            files: vec![],
        };
        let v = serde_json::to_value(&task_stats).unwrap();
        assert_eq!(v["total"], 3);
        assert_eq!(v["completed"], 1);
        assert_eq!(v["inProgress"], 1);

        let v = serde_json::to_value(&summary).unwrap();
        assert_eq!(v["count"], 1);
        assert_eq!(v["agents"][0]["id"], "agent-1");
        assert_eq!(v["agents"][0]["name"], "Builder");
        assert_eq!(v["agents"][0]["status"], "active");
        assert_eq!(v["agents"][0]["specialist"], "implementor");
        assert_eq!(v["agents"][0]["lastActivity"], ts);
        assert_eq!(v["agents"][0]["isStreaming"], false);
        assert_eq!(v["agents"][0]["isResponding"], false);
        // `parentAgentId` (v2.9): the delegating agent, camelCased on the wire.
        assert_eq!(v["agents"][0]["parentAgentId"], "agent-root");
        // `agentIds` is emitted alongside `agents` (forward-compat TS parity).
        assert_eq!(v["agentIds"][0], "agent-1");
        assert_eq!(v["agentIds"].as_array().unwrap().len(), 1);

        let v = serde_json::to_value(&diff).unwrap();
        assert_eq!(v["schemaVersion"], 1);
        assert_eq!(v["totalFiles"], 2);
        assert_eq!(v["totalAdditions"], 10);
        assert_eq!(v["totalDeletions"], 4);
        assert!(v["files"].is_array());
    }

    /// `WorkspaceUpdate` distinguishes an explicit wire `null` on the
    /// clearable PR fields from a missing field: `null` → `Some(None)`
    /// (clear the stored value), missing → `None` (no change), value →
    /// `Some(Some(v))` (set the stored value). Mirrors PROTOCOL §5.1.
    #[test]
    fn workspace_update_pr_fields_null_clear_semantics() {
        // Missing fields → outer `None` (no change).
        let empty: WorkspaceUpdate = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(empty.pr_url.is_none(), "missing prUrl → None (no change)");
        assert!(empty.pr_number.is_none());
        assert!(empty.pr_status.is_none());
        assert!(empty.active_pull_request.is_none());
        assert!(empty.pull_requests.is_none());

        // Explicit wire `null` → `Some(None)` (explicit clear).
        let cleared: WorkspaceUpdate = serde_json::from_value(serde_json::json!({
            "prUrl": null,
            "prNumber": null,
            "prStatus": null,
            "activePullRequest": null,
            "pullRequests": null,
        }))
        .unwrap();
        assert_eq!(cleared.pr_url, Some(None));
        assert_eq!(cleared.pr_number, Some(None));
        assert_eq!(cleared.pr_status, Some(None));
        assert_eq!(cleared.active_pull_request, Some(None));
        assert_eq!(cleared.pull_requests, Some(None));

        // Present value → `Some(Some(v))`.
        let set: WorkspaceUpdate = serde_json::from_value(serde_json::json!({
            "prUrl": "https://example.com/pr/1",
            "prNumber": 42,
            "prStatus": "Open",
            "pullRequests": [],
        }))
        .unwrap();
        assert_eq!(
            set.pr_url,
            Some(Some("https://example.com/pr/1".to_string()))
        );
        assert_eq!(set.pr_number, Some(Some(42)));
        assert_eq!(set.pr_status, Some(Some(PullRequestStatus::Open)));
        assert_eq!(set.pull_requests, Some(Some(vec![])));
    }

    /// The `workspace:updated` event carries the applied `WorkspaceUpdate`
    /// delta as `changes` (§6.5). Missing fields are omitted from the wire
    /// (`skip_serializing_if = "Option::is_none"`); an explicit `Some(None)`
    /// clear serializes as JSON `null`, distinguishable from omission.
    #[test]
    fn workspace_update_serializes_omits_missing_and_emits_null_for_clear() {
        let empty = WorkspaceUpdate::default();
        let v = serde_json::to_value(&empty).unwrap();
        for key in [
            "prUrl",
            "prNumber",
            "prStatus",
            "activePullRequest",
            "pullRequests",
        ] {
            assert!(v.get(key).is_none(), "expected `{key}` to be omitted");
        }

        let clear = WorkspaceUpdate {
            pr_url: Some(None),
            pr_number: Some(None),
            pr_status: Some(None),
            active_pull_request: Some(None),
            pull_requests: Some(None),
            ..Default::default()
        };
        let v = serde_json::to_value(&clear).unwrap();
        assert_eq!(v["prUrl"], serde_json::Value::Null);
        assert_eq!(v["prNumber"], serde_json::Value::Null);
        assert_eq!(v["prStatus"], serde_json::Value::Null);
        assert_eq!(v["activePullRequest"], serde_json::Value::Null);
        assert_eq!(v["pullRequests"], serde_json::Value::Null);
    }

    /// `WorkspaceDiffSummaryFile` matches the TS per-file wire shape.
    #[test]
    fn workspace_diff_summary_file_wire_shape() {
        let f = WorkspaceDiffSummaryFile {
            path: "src/main.rs".to_string(),
            action: "modify".to_string(),
            additions: 3,
            deletions: 1,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["path"], "src/main.rs");
        assert_eq!(v["action"], "modify");
        assert_eq!(v["additions"], 3);
        assert_eq!(v["deletions"], 1);
    }

    /// `TokenUsage`/`TokenUsageTotals` serialize with the camelCase counter names
    /// and `agent-{uuid}`/model keys the protocol specifies (§5.23); `lastScanAt`
    /// is `null` (not omitted) before the first scan and round-trips.
    #[test]
    fn token_usage_wire_shape() {
        let mut by_agent_id = BTreeMap::new();
        by_agent_id.insert(
            "agent-123".to_string(),
            TokenUsageTotals {
                input_tokens: 12000,
                output_tokens: 3400,
                cache_read_tokens: 8000,
                cache_creation_tokens: 1200,
                thought_tokens: 0,
                cost: None,
            },
        );
        let mut by_model = BTreeMap::new();
        by_model.insert("opus-4.8".to_string(), by_agent_id["agent-123"].clone());
        let usage = TokenUsage {
            by_agent_id,
            totals: by_model["opus-4.8"].clone(),
            by_model,
            last_scan_at: None,
        };
        let v = serde_json::to_value(&usage).unwrap();
        assert_eq!(v["byAgentId"]["agent-123"]["inputTokens"], 12000);
        assert_eq!(v["byAgentId"]["agent-123"]["cacheReadTokens"], 8000);
        assert_eq!(v["byAgentId"]["agent-123"]["cacheCreationTokens"], 1200);
        assert_eq!(v["byModel"]["opus-4.8"]["outputTokens"], 3400);
        assert_eq!(v["totals"]["inputTokens"], 12000);
        assert_eq!(v["lastScanAt"], serde_json::Value::Null);
        // Absent cost is OMITTED (not null) so existing clients see the
        // pre-cost shape byte-for-byte.
        assert!(v["totals"].get("cost").is_none());
        // Same for an unreported thought count — zero omits the key.
        assert!(v["totals"].get("thoughtTokens").is_none());
        let back: TokenUsage = serde_json::from_value(v).unwrap();
        assert_eq!(back, usage);
    }

    /// Reported reasoning tokens serialize as the additive camelCase
    /// `thoughtTokens` counter on every `TokenUsage` bucket and round-trip
    /// (§5.23).
    #[test]
    fn token_usage_thought_tokens_wire_shape() {
        let totals = TokenUsageTotals {
            input_tokens: 10,
            output_tokens: 5,
            thought_tokens: 42,
            ..TokenUsageTotals::default()
        };
        let mut by_agent_id = BTreeMap::new();
        by_agent_id.insert("agent-123".to_string(), totals.clone());
        let mut by_model = BTreeMap::new();
        by_model.insert("opus-4.8".to_string(), totals.clone());
        let usage = TokenUsage {
            by_agent_id,
            totals,
            by_model,
            last_scan_at: None,
        };
        let v = serde_json::to_value(&usage).unwrap();
        assert_eq!(v["totals"]["thoughtTokens"], 42);
        assert_eq!(v["byAgentId"]["agent-123"]["thoughtTokens"], 42);
        assert_eq!(v["byModel"]["opus-4.8"]["thoughtTokens"], 42);
        let back: TokenUsage = serde_json::from_value(v).unwrap();
        assert_eq!(back, usage);
    }

    /// A thought-only tally still counts as a token report, so it does not
    /// fall through to the per-message usage fallback (§5.23).
    #[test]
    fn thought_tokens_alone_count_as_a_report() {
        let thought_only = TokenUsageTotals {
            thought_tokens: 5,
            ..TokenUsageTotals::default()
        };
        assert!(!thought_only.counters_are_zero());
        assert!(token_usage_reported(None, Some(&thought_only)));
        assert!(TokenUsageTotals::default().counters_are_zero());
    }

    /// A reported cost serializes as camelCase `cost: { amount, currency }`
    /// on every `TokenUsage` bucket and round-trips (§5.23).
    #[test]
    fn token_usage_cost_wire_shape() {
        let totals = TokenUsageTotals {
            input_tokens: 10,
            output_tokens: 5,
            cost: Some(UsageCost {
                amount: 1.25,
                currency: "USD".to_string(),
            }),
            ..TokenUsageTotals::default()
        };
        let mut by_agent_id = BTreeMap::new();
        by_agent_id.insert("agent-123".to_string(), totals.clone());
        let mut by_model = BTreeMap::new();
        by_model.insert("opus-4.8".to_string(), totals.clone());
        let usage = TokenUsage {
            by_agent_id,
            totals,
            by_model,
            last_scan_at: None,
        };
        let v = serde_json::to_value(&usage).unwrap();
        assert_eq!(v["totals"]["cost"]["amount"], 1.25);
        assert_eq!(v["totals"]["cost"]["currency"], "USD");
        assert_eq!(v["byAgentId"]["agent-123"]["cost"]["amount"], 1.25);
        assert_eq!(v["byModel"]["opus-4.8"]["cost"]["currency"], "USD");
        let back: TokenUsage = serde_json::from_value(v).unwrap();
        assert_eq!(back, usage);
    }

    /// `UsageCost::merge`: matching currencies sum, a mismatch keeps the
    /// larger amount, and absent operands contribute nothing.
    #[test]
    fn usage_cost_merge_rules() {
        let usd = |amount: f64| UsageCost {
            amount,
            currency: "USD".to_string(),
        };
        let eur = |amount: f64| UsageCost {
            amount,
            currency: "EUR".to_string(),
        };
        assert_eq!(UsageCost::merge(None, None), None);
        assert_eq!(UsageCost::merge(Some(&usd(1.0)), None), Some(usd(1.0)));
        assert_eq!(UsageCost::merge(None, Some(&usd(2.0))), Some(usd(2.0)));
        assert_eq!(
            UsageCost::merge(Some(&usd(1.5)), Some(&usd(2.5))),
            Some(usd(4.0))
        );
        assert_eq!(
            UsageCost::merge(Some(&usd(1.0)), Some(&eur(9.0))),
            Some(eur(9.0)),
            "mismatched currencies keep the larger amount"
        );
        assert_eq!(
            UsageCost::merge(Some(&usd(2.0)), Some(&eur(2.0))),
            Some(usd(2.0)),
            "an equal-amount cross-currency tie keeps the lhs (banked baseline)"
        );
    }

    /// `SetupScript` serializes with the camelCase `updatedAt`/`generatedBy` keys
    /// and lowercase `projectType`/`generatedBy` enum values the protocol
    /// specifies (§5.25); optional fields round-trip and are omitted when absent.
    #[test]
    fn setup_script_wire_shape() {
        let s = SetupScript {
            script: "#!/usr/bin/env bash\ncargo fetch\n".to_string(),
            project_type: Some(ProjectType::Rust),
            updated_at: 1_750_000_000_000,
            generated_by: Some(SetupScriptGeneratedBy::Agent),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["script"], "#!/usr/bin/env bash\ncargo fetch\n");
        assert_eq!(v["projectType"], "rust");
        assert_eq!(v["updatedAt"], 1_750_000_000_000u64);
        assert_eq!(v["generatedBy"], "agent");
        let back: SetupScript = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);

        // Optional fields omitted (not null) when absent.
        let bare = SetupScript {
            script: String::new(),
            project_type: None,
            updated_at: 0,
            generated_by: None,
        };
        let v = serde_json::to_value(&bare).unwrap();
        assert_eq!(v.get("projectType"), None);
        assert_eq!(v.get("generatedBy"), None);
        assert_eq!(v["updatedAt"], 0);
        let back: SetupScript = serde_json::from_value(v).unwrap();
        assert_eq!(back, bare);
    }

    /// `RepoScript` serializes with camelCase keys (`autoStart` not `auto_start`),
    /// lowercase mode/category enum values, and omits optional fields when absent.
    #[test]
    fn repo_script_wire_shape() {
        use std::collections::BTreeMap;

        let s = RepoScript {
            name: "dev".to_string(),
            command: "pnpm dev".to_string(),
            mode: RepoScriptMode::Service,
            category: Some(RepoScriptCategory::Dev),
            cwd: Some("frontend".to_string()),
            env: {
                let mut m = BTreeMap::new();
                m.insert("PORT".to_string(), "3000".to_string());
                Some(m)
            },
            auto_start: Some(true),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["name"], "dev");
        assert_eq!(v["command"], "pnpm dev");
        assert_eq!(v["mode"], "service");
        assert_eq!(v["category"], "dev");
        assert_eq!(v["cwd"], "frontend");
        assert_eq!(v["env"]["PORT"], "3000");
        assert_eq!(v["autoStart"], true);
        let back: RepoScript = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);

        // Optional fields omitted (not null) when absent.
        let bare = RepoScript {
            name: "test".to_string(),
            command: "cargo test".to_string(),
            mode: RepoScriptMode::Command,
            category: None,
            cwd: None,
            env: None,
            auto_start: None,
        };
        let v = serde_json::to_value(&bare).unwrap();
        assert_eq!(v.get("category"), None);
        assert_eq!(v.get("cwd"), None);
        assert_eq!(v.get("env"), None);
        assert_eq!(v.get("autoStart"), None);
        let back: RepoScript = serde_json::from_value(v).unwrap();
        assert_eq!(back, bare);
    }

    /// `RepoConfig` serializes with camelCase keys (`branchPrefix`, `setupScript`, etc.),
    /// omits optional fields when absent, and preserves unknown keys via `extra`.
    #[test]
    fn repo_config_wire_shape() {
        use std::collections::BTreeMap;

        let cfg = RepoConfig {
            branch_prefix: Some("feature/".to_string()),
            setup_script: Some("npm install".to_string()),
            instructions: Some("Use TypeScript strict mode".to_string()),
            run_script: Some("npm run dev".to_string()),
            archive_script: Some("docker compose down".to_string()),
            scripts: Some(vec![RepoScript {
                name: "build".to_string(),
                command: "npm run build".to_string(),
                mode: RepoScriptMode::Command,
                category: Some(RepoScriptCategory::Build),
                cwd: None,
                env: None,
                auto_start: None,
            }]),
            cow_clone_exclude: Some(vec![
                "node_modules".to_string(),
                "packages/big/cache".to_string(),
            ]),
            extra: {
                let mut m = BTreeMap::new();
                m.insert("customKey".to_string(), serde_json::json!("customValue"));
                m
            },
        };
        let v = serde_json::to_value(&cfg).unwrap();
        assert_eq!(v["branchPrefix"], "feature/");
        assert_eq!(v["setupScript"], "npm install");
        assert_eq!(v["instructions"], "Use TypeScript strict mode");
        assert_eq!(v["runScript"], "npm run dev");
        assert_eq!(v["archiveScript"], "docker compose down");
        assert_eq!(v["scripts"][0]["name"], "build");
        assert_eq!(v["scripts"][0]["mode"], "command");
        assert_eq!(
            v["cowCloneExclude"],
            serde_json::json!(["node_modules", "packages/big/cache"])
        );
        assert_eq!(v["customKey"], "customValue");
        let back: RepoConfig = serde_json::from_value(v).unwrap();
        assert_eq!(back, cfg);

        // Optional fields omitted (not null) when absent; extra preserves unknown keys.
        let bare = RepoConfig::default();
        let v = serde_json::to_value(&bare).unwrap();
        assert_eq!(v.get("branchPrefix"), None);
        assert_eq!(v.get("setupScript"), None);
        assert_eq!(v.get("instructions"), None);
        assert_eq!(v.get("runScript"), None);
        assert_eq!(v.get("archiveScript"), None);
        assert_eq!(v.get("scripts"), None);
        assert_eq!(v.get("cowCloneExclude"), None);
        let back: RepoConfig = serde_json::from_value(v).unwrap();
        assert_eq!(back, bare);

        // Unknown keys in JSON are preserved in `extra` on round-trip.
        let json = serde_json::json!({
            "branchPrefix": "bugfix/",
            "unknownField": "some-value",
            "anotherUnknown": 42
        });
        let parsed: RepoConfig = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.branch_prefix.as_deref(), Some("bugfix/"));
        assert_eq!(parsed.extra.get("unknownField").unwrap(), "some-value");
        assert_eq!(parsed.extra.get("anotherUnknown").unwrap(), 42);
        let v = serde_json::to_value(&parsed).unwrap();
        assert_eq!(v["branchPrefix"], "bugfix/");
        assert_eq!(v["unknownField"], "some-value");
        assert_eq!(v["anotherUnknown"], 42);
    }

    /// `WorkspaceAgentInfo` omits the optional `specialist`/`lastActivity`/
    /// `parentAgentId` keys when absent (not `null`), while the non-optional
    /// flags stay present.
    #[test]
    fn workspace_agent_info_optionals_absent() {
        let agent = WorkspaceAgentInfo {
            id: AgentId::from("agent-2"),
            name: "Plain".to_string(),
            status: AgentStatus::Pending,
            specialist: None,
            last_activity: None,
            is_streaming: false,
            is_responding: false,
            parent_agent_id: None,
        };
        let v = serde_json::to_value(&agent).unwrap();
        assert!(v.get("specialist").is_none());
        assert!(v.get("lastActivity").is_none());
        assert!(v.get("parentAgentId").is_none());
        assert_eq!(v["status"], "pending");
        assert_eq!(v["isStreaming"], false);
        assert_eq!(v["isResponding"], false);
    }

    /// `AgentLite` carries the nested `metadata` object (`isBackground`/
    /// `specialist`/`createdByAgentId`/`taskNoteId`) and the activity flags the
    /// iOS `AgentSession.parseAgent` reads.
    #[test]
    fn agent_lite_metadata_and_activity_wire_shape() {
        let ts = "t1".to_string();
        let session = AgentSession {
            id: AgentId::from("agent-1"),
            workspace_id: WorkspaceId::from("ws-1"),
            parent_agent_id: Some(AgentId::from("agent-parent")),
            backend_session_id: None,
            acp_session_id: None,
            name: "Builder".to_string(),
            name_explicitly_set: true,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".to_string()),
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: true,
            metadata: Some(json!({
                DISMISSED_QUESTIONS_MESSAGE_ID_KEY: "msg-q1",
                LAST_SEEN_MESSAGE_ID_KEY: "msg-seen",
                IS_INITIAL_AGENT_KEY: true,
            })),
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            created_at: "t0".to_string(),
            updated_at: ts.clone(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        let lite = AgentLite::from_session(
            session,
            0,
            None,
            Some("hi".to_string()),
            None,
            Some("user".to_string()),
            Some("msg-last".to_string()),
        );
        let v = serde_json::to_value(&lite).unwrap();
        assert_eq!(v["metadata"]["specialist"], "implementor");
        // The question-dismissal marker is lifted out of the free-form session
        // metadata into the AgentLite metadata projection.
        assert_eq!(v["metadata"]["dismissedQuestionsMessageId"], "msg-q1");
        // The per-conversation seen marker is lifted the same way.
        assert_eq!(v["metadata"]["lastSeenMessageId"], "msg-seen");
        // The daemon-stamped initial-agent flag is lifted the same way.
        assert_eq!(v["metadata"]["isInitialAgent"], true);
        // The persisted session value is served, not a hard-coded `false`
        // (G-A1/P3-1.2c).
        assert_eq!(v["metadata"]["isBackground"], true);
        assert_eq!(v["metadata"]["createdByAgentId"], "agent-parent");
        assert_eq!(v["isStreaming"], false);
        assert_eq!(v["isProcessing"], false);
        assert_eq!(v["isResponding"], false);
        assert_eq!(v["isWaitingOnTool"], false);
        assert_eq!(v["isWaitingForOtherAgents"], false);
        // `waitingForAgentIds` is always emitted (never null/omitted), defaulting
        // to `[]` when no completion watches are pending (PROTOCOL §5.5/§7.1).
        assert_eq!(v["waitingForAgentIds"], json!([]));
        assert_eq!(v["lastUserMessage"], "hi");
        assert_eq!(v["lastMessageRole"], "user");
        assert_eq!(v["lastMessageId"], "msg-last");
        assert_eq!(v["lastActivity"], "t1");
    }

    /// `metadata.isInitialAgent` is presence-detected: omitted from the
    /// `AgentLite` metadata projection when the raw session metadata lacks the
    /// key, and equally omitted when the key is present but not the JSON
    /// boolean `true` (`false` or a non-boolean value) — never `false`, never
    /// `null` (PROTOCOL §5.5).
    #[test]
    fn agent_lite_is_initial_agent_omitted_unless_true() {
        let session = |metadata: Option<serde_json::Value>| AgentSession {
            id: AgentId::from("agent-1"),
            workspace_id: WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Builder".to_string(),
            name_explicitly_set: true,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            created_at: "t0".to_string(),
            updated_at: "t1".to_string(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        let project = |metadata: Option<serde_json::Value>| {
            let lite = AgentLite::from_session(session(metadata), 0, None, None, None, None, None);
            serde_json::to_value(&lite).unwrap()
        };

        // No metadata at all, key absent, `false`, and non-boolean values all
        // omit the key entirely.
        for metadata in [
            None,
            Some(json!({})),
            Some(json!({ IS_INITIAL_AGENT_KEY: false })),
            Some(json!({ IS_INITIAL_AGENT_KEY: "true" })),
        ] {
            let v = project(metadata.clone());
            assert!(
                v["metadata"].get("isInitialAgent").is_none(),
                "expected isInitialAgent omitted for metadata {metadata:?}: {v}"
            );
        }

        // Only the JSON boolean `true` surfaces the flag.
        let v = project(Some(json!({ IS_INITIAL_AGENT_KEY: true })));
        assert_eq!(v["metadata"]["isInitialAgent"], true);
    }

    /// `AgentSession` serializes to the camelCase `agent-session.ts` wire shape:
    /// `backendSessionId`/`acpSessionId`/`nameExplicitlySet`/`isActive`/
    /// `systemPrompt`, with absent optionals omitted and a nested message log.
    #[test]
    fn agent_session_camel_case_parity() {
        let session = AgentSession {
            id: AgentId::from("agent-1"),
            workspace_id: WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: Some(AgentId::from("backend-9")),
            acp_session_id: Some("acp-uuid".to_string()),
            name: "Builder".to_string(),
            name_explicitly_set: true,
            model: Some("opus".to_string()),
            reasoning_effort: None,
            effort_levels: Some(vec!["low".to_string(), "high".to_string()]),
            provider: Some("auggie".to_string()),
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![AgentMessage {
                id: "msg-1".to_string(),
                agent_id: AgentId::from("agent-1"),
                seq: 0,
                role: "user".to_string(),
                content: json!([{ "type": "text", "text": "hi" }]),
                metadata: None,
                app_message_id: None,
                created_at: "t0".to_string(),
            }],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            created_at: "t0".to_string(),
            updated_at: "t1".to_string(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            json!({
                "id": "agent-1",
                "workspaceId": "ws-1",
                "backendSessionId": "backend-9",
                "acpSessionId": "acp-uuid",
                "name": "Builder",
                "nameExplicitlySet": true,
                "model": "opus",
                "effortLevels": ["low", "high"],
                "provider": "auggie",
                "status": "active",
                "isActive": true,
                "messages": [{
                    "id": "msg-1",
                    "agentId": "agent-1",
                    "seq": 0,
                    "role": "user",
                    "contentBlocks": [{ "type": "text", "text": "hi" }],
                    "timestamp": "t0"
                }],
                "createdAt": "t0",
                "updatedAt": "t1"
            })
        );
        let back: AgentSession =
            serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
        assert_eq!(back, session);
    }

    /// `AgentMessage` serializes its block array and timestamp under the TS wire
    /// names `contentBlocks`/`timestamp` (`agent-message.ts`) — never `content`/
    /// `createdAt` — so the iOS conversation view renders populated bubbles.
    #[test]
    fn agent_message_content_blocks_timestamp_wire_parity() {
        let message = AgentMessage {
            id: "msg-1".to_string(),
            agent_id: AgentId::from("agent-1"),
            seq: 0,
            role: "user".to_string(),
            content: json!([{ "type": "text", "text": "hi" }]),
            metadata: None,
            app_message_id: None,
            created_at: "t0".to_string(),
        };
        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "msg-1",
                "agentId": "agent-1",
                "seq": 0,
                "role": "user",
                "contentBlocks": [{ "type": "text", "text": "hi" }],
                "timestamp": "t0"
            })
        );
        let obj = value.as_object().unwrap();
        assert!(obj.contains_key("contentBlocks"));
        assert!(obj.contains_key("timestamp"));
        assert!(!obj.contains_key("content"));
        assert!(!obj.contains_key("createdAt"));
        // Absent client id stays off the wire (backward compatible).
        assert!(!obj.contains_key("appMessageId"));
        let back: AgentMessage = serde_json::from_value(value).unwrap();
        assert_eq!(back, message);
    }

    /// `appMessageId` (PROTOCOL §5.5): the client-minted `userAppMessageId`
    /// serializes under the TS wire name `appMessageId` when present, and
    /// [`lift_app_message_id`] extracts it from a persisted row `metadata`
    /// payload (object with a non-empty string) only.
    #[test]
    fn agent_message_app_message_id_wire_parity_and_lift() {
        let message = AgentMessage {
            id: "msg-1".to_string(),
            agent_id: AgentId::from("agent-1"),
            seq: 0,
            role: "user".to_string(),
            content: json!([{ "type": "text", "text": "hi" }]),
            metadata: Some(json!({ "userAppMessageId": "app-msg-1" })),
            app_message_id: Some("app-msg-1".to_string()),
            created_at: "t0".to_string(),
        };
        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(value["appMessageId"], json!("app-msg-1"));
        let back: AgentMessage = serde_json::from_value(value).unwrap();
        assert_eq!(back, message);

        assert_eq!(
            lift_app_message_id(Some(&json!({ "userAppMessageId": "app-msg-1" }))),
            Some("app-msg-1".to_string())
        );
        assert_eq!(lift_app_message_id(None), None);
        assert_eq!(lift_app_message_id(Some(&json!({ "other": 1 }))), None);
        assert_eq!(
            lift_app_message_id(Some(&json!({ "userAppMessageId": "" }))),
            None
        );
        assert_eq!(
            lift_app_message_id(Some(&json!({ "userAppMessageId": "   " }))),
            None
        );
        assert_eq!(
            lift_app_message_id(Some(&json!({ "userAppMessageId": " padded " }))),
            Some("padded".to_string())
        );
        assert_eq!(
            lift_app_message_id(Some(&json!({ "userAppMessageId": 42 }))),
            None
        );
    }

    #[test]
    fn workspace_create_initial_agent_parses_full_payload() {
        // PROTOCOL §5.1: `initialAgent` mirrors the agent.create payload so the
        // daemon can own initial-agent orchestration inside workspace.create.
        let input: WorkspaceCreate = serde_json::from_value(json!({
            "title": "WS",
            "initialAgent": {
                "prompt": "fix the auth flow",
                "name": "Auth fixer",
                "model": "opus",
                "specialist": "implementor",
                "provider": "auggie",
                "behaviorPrompt": "be terse",
                "agentType": "task-loop",
                "contextReferences": [{ "path": "src/auth.ts" }],
                "imageBlocks": [{ "type": "image", "data": "abc" }],
                "metadata": { "initialMessage": "fix the auth flow" }
            }
        }))
        .unwrap();
        let agent = input.initial_agent.expect("initialAgent");
        assert_eq!(agent.prompt.as_deref(), Some("fix the auth flow"));
        assert_eq!(agent.name.as_deref(), Some("Auth fixer"));
        assert_eq!(agent.model.as_deref(), Some("opus"));
        assert_eq!(agent.specialist.as_deref(), Some("implementor"));
        assert_eq!(agent.provider.as_deref(), Some("auggie"));
        assert_eq!(agent.behavior_prompt.as_deref(), Some("be terse"));
        assert_eq!(agent.agent_type.as_deref(), Some("task-loop"));
        assert_eq!(
            agent.context_references,
            Some(json!([{ "path": "src/auth.ts" }]))
        );
        assert_eq!(
            agent.image_blocks,
            Some(json!([{ "type": "image", "data": "abc" }]))
        );
        assert_eq!(
            agent.metadata,
            Some(json!({ "initialMessage": "fix the auth flow" }))
        );

        // Absent sub-fields stay None (all optional, `default` container).
        let bare: WorkspaceCreate =
            serde_json::from_value(json!({ "initialAgent": { "prompt": "p" } })).unwrap();
        let bare = bare.initial_agent.expect("initialAgent");
        assert_eq!(bare.prompt.as_deref(), Some("p"));
        assert!(bare.specialist.is_none());
        assert!(bare.metadata.is_none());
    }

    #[test]
    fn workspace_create_skip_isolation_accepts_both_wire_names() {
        // PROTOCOL §5.1: `skipIsolation` is canonical; `skipWorktree` is the
        // deprecated pre-CoW alias. Either set ⇒ direct mode.
        let new_name: WorkspaceCreate =
            serde_json::from_value(json!({ "skipIsolation": true })).unwrap();
        assert_eq!(new_name.skip_isolation, Some(true));

        let old_name: WorkspaceCreate =
            serde_json::from_value(json!({ "skipWorktree": true })).unwrap();
        assert_eq!(old_name.skip_isolation, Some(true));

        let absent: WorkspaceCreate = serde_json::from_value(json!({})).unwrap();
        assert_eq!(absent.skip_isolation, None);
    }

    #[test]
    fn workspace_update_skip_isolation_accepts_new_wire_name() {
        // PROTOCOL §5.1: `skipIsolation` is the canonical `workspace.update`
        // param, mirroring the create-side rename.
        let update: WorkspaceUpdate =
            serde_json::from_value(json!({ "skipIsolation": true })).unwrap();
        assert_eq!(update.skip_isolation, Some(true));

        // Serialization (the `workspace:updated { changes }` delta) emits the
        // canonical name and omits the field when absent.
        let v = serde_json::to_value(&update).unwrap();
        assert_eq!(v["skipIsolation"], json!(true));
        assert!(v.get("skipWorktree").is_none());

        let absent: WorkspaceUpdate = serde_json::from_value(json!({})).unwrap();
        assert_eq!(absent.skip_isolation, None);
        let v = serde_json::to_value(&absent).unwrap();
        assert!(v.get("skipIsolation").is_none());
    }

    #[test]
    fn workspace_update_skip_isolation_accepts_deprecated_alias() {
        // The deprecated pre-CoW `skipWorktree` alias still deserializes into
        // the canonical field (either set ⇒ same skip behavior).
        let update: WorkspaceUpdate =
            serde_json::from_value(json!({ "skipWorktree": false })).unwrap();
        assert_eq!(update.skip_isolation, Some(false));
    }

    #[test]
    fn note_create_result_decodes_legacy_bare_note_idempotency_record() {
        // `note.create` idempotency records persisted before the conversion
        // fields existed contain a serialized bare `Note` — a replayed key
        // must decode it as a zeroed conversion outcome, not fail.
        let note_json = json!({
            "id": "n-1",
            "workspaceId": "ws-1",
            "title": "T",
            "content": "c",
            "contentType": "markdown",
            "tags": [],
            "isPinned": false,
            "isArchived": false,
            "isDefault": false,
            "parentId": null,
            "visibility": "workspace",
            "createdAt": "2026-01-01T00:00:00Z",
            "rev": 0,
            "updatedAt": "2026-01-01T00:00:00Z"
        });
        let legacy: NoteCreateResult = serde_json::from_value(note_json.clone()).unwrap();
        assert_eq!(legacy.note.id.0, "n-1");
        assert_eq!(legacy.converted_count, 0);
        assert!(legacy.created_task_note_ids.is_empty());
        assert!(legacy.created_tasks.is_empty());
        assert!(legacy.warnings.is_empty());

        // The current shape round-trips (serialize → deserialize → equal).
        let current = NoteCreateResult {
            note: legacy.note.clone(),
            converted_count: 2,
            created_task_note_ids: vec!["t-1".into(), "t-2".into()],
            created_tasks: vec![CreatedTaskEntry {
                key: Some("k".into()),
                title: "Child".into(),
                note_id: "t-1".into(),
            }],
            warnings: vec!["w".into()],
        };
        let back: NoteCreateResult =
            serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
        assert_eq!(back, current);

        // An enveloped record missing the conversion fields (older-daemon
        // response shape) also decodes with defaults.
        let sparse: NoteCreateResult =
            serde_json::from_value(json!({ "note": note_json })).unwrap();
        assert_eq!(sparse.note.id.0, "n-1");
        assert_eq!(sparse.converted_count, 0);
        assert!(sparse.warnings.is_empty());
    }
}

//! The data the remote-launch feature port (`intent_services::RemoteLaunchPort`)
//! exchanges: where a remote agent runs, as recorded on head and projected
//! to clients.

use serde::{Deserialize, Serialize};

use crate::placement::{AgentPlacement, CapacityRefusal};
use crate::{AgentId, WorkspaceId};

pub mod workspace_api;

/// Place the just-created agent `agent_id` on the node its admission
/// resolved (`view`): record it on the node's lease and point its `base`
/// ref at `view.base_commit`. Its provider process starts on its first turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLaunchRequest {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub view: RemotePlacementView,
}

/// One agent head runs on an execution node, under `lease_id`. Head claims
/// these agents when it binds a link; the node stops agents it runs that
/// head does not claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteAgentRecord {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub node_id: String,
    pub lease_id: String,
    pub created_at: String,
}

/// Why a remote agent ends for good on its machine: its checkout is
/// removed there after a final WIP push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FinalStopReason {
    AgentDeleted,
    AgentRetired,
    WorkspaceRetired,
    WorkspaceDeleted,
    MachineRemoved,
}

impl FinalStopReason {
    /// The stored and documented spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AgentDeleted => "agent-deleted",
            Self::AgentRetired => "agent-retired",
            Self::WorkspaceRetired => "workspace-retired",
            Self::WorkspaceDeleted => "workspace-deleted",
            Self::MachineRemoved => "machine-removed",
        }
    }

    /// Parse the stored spelling.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        [
            Self::AgentDeleted,
            Self::AgentRetired,
            Self::WorkspaceRetired,
            Self::WorkspaceDeleted,
            Self::MachineRemoved,
        ]
        .into_iter()
        .find(|reason| reason.as_str() == value)
    }

    /// Whether the agent's session can come back (a retired agent or
    /// workspace can be restored), so its placement record is kept after
    /// the final stop is acknowledged.
    #[must_use]
    pub fn keeps_placement(self) -> bool {
        matches!(self, Self::AgentRetired | Self::WorkspaceRetired)
    }
}

/// A final stop head has asked for and the machine has not acknowledged
/// yet: kept durably, resent at every bind, and while it exists the
/// machine's final WIP push for the agent is accepted into `repo_path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFinalStopRecord {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub machine_id: String,
    pub lease_id: String,
    /// The stop's id, reused by every resend.
    pub stop_id: String,
    pub reason: FinalStopReason,
    /// The workspace's repository (refs are shared with its worktrees), so
    /// the push lands even after the worktree is removed.
    pub repo_path: String,
    pub requested_at: String,
    /// The agent's submodule pins with their repositories, so its last
    /// module pushes land too.
    pub submodules: Vec<FinalStopSubmodule>,
}

/// A pinned submodule repository a final push may land in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FinalStopSubmodule {
    pub path: String,
    pub module_key: String,
    /// The submodule repository's git dir when the record was written.
    pub git_dir: String,
}

/// How long head keeps an unacknowledged final stop before it drops the
/// record anyway (the machine's next bind sweep removes the checkout).
pub const FINAL_STOP_BOUND: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Session `metadata` key recording where a remote agent runs: the agent
/// projection reads it, and an agent carrying it never runs locally.
/// Daemon-owned: it is written when the agent is created with a remote
/// placement and by the remote-launch writers only; whole-document metadata
/// writes and create-time caller metadata can neither set nor clear it. The
/// lease an agent runs under is the remote-agent record's.
pub const REMOTE_PLACEMENT_METADATA_KEY: &str = "remotePlacement";

/// `effectiveIsolation` of a remote agent: `pending` until the node has its
/// isolated checkout ready, then `isolated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectiveIsolation {
    Pending,
    Isolated,
}

/// `placementError`: why a remote agent could not be started on its node.
/// `code` is the node's error code (for example `node-capacity`), `detail`
/// its message. A `node-capacity` refusal also carries the limit it hit
/// (`data`) and `retryable: true`; both are absent otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementError {
    pub code: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<PlacementErrorData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

/// `placementError.data`: a machine's capacity refusal, or a cloud machine
/// error's data (`cloud-quota`, `cloud-machine-unavailable`) without its
/// `code`. Never a key or token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PlacementErrorData {
    Capacity(CapacityRefusal),
    Cloud(serde_json::Map<String, serde_json::Value>),
}

/// Longest `placementError.detail` kept, in UTF-8 bytes: the field rides
/// every `agent.list` row, and node errors can quote command output.
pub const PLACEMENT_ERROR_DETAIL_MAX_BYTES: usize = 512;

/// Longest `placementError.code` kept, in bytes.
const PLACEMENT_ERROR_CODE_MAX_BYTES: usize = 64;

fn cap(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

impl PlacementError {
    /// A placement error with `code` and `detail` capped to their list-row
    /// budgets.
    #[must_use]
    pub fn new(code: &str, detail: &str) -> Self {
        Self {
            code: cap(code, PLACEMENT_ERROR_CODE_MAX_BYTES),
            detail: cap(detail, PLACEMENT_ERROR_DETAIL_MAX_BYTES),
            data: None,
            retryable: None,
        }
    }

    /// A machine's capacity refusal: `node-capacity` with the limit it hit,
    /// retryable.
    #[must_use]
    pub fn capacity(detail: &str, refusal: CapacityRefusal) -> Self {
        Self {
            data: Some(PlacementErrorData::Capacity(refusal)),
            retryable: Some(true),
            ..Self::new("node-capacity", detail)
        }
    }

    /// A cloud machine start failure: `data` is the error's data; its `code`
    /// becomes the placement error's code and `retryable` its own field.
    #[must_use]
    pub fn cloud(detail: &str, data: &serde_json::Value) -> Self {
        let mut data = data.as_object().cloned().unwrap_or_default();
        let code = data
            .remove("code")
            .and_then(|code| code.as_str().map(str::to_owned))
            .unwrap_or_else(|| "remote-launch-failed".to_owned());
        let retryable = data.remove("retryable").and_then(|r| r.as_bool());
        Self {
            data: (!data.is_empty()).then_some(PlacementErrorData::Cloud(data)),
            retryable,
            ..Self::new(&code, detail)
        }
    }

    /// Split a node error (`"<code>: <detail>"`) into its code and detail;
    /// an error without a code is `node-agent-start-failed`.
    #[must_use]
    pub fn from_node_error(error: &str) -> Self {
        match error.split_once(": ") {
            Some((code, detail))
                if !code.is_empty()
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-') =>
            {
                Self::new(code, detail)
            }
            _ => Self::new("node-agent-start-failed", error),
        }
    }
}

/// What a remote launch needs from the machine it is placed on, for
/// placement across exposed machines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteLaunchNeeds {
    /// The base provider id the machines report (`claude-code`, `codex`),
    /// without a profile suffix.
    pub provider: String,
    /// The requested model; `None` for the provider's default.
    pub model: Option<String>,
    /// The launch sends a selected profile's environment, so the machine's
    /// own sign-in does not apply.
    pub with_profile: bool,
    /// The launch's profile environment sets at least one variable (a
    /// `kind: "set"` entry). A cloud machine has no sign-in of its own and
    /// runs only such launches.
    pub sets_profile_env: bool,
    /// The provider reference as given (`claude-code`, `codex@work`), for
    /// `cloud-profile-required` data.
    pub provider_ref: String,
    /// The placement is the parent's, inherited by a sub-agent created
    /// with none: from an agent on a cloud machine it means that sandbox.
    /// An explicit `target: "e2b"` always means a new sandbox.
    pub inherited: bool,
}

/// The [`REMOTE_PLACEMENT_METADATA_KEY`] value: the projection fields of an
/// agent placed on a node, computed when they change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemotePlacementView {
    pub node_id: String,
    pub lease_id: String,
    pub node_name: String,
    /// The resolved placement: the request with the node's platform and id.
    pub placement: AgentPlacement,
    pub effective_isolation: EffectiveIsolation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_error: Option<PlacementError>,
    /// The commit the agent starts from (its `base` ref).
    pub base_commit: String,
    /// The submodules populated in the workspace's checkout when the agent
    /// was placed, each with its module `base` ref. They name the only
    /// submodule repositories its machine may reach. `None` for an agent
    /// placed before pins were captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submodules: Option<Vec<crate::node_link::SubmodulePin>>,
    /// The setup of the agent's checkout on its machine, from its first
    /// nodeProtocol 10 launch; stored without output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<RemoteSetupStatus>,
    /// `workspace.create`'s explicit setup script, kept for its initial
    /// agent's later launches; never projected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_script: Option<String>,
    /// The skills its machine last reported for it (nodeProtocol 12), for
    /// `skill.list` with its `agentId`; never projected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills: Option<Vec<crate::node_link::MachineSkill>>,
    /// The agent whose checkout this sub-agent works in on their machine
    /// (nodeProtocol 13), sent as `checkoutOf` at every launch; never
    /// projected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_checkout_of: Option<crate::AgentId>,
    /// The parent whose work this sub-agent, on another machine than the
    /// parent's, starts from (nodeProtocol 13); never projected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_work: Option<ParentWork>,
    /// While the agent's cloud machine starts (`AgentLite.cloudStart`).
    /// `node_id` and `lease_id` stay empty until its link binds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_start: Option<crate::machines::CloudStart>,
}

/// Where a remote agent's sub-agent on another machine starts from: the
/// owner of the parent's checkout, and how its work was copied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParentWork {
    pub agent_id: crate::AgentId,
    /// `None` until copied, and when the parent had no saved work: the
    /// sub-agent then starts from the workspace's last commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copied: Option<ParentWorkCopy>,
}

/// How a sub-agent got its parent's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ParentWorkCopy {
    /// The parent's machine pushed it when the sub-agent was created.
    Pushed,
    /// The parent's machine could not push now: its last push.
    LastPush,
}

/// Session `metadata` key of a backend sub-agent of a remote agent: where
/// the parent's work was saved when the sub-agent was created
/// ([`BackendParentWork`]). Written only by the daemon.
pub const BACKEND_PARENT_WORK_METADATA_KEY: &str = "backendParentWork";

/// The parent's work a backend (`target: "local"`) sub-agent of a remote
/// agent starts from: the commits of the owner of the parent's checkout
/// and its uncommitted changes, as saved in its refs. A sub-agent with its
/// own copy-on-write checkout starts from them (`seeded`); one in the
/// workspace's shared checkout is told where they are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendParentWork {
    pub agent_id: crate::AgentId,
    /// The parent's `head` commit; `None` when it has saved no work yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// The parent's `wip` commit, when it differs from `head`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wip: Option<String>,
    /// The same for each pinned submodule the parent saved work in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub submodules: Vec<BackendParentModule>,
    pub copied: ParentWorkCopy,
    /// The sub-agent's own checkout was reset to this work.
    #[serde(default)]
    pub seeded: bool,
}

/// A submodule's part of [`BackendParentWork`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendParentModule {
    /// From the superproject root.
    pub path: String,
    pub head: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wip: Option<String>,
}

impl BackendParentWork {
    /// The parent's ref `name` (`head` or `wip`) in workspace `workspace_id`.
    #[must_use]
    pub fn parent_ref(&self, workspace_id: &str, name: &str) -> String {
        format!(
            "refs/intent/ws/{workspace_id}/agents/{}/{name}",
            self.agent_id
        )
    }

    fn saved(&self) -> &'static str {
        match self.copied {
            ParentWorkCopy::Pushed => "saved when this agent was created",
            ParentWorkCopy::LastPush => {
                "as its machine last pushed it (it could not push again now)"
            }
        }
    }

    /// Where the parent's work is, for the sub-agent's instructions and the
    /// create result's `notice`.
    #[must_use]
    pub fn notice(&self, workspace_id: &str) -> String {
        let short = |commit: &str| commit.get(..12).unwrap_or(commit).to_owned();
        let Some(head) = self.head.as_deref() else {
            return format!(
                "This sub-agent runs on the backend, and its parent has saved no work yet: its commits will be at `{}` and its uncommitted changes at `{}` once its machine pushes them (at the end of its turns).",
                self.parent_ref(workspace_id, "head"),
                self.parent_ref(workspace_id, "wip")
            );
        };
        if self.seeded {
            return format!(
                "This sub-agent runs on the backend in its own checkout, which starts from its parent's work: commit {} with the parent's uncommitted changes, {}. Later work of either agent is not shared.",
                short(head),
                self.saved()
            );
        }
        let wip = self.wip.as_deref().map_or_else(
            || " The parent had no uncommitted changes.".to_owned(),
            |wip| {
                format!(
                    " Its uncommitted changes are commit {} at `{}` (its tree is the parent's working tree).",
                    short(wip),
                    self.parent_ref(workspace_id, "wip")
                )
            },
        );
        format!(
            "This sub-agent runs on the backend in the workspace's checkout, which does not have its parent's work. The parent's commits are at `{}` (commit {}), {}.{wip} Read them with git (for example `git show <ref>:<path>` or `git diff HEAD <ref>`); do not reset the shared checkout to them.",
            self.parent_ref(workspace_id, "head"),
            short(head),
            self.saved()
        )
    }

    /// [`Self::notice`] for a sub-agent whose own copy-on-write checkout is
    /// still being made: what it starts from once ready, and the fallback.
    #[must_use]
    pub fn pending_notice(&self, workspace_id: &str) -> String {
        if self.head.is_none() {
            return self.notice(workspace_id);
        }
        format!(
            "This sub-agent runs on the backend in its own checkout, which is being made and then starts from its parent's work, {}. If that copy cannot be made or reset, it runs in the workspace's checkout and its instructions name the parent's refs: `{}` and `{}`.",
            self.saved(),
            self.parent_ref(workspace_id, "head"),
            self.parent_ref(workspace_id, "wip")
        )
    }

    /// Read the value from a session's `metadata`, if it carries one.
    #[must_use]
    pub fn from_metadata(metadata: Option<&serde_json::Value>) -> Option<Self> {
        let value = metadata?.get(BACKEND_PARENT_WORK_METADATA_KEY)?;
        serde_json::from_value(value.clone()).ok()
    }
}

/// Where a remote agent's checkout setup is (`methods/nodes.md` §
/// Prepared checkout).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoteSetupState {
    /// The machine is preparing the checkout.
    Pending,
    /// The setup script runs.
    Running,
    Completed,
    Failed,
    /// The workspace has no setup script.
    Skipped,
}

/// A remote agent's checkout setup on its machine. `output` and
/// `outputTruncated` are on detail reads only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteSetupStatus {
    pub state: RemoteSetupState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_truncated: Option<bool>,
}

/// The longest `setup.error` a placement keeps, in bytes.
pub const SETUP_ERROR_CAP_BYTES: usize = 160;

impl RemoteSetupStatus {
    /// `error` cut to [`SETUP_ERROR_CAP_BYTES`] on a character boundary.
    #[must_use]
    /// Control characters become spaces, so the cap holds after JSON
    /// escaping too.
    pub fn capped_error(error: &str) -> String {
        let error: String = error
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        let mut end = error.len().min(SETUP_ERROR_CAP_BYTES);
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        error[..end].to_owned()
    }

    #[must_use]
    pub fn pending() -> Self {
        Self {
            state: RemoteSetupState::Pending,
            exit_code: None,
            error: None,
            started_at: None,
            finished_at: None,
            output: None,
            output_truncated: None,
        }
    }
}

impl RemotePlacementView {
    /// The agent's submodule pins; empty when it has none.
    #[must_use]
    pub fn pins(&self) -> &[crate::node_link::SubmodulePin] {
        self.submodules.as_deref().unwrap_or_default()
    }

    /// Whether the agent was placed on a cloud machine (`target: "e2b"`).
    #[must_use]
    pub fn is_cloud(&self) -> bool {
        self.placement.is_e2b()
    }

    /// Whether the agent runs on no machine yet: a cloud agent whose
    /// sandbox starts or is gone.
    #[must_use]
    pub fn is_unlinked(&self) -> bool {
        self.node_id.is_empty()
    }

    /// Read the view from a session's `metadata`, if it carries one.
    #[must_use]
    pub fn from_metadata(metadata: Option<&serde_json::Value>) -> Option<Self> {
        let value = metadata?.get(REMOTE_PLACEMENT_METADATA_KEY)?;
        serde_json::from_value(value.clone()).ok()
    }
}

/// A placed agent's submodule pin as the agent projection shows it
/// (detail reads only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteSubmodulePin {
    /// From the superproject root.
    pub path: String,
    pub commit: String,
}

/// What a remote agent starts from, captured together when it is placed:
/// the checkout's `HEAD` commit and its populated submodules' commits.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteCheckoutBase {
    pub commit: String,
    pub submodules: Vec<crate::node_link::SubmodulePin>,
}

/// The message a remote launch reports to its caller: what the agent starts
/// from. Uncommitted changes in the workspace (and, with `submodules`,
/// in its submodules) stay on head.
#[must_use]
pub fn launch_notice(node_name: &str, base_commit: &str, submodules: bool) -> String {
    launch_notice_with_setup(node_name, base_commit, submodules, false)
}

/// [`launch_notice`], saying also that the workspace's setup script runs on
/// the machine before the agent starts when `setup`.
#[must_use]
pub fn launch_notice_with_setup(
    node_name: &str,
    base_commit: &str,
    submodules: bool,
    setup: bool,
) -> String {
    let short = base_commit.get(..12).unwrap_or(base_commit);
    let mut notice = format!(
        "The agent runs on execution node \"{node_name}\" and starts from commit {short} (the workspace's last commit). Uncommitted changes in the workspace are not carried to the remote agent; commit them first if it needs them."
    );
    if !crate::node_link::offers_prepared_checkout() {
        return notice;
    }
    if submodules {
        notice.push_str(" Its submodules start from the commits checked out in the workspace; uncommitted changes inside them are not carried either.");
    }
    if setup {
        notice.push_str(
            " The workspace's setup script runs on that machine before the agent starts.",
        );
    }
    notice
}

/// The launch notice of a remote agent's sub-agent that starts from its
/// parent's work (nodeProtocol 13), or `None` for any other agent.
#[must_use]
pub fn sub_agent_notice(view: &RemotePlacementView) -> Option<String> {
    let node_name = &view.node_name;
    if view.shared_checkout_of.is_some() {
        return Some(format!(
            "The agent runs on execution node \"{node_name}\" in its parent's checkout there: it sees the parent's uncommitted changes as they happen, and its work reaches the backend in the parent's refs."
        ));
    }
    let short = view.base_commit.get(..12).unwrap_or(&view.base_commit);
    match view.parent_work.as_ref()?.copied? {
        ParentWorkCopy::Pushed => Some(format!(
            "The agent runs on execution node \"{node_name}\" and starts from its parent's work: commit {short} with the parent's uncommitted changes, saved when the agent was created. Later work of either agent is not shared."
        )),
        ParentWorkCopy::LastPush => Some(format!(
            "The agent runs on execution node \"{node_name}\" and starts from its parent's last saved work: commit {short} with the uncommitted changes the parent's machine last pushed (it could not push again now). Later work of either agent is not shared."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-agent `CoW` sandboxes are retired: a backend sub-agent of a remote
    /// agent always works in the shared workspace checkout, so its notice
    /// names the parent's refs, even for metadata an older daemon marked
    /// `seeded` (its own checkout started from the parent's work).
    #[test]
    fn backend_parent_work_notice_always_names_the_shared_checkout() {
        for seeded in [false, true] {
            let work: BackendParentWork = serde_json::from_value(serde_json::json!({
                "agentId": "agent-parent",
                "head": "0123456789abcdef0123",
                "wip": "fedcba9876543210fedc",
                "copied": "pushed",
                "seeded": seeded,
            }))
            .expect("legacy metadata still parses");
            let notice = work.notice("ws-1");
            assert!(
                notice.contains("in the workspace's checkout"),
                "seeded={seeded}: {notice}"
            );
            assert!(
                notice.contains("`refs/intent/ws/ws-1/agents/agent-parent/head`")
                    && notice.contains("`refs/intent/ws/ws-1/agents/agent-parent/wip`"),
                "seeded={seeded}: names the parent's refs: {notice}"
            );
            assert!(
                !notice.contains("its own checkout"),
                "seeded={seeded}: no own checkout: {notice}"
            );
        }
        let fresh: BackendParentWork = serde_json::from_value(serde_json::json!({
            "agentId": "agent-parent",
            "head": "0123456789abcdef0123",
            "copied": "pushed",
        }))
        .expect("metadata without seeded parses");
        let value = serde_json::to_value(&fresh).unwrap();
        assert!(value.get("seeded").is_none(), "new metadata drops seeded: {value}");
    }

    #[test]
    fn placement_errors_split_the_node_code_and_cap_the_detail() {
        let error =
            PlacementError::from_node_error("node-capacity: this node is running 2 of 2 agents");
        assert_eq!(error.code, "node-capacity");
        assert_eq!(error.detail, "this node is running 2 of 2 agents");
        let plain = PlacementError::from_node_error("Something: broke");
        assert_eq!(plain.code, "node-agent-start-failed");
        assert_eq!(plain.detail, "Something: broke");
        let long =
            PlacementError::from_node_error(&format!("node-checkout-failed: {}", "é".repeat(1000)));
        assert!(long.detail.len() <= PLACEMENT_ERROR_DETAIL_MAX_BYTES);
        assert!(long.detail.ends_with('…'));
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::json!({ "code": "node-agent-start-failed", "detail": "Something: broke" }),
            "data and retryable stay absent outside node-capacity"
        );
    }

    #[test]
    fn capacity_placement_errors_carry_the_limit_and_retry() {
        let error = PlacementError::capacity(
            "this machine is running 4 of 4 agents",
            CapacityRefusal::Count {
                running_agents: 4,
                agent_cap: 4,
            },
        );
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "code": "node-capacity",
                "detail": "this machine is running 4 of 4 agents",
                "data": { "limit": "count", "runningAgents": 4, "agentCap": 4 },
                "retryable": true,
            })
        );
        assert_eq!(
            serde_json::from_value::<PlacementError>(wire).unwrap(),
            error
        );
        let legacy: PlacementError =
            serde_json::from_value(serde_json::json!({ "code": "x", "detail": "y" })).unwrap();
        assert_eq!(legacy, PlacementError::new("x", "y"));
    }
}

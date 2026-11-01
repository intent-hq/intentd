//! Per-namespace `workspace_api` bindings (WSAPI-3+).
//!
//! Each submodule owns:
//!  * a `PRELUDE` JS fragment that populates its `ws.<ns>` object, and
//!  * a `dispatch` fn that routes one `host({ method, args })` frame's
//!    method-suffix (the part after `"<ns>."`) to the shared
//!    [`WorkspaceApi`].
//!
//! `dispatch.rs` concatenates every namespace prelude into the single JS
//! prelude installed before user code, and delegates every host frame here.
//! Splitting the surface this way lets each future WSAPI wave own one file
//! without touching the shared bootstrap.

use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, TurnAttachmentRegistry, WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) mod agent;
pub(crate) mod app;
pub(crate) mod browser;
pub(crate) mod comment;
pub(crate) mod cross_workspace;
pub(crate) mod desktop;
pub(crate) mod discussion;
pub(crate) mod event;
pub(crate) mod file;
pub(crate) mod git;
pub(crate) mod help;
pub(crate) mod hook;
pub(crate) mod host;
pub(crate) mod mcp;
pub(crate) mod note;
pub(crate) mod pr;
pub(crate) mod primitive;
pub(crate) mod script;
pub(crate) mod secret;
pub(crate) mod task;
pub(crate) mod terminal;
pub(crate) mod workspace;

/// Build the JS installed before user code. Each per-namespace fragment
/// attaches its `ws.<ns>` object next to the others on the shared `ws`
/// global. Concatenation happens at call time because the per-namespace
/// fragments are `const &str` expressions, and `concat!` only accepts
/// literals.
///
/// Test-only shorthand for [`prelude_for`] with default features; the
/// prelude tests compare gated outputs against this baseline.
#[cfg(test)]
pub fn prelude() -> String {
    prelude_for(&AgentFeaturesSettings::default())
}

/// Feature-aware prelude: namespaces disabled in `[agentFeatures]` are
/// omitted entirely, so agent code touching them fails with a clear
/// `ws.<ns> is undefined` `TypeError`. With every toggle on — the default —
/// nothing is omitted.
#[cfg(test)]
pub(crate) fn prelude_for(features: &AgentFeaturesSettings) -> String {
    prelude_for_bridge(features, false)
}

/// `prelude_for` plus the sub-agent flag: a sub-agent environment forces
/// `structuredQuestions` off so `ws.app.question` is omitted through the
/// exact same pruning machinery as the settings toggle (the surfaces cannot
/// drift). Mirrors `WorkspaceMcpServer::effective_agent_features`; used by
/// the background hook scheduler for hooks owned by background/delegated
/// sessions.
#[must_use]
pub fn prelude_for_bridge(features: &AgentFeaturesSettings, is_sub_agent: bool) -> String {
    let forced;
    let features = if (is_sub_agent && (features.structured_questions || features.secret_requests))
        || (features.secret_requests && !features.host_exec)
    {
        forced = AgentFeaturesSettings {
            structured_questions: features.structured_questions && !is_sub_agent,
            secret_requests: features.secret_requests && features.host_exec && !is_sub_agent,
            ..features.clone()
        };
        &forced
    } else {
        features
    };
    let pr = pr::prelude_for(features);
    let workspace = workspace::prelude_for(is_sub_agent);
    let mut fragments: Vec<&str> = vec![
        help::PRELUDE,
        workspace.as_str(),
        note::PRELUDE,
        task::PRELUDE,
        comment::PRELUDE,
        discussion::PRELUDE,
        primitive::PRELUDE,
        cross_workspace::PRELUDE,
        pr.as_str(),
    ];
    if features.desktop_control {
        fragments.push(desktop::PRELUDE);
    }
    if features.browser_automation {
        fragments.push(browser::PRELUDE);
    }
    let agent = agent::prelude_for(features);
    fragments.extend([agent.as_ref(), event::PRELUDE, git::PRELUDE]);
    if features.host_exec {
        fragments.push(host::PRELUDE);
    }
    if features.secret_requests {
        fragments.push(secret::PRELUDE);
    }
    if features.background_hooks {
        fragments.push(hook::PRELUDE);
    }
    if features.scripts {
        fragments.push(script::PRELUDE);
    }
    if features.terminal_access {
        fragments.push(terminal::PRELUDE);
    }
    if features.mcp_tools {
        fragments.push(mcp::PRELUDE);
    }
    fragments.push(file::PRELUDE);
    let app = app::prelude_for(features);
    fragments.push(&app);
    let mut out = fragments.join("\n");
    out.push('\n');
    out
}

/// Install Assistant management tools only in the daemon-selected Assistant workspace.
#[must_use]
pub fn prelude_for_workspace(
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    workspace_id: &WorkspaceId,
) -> String {
    let chief_features;
    let features = if workspace_id.is_chief() && features.secret_requests {
        chief_features = AgentFeaturesSettings {
            secret_requests: false,
            ..features.clone()
        };
        &chief_features
    } else {
        features
    };
    let mut prelude = prelude_for_bridge(features, is_sub_agent);
    if workspace_id.is_chief() {
        prelude.push_str(app::mcp::PRELUDE);
        prelude.push_str(app::chats::PRELUDE);
    }
    prelude
}

/// The enclosing eval's wall-clock budget: its total duration and the
/// absolute deadline it counts toward. One eval issues many host frames,
/// and every frame before this one already consumed budget, so a binding
/// that bounds a wait must use [`EvalBudget::remaining`] — a fresh
/// `total`-sized wait started late in the eval would outlive the transport's
/// timeout and lose its explicit result to the generic eval timeout
/// (intent-hq/intent#5387).
#[derive(Clone, Copy, Debug)]
pub(crate) struct EvalBudget {
    /// The eval's full wall-clock budget.
    pub(crate) total: Duration,
    /// `start + total`; `None` only when that addition overflows `Instant`,
    /// in which case [`Self::remaining`] reports the full `total`.
    deadline: Option<Instant>,
}

impl EvalBudget {
    /// A budget of `total` whose clock starts now — call it when the eval
    /// starts (host construction), not when a frame is dispatched.
    pub(crate) fn starting_now(total: Duration) -> Self {
        Self {
            total,
            deadline: Instant::now().checked_add(total),
        }
    }

    /// Wall-clock time left before the eval's deadline (zero once passed).
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline
            .map_or(self.total, |d| d.saturating_duration_since(Instant::now()))
    }
}

/// Methods whose binding runs the retired-caller `agent_is_retired` read
/// itself, inside its spawned, budget-bounded send operation (after the
/// message-id mint), instead of `workspace_host_dispatch` awaiting it ahead
/// of the binding. The read is a store round-trip on the send path, and an
/// eval-timeout drop while it is parked would surface the generic timeout
/// with no send to land (intent-hq/intent#5387). The outcome is identical: a
/// retired caller's send returns the retired error and enqueues nothing.
pub(crate) const RETIRED_SEND_METHODS: [&str; 2] = ["agent.send", "agent.sendToTask"];

/// `ws.*` bindings a caller in a retired workspace may still use (PROTOCOL
/// §5.1 "Retired workspaces"): reads of the kept rows and daemon-global
/// reads (including `file.read` / `file.list`, which report the missing
/// folder with their own error), plus the documented revive trigger
/// `workspace.unarchive`. Every other binding — note / task / comment /
/// primitive writes, file writes and `file.getAttachment` (it copies into
/// the workspace), scripts, terminals, git, hooks, agents — is
/// refused with `workspace-retired`. Fail-closed: a binding not listed here
/// is treated as a mutation.
pub(crate) const RETIRED_ALLOWED_METHODS: &[&str] = &[
    "agent.diagnostics",
    "agent.getMessageBlock",
    "agent.getQueue",
    "agent.list",
    "agent.listSpecialists",
    "agent.readConversation",
    "agent.snapshot",
    "agent.status",
    "agent.summary",
    "browser.clients",
    "browser.docs",
    "browser.listTabs",
    "comment.getThread",
    "comment.list",
    "crossWorkspace.listNotes",
    "crossWorkspace.listSiblings",
    "crossWorkspace.readNote",
    "crossWorkspace.search",
    "discussion.conversations",
    "discussion.list",
    "event.agentActivity",
    "event.query",
    "event.workspaceSummary",
    "file.list",
    "file.read",
    "git.listRoots",
    "hook.get",
    "hook.list",
    "mcp.listServers",
    "mcp.listTools",
    "note.list",
    "note.listTasks",
    "note.read",
    "note.readAsset",
    "pr.monitors",
    "pr.snapshot",
    "script.get",
    "script.list",
    "script.listPage",
    "script.monitorDetails",
    "script.monitors",
    "script.monitorsPage",
    "script.output",
    "script.status",
    "task.getMyTask",
    "terminal.list",
    "terminal.readOutput",
    "workspace.context",
    "workspace.details",
    "workspace.info",
    "workspace.referenceDocs",
    "workspace.timeline",
    "workspace.unarchive",
];

/// Dispatch one `host({ method, args })` frame to the matching per-namespace
/// handler. Returns `Ok(None)` when the namespace is not owned here (unknown
/// method); `Ok(Some(v))` on success and `Err(msg)` on a JS-visible failure.
/// `caller_agent_id` threads the tool-call's agent context so the bindings
/// that attribute their calls back to the spawning agent
/// (`workspace.setAgentName` / `git.commit` /
/// `ws.browser.exec`, and the caller-aware `ws.agent.*` methods — `create`,
/// `delegate`, `send`, `sendToTask`, `wakeOrCreate`, `reportToParent`,
/// `requestDiscussion`, `reportBlocker`, `retire`) can do so.
/// `turn_attachments` threads the §7.1 turn-attachment registry to the
/// bindings that register attachments mid-dispatch (`ws.app.question.ask`);
/// `None` keeps those bindings inert (FE front door, tests).
/// `features` threads the caller's effective `[agentFeatures]` so `ws.help`
/// renders the same gated docs its tool description advertises;
/// `is_sub_agent` lets the help error path attribute `app.question` pruning
/// to the top-level-only rule instead of a settings toggle.
/// `eval_budget` is the caller's effective `workspace_api` wall-clock budget,
/// so bindings that wait on a process (`ws.script.run`) can refuse a wait the
/// transport could never honor, and `ws.agent.send` / `sendToTask` can bound
/// their wait on the (spawned, cancellation-safe) daemon-side delivery by
/// the budget's REMAINING time.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn try_dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    turn_attachments: Option<&Arc<TurnAttachmentRegistry>>,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    eval_budget: EvalBudget,
    method: &str,
    args: &Value,
) -> Result<Option<Value>, String> {
    if let Some(rest) = method.strip_prefix("help.") {
        let plain_folder = is_plain_folder(api, workspace_id).await;
        return help::dispatch(
            workspace_id,
            features,
            is_sub_agent,
            plain_folder,
            rest,
            args,
        )
        .map(Some);
    }
    // Retired workspaces are read-only (PROTOCOL §5.1): an agent or hook
    // still running there (the agent that retired its own workspace, mid
    // turn) must not write notes, tasks or files into it. Reads of the kept
    // rows and the revive trigger stay available
    // ([`RETIRED_ALLOWED_METHODS`]); every other ws.* call is refused until
    // the workspace is revived.
    if !workspace_id.is_chief() && !RETIRED_ALLOWED_METHODS.contains(&method) {
        if let Ok(Some(retired)) = api
            .retired_workspace_for(Some(workspace_id.clone()), None)
            .await
        {
            return Err(map_err(intent_core::Error::WorkspaceRetired {
                workspace_id: retired.as_str().to_string(),
            }));
        }
    }
    // Plain-folder workspaces (§5.1) have no git or forge: `ws.git.*`,
    // `ws.pr.*`, `ws.mr.*` and `ws.workspace.proposeSibling` fail with the
    // documented refusal before any binding runs. Only these frames pay the
    // workspace lookup.
    if let Some(refusal) = super::tools::plain_folder_refusal(method) {
        if is_plain_folder(api, workspace_id).await {
            return Err(refusal);
        }
    }
    if let Some(rest) = method.strip_prefix("workspace.") {
        return workspace::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("note.") {
        return note::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("task.") {
        return task::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("discussion.") {
        return discussion::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("comment.") {
        return comment::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("primitive.") {
        return primitive::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("crossWorkspace.") {
        return cross_workspace::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method
        .strip_prefix("pr.")
        .or_else(|| method.strip_prefix("mr."))
    {
        return pr::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("desktop.") {
        return desktop::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("browser.") {
        return browser::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("agent.") {
        return agent::dispatch(api, workspace_id, caller_agent_id, eval_budget, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("event.") {
        return event::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("git.") {
        return git::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("host.") {
        let secrets = secret_requests_effective(features, is_sub_agent, workspace_id);
        return host::dispatch(api, workspace_id, caller_agent_id, secrets, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("secret.") {
        if !secret_requests_effective(features, is_sub_agent, workspace_id) {
            return Err(format!("host: {}", secret::SECRET_REQUESTS_UNAVAILABLE));
        }
        return secret::dispatch(
            api,
            workspace_id,
            caller_agent_id,
            turn_attachments,
            rest,
            args,
        )
        .await
        .map(Some);
    }
    if let Some(rest) = method.strip_prefix("hook.") {
        return hook::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("script.") {
        return script::dispatch(
            api,
            workspace_id,
            eval_budget.total,
            caller_agent_id,
            rest,
            args,
        )
        .await
        .map(Some);
    }
    if let Some(rest) = method.strip_prefix("terminal.") {
        return terminal::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("mcp.") {
        return mcp::dispatch(api, workspace_id, rest, args).await.map(Some);
    }
    if let Some(rest) = method.strip_prefix("file.") {
        return file::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("app.") {
        return app::try_dispatch(
            api,
            workspace_id,
            caller_agent_id,
            turn_attachments,
            rest,
            args,
        )
        .await;
    }
    Ok(None)
}

/// Whether agent secret requests (PROTOCOL §5.56) are available to this
/// caller: the `secretRequests` toggle with `hostExec` (the only way to use a
/// secret), a top-level caller, and a non-chief workspace (whose tool
/// description has no `ws.host.*` surface).
pub(crate) fn secret_requests_effective(
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    workspace_id: &WorkspaceId,
) -> bool {
    features.secret_requests && features.host_exec && !is_sub_agent && !workspace_id.is_chief()
}

/// Pull a required string field from a JS `args` object, surfacing the same
/// "X is required" style errors as the TS reference bindings.
pub(crate) fn req_str(args: &Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{key} is required"))
}

/// Pull an optional string field from a JS `args` object.
pub(crate) fn opt_str(args: &Value, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Pull a required i64 field, tolerating JS numbers that arrived as strings
/// (some reference builders accept `"12"` alongside `12`).
pub(crate) fn req_i64(args: &Value, key: &str) -> Result<i64, String> {
    if let Some(v) = args.get(key).and_then(Value::as_i64) {
        return Ok(v);
    }
    if let Some(s) = args.get(key).and_then(Value::as_str) {
        return s
            .parse::<i64>()
            .map_err(|_| format!("{key} must be an integer"));
    }
    Err(format!("{key} is required"))
}

/// Pull an optional bool field.
pub(crate) fn opt_bool(args: &Value, key: &str) -> Option<bool> {
    args.get(key).and_then(Value::as_bool)
}

/// A strictly-typed optional boolean: absent is `Ok(None)`; any present
/// value that is not a JSON boolean (`"true"`, `null`, a number, an object)
/// is an invalid-params error rather than a silent `false`.
pub(crate) fn opt_strict_bool(args: &Value, key: &str) -> Result<Option<bool>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(format!("invalid params: `{key}` must be a boolean")),
    }
}

/// Pull an optional i64 field, tolerating JS numbers that arrived as strings.
pub(crate) fn opt_i64(args: &Value, key: &str) -> Option<i64> {
    if let Some(v) = args.get(key).and_then(Value::as_i64) {
        return Some(v);
    }
    args.get(key)
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<i64>().ok())
}

/// Pull an optional string-array field, either as a JSON array of strings or
/// as a comma-separated string (the TS `normalizeTags` fallback).
pub(crate) fn opt_vec_str(args: &Value, key: &str) -> Option<Vec<String>> {
    if let Some(a) = args.get(key).and_then(Value::as_array) {
        return Some(
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
        );
    }
    if let Some(s) = args.get(key).and_then(Value::as_str) {
        return Some(
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        );
    }
    None
}

/// Whether `workspace_id` is a plain-folder workspace, read from the
/// persisted `Workspace.plainFolder` flag (§5.1). A failed lookup counts as
/// not plain, so git workspaces keep their surface; the Assistant workspace
/// is never a plain folder.
pub(crate) async fn is_plain_folder(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
) -> bool {
    !workspace_id.is_chief()
        && api
            .get_workspace(workspace_id.clone())
            .await
            .is_ok_and(|w| w.plain_folder)
}

/// Convert a [`intent_core::Error`] into the JS-visible error text used
/// throughout these bindings (the trait's `Display` impl already renders the
/// message content the reference builders threw).
// By-value so it slots point-free into `map_err(map_err)` in every binding.
#[expect(clippy::needless_pass_by_value)]
pub(crate) fn map_err(e: intent_core::Error) -> String {
    e.to_string()
}

/// A Markdown link `[label](url)` whose label has `[`, `]` and `\\` escaped,
/// so a title like `Fix [beta] login` still forms one link.
pub(crate) fn markdown_link(label: &str, url: &str) -> String {
    let mut out = String::with_capacity(label.len() + url.len() + 4);
    out.push('[');
    for c in label.chars() {
        if matches!(c, '[' | ']' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push_str("](");
    out.push_str(url);
    out.push(')');
    out
}

/// The app link that opens workspace `id`, plus a Markdown link labelled with
/// its title (`Untitled` when blank): `(intent://local/workspace/{id},
/// [Title](intent://local/workspace/{id}))`. Returned beside workspace IDs so
/// agents share a clickable workspace instead of its internal ID.
pub(crate) fn workspace_links(id: &str, title: &str) -> (String, String) {
    let link = format!("intent://local/workspace/{id}");
    let label = if title.trim().is_empty() {
        "Untitled"
    } else {
        title
    };
    let markdown_link = markdown_link(label, &link);
    (link, markdown_link)
}

/// Add `link` and `markdownLink` (see [`workspace_links`]) to a JSON object
/// that has string `id` and `title` fields; any other value is left alone.
pub(crate) fn insert_workspace_links(row: &mut Value) {
    let Some(obj) = row.as_object_mut() else {
        return;
    };
    let (Some(id), Some(title)) = (
        obj.get("id").and_then(Value::as_str),
        obj.get("title").and_then(Value::as_str),
    ) else {
        return;
    };
    let (link, markdown_link) = workspace_links(id, title);
    obj.insert("link".to_string(), Value::String(link));
    obj.insert("markdownLink".to_string(), Value::String(markdown_link));
}

/// Per-session fields the agent must never learn about
/// (`intent_core::model::AGENT_HIDDEN_FIELDS`): stripped from every
/// `ws.agent.*` and `ws.event.*` result (however deeply nested) before it
/// reaches the caller. The same scrub runs in `intent-services` on the
/// per-event `data` copied into parent-wake message metadata, so the shared
/// definition lives in `intent-core`.
pub(crate) use intent_core::model::strip_agent_hidden_fields;

#[cfg(test)]
mod workspace_link_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn workspace_links_build_the_app_link_and_escape_the_label() {
        assert_eq!(
            workspace_links("cold-heron", "Fix login"),
            (
                "intent://local/workspace/cold-heron".to_string(),
                "[Fix login](intent://local/workspace/cold-heron)".to_string(),
            )
        );
        assert_eq!(
            workspace_links("w", r"Fix [beta] a\b").1,
            r"[Fix \[beta\] a\\b](intent://local/workspace/w)"
        );
        assert_eq!(
            workspace_links("w", "  ").1,
            "[Untitled](intent://local/workspace/w)"
        );
    }

    #[test]
    fn insert_workspace_links_needs_an_id_and_a_title() {
        let mut row = json!({ "id": "w", "title": "T", "branch": "b" });
        insert_workspace_links(&mut row);
        assert_eq!(row["link"], "intent://local/workspace/w");
        assert_eq!(row["markdownLink"], "[T](intent://local/workspace/w)");
        assert_eq!(row["branch"], "b");
        for mut other in [json!({ "id": "w" }), json!({ "title": "T" }), json!("w")] {
            let before = other.clone();
            insert_workspace_links(&mut other);
            assert_eq!(other, before);
        }
    }
}

#[cfg(test)]
mod hidden_field_tests {
    use super::*;
    use serde_json::json;

    /// The scrub removes `notificationsMuted` from a bare object, a list of
    /// them, and nested envelopes (`{ agent }`, event `data`), leaving every
    /// other key.
    #[test]
    fn strip_agent_hidden_fields_removes_notifications_muted_everywhere() {
        let mut v = json!({
            "ok": true,
            "notificationsMuted": true,
            "agent": { "id": "agent-1", "notificationsMuted": false, "metadata": { "isBackground": true } },
            "agents": [
                { "id": "agent-2", "notificationsMuted": true },
                { "id": "agent-3", "status": "idle" }
            ],
            "events": [
                { "eventType": "agent:idle", "data": { "agentId": "agent-2", "notificationsMuted": true } }
            ],
        });
        strip_agent_hidden_fields(&mut v);
        assert_eq!(
            v,
            json!({
                "ok": true,
                "agent": { "id": "agent-1", "metadata": { "isBackground": true } },
                "agents": [
                    { "id": "agent-2" },
                    { "id": "agent-3", "status": "idle" }
                ],
                "events": [
                    { "eventType": "agent:idle", "data": { "agentId": "agent-2" } }
                ],
            })
        );
    }
}

#[cfg(test)]
mod prelude_tests {
    use super::*;

    // Hard requirement: with all `[agentFeatures]` toggles on (the default),
    // the feature-aware prelude is byte-identical to the legacy one the hook
    // scheduler installs — the two environments cannot drift.
    #[test]
    fn all_defaults_prelude_is_byte_identical() {
        assert_eq!(prelude_for(&AgentFeaturesSettings::default()), prelude());
    }

    // Each disabled toggle removes exactly its own `ws.<ns> = {` installer
    // from the prelude, leaving every other namespace in place.
    // A gated prelude installer marker paired with the mutator that flips
    // its `[agentFeatures]` toggle off.
    type PreludeCase = (&'static str, fn(&mut AgentFeaturesSettings));

    #[test]
    fn disabled_features_are_omitted_from_prelude() {
        let cases: Vec<PreludeCase> = vec![
            ("ws.hook = {", |f| f.background_hooks = false),
            ("ws.host = {", |f| f.host_exec = false),
            ("ws.script = {", |f| f.scripts = false),
            ("ws.terminal = {", |f| f.terminal_access = false),
            ("ws.browser = {", |f| f.browser_automation = false),
            ("ws.app.question = {", |f| f.structured_questions = false),
            ("ws.mcp = {", |f| f.mcp_tools = false),
        ];
        let markers: Vec<&str> = cases.iter().map(|(m, _)| *m).collect();
        for (marker, disable) in &cases {
            let mut features = AgentFeaturesSettings::default();
            disable(&mut features);
            let js = prelude_for(&features);
            assert!(
                !js.contains(marker),
                "`{marker}` still installed when disabled"
            );
            for other in &markers {
                if other != marker {
                    assert!(
                        js.contains(other),
                        "disabling `{marker}` also dropped `{other}`"
                    );
                }
            }
            // Un-gated namespaces always survive.
            for kept in [
                "ws.help = ",
                "ws.note = {",
                "ws.git = {",
                "ws.file = {",
                "ws.crossWorkspace = {",
            ] {
                assert!(
                    js.contains(kept),
                    "disabling `{marker}` dropped un-gated `{kept}`"
                );
            }
        }
    }

    // `structuredQuestions` off removes only `ws.app.question`; the rest of
    // the `ws.app.*` prelude (chief-gated server-side) stays installed.
    #[test]
    fn structured_questions_off_keeps_other_app_prelude() {
        let features = AgentFeaturesSettings {
            structured_questions: false,
            ..AgentFeaturesSettings::default()
        };
        let js = prelude_for(&features);
        assert!(!js.contains("ws.app.question = {"));
        for kept in [
            "ws.app.workspaces = {",
            "ws.app.settings = {",
            "ws.app.ui = {",
        ] {
            assert!(js.contains(kept), "`{kept}` was wrongly dropped");
        }
    }

    // Guard: the attention-request segment gated by `attentionRequests` still
    // matches the `ws.agent` prelude verbatim, so the `replacen` scrub cannot
    // silently become a no-op after a prelude edit.
    #[test]
    fn attention_prelude_segment_matches_agent_prelude() {
        assert!(agent::PRELUDE.contains(agent::ATTENTION_PRELUDE_SEGMENT));
    }

    // `attentionRequests` off removes only the two attention-request
    // installers from `ws.agent`; the namespace itself — `reportToParent`
    // included — stays installed.
    #[test]
    fn attention_requests_off_keeps_rest_of_agent_prelude() {
        let features = AgentFeaturesSettings {
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        };
        let js = prelude_for(&features);
        assert!(!js.contains("requestDiscussion:"));
        assert!(!js.contains("reportBlocker:"));
        for kept in ["ws.agent = {", "reportToParent:", "wakeOrCreate:"] {
            assert!(js.contains(kept), "`{kept}` was wrongly dropped");
        }
    }

    // Guard: the retire segment gated by `peerAgents` still matches the
    // `ws.agent` prelude verbatim, so the `replacen` scrub cannot silently
    // become a no-op after a prelude edit.
    #[test]
    fn retire_prelude_segment_matches_agent_prelude() {
        assert!(agent::PRELUDE.contains(agent::RETIRE_PRELUDE_SEGMENT));
    }

    // `peerAgents` defaults ON: the default prelude installs `retire`;
    // opting out omits it, and the `ws.agent` scrubs compose independently.
    #[test]
    fn peer_agents_gates_retire_installer_in_prelude() {
        let js = prelude_for(&AgentFeaturesSettings::default());
        assert!(js.contains("retire:"), "default must install retire");

        let js = prelude_for(&AgentFeaturesSettings {
            peer_agents: false,
            ..AgentFeaturesSettings::default()
        });
        assert!(
            !js.contains("retire:"),
            "retire installed with peerAgents off"
        );
        assert!(
            js.contains("reportBlocker:"),
            "attention installers must survive"
        );

        let js = prelude_for(&AgentFeaturesSettings {
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        });
        assert!(js.contains("retire:"));
        assert!(!js.contains("reportBlocker:"));
        assert!(js.contains("reportToParent:"));
    }

    // The merged create API: `spawnPeer` no longer exists as a binding —
    // the `ws.agent` prelude installs `create` (which carries the
    // `topLevel` option) and never a `spawnPeer` installer, on both
    // top-level and sub-agent bridges.
    #[test]
    fn prelude_has_no_spawn_peer_installer() {
        let features = AgentFeaturesSettings {
            peer_agents: true,
            ..AgentFeaturesSettings::default()
        };
        for is_sub_agent in [false, true] {
            let js = prelude_for_bridge(&features, is_sub_agent);
            assert!(
                !js.contains("spawnPeer"),
                "spawnPeer must not appear in the prelude (is_sub_agent = {is_sub_agent})"
            );
            assert!(js.contains("create:"), "create installer must survive");
        }
    }
}

#[cfg(test)]
mod retired_workspace_tests {
    use super::*;
    use intent_core::BoxFuture;
    use serde_json::json;

    /// `ws-retired` is retired; every other workspace is live. Note writes
    /// succeed on a live workspace so the refusal is the gate's, not the
    /// binding's.
    struct RetiredApi;

    impl WorkspaceApi for RetiredApi {
        fn retired_workspace_for(
            &self,
            workspace_id: Option<WorkspaceId>,
            _agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, intent_core::Result<Option<WorkspaceId>>> {
            Box::pin(async move { Ok(workspace_id.filter(|w| w.as_str() == "ws-retired")) })
        }
    }

    /// Every ws.* call from a retired workspace (other than help) is refused
    /// with `workspace-retired` before it reaches a binding — so an agent
    /// still mid-turn there cannot write notes, tasks or files.
    #[tokio::test]
    async fn bindings_refuse_calls_from_a_retired_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(RetiredApi);
        let features = AgentFeaturesSettings::default();
        let call = |ws: &'static str, method: &'static str| {
            let api = api.clone();
            let features = features.clone();
            async move {
                try_dispatch(
                    &api,
                    &WorkspaceId::from(ws),
                    None,
                    None,
                    &features,
                    false,
                    EvalBudget::starting_now(Duration::from_secs(30)),
                    method,
                    &json!({ "title": "t", "content": "c" }),
                )
                .await
            }
        };
        for method in [
            "note.create",
            "task.updateStatus",
            "file.write",
            "file.delete",
            "file.getAttachment",
            "git.commit",
        ] {
            let err = call("ws-retired", method).await.unwrap_err();
            assert!(err.contains("retired"), "{method}: {err}");
        }
        // A live workspace reaches the binding (which then fails on the
        // fake API for its own reasons, never with the retired refusal).
        let live = call("ws-live", "note.create").await;
        assert!(
            live.as_ref().err().is_none_or(|e| !e.contains("retired")),
            "{live:?}"
        );
        // Reads of the kept rows and the revive trigger are not refused by
        // the gate (the fake API may fail them for its own reasons).
        for method in [
            "note.read",
            "note.list",
            "task.getMyTask",
            "comment.list",
            "agent.readConversation",
            "workspace.details",
            "workspace.unarchive",
            "file.read",
            "file.list",
        ] {
            let out = call("ws-retired", method).await;
            assert!(
                out.as_ref().err().is_none_or(|e| !e.contains("retired")),
                "{method}: {out:?}"
            );
        }
    }

    /// Every allowed method is a real binding (named by some binding's
    /// `host({ method })` shim), so the list cannot silently go stale.
    #[test]
    fn retired_allowed_methods_are_binding_methods() {
        fn collect(dir: &std::path::Path, out: &mut String) {
            for entry in std::fs::read_dir(dir).expect("read bindings dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    collect(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push_str(&std::fs::read_to_string(&path).expect("read binding"));
                }
            }
        }
        let mut sources = String::new();
        collect(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp_server/bindings"),
            &mut sources,
        );
        for m in RETIRED_ALLOWED_METHODS {
            assert!(
                sources.contains(&format!("method: '{m}'"))
                    || sources.contains(&format!("method: \"{m}\"")),
                "{m} is not a ws.* binding method"
            );
        }
    }
}

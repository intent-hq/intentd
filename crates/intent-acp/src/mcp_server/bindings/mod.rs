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

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, TurnAttachmentRegistry, WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) mod agent;
pub(crate) mod app;
pub(crate) mod browser;
pub(crate) mod comment;
pub(crate) mod cross_workspace;
pub(crate) mod event;
pub(crate) mod file;
pub(crate) mod git;
pub(crate) mod help;
pub(crate) mod hook;
pub(crate) mod host;
pub(crate) mod note;
pub(crate) mod pr;
pub(crate) mod primitive;
pub(crate) mod script;
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
    let features = if is_sub_agent && features.structured_questions {
        forced = AgentFeaturesSettings {
            structured_questions: false,
            ..features.clone()
        };
        &forced
    } else {
        features
    };
    let pr = pr::prelude_for(features);
    let mut fragments: Vec<&str> = vec![
        help::PRELUDE,
        workspace::PRELUDE,
        note::PRELUDE,
        task::PRELUDE,
        comment::PRELUDE,
        primitive::PRELUDE,
        cross_workspace::PRELUDE,
        pr.as_str(),
    ];
    if features.browser_automation {
        fragments.push(browser::PRELUDE);
    }
    let agent = agent::prelude_for(features, is_sub_agent);
    fragments.extend([agent.as_ref(), event::PRELUDE, git::PRELUDE]);
    if features.host_exec {
        fragments.push(host::PRELUDE);
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
    fragments.push(file::PRELUDE);
    let app = app::prelude_for(features);
    fragments.push(&app);
    let mut out = fragments.join("\n");
    out.push('\n');
    out
}

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
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    turn_attachments: Option<&Arc<TurnAttachmentRegistry>>,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    method: &str,
    args: &Value,
) -> Result<Option<Value>, String> {
    if let Some(rest) = method.strip_prefix("help.") {
        return help::dispatch(workspace_id, features, is_sub_agent, rest, args).map(Some);
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
    if let Some(rest) = method.strip_prefix("pr.") {
        return pr::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("browser.") {
        return browser::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("agent.") {
        return agent::dispatch(api, workspace_id, caller_agent_id, rest, args)
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
        return host::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("hook.") {
        return hook::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("script.") {
        return script::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("terminal.") {
        return terminal::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
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

/// Convert a [`intent_core::Error`] into the JS-visible error text used
/// throughout these bindings (the trait's `Display` impl already renders the
/// message content the reference builders threw).
// By-value so it slots point-free into `map_err(map_err)` in every binding.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn map_err(e: intent_core::Error) -> String {
    e.to_string()
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

    // Guard: the spawnPeer segment gated by `peerAgents` + the top-level-only
    // rule still matches the `ws.agent` prelude verbatim, so the `replacen`
    // scrub cannot silently become a no-op after a prelude edit.
    #[test]
    fn spawn_peer_prelude_segment_matches_agent_prelude() {
        assert!(agent::PRELUDE.contains(agent::SPAWN_PEER_PRELUDE_SEGMENT));
    }

    // `peerAgents` defaults OFF (the one opt-in toggle): the default prelude
    // omits the `retire` and `spawnPeer` installers; opting in installs
    // them, and the `ws.agent` scrubs compose independently.
    #[test]
    fn peer_agents_gates_retire_installer_in_prelude() {
        let js = prelude_for(&AgentFeaturesSettings::default());
        assert!(
            !js.contains("retire:"),
            "retire installed with peerAgents off"
        );
        assert!(
            !js.contains("spawnPeer:"),
            "spawnPeer installed with peerAgents off"
        );
        assert!(
            js.contains("reportBlocker:"),
            "attention installers must survive"
        );

        let js = prelude_for(&AgentFeaturesSettings {
            peer_agents: true,
            ..AgentFeaturesSettings::default()
        });
        assert!(js.contains("retire:"), "opting in must install retire");
        assert!(
            js.contains("spawnPeer:"),
            "opting in must install spawnPeer"
        );

        let js = prelude_for(&AgentFeaturesSettings {
            peer_agents: true,
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        });
        assert!(js.contains("retire:"));
        assert!(js.contains("spawnPeer:"));
        assert!(!js.contains("reportBlocker:"));
        assert!(js.contains("reportToParent:"));
    }

    // Top-level-only rule: a sub-agent bridge's prelude omits the
    // `spawnPeer` installer even with `peerAgents` on; `retire` (self-scoped,
    // available to every agent when the toggle is on) survives.
    #[test]
    fn sub_agent_bridge_prelude_omits_spawn_peer() {
        let features = AgentFeaturesSettings {
            peer_agents: true,
            ..AgentFeaturesSettings::default()
        };
        let js = prelude_for_bridge(&features, true);
        assert!(
            !js.contains("spawnPeer:"),
            "spawnPeer must be scrubbed on sub-agent bridges"
        );
        assert!(js.contains("retire:"), "retire must survive for sub-agents");

        let js = prelude_for_bridge(&features, false);
        assert!(js.contains("spawnPeer:"));
    }
}

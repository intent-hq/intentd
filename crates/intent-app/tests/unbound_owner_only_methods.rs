//! Owner-only method surface, unbound (multiplayer w4b: fail-closed caller).
//!
//! Lives in intent-app because it drives the gated `workspace.export.*`
//! methods, which need the transfer feature registered by
//! [`intent_app::compose`]; intent-transport must not depend on a feature crate.
//!
//! Every owner-only **router** method — `ROUTER_METHODS \ COLLABORATOR_METHODS`,
//! the service-layer half of [`COLLABORATOR_REFUSED_METHODS`] — dispatched
//! through the real router into a real `Services` with **no caller bound**
//! must answer `-32003 Forbidden` from the capability gate — never a
//! successful early return ahead of a gate (the `agent.respondPermission`
//! no-manager path was one, fixed and pinned by
//! `unbound_respond_permission_is_forbidden_without_a_manager`) — unless the
//! method is pinned by name in one of two goldens: its gate is conditional
//! on something the minimal call omits — an optional scope argument or a
//! live export session ([`CONDITIONALLY_GATED_AT_SERVICE_LAYER`], whose
//! armed modes `armed_conditional_gates_are_forbidden_unbound` asserts
//! separately) — or it has no service-layer gate at all
//! ([`UNGATED_AT_SERVICE_LAYER`]).
//!
//! The table below is params-only: it supplies the minimal *valid* arguments
//! so each arm gets past `-32602` parsing and reaches the service method
//! (the fixture workspace exists, so a lookup ordered before the gate still
//! ends at the gate). It is derived from the catalog, so a new owner-only
//! router method fails here until it is given a row — and then fails again
//! until its service method is gated or it is named in
//! [`UNGATED_AT_SERVICE_LAYER`] on purpose.
//!
//! The remaining refused methods — the connection-task fast paths (`host.*`,
//! `browser.*`, `forward.*`, `system.*`, `pairing.*`, `server.*`,
//! `providers.setup.*`, `invite.inspect` / `invite.accept` /
//! `invite.challenge` / `invite.prove`)
//! and the subscription channels — have
//! no `WorkspaceApi` method to gate; they are protected only by the
//! transport allowlist in `conn::process_frame` (`-32003`) and stay out of
//! this table by construction. That partition is asserted, not assumed.

use intent_core::{chief_workspace, AgentId, Error, WorkspaceApi as _, WorkspaceId};
use intent_services::Services;
use intent_store::Store;
use intent_transport::catalog::{COLLABORATOR_METHODS, ROUTER_METHODS};
use intent_transport::handle_message;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// How a [`CONDITIONALLY_GATED_AT_SERVICE_LAYER`] row's gate is armed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arming {
    /// `require_member` runs only when the optional `workspaceId` is
    /// given.
    WorkspaceId,
    /// `require_agent_member` runs only when the optional `agentId` is
    /// given.
    AgentId,
}

/// Golden: owner-only router methods whose service-layer gate is
/// **conditional** on an optional scope argument, so the sweep's minimal
/// call never reaches it. Transfers have an unconditional actor gate.
/// Each row is `(method, unarmed outcome, what arms the gate)`. The
/// unarmed outcome is pinned here so the sweep sees it as classified,
/// not as ungated; the unarmed mode is owner-only by the transport
/// allowlist in `conn::process_frame` alone. Every armed mode is
/// asserted by [`armed_conditional_gates_are_forbidden_unbound`], which
/// arms the gate and requires `-32003`. A row leaves this list when its
/// gate stops being conditional (then it is simply `-32003` in the
/// sweep); making a gate unconditional is a capability-rule change and
/// not something this table decides.
const CONDITIONALLY_GATED_AT_SERVICE_LAYER: &[(&str, &str, Arming)] = &[
    ("agent.completeOnce", "ok", Arming::WorkspaceId),
    ("agent.enhancePrompt", "ok", Arming::WorkspaceId),
    // Unscoped, the call proceeds to the worktree lookup (Internal here).
    ("git.agentCommit", "-32603", Arming::AgentId),
    ("rules.list", "ok", Arming::WorkspaceId),
];

/// Golden: owner-only router methods whose service method has **no
/// service-layer gate at all**, with the outcome an unbound caller
/// observes today (`ok` or the error code). Each is owner-only by the
/// transport allowlist in `conn::process_frame` alone, and its unbound
/// behaviour is whatever the body does. Shrinking this list is the goal;
/// growing it needs a reason on the row. The failure message prints the
/// recomputed list.
const UNGATED_AT_SERVICE_LAYER: &[(&str, &str)] = &[
    // No gate: daemon-wide per-agent memory read; no-manager early
    // return `{ sampledAt: null, totalBytes: null, agents: [] }`.
    ("agent.memoryUsage", "ok"),
    // No gate: process-wide stack sampler.
    ("debug.sampleStacks", "ok"),
    // No gate: no-manager early return `{ running: false }`.
    ("unsloth.status", "ok"),
    // No gate: no-manager early return `{ stopped: false }`.
    ("unsloth.stop", "ok"),
    // No gate: proceeds to provider selection (Internal without an engine).
    ("voice.transcribe", "-32603"),
];

struct Fixture {
    services: Services,
    ws: WorkspaceId,
    /// Swept on drop; every path the table hands out lives under it.
    dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::Builder::new()
        .prefix("intentd-unbound-table-")
        .tempdir()
        .expect("tempdir");
    let store = Store::open(&dir.path().join("svc.db"))
        .await
        .expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&intent_core::Workspace {
            id: ws.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            ..chief_workspace()
        })
        .await
        .expect("insert workspace");
    let services = intent_app::compose(Services::new(store))
        .with_workspaces_root(dir.path().join("workspaces"))
        .with_assets_root(dir.path().join("assets"));
    Fixture { services, ws, dir }
}

/// Minimal valid params per owner-only router method. Values are inert
/// when a gate is missing: unknown ids, closed ports, empty edits, and
/// every path stays inside the fixture tempdir.
fn minimal_params(f: &Fixture) -> Vec<(&'static str, Value)> {
    let ws = f.ws.as_str();
    let dir = f.dir.path().to_string_lossy().into_owned();
    let gh = json!({ "owner": "o", "repo": "r" });
    let gh_n = json!({ "owner": "o", "repo": "r", "number": 1 });
    let sid = json!({ "serverId": "srv" });
    let script = json!({ "workspaceId": ws, "scriptId": "s1" });
    let term = json!({ "terminalId": "t1" });
    vec![
        (
            "accept-changes.addRemote",
            json!({ "workspaceId": ws, "remoteUrl": "u" }),
        ),
        (
            "accept-changes.execute",
            json!({ "workspaceId": ws, "action": "a" }),
        ),
        ("accept-changes.getStatus", json!({ "workspaceId": ws })),
        (
            "accept-changes.mergePR",
            json!({ "workspaceId": ws, "prNumber": 1 }),
        ),
        (
            "accept-changes.prepare",
            json!({ "workspaceId": ws, "action": "a" }),
        ),
        (
            "accept-changes.reconcile",
            json!({"workspaceId":ws,"operationId":"held-operation","root":{"workspaceId":ws,"kind":"primary"}}),
        ),
        (
            "accept-changes.release",
            json!({"workspaceId":ws,"operationId":"held-operation","root":{"workspaceId":ws,"kind":"primary"}}),
        ),
        ("agent.cancelDelete", json!({ "agentId": "a1" })),
        (
            "agent.completeOnce",
            json!({ "prompt": "p", "timeoutMs": 1 }),
        ),
        (
            "agent.delegate",
            json!({ "workspaceId": ws, "taskNoteId": "t1" }),
        ),
        ("agent.delete", json!({ "agentId": "a1" })),
        ("agent.diagnostics", json!({ "workspaceId": ws })),
        (
            "agent.enhancePrompt",
            json!({ "prompt": "p", "timeoutMs": 1 }),
        ),
        ("agent.memoryUsage", json!({})),
        (
            "agent.replaceMessages",
            json!({ "agentId": "a1", "messages": [] }),
        ),
        (
            "agent.reportToParent",
            json!({ "workspaceId": ws, "report": "r" }),
        ),
        (
            "agent.resolveProposal",
            json!({ "workspaceId": ws, "agentId": "a1", "proposalId": "p1", "outcome": "reject" }),
        ),
        (
            "agent.wakeOrCreate",
            json!({ "workspaceId": ws, "taskNoteId": "t1", "contextMessage": "c" }),
        ),
        ("debug.sampleStacks", json!({ "durationMs": 1 })),
        ("file-tracking.getAgentLocks", json!({ "workspaceId": ws })),
        ("file-tracking.getChanges", json!({ "workspaceId": ws })),
        ("file-tracking.loadCommits", json!({ "workspaceId": ws })),
        (
            "file-tracking.stage",
            json!({ "workspaceId": ws, "paths": [] }),
        ),
        (
            "file-tracking.unstage",
            json!({ "workspaceId": ws, "paths": [] }),
        ),
        (
            "git.agentCommit",
            json!({ "workspaceId": ws, "message": "m" }),
        ),
        (
            "git.clone",
            json!({ "url": "file:///nonexistent", "parentDir": dir }),
        ),
        ("github.authStatus", json!({})),
        ("github.branches.list", gh.clone()),
        ("github.branches.listCached", gh.clone()),
        ("github.cancelAuth", json!({})),
        ("github.connect", json!({})),
        ("github.getReviewThreads", gh_n.clone()),
        ("github.getUser", json!({})),
        (
            "github.identityProof.create",
            json!({ "nonce": "n", "hostLabel": "h" }),
        ),
        ("github.identityProof.delete", json!({ "gistId": "g" })),
        ("github.issues.get", gh_n.clone()),
        ("github.issues.list", gh.clone()),
        ("github.issues.search", gh.clone()),
        ("github.listReviewComments", gh_n.clone()),
        (
            "github.pulls.create",
            json!({ "owner": "o", "repo": "r", "title": "t", "body": "b", "head": "h", "base": "b" }),
        ),
        ("github.pulls.checks", gh_n.clone()),
        ("github.pulls.files", gh_n.clone()),
        ("github.pulls.reviews", gh_n.clone()),
        ("github.pulls.get", gh_n.clone()),
        ("github.pulls.list", gh.clone()),
        ("github.pulls.merge", gh_n.clone()),
        ("github.pulls.search", gh.clone()),
        ("github.pulls.updateBranch", gh_n.clone()),
        ("github.relatedRepos.list", gh.clone()),
        (
            "github.replyReviewComment",
            json!({ "owner": "o", "repo": "r", "number": 1, "commentId": 1, "body": "b" }),
        ),
        ("github.repoConfig.get", gh.clone()),
        ("github.repos.get", gh.clone()),
        ("github.repos.list", json!({})),
        ("github.repos.search", json!({ "query": "q" })),
        ("github.resolveThread", json!({ "threadId": "t" })),
        ("github.revoke", json!({})),
        ("github.unresolveThread", json!({ "threadId": "t" })),
        ("github.users.search", json!({ "query": "q" })),
        ("hook.cancel", json!({ "workspaceId": ws, "hookId": "h1" })),
        ("hook.runNow", json!({ "workspaceId": ws, "hookId": "h1" })),
        ("host.executionContext", json!({})),
        ("host.invite.list", json!({})),
        (
            "host.invite.searchAccounts",
            json!({"provider":"github","query":"ab"}),
        ),
        ("host.invite.revoke", json!({ "inviteId": "missing" })),
        ("host.members.list", json!({})),
        ("host.members.remove", json!({"principalId":"missing"})),
        ("linear.authStatus", json!({})),
        (
            "linear.createIssue",
            json!({ "title": "t", "teamId": "team" }),
        ),
        ("linear.getIssue", json!({ "id": "i" })),
        ("linear.listIssues", json!({})),
        ("linear.listLabels", json!({})),
        ("linear.listProjects", json!({})),
        ("linear.listTeams", json!({})),
        ("linear.listWorkflowStates", json!({})),
        ("linear.searchIssues", json!({ "query": "q" })),
        ("linear.updateIssue", json!({ "issueId": "i" })),
        ("linear.viewer", json!({})),
        ("mcp.oauth.delete", sid.clone()),
        ("mcp.oauth.get", sid.clone()),
        ("mcp.oauth.list", json!({})),
        (
            "mcp.oauth.set",
            json!({ "serverId": "srv", "tokenBag": {} }),
        ),
        ("mcp.servers.create", json!({ "config": {} })),
        ("mcp.servers.delete", sid.clone()),
        ("mcp.servers.getStatus", sid.clone()),
        ("mcp.servers.list", json!({})),
        ("mcp.servers.restart", sid.clone()),
        (
            "mcp.servers.toggle",
            json!({ "serverId": "srv", "enabled": false }),
        ),
        (
            "mcp.servers.update",
            json!({ "serverId": "srv", "config": {} }),
        ),
        (
            "mcp.testConnection",
            json!({ "url": "http://127.0.0.1:9/" }),
        ),
        (
            "prMonitor.cancel",
            json!({ "workspaceId": ws, "monitorId": "m1" }),
        ),
        (
            "prMonitor.flush",
            json!({ "workspaceId": ws, "monitorId": "m1" }),
        ),
        ("principal.list", json!({})),
        ("repo.list", json!({})),
        ("repo.remove", json!({ "path": dir })),
        (
            "repo.warmCache",
            json!({ "githubUrl": "https://github.com/o/r" }),
        ),
        ("repoConfig.ensureDir", json!({ "workspaceId": ws })),
        ("repoConfig.get", json!({ "workspaceId": ws })),
        ("repoConfig.has", json!({ "workspaceId": ws })),
        (
            "repoConfig.save",
            json!({ "workspaceId": ws, "config": {} }),
        ),
        ("rules.list", json!({})),
        (
            "rules.update",
            json!({ "workspaceId": ws, "ruleType": "agents", "content": "" }),
        ),
        (
            "sandbox.cow.discard",
            json!({ "workspaceId": ws, "agentId": "a1" }),
        ),
        (
            "sandbox.cow.merge",
            json!({ "workspaceId": ws, "agentId": "a1" }),
        ),
        (
            "script.archive",
            json!({"workspaceId": ws, "scriptIds": ["s1"]}),
        ),
        (
            "script.restore",
            json!({"workspaceId": ws, "scriptIds": ["s1"]}),
        ),
        (
            "script.create",
            json!({ "workspaceId": ws, "name": "n", "command": "true", "mode": "command" }),
        ),
        ("script.remove", script.clone()),
        (
            "scriptMonitor.cancel",
            json!({ "workspaceId": ws, "monitorId": "m1" }),
        ),
        (
            "scriptMonitor.cancelRun",
            json!({ "workspaceId": ws, "monitorId": "m1" }),
        ),
        (
            "providers.profiles.create",
            json!({ "baseProviderId": "codex", "name": "n" }),
        ),
        ("providers.profiles.delete", json!({ "profileId": "p1" })),
        ("providers.profiles.list", json!({})),
        (
            "providers.profiles.reorder",
            json!({ "baseProviderId": "codex", "profileIds": [] }),
        ),
        (
            "providers.profiles.update",
            json!({ "profileId": "p1", "name": "n" }),
        ),
        (
            "providers.profiles.updateDefault",
            json!({ "baseProviderId": "codex", "name": "n" }),
        ),
        ("sentry.assignIssue", json!({ "id": "i" })),
        ("sentry.authStatus", json!({})),
        ("sentry.getIssue", json!({ "id": "i" })),
        ("sentry.ignoreIssue", json!({ "id": "i" })),
        ("sentry.listIssues", json!({})),
        ("sentry.listProjects", json!({})),
        ("sentry.resolveIssue", json!({ "id": "i" })),
        ("sentry.searchIssues", json!({ "query": "q" })),
        ("settings.get", json!({ "path": "model.defaultProvider" })),
        ("settings.list", json!({})),
        ("settings.reset", json!({ "path": "model.defaultProvider" })),
        ("settings.update", json!({ "changes": {} })),
        ("identity.authStatus", json!({"provider":"github"})),
        (
            "identity.cancelAuth",
            json!({"provider":"github", "flowId":"f"}),
        ),
        ("identity.connect", json!({"provider":"github"})),
        ("identity.getUser", json!({"provider":"github"})),
        ("identity.revoke", json!({"provider":"github"})),
        (
            "identity.select",
            json!({"provider":"github", "externalUserId":"1"}),
        ),
        ("sourceControl.authStatus", json!({ "provider": "github" })),
        ("sourceControl.cancelAuth", json!({ "provider": "github" })),
        ("sourceControl.connect", json!({ "provider": "github" })),
        ("sourceControl.getUser", json!({ "provider": "github" })),
        ("sourceControl.gitlab.childGit.get", json!({})),
        (
            "sourceControl.gitlab.childGit.set",
            json!({"binding":{"instanceBaseUrl":"https://forge.test/install",
                "connection":{"connectionId":"original","accountId":"account",
                    "connectionGeneration":"1"},"revision":"1"},"enabled":false}),
        ),
        (
            "sourceControl.identityProof.create",
            json!({ "provider": "github", "nonce": "n", "hostLabel": "h" }),
        ),
        (
            "sourceControl.identityProof.delete",
            json!({ "provider": "github", "proofId": "g" }),
        ),
        (
            "sourceControl.read.resolveKnownRepository",
            json!({ "context": { "kind": "host" }, "readLifetimeId": "original", "path": dir }),
        ),
        ("sourceControl.revoke", json!({ "provider": "github" })),
        ("specialist.create", json!({ "id": "s", "spec": {} })),
        ("specialist.delete", json!({ "id": "s", "scope": "global" })),
        (
            "specialist.edit",
            json!({ "id": "s", "spec": {}, "scope": "global" }),
        ),
        ("terminal.create", json!({ "workspaceId": ws })),
        ("terminal.getBuffer", term.clone()),
        ("terminal.kill", term.clone()),
        ("terminal.list", json!({ "workspaceId": ws })),
        (
            "terminal.readOutput",
            json!({ "workspaceId": ws, "terminalId": "t1" }),
        ),
        ("terminal.resize", term.clone()),
        ("terminal.write", json!({ "terminalId": "t1", "data": "" })),
        ("unsloth.status", json!({})),
        ("unsloth.stop", json!({})),
        ("voice.getWorkspaceVocabulary", json!({ "workspaceId": ws })),
        ("voice.transcribe", json!({ "audio": "AAAA" })),
        ("workspace.archive", json!({ "workspaceId": ws })),
        ("workspace.cancelDelete", json!({ "workspaceId": ws })),
        ("workspace.cleanup", json!({ "workspaceId": ws })),
        (
            "workspace.create",
            json!({ "title": "t", "repositoryPath": format!("{dir}/missing-repo") }),
        ),
        ("workspace.delete", json!({ "workspaceId": ws })),
        ("workspace.detectProjectType", json!({ "workspaceId": ws })),
        ("workspace.diskUsage", json!({ "workspaceId": ws })),
        ("workspace.duplicate", json!({ "workspaceId": ws })),
        ("workspace.export.abort", json!({ "exportId": "e1" })),
        ("workspace.export.finalize", json!({ "exportId": "e1" })),
        (
            "workspace.export.read",
            json!({ "exportId": "e1", "seq": 0 }),
        ),
        ("workspace.export.start", json!({ "workspaceId": ws })),
        ("workspace.findRepositories", json!({ "directory": dir })),
        (
            "workspace.generateSetupScript",
            json!({ "workspaceId": ws }),
        ),
        ("workspace.getBrowserClient", json!({ "workspaceId": ws })),
        ("workspace.getSetupScript", json!({ "workspaceId": ws })),
        ("workspace.import.abort", json!({ "importId": "i1" })),
        (
            "workspace.import.begin",
            json!({ "manifest": {}, "archiveSizeBytes": 1, "archiveSha256": "00" }),
        ),
        (
            "workspace.import.chunk",
            json!({ "importId": "i1", "seq": 0, "data": "" }),
        ),
        ("workspace.import.commit", json!({ "importId": "i1" })),
        (
            "workspace.initializeRepository",
            json!({ "path": format!("{dir}/missing-repo") }),
        ),
        ("workspace.invite.list", json!({ "workspaceId": ws })),
        (
            "workspace.invite.revoke",
            json!({ "workspaceId": ws, "inviteId": "inv" }),
        ),
        (
            "workspace.members.add",
            json!({ "workspaceId": ws, "principalId": "p" }),
        ),
        (
            "workspace.members.remove",
            json!({ "workspaceId": ws, "principalId": "p" }),
        ),
        ("workspace.restore", json!({ "workspaceId": ws })),
        ("workspace.retire", json!({ "workspaceId": ws })),
        (
            "workspace.saveSetupScript",
            json!({ "workspaceId": ws, "script": "" }),
        ),
        (
            "workspace.setAutoCommit",
            json!({ "workspaceId": ws, "enabled": false }),
        ),
        (
            "workspace.setBrowserClient",
            json!({ "workspaceId": ws, "clientId": null }),
        ),
        ("workspace.transfer.plan", json!({ "workspaceId": ws })),
        ("workspace.unarchive", json!({ "workspaceId": ws })),
    ]
}

/// The derived universe: owner-only router methods, sorted.
fn owner_only_router_methods() -> Vec<&'static str> {
    let allowed: std::collections::HashSet<&str> =
        COLLABORATOR_METHODS.iter().map(|(m, _)| *m).collect();
    ROUTER_METHODS
        .iter()
        .copied()
        .filter(|m| !allowed.contains(m))
        .collect()
}

/// `intent_services::capability::ASSERT_BOUND_CALLER_ENV`: armed, an
/// unbound capability gate aborts the process instead of answering
/// `-32003`, which is the very outcome this module observes.
const ASSERT_BOUND_CALLER_ENV: &str = "INTENTD_ASSERT_BOUND_CALLER";

/// CI's coverage jobs run the suite with [`ASSERT_BOUND_CALLER_ENV`]
/// set, so when it is, this re-runs `test` (a name in this module) in a
/// copy of this binary with the variable removed, asserts that it
/// passed, and answers `true` so the caller returns; unarmed (locally,
/// and inside that child) it answers `false` and the caller runs its
/// body inline. A re-exec rather than `remove_var`: the harness is
/// multi-threaded.
fn reran_unarmed(test: &str) -> bool {
    if std::env::var_os(ASSERT_BOUND_CALLER_ENV).is_none() {
        return false;
    }
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args(["--exact", test, "--nocapture"])
        .env_remove(ASSERT_BOUND_CALLER_ENV)
        .output()
        .expect("run unarmed child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "unarmed `{test}`: {:?}\n{stdout}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    true
}

/// One unbound dispatch through the real router; the outcome label is
/// `ok` or the JSON-RPC error code.
async fn dispatch_unbound(services: &Services, method: &str, params: &Value) -> String {
    assert_eq!(
        intent_core::current_caller(),
        None,
        "{method}: caller leaked"
    );
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let raw = handle_message(services, &frame.to_string())
        .await
        .expect("request gets a response");
    let response: Value = serde_json::from_str(&raw).expect("response is JSON");
    match response.get("error").and_then(|e| e["code"].as_i64()) {
        Some(code) => code.to_string(),
        None => "ok".to_string(),
    }
}

#[tokio::test]
async fn every_owner_only_router_method_is_forbidden_unbound() {
    if reran_unarmed("every_owner_only_router_method_is_forbidden_unbound") {
        return;
    }
    let f = fixture().await;
    let table: BTreeMap<&str, Value> = minimal_params(&f).into_iter().collect();
    let universe = owner_only_router_methods();

    // The table and the derived universe must match exactly, so a new
    // owner-only router method cannot escape the sweep.
    let missing: Vec<&str> = universe
        .iter()
        .copied()
        .filter(|m| !table.contains_key(m))
        .collect();
    let stale: Vec<&str> = table
        .keys()
        .copied()
        .filter(|m| !universe.contains(m))
        .collect();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "owner-only router table drift — add a minimal-params row for each new \
         method (missing: {missing:?}); drop rows for methods no longer owner-only \
         or no longer routed (stale: {stale:?})"
    );

    let mut ungated: Vec<(String, String)> = Vec::new();
    for method in &universe {
        let outcome = dispatch_unbound(&f.services, method, &table[method]).await;
        if outcome != "-32003" {
            ungated.push(((*method).to_string(), outcome));
        }
    }
    // The two goldens are disjoint and, merged by method, are exactly
    // what the sweep may observe: a conditionally gated row's *unarmed*
    // outcome and a genuinely ungated method's outcome.
    let mut golden: BTreeMap<String, String> = BTreeMap::new();
    for (m, o, _) in CONDITIONALLY_GATED_AT_SERVICE_LAYER {
        assert!(
            golden.insert((*m).to_string(), (*o).to_string()).is_none(),
            "{m}: duplicated in CONDITIONALLY_GATED_AT_SERVICE_LAYER"
        );
    }
    for (m, o) in UNGATED_AT_SERVICE_LAYER {
        assert!(
            golden.insert((*m).to_string(), (*o).to_string()).is_none(),
            "{m}: named in both CONDITIONALLY_GATED_AT_SERVICE_LAYER and \
             UNGATED_AT_SERVICE_LAYER"
        );
    }
    let golden: Vec<(String, String)> = golden.into_iter().collect();
    if ungated != golden {
        let mut msg = String::from(
            "Unbound owner-only router methods not answering -32003 Forbidden drifted \
             from CONDITIONALLY_GATED_AT_SERVICE_LAYER ∪ UNGATED_AT_SERVICE_LAYER.\n\
             A `-32602` here usually means the table row is not valid enough to reach \
             the service method; an `ok` or other code means the service method \
             returns before its capability gate — move the gate first (see \
             `agent_respond_permission`), or name it in \
             CONDITIONALLY_GATED_AT_SERVICE_LAYER (gate conditional on an optional \
             argument or a live session; add its armed cell) or in \
             UNGATED_AT_SERVICE_LAYER (no gate) with a reason.\n\
             Recomputed (merged) golden:\n",
        );
        for (m, o) in &ungated {
            let _ = writeln!(msg, "    (\"{m}\", \"{o}\"),");
        }
        panic!("{msg}");
    }
}

/// One dispatch through the real router with the daemon caller bound;
/// the decoded response envelope.
async fn dispatch_as_daemon(services: &Services, method: &str, params: &Value) -> Value {
    intent_core::with_caller(intent_core::Caller::Daemon, async {
        let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let raw = handle_message(services, &frame.to_string())
            .await
            .expect("request gets a response");
        serde_json::from_str::<Value>(&raw).expect("response is JSON")
    })
    .await
}

/// Drive the gated `workspace.export.start` as the daemon on the fixture
/// workspace (repo-less, so the build is a manifest and no bundle) and
/// wait for the session to seal: `workspace.export.read` answers
/// `-32602` while building and `ok` once Ready. Returns the `exportId`.
async fn ready_export(f: &Fixture) -> String {
    let start = dispatch_as_daemon(
        &f.services,
        "workspace.export.start",
        &json!({ "workspaceId": f.ws.as_str() }),
    )
    .await;
    let export_id = start["result"]["exportId"]
        .as_str()
        .unwrap_or_else(|| panic!("daemon-bound workspace.export.start: {start}"))
        .to_string();
    let read = json!({ "exportId": export_id, "seq": 0 });
    for _ in 0..400 {
        let response = dispatch_as_daemon(&f.services, "workspace.export.read", &read).await;
        match response.get("error") {
            None => return export_id,
            Some(e) if e["data"]["code"] == json!("not-found") => {
                panic!("export {export_id} failed to build: {response}")
            }
            Some(_) => {
                // timing-guard: poll interval between export.read probes; the session has no ready event to await
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    }
    panic!("export {export_id} did not become Ready");
}

/// Every armed mode of every [`CONDITIONALLY_GATED_AT_SERVICE_LAYER`]
/// row, through the real router into the same unbound `Services`, must
/// be `-32003`:
///
/// - Scope arguments: the sweep's minimal params plus `workspaceId` /
///   `agentId`. The gate runs before any lookup, so an unknown `agentId`
///   is still `Forbidden` unbound, never `NotFound`.
/// - `workspace.export.finalize`: a Ready session (built by the daemon)
///   with `finalStatusMessage` reaches the gated `update_workspace`, and
///   with `archiveSource: true` the gated `archive_workspace`. Both
///   mutations run before the session is retired, so the one session
///   serves both cells and stays Ready after each refusal. The same
///   session then pins the row's live unarmed mode: finalize with
///   neither option runs no gated call, so unbound it is `ok` and
///   retires the session (the next read is `-32602`). Gating that
///   mode is a capability-rule change, out of this table's remit.
///
/// `git.agentCommit` is the one row whose router arm cannot arm the gate:
/// it passes `agent_id = None` unconditionally (the wire shape has no
/// `agentId`; the MCP bridge is the only caller that supplies one). Both
/// halves are asserted — the wire call with an `agentId` keeps the
/// unscoped outcome, and the service method called directly with one is
/// `Forbidden` — so a router arm that starts forwarding `agentId` fails
/// here and moves the cell onto the router path.
#[tokio::test]
async fn ready_export_requires_bound_caller_without_finalize_options() {
    if reran_unarmed("ready_export_requires_bound_caller_without_finalize_options") {
        return;
    }
    let f = fixture().await;
    let export_id = ready_export(&f).await;
    for method in [
        "workspace.export.read",
        "workspace.export.abort",
        "workspace.export.finalize",
    ] {
        let params = json!({"exportId":export_id,"seq":0});
        assert_eq!(
            dispatch_unbound(&f.services, method, &params).await,
            "-32003"
        );
    }
    let response = dispatch_as_daemon(
        &f.services,
        "workspace.export.read",
        &json!({"exportId":export_id,"seq":0}),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
}

#[tokio::test]
async fn armed_conditional_gates_are_forbidden_unbound() {
    if reran_unarmed("armed_conditional_gates_are_forbidden_unbound") {
        return;
    }
    let f = fixture().await;
    let table: BTreeMap<&str, Value> = minimal_params(&f).into_iter().collect();
    for (method, unarmed, arming) in CONDITIONALLY_GATED_AT_SERVICE_LAYER {
        let mut params = table[method].clone();
        let (scope, scope_value) = match arming {
            Arming::WorkspaceId => ("workspaceId", f.ws.as_str().to_string()),
            Arming::AgentId => ("agentId", "a1".to_string()),
        };
        assert!(
            params.get(scope).is_none(),
            "{method}: minimal params already carry `{scope}`, so the sweep is not \
             exercising the unscoped call"
        );
        params[scope] = json!(scope_value);
        let outcome = dispatch_unbound(&f.services, method, &params).await;
        if *method == "git.agentCommit" {
            assert_eq!(
                outcome, *unarmed,
                "git.agentCommit: the router arm now forwards `agentId` — assert \
                 -32003 through the router here and drop the direct call"
            );
            continue;
        }
        assert_eq!(
            outcome, "-32003",
            "{method} with `{scope}` supplied must reach its capability gate unbound"
        );
    }

    assert_eq!(intent_core::current_caller(), None, "caller leaked");
    let direct = f
        .services
        .git_agent_commit(
            f.ws.clone(),
            "m".to_string(),
            Some(AgentId::from("a1")),
            None,
            None,
            false,
            None,
        )
        .await;
    assert!(
        matches!(direct, Err(Error::Forbidden(_))),
        "git_agent_commit with an agentId, unbound: {direct:?}"
    );
}

/// Positive control for the table: the same real router + `Services`
/// answers a bound daemon caller on a gated method, so `-32003` above is
/// the missing binding and not the fixture.
#[tokio::test]
async fn bound_daemon_control_passes_a_gated_method() {
    if reran_unarmed("bound_daemon_control_passes_a_gated_method") {
        return;
    }
    let f = fixture().await;
    let params = json!({ "workspaceId": f.ws.as_str() });
    let unbound = dispatch_unbound(&f.services, "workspace.invite.list", &params).await;
    assert_eq!(unbound, "-32003");
    let bound = intent_core::with_caller(intent_core::Caller::Daemon, async {
        let frame = json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "workspace.invite.list", "params": params,
        });
        let raw = handle_message(&f.services, &frame.to_string())
            .await
            .expect("response");
        serde_json::from_str::<Value>(&raw).expect("json")
    })
    .await;
    assert!(
        bound.get("error").is_none(),
        "daemon-bound workspace.invite.list: {bound}"
    );
}

/// Regression (PR #1877 review): `agent.respondPermission` returned
/// `Ok({ resolved: false })` from its no-manager early return *before*
/// the caller gate, so an unbound context saw a success. The gate now
/// runs first; with no runtime manager (this fixture) the unbound call is
/// `-32003`, while a bound daemon caller keeps the unresolved answer.
#[tokio::test]
async fn unbound_respond_permission_is_forbidden_without_a_manager() {
    if reran_unarmed("unbound_respond_permission_is_forbidden_without_a_manager") {
        return;
    }
    let f = fixture().await;
    let params = json!({ "requestId": "req-1", "outcome": { "outcome": "cancelled" } });
    let unbound = dispatch_unbound(&f.services, "agent.respondPermission", &params).await;
    assert_eq!(unbound, "-32003");
    let bound = intent_core::with_caller(intent_core::Caller::Daemon, async {
        let frame = json!({
            "jsonrpc": "2.0", "id": 1,
            "method": "agent.respondPermission", "params": params,
        });
        let raw = handle_message(&f.services, &frame.to_string())
            .await
            .expect("response");
        serde_json::from_str::<Value>(&raw).expect("json")
    })
    .await;
    assert_eq!(
        bound["result"],
        json!({ "resolved": false }),
        "daemon-bound agent.respondPermission: {bound}"
    );
}

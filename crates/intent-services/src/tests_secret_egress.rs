//! Secret-value egress contract (PROTOCOL §5.56 Verification requirements).
//!
//! Plants a sentinel secret value for agent A through the real request and
//! provide path, then drives every agent-facing egress in
//! [`intent_acp::egress_registry::AGENT_EGRESS_CALLS`] through the real
//! `workspace_api` tool on the real [`Services`], as A and as a second agent B
//! in the same workspace, plus the secret-specific surfaces (`ws.secret.list`,
//! `ws.host.exec` with and without `secretEnv`) and the wake-metadata builder.
//! The test fails naming every egress whose output contains the value.
//!
//! The registry is shared with the hidden-field egress contract in
//! intent-acp, so a new agent-facing egress appended there is covered here
//! too.

#![cfg(test)]

use std::sync::Arc;

use intent_acp::egress_registry::{EgressServer, AGENT_EGRESS_CALLS};
use intent_acp::WorkspaceMcpServer;
use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};

use crate::secret_requests::Outcome;
use crate::test_support::fixtures::{session, workspace, TempDb};
use crate::Services;

/// The planted value. Distinct enough that a substring hit is a real leak.
const SENTINEL: &str = "sentinel-egress-7Hq4-zX9w-never-shown";
const SECRET: &str = "EGRESS_KEY";

fn features() -> AgentFeaturesSettings {
    AgentFeaturesSettings {
        host_exec: true,
        secret_requests: true,
        ..AgentFeaturesSettings::default()
    }
}

/// One `workspace_api` call: the raw tool text (result or error) and whether
/// it was an error. The raw text is what the agent sees either way.
async fn call(server: &WorkspaceMcpServer, code: &str) -> (String, bool) {
    let resp = server
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "secret egress contract" }
            }
        }))
        .await
        .expect("tools/call must produce a response");
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (text, resp["result"]["isError"] == json!(true))
}

struct Fixture {
    services: Arc<Services>,
    ws: WorkspaceId,
    owner: AgentId,
    other: AgentId,
    _tmp: TempDb,
    _root: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let tmp = TempDb::new();
    let root = tempfile::tempdir().expect("workspace root");
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let mut row = workspace(&ws);
    row.path = Some(root.path().to_string_lossy().into_owned());
    row.worktree_path = row.path.clone();
    store.insert_workspace(&row).await.expect("workspace");
    let owner = AgentId::from("agent-secret-owner");
    let other = AgentId::from("agent-secret-other");
    store
        .insert_agent_session(&session(&owner, &ws))
        .await
        .expect("owner");
    store
        .insert_agent_session(&session(&other, &ws))
        .await
        .expect("other");
    let services = Arc::new(Services::new_isolated(store));
    let ticket = services
        .secret_request_op(ws.clone(), owner.clone(), SECRET.into(), "tests".into())
        .await
        .expect("request");
    services
        .secret_answer_op(
            ws.clone(),
            ticket["requestId"].as_str().expect("requestId").to_string(),
            Outcome::Provided,
            Some(zeroize::Zeroizing::new(SENTINEL.to_string())),
        )
        .await
        .expect("provide");
    Fixture {
        services,
        ws,
        owner,
        other,
        _tmp: tmp,
        _root: root,
    }
}

impl Fixture {
    fn server(&self, caller: &AgentId) -> WorkspaceMcpServer {
        let api: Arc<dyn WorkspaceApi> = self.services.clone();
        WorkspaceMcpServer::new(api, self.ws.clone())
            .with_caller_agent_id(Some(caller.clone()))
            .with_agent_features(features())
    }

    fn chief(&self) -> WorkspaceMcpServer {
        let api: Arc<dyn WorkspaceApi> = self.services.clone();
        WorkspaceMcpServer::new(api, WorkspaceId::chief())
            .with_caller_agent_id(Some(AgentId::from("chief-agent")))
    }
}

#[intent_test_macros::daemon_test]
async fn no_secret_value_survives_any_egress() {
    let f = fixture().await;
    let as_owner = f.server(&f.owner);
    let as_other = f.server(&f.other);
    let chief = f.chief();
    let mut leaks: Vec<String> = Vec::new();
    let mut check = |name: &str, text: &str| {
        if text.contains(SENTINEL) {
            leaks.push(format!("{name}: {text}"));
        }
    };

    // Every registered agent-facing egress, reading the owner's data, as the
    // owner and as a second agent in the same workspace.
    for egress in AGENT_EGRESS_CALLS {
        let code = egress.render(&f.owner.0, &f.ws.0);
        match egress.server {
            EgressServer::Workspace => {
                for (who, server) in [("owner", &as_owner), ("other agent", &as_other)] {
                    let (text, is_error) = call(server, &code).await;
                    assert!(
                        !is_error,
                        "{} as {who} failed, so the egress was not exercised: {text}",
                        egress.name
                    );
                    check(&format!("{} as {who}", egress.name), &text);
                }
            }
            // Chief calls may legitimately refuse a non-Chief agent id; the
            // text the agent would see is checked either way.
            EgressServer::Chief => {
                let (text, _) = call(&chief, &code).await;
                check(egress.name, &text);
            }
        }
    }

    // The secret-specific surfaces.
    let (owner_list, err) = call(&as_owner, "return await ws.secret.list();").await;
    assert!(!err, "ws.secret.list as owner: {owner_list}");
    assert!(
        owner_list.contains(SECRET),
        "the owner's listing names the secret: {owner_list}"
    );
    check("ws.secret.list as owner", &owner_list);
    let (other_list, err) = call(&as_other, "return await ws.secret.list();").await;
    assert!(!err, "ws.secret.list as other agent: {other_list}");
    assert!(
        !other_list.contains(SECRET),
        "another agent's listing must not name the owner's secret: {other_list}"
    );
    check("ws.secret.list as other agent", &other_list);

    let echo = format!(
        "return await ws.host.exec({{ command: 'sh', args: ['-c', 'printf \"%s\" \"${SECRET}\"; printf \"%s\" \"${SECRET}\" >&2'], secretEnv: ['{SECRET}'] }});"
    );
    let (used, err) = call(&as_owner, &echo).await;
    assert!(!err, "ws.host.exec with secretEnv as owner: {used}");
    check(
        "ws.host.exec with secretEnv as owner (stdout/stderr)",
        &used,
    );

    let (env_dump, err) = call(
        &as_owner,
        "return await ws.host.exec({ command: 'sh', args: ['-c', 'env'] });",
    )
    .await;
    assert!(!err, "ws.host.exec without secretEnv: {env_dump}");
    check("ws.host.exec without secretEnv (env dump)", &env_dump);

    let (refused, err) = call(&as_other, &echo).await;
    assert!(err, "another agent's secretEnv must be refused: {refused}");
    assert!(
        refused.contains("secret-unavailable"),
        "another agent gets secret-unavailable: {refused}"
    );
    check("ws.host.exec with secretEnv as other agent", &refused);

    // Wake metadata built from the workspace's real events, including the
    // provided notice delivery.
    let events = f
        .services
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(f.ws.clone()),
            limit: Some(500),
            ..Default::default()
        })
        .await
        .expect("events");
    let refs: Vec<&intent_core::Event> = events.iter().collect();
    let metadata: Value = crate::build_event_notification_metadata(&refs);
    check(
        "wake metadata (build_event_notification_metadata)",
        &metadata.to_string(),
    );

    assert!(
        leaks.is_empty(),
        "secret value leaked on {} egress(es):\n  {}",
        leaks.len(),
        leaks.join("\n  ")
    );
    // The secret really reached the command: its output carries the
    // placeholder where the value was.
    assert!(
        used.contains(&format!("[secret:{SECRET}]")),
        "the owner's output shows the redaction placeholder: {used}"
    );
}

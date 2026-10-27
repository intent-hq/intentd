//! End to end through the composed daemon services: the runtime feature
//! (hooks, script monitors) and core's agent side, driven only through the
//! public `WorkspaceApi` handlers.
//!
//! Core's own agent/watch tests run against a runtime test double; these
//! cover the real runtime behaviour those tests stub: a hook run waking its
//! owner, a hook cancel or an agent retire settling the hooks that kept a
//! watch waiting, and a script monitor delivering its result to its owner.
//! Fixture rows (the workspace and the agent sessions) are seeded through
//! the public store API, because creating an agent through `agent.create`
//! needs a configured provider; secrets live in a per-test file.

use std::future::Future;
use std::time::Duration;

use intent_core::{
    now_iso, with_caller, AgentId, AgentSession, AgentStatus, Caller, HookId, Workspace,
    WorkspaceApi, WorkspaceId, CURRENT_HARNESS_VERSION,
};
use intent_services::Services;
use intent_store::Store;
use serde_json::{json, Value};

/// `base` stretched by `INTENTD_TEST_TIMEOUT_MULTIPLIER`, as
/// `intentd_test_support::test_timeout` does (unset or invalid means 1,
/// never below 1, capped at 20), for the hang guards below.
fn hang_guard(base: Duration) -> Duration {
    let multiplier = std::env::var("INTENTD_TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(1.0)
        .clamp(1.0, 20.0);
    base.mul_f64(multiplier)
}

/// Every string value under `value`, one per line: message text and
/// metadata as the agent reads them, with no JSON escaping.
fn collect_strings(value: &Value, out: &mut String) {
    match value {
        Value::String(s) => {
            out.push_str(s);
            out.push('\n');
        }
        Value::Array(items) => items.iter().for_each(|v| collect_strings(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_strings(v, out)),
        _ => {}
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    services: Services,
    workspace: WorkspaceId,
    owner: intent_core::PrincipalId,
    /// `Services` holds the attached manager weakly; keep it alive.
    _agent_manager: Option<std::sync::Arc<intent_services::AgentManager>>,
}

async fn fixture() -> Fixture {
    fixture_with(false, &[]).await
}

/// The fixture with the daemon's agent manager attached, as intentd wires
/// it: queued agent wakes are drained by the manager.
#[cfg(unix)]
async fn fixture_with_agent_manager() -> Fixture {
    fixture_with(true, &[]).await
}

async fn fixture_with(agent_manager: bool, settings: &[(&str, Value)]) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&dir.path().join("e2e.db"))
        .await
        .expect("open store");
    let workspace = WorkspaceId::new();
    let worktree = dir.path().join("worktree");
    std::fs::create_dir_all(&worktree).expect("worktree");
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: workspace.clone(),
            title: "Runtime e2e".into(),
            branch: "main".into(),
            worktree_path: Some(worktree.to_string_lossy().into_owned()),
            created_at: ts.clone(),
            updated_at: ts,
            ..Default::default()
        })
        .await
        .expect("insert workspace");
    let owner = store
        .get_primary_principal()
        .await
        .expect("primary principal")
        .id;
    let bus = intent_services::EventBus::new(store.clone());
    let registry = std::sync::Arc::new(
        intent_services::SettingsRegistry::load(dir.path().join("config.toml"))
            .expect("settings registry"),
    );
    if !settings.is_empty() {
        let updates: Vec<(String, Value)> = settings
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect();
        registry.apply(&updates).expect("apply settings");
    }
    // Repository and collaboration credentials stay in this TempDir: the
    // default `FileSecretStore` would use the developer's own secrets file.
    let secrets = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    let services = intent_app::compose(
        Services::new_repository_fixture(store, secrets, None)
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_settings_registry(registry)
            .with_event_bus(bus.clone()),
    );
    let agent_manager = agent_manager.then(|| {
        let manager = std::sync::Arc::new(intent_services::AgentManager::new(
            services.clone(),
            std::sync::Arc::new(intent_services::BusEventSink::new(bus.clone())),
            2,
        ));
        services.attach_agent_manager(&manager);
        manager
    });
    Fixture {
        _dir: dir,
        services,
        workspace,
        owner,
        _agent_manager: agent_manager,
    }
}

impl Fixture {
    /// Seed an idle agent session, optionally a sub-agent of `parent`.
    async fn agent(&self, name: &str, parent: Option<&AgentId>) -> AgentId {
        let id = AgentId::new();
        let ts = now_iso();
        let session: AgentSession = serde_json::from_value(json!({
            "id": id,
            "workspaceId": self.workspace,
            "parentAgentId": parent,
            "name": name,
            "nameExplicitlySet": true,
            "status": AgentStatus::RuntimeIdle,
            "isActive": false,
            "messages": [],
            "harnessVersion": CURRENT_HARNESS_VERSION,
            "createdAt": ts,
            "updatedAt": ts,
        }))
        .expect("agent session fixture");
        self.services
            .store()
            .insert_agent_session(&session)
            .await
            .expect("insert agent session");
        id
    }

    /// Run `call` as `agent` (the MCP caller identity).
    async fn as_agent<T>(&self, agent: &AgentId, call: impl Future<Output = T>) -> T {
        with_caller(
            Caller::Agent {
                agent_id: agent.clone(),
            },
            call,
        )
        .await
    }

    /// Run `call` as the app: the host owner's wire identity, as the
    /// transport binds it for the local client.
    async fn as_app<T>(&self, call: impl Future<Output = T>) -> T {
        with_caller(
            Caller::Wire {
                principal_id: self.owner.clone(),
                host_role: intent_core::HostRole::Owner,
            },
            call,
        )
        .await
    }

    /// Schedule a hook owned by `agent` through the public handler.
    async fn schedule_hook(&self, agent: &AgentId, name: &str, code: &str) -> Value {
        self.as_agent(
            agent,
            self.services.hook_schedule(
                self.workspace.clone(),
                agent.clone(),
                json!({ "name": name, "code": code, "delayMs": 600_000 }),
            ),
        )
        .await
        .expect("hook.schedule")
    }

    /// The text of every message `agent` has received, joined.
    async fn received(&self, agent: &AgentId) -> String {
        let conversation = self
            .as_app(self.services.agent_get_conversation(
                agent.clone(),
                Some(200),
                Some(self.workspace.clone()),
                None,
                None,
                None,
                None,
                false,
            ))
            .await
            .expect("agent.getConversation");
        let queue = self
            .as_app(
                self.services
                    .agent_get_queue(agent.clone(), Some(self.workspace.clone())),
            )
            .await
            .expect("agent.getQueue");
        let mut text = String::new();
        collect_strings(&conversation, &mut text);
        collect_strings(&queue, &mut text);
        text
    }

    /// Wait until `agent` has received a message containing `needle`.
    async fn wait_received(&self, agent: &AgentId, needle: &str) -> String {
        let deadline = tokio::time::Instant::now() + hang_guard(Duration::from_secs(30));
        loop {
            let text = self.received(agent).await;
            if text.contains(needle) {
                return text;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{agent} never received {needle:?}; got: {text}"
            );
            // timing-guard: poll interval of this wait loop; the scaled deadline above bounds it
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn subscriptions(&self, agent: &AgentId) -> String {
        self.as_app(
            self.services
                .agent_get_subscriptions(self.workspace.clone(), agent.clone()),
        )
        .await
        .expect("agent.getSubscriptions")
        .to_string()
    }

    async fn hook_state(&self, hook_id: &str) -> String {
        let listed = self
            .as_app(self.services.hook_list(self.workspace.clone(), None, true))
            .await
            .expect("hook.list");
        listed["hooks"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|h| h["hookId"] == hook_id)
            .map_or_else(
                || "absent".into(),
                |h| h["state"].as_str().unwrap_or("").into(),
            )
    }

    async fn waiting_on_hooks(&self, agent: &AgentId) -> Vec<Value> {
        let got = serde_json::to_value(
            self.as_app(
                self.services
                    .agent_get(agent.clone(), Some(self.workspace.clone())),
            )
            .await
            .expect("agent.get"),
        )
        .expect("serialize agent");
        got["agent"]["waitingOnHooks"]
            .as_array()
            .or_else(|| got["waitingOnHooks"].as_array())
            .cloned()
            .unwrap_or_default()
    }
}

/// A hook whose run dispatches wakes its owning agent with the message.
#[tokio::test]
async fn hook_dispatch_wakes_its_owner() {
    let f = fixture().await;
    let owner = f.agent("Owner", None).await;
    f.schedule_hook(
        &owner,
        "pinger",
        "return { dispatch: true, message: 'e2e hook says hi' };",
    )
    .await;
    let text = f.wait_received(&owner, "e2e hook says hi").await;
    assert!(text.contains("pinger"), "wake names the hook: {text}");
}

/// While a hook is active its owner reports `waitingOnHooks`; an app cancel
/// ends the hook, clears the waiting reason and wakes the owner.
#[tokio::test]
async fn app_hook_cancel_clears_waiting_and_wakes_the_owner() {
    let f = fixture().await;
    let owner = f.agent("Owner", None).await;
    let hook = f
        .schedule_hook(&owner, "watcher", "return { dispatch: false };")
        .await;
    let hook_id = hook["hook"]["hookId"].as_str().expect("hookId").to_owned();
    assert_eq!(f.hook_state(&hook_id).await, "scheduled");
    let waiting = f.waiting_on_hooks(&owner).await;
    assert_eq!(
        waiting.len(),
        1,
        "active hook is a waiting reason: {waiting:?}"
    );
    assert_eq!(waiting[0]["name"], "watcher");

    f.as_app(
        f.services
            .hook_cancel(f.workspace.clone(), HookId::from(hook_id.as_str()), None),
    )
    .await
    .expect("hook.cancel");
    assert_eq!(f.hook_state(&hook_id).await, "cancelled");
    assert!(f.waiting_on_hooks(&owner).await.is_empty());
    f.wait_received(&owner, "watcher").await;
}

/// A parent's watch on a child that holds a hook shows the hook as the
/// child's waiting reason; when the child cancels the hook the reason clears,
/// the watch stays armed, and the child's report then reaches the parent.
#[tokio::test]
async fn watch_tracks_a_childs_hook_and_delivers_after_it_ends() {
    let f = fixture().await;
    let parent = f.agent("Parent", None).await;
    let child = f.agent("Child", Some(&parent)).await;
    let hook = f
        .schedule_hook(&child, "ci-poll", "return { dispatch: false };")
        .await;
    let hook_id = hook["hook"]["hookId"].as_str().expect("hookId").to_owned();
    f.as_agent(
        &parent,
        f.services
            .agent_watch(f.workspace.clone(), parent.clone(), child.clone()),
    )
    .await
    .expect("agent.watch");
    let subscriptions = f.subscriptions(&parent).await;
    assert!(
        subscriptions.contains(child.as_str()) && subscriptions.contains("ci-poll"),
        "the watch is armed and the child waits on its hook: {subscriptions}"
    );

    f.as_agent(
        &child,
        f.services.hook_cancel(
            f.workspace.clone(),
            HookId::from(hook_id.as_str()),
            Some(child.clone()),
        ),
    )
    .await
    .expect("owner cancels its hook");
    let subscriptions = f.subscriptions(&parent).await;
    assert!(
        subscriptions.contains(child.as_str()) && !subscriptions.contains("ci-poll"),
        "the watch survives the hook's end and the waiting reason clears: {subscriptions}"
    );

    f.as_agent(
        &child,
        f.services.agent_report_to_parent(
            f.workspace.clone(),
            json!({ "report": "e2e child finished after its hook" }),
            Some(child.clone()),
        ),
    )
    .await
    .expect("agent.reportToParent");
    f.wait_received(&parent, "e2e child finished after its hook")
        .await;
}

/// `agent.unwatch` removes a watch on a hook-holding child, so the hook's end
/// wakes nobody but the child.
#[tokio::test]
async fn unwatch_before_the_hook_ends_delivers_nothing_to_the_parent() {
    let f = fixture().await;
    let parent = f.agent("Parent", None).await;
    let child = f.agent("Child", Some(&parent)).await;
    let hook = f
        .schedule_hook(&child, "ci-poll", "return { dispatch: false };")
        .await;
    let hook_id = hook["hook"]["hookId"].as_str().expect("hookId").to_owned();
    f.as_agent(
        &parent,
        f.services
            .agent_watch(f.workspace.clone(), parent.clone(), child.clone()),
    )
    .await
    .expect("agent.watch");
    let removed = f
        .as_agent(
            &parent,
            f.services.agent_unwatch(
                f.workspace.clone(),
                parent.clone(),
                None,
                Some(child.clone()),
            ),
        )
        .await
        .expect("agent.unwatch");
    assert_eq!(removed["ok"], json!(true), "{removed}");

    f.as_app(
        f.services
            .hook_cancel(f.workspace.clone(), HookId::from(hook_id.as_str()), None),
    )
    .await
    .expect("hook.cancel");
    f.wait_received(&child, "ci-poll").await;
    // timing-guard: grace for a stray parent wake — none is expected, and the child's own wake already arrived
    tokio::time::sleep(hang_guard(Duration::from_millis(300))).await;
    assert!(
        !f.received(&parent).await.contains(child.as_str()),
        "an unwatched child's hook end does not wake the parent"
    );
}

/// Retiring an agent cancels its active hooks.
#[tokio::test]
async fn retiring_an_agent_cancels_its_hooks() {
    let f = fixture().await;
    let owner = f.agent("Owner", None).await;
    let hook = f
        .schedule_hook(&owner, "poller", "return { dispatch: false };")
        .await;
    let hook_id = hook["hook"]["hookId"].as_str().expect("hookId").to_owned();
    f.as_app(
        f.services
            .agent_retire(owner.clone(), Some(f.workspace.clone()), None),
    )
    .await
    .expect("agent.retire");
    assert_eq!(f.hook_state(&hook_id).await, "cancelled");
}

/// A script monitor delivers its settled result to the owning agent.
#[cfg(unix)]
#[tokio::test]
async fn script_monitor_delivers_the_run_result_to_its_owner() {
    let f = fixture_with_agent_manager().await;
    let owner = f.agent("Owner", None).await;
    let script = f
        .as_app(f.services.script_create(
            f.workspace.clone(),
            intent_core::ScriptCreateParams {
                name: "E2E monitored".into(),
                command: "echo e2e-monitored-output".into(),
                mode: intent_core::ScriptMode::Command,
                ..Default::default()
            },
        ))
        .await
        .expect("script.create");
    let script_id = script["id"].as_str().expect("script id").to_owned();
    let started = f
        .as_agent(
            &owner,
            f.services
                .script_start(f.workspace.clone(), script_id.clone()),
        )
        .await
        .expect("script.start");
    let run_id = started["runId"]
        .as_str()
        .unwrap_or_else(|| panic!("script.start reports the run: {started}"))
        .to_owned();
    let monitor = f
        .as_agent(
            &owner,
            f.services.script_start_monitor(
                f.workspace.clone(),
                owner.clone(),
                script_id,
                run_id.clone(),
            ),
        )
        .await
        .expect("arm the start monitor");
    assert_eq!(monitor["ok"], json!(true), "{monitor}");
    let text = f
        .wait_received(&owner, &format!("run {run_id}: finished"))
        .await;
    assert!(
        text.contains(r#""outcome":"succeeded""#) && text.contains(r#""exitCode":0"#),
        "the wake carries the settled run result: {text}"
    );
}

/// A loopback GitLab answering the PAT validation `identity.connect` makes.
async fn mock_gitlab() -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock gitlab");
    let origin = format!("http://{}", listener.local_addr().expect("mock addr"));
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            if reader.read_line(&mut request).await.is_err() {
                continue;
            }
            let mut header = String::new();
            while reader.read_line(&mut header).await.is_ok_and(|n| n > 0) && header != "\r\n" {
                header.clear();
            }
            let body = if request.contains("/api/v4/personal_access_tokens/self") {
                json!({ "id": 7, "scopes": ["api"] })
            } else if request.contains("/api/v4/user") {
                json!({ "id": 42, "username": "e2e-user", "name": "E2E User" })
            } else {
                json!({})
            }
            .to_string();
            let _ = reader
                .get_mut()
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
        }
    });
    (origin, server)
}

/// A collaboration credential connected through `identity.connect` (stored in
/// the scoped collaboration store) never reaches the host execution context:
/// execution reads only repository-purpose credentials.
#[tokio::test]
async fn execution_context_ignores_a_connected_collaboration_credential() {
    const SECRET: &str = "glpat-e2e-collaboration-secret";
    const HOST: &str = "gitlab.e2e.test";
    let (origin, server) = mock_gitlab().await;
    let f = fixture_with(
        false,
        &[
            // Only stored credentials count, not the test host's gh CLI or env.
            ("sourceControl.github.tokenSource", json!("explicit")),
            ("sourceControl.gitlab.host", json!(HOST)),
            ("sourceControl.gitlab.apiBaseUrl", json!(origin)),
        ],
    )
    .await;
    f.as_app(f.services.identity_connect(
        "gitlab".into(),
        Some(HOST.into()),
        Some("pat".into()),
        Some(SECRET.into()),
    ))
    .await
    .expect("identity.connect");
    let status = f
        .as_app(
            f.services
                .identity_auth_status("gitlab".into(), Some(HOST.into())),
        )
        .await
        .expect("identity.authStatus");
    assert!(
        status.to_string().contains("e2e-user"),
        "the collaboration identity is connected: {status}"
    );

    let context = f
        .as_app(f.services.host_execution_context())
        .await
        .expect("host.executionContext");
    let connections = context["repositoryConnections"]
        .as_array()
        .unwrap_or_else(|| panic!("repository connections: {context}"));
    assert!(!connections.is_empty(), "{context}");
    for connection in connections {
        assert_eq!(connection["configured"], json!(false), "{context}");
    }
    assert!(
        !context.to_string().contains(SECRET),
        "the collaboration secret never appears: {context}"
    );
    server.abort();
}

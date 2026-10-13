//! Real Services creation, settings, Store, Git and manager ACP output. The
//! existing deterministic mock is an ordinary ACP peer, not a native provider
//! or evidence of a provider-owned loop or a system/developer-role update.

use super::*;
use crate::agent_manager::{AgentManager, BusEventSink, StartedSession, TurnOptions};
use crate::events::EventBus;
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use intent_core::{AgentId, AgentSession, FileSecretStore};
use intent_store::RepositorySelectionChange;
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const FACTS: &str = "[Repository facts — inert JSON lines]";
const POLICY: &str =
    include_str!("../../../../resources/agent-instructions/v3.2/repository-guidance.md");
const WAIT: Duration = Duration::from_secs(15);

struct Harness {
    git: GitFixture,
    services: Arc<Services>,
    manager: Arc<AgentManager>,
    row: AgentSession,
    _env: crate::agent_manager::tests::EnvGuard,
}

impl Harness {
    async fn new(stamp: Option<&str>, session_mcp: bool) -> Self {
        Self::with_behavior(
            stamp,
            session_mcp,
            &json!({"loadSession":true,"response":"ordinary completion"}),
        )
        .await
    }

    async fn with_behavior(stamp: Option<&str>, session_mcp: bool, behavior: &Value) -> Self {
        let git = GitFixture::new().await;
        let original_script = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../intentd/tests/fixtures/mock-acp-agent.mjs")
            .canonicalize()
            .unwrap();
        let logs = git.dir.path();
        // Ordinary ACP fixture behavior only: a fresh child creates a distinct
        // opaque session; load preserves the requested session. Reusing the
        // fixture's fixed ID on recreate correctly supplies no new owner.
        let script = logs.join("ordinary-acp.mjs");
        let source = std::fs::read_to_string(original_script).unwrap();
        assert!(source.contains("const SESSION_ID = 'mock-session-1';"));
        assert_eq!(source.matches("case 'session/load':").count(), 1);
        let source = source
            .replace(
                "const SESSION_ID = 'mock-session-1';",
                "let SESSION_ID = `ordinary-session-${process.pid}`;",
            )
            .replace(
                "case 'session/load':",
                "case 'session/load':\n\
                 if (behavior.rejectLoad) return send({jsonrpc:'2.0',id:msg.id,\
                 error:{code:-32603,message:'saved session unavailable'}});\n\
                 SESSION_ID = msg.params.sessionId;",
            );
        std::fs::write(&script, source).unwrap();
        let behavior = behavior.to_string();
        let env = crate::agent_manager::tests::EnvGuard::apply(&[
            ("MOCK_AGENT_SCRIPT_PATH", script.to_str()),
            (
                "MOCK_AGENT_SESSION_MCP",
                Some(if session_mcp { "1" } else { "0" }),
            ),
            ("MOCK_AGENT_BEHAVIOR", Some(&behavior)),
            ("MOCK_AGENT_RPC_LOG", logs.join("rpc.jsonl").to_str()),
            ("MOCK_AGENT_PROMPT_LOG", logs.join("prompts.jsonl").to_str()),
            (
                "MOCK_AGENT_SESSION_LOG",
                logs.join("sessions.jsonl").to_str(),
            ),
            ("MOCK_AGENT_ATTEMPT_FILE", logs.join("attempt").to_str()),
            ("GIT_CONFIG_GLOBAL", logs.join("global").to_str()),
            ("GIT_CONFIG_SYSTEM", logs.join("system").to_str()),
            ("GIT_CONFIG_COUNT", None),
            ("GIT_DIR", None),
            ("GIT_WORK_TREE", None),
            ("GIT_COMMON_DIR", None),
            ("INTENTD_TRANSIENT_PROMPT_RETRY_BASE_MS", Some("1")),
        ]);
        let registry = Arc::new(crate::SettingsRegistry::load(logs.join("config.toml")).unwrap());
        registry
            .apply(&[
                ("model.defaultProvider".into(), json!("mock")),
                (
                    "sourceControl.github.exposeGitCredentialToChildren".into(),
                    json!(false),
                ),
            ])
            .unwrap();
        let bus = EventBus::new(git.store.clone());
        let services = Arc::new(
            Services::new_with_file_secrets(
                git.store.clone(),
                FileSecretStore::with_path(logs.join("secrets.json")),
            )
            .with_settings_registry(registry)
            .with_event_bus(bus.clone())
            .with_workspaces_root(logs.join("workspaces")),
        );
        // Genuine boot metadata, with an empty private secret file. No account,
        // provider request or injected successful authorization is involved.
        services
            .initialize_gitlab_repository_binding()
            .await
            .unwrap();
        let row = if let Some(stamp) = stamp {
            let mut saved = json!({
                "id":AgentId::new(),"workspaceId":git.workspace.id,
                "name":"historical envelope control","provider":"mock","status":"idle",
                "harnessFeatures":{"stateSnapshot":false},
                "createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
            });
            if stamp != "<missing>" {
                saved["harnessVersion"] = json!(stamp);
            }
            let row: AgentSession = serde_json::from_value(saved).unwrap();
            services.store.insert_agent_session(&row).await.unwrap();
            row
        } else {
            let created = services
                .agent_create_op(
                    git.workspace.id.clone(),
                    Some("ordinary guidance".into()),
                    None,
                    None,
                    None,
                    None,
                    false,
                    intent_core::AgentCreateExtra {
                        provider: Some("mock".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(created["agent"]["harnessVersion"], "3.2");
            services
                .store
                .get_agent_session(&AgentId::from(created["agent"]["id"].as_str().unwrap()))
                .await
                .unwrap()
        };
        let manager = Arc::new(
            AgentManager::new(
                services.as_ref().clone(),
                Arc::new(BusEventSink::new(bus)),
                4,
            )
            .with_mcp_bridge_exe(logs.join("unused-native-bridge"))
            .with_agent_config_root(logs.join("agent-config")),
        );
        services.attach_agent_manager(&manager);
        Self {
            git,
            services,
            manager,
            row,
            _env: env,
        }
    }

    async fn daemon<T: Send + 'static>(
        &self,
        body: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T {
        intent_core::spawn_daemon(crate::host_execution::background_execution(
            self.services.as_ref().clone(),
            None,
            body,
        ))
        .await
        .unwrap()
    }

    async fn start(&self) -> StartedSession {
        let manager = self.manager.clone();
        let row = self.row.clone();
        self.daemon(async move {
            manager
                .ensure_started_owned(&row.id, &row.workspace_id)
                .await
                .unwrap()
        })
        .await
    }

    async fn dispatch(
        &self,
        started: StartedSession,
        text: &str,
    ) -> intent_core::Result<intent_acp::session::StopReason> {
        let manager = self.manager.clone();
        let row = self.row.clone();
        let text = text.to_owned();
        self.daemon(async move {
            let prompt = manager
                .build_turn_prompt(&row.id, &row.workspace_id, &text, &TurnOptions::default())
                .await?;
            manager
                .run_turn_owned(
                    &row.id,
                    &row.workspace_id,
                    &started.session_id,
                    crate::provider_images::PreparedPrompt::new(prompt).await?,
                    None,
                    started.turn,
                )
                .await
        })
        .await
    }

    async fn turn(&self, text: &str) {
        self.dispatch(self.start().await, text).await.unwrap();
    }

    fn records(&self, file: &str) -> Vec<Value> {
        std::fs::read_to_string(self.git.dir.path().join(file))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn guidance(&self) -> Vec<String> {
        self.records("prompts.jsonl")
            .iter()
            .filter_map(|row| row["blocks"].as_array())
            .flatten()
            .filter_map(|block| block["text"].as_str())
            .filter(|text| text.contains(FACTS))
            .map(str::to_owned)
            .collect()
    }

    fn origin(&self) -> Arc<RepositoryOrigin> {
        self.manager.handles.lock().unwrap()[&self.row.id]
            .repository_origin
            .clone()
    }

    async fn select(&self, choice: RepositorySelectionChange) {
        let snapshot = self
            .services
            .store
            .repository_selection_snapshot(&self.git.root())
            .await
            .unwrap();
        self.services
            .store
            .write_repository_selection(&snapshot, choice)
            .await
            .result
            .unwrap();
    }

    #[expect(
        clippy::used_underscore_binding,
        reason = "observe the actual owned bridge address"
    )]
    async fn tool(&self, code: &str) -> Value {
        let address = self.manager.handles.lock().unwrap()[&self.row.id]
            .execution
            .local
            .as_ref()
            .unwrap()
            .resources
            .lock()
            .unwrap()
            ._mcp_bridge
            .as_ref()
            .unwrap()
            .connect_addr();
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
            "name":"workspace_api","arguments":{"code":code,"summary":"ordinary output control"}
        }});
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(WAIT, read.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }

    async fn retire(&self) {
        self.manager.kill_child_only(&self.row.id).await;
    }

    async fn finish(self) {
        self.manager.shutdown().await;
    }
}

fn root_facts(text: &str) -> Value {
    let (_, facts) = text.split_once(FACTS).expect("actual guidance block");
    let mut roots: Vec<_> = facts
        .lines()
        .filter(|line| line.starts_with('{'))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|row| row.get("root").is_some())
        .collect();
    assert_eq!(roots.len(), 1, "one actual repository root: {facts}");
    roots.remove(0)
}

fn assert_github_facts(target: &Value, project: &str) {
    assert_eq!(
        target["target"],
        json!({
            "provider":"github","instanceBaseUrl":"https://github.com","projectPath":project
        })
    );
    assert_eq!(target["availability"], "unknown");
    assert!(target.get("connection").is_none());
    let capabilities: Vec<_> = [
        "read-review",
        "read-issue",
        "create-review",
        "clone",
        "fetch",
        "push",
    ]
    .into_iter()
    .map(|operation| json!({"operation":operation,"state":"unknown"}))
    .collect();
    assert_eq!(target["capabilities"], json!(capabilities));
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_normal_creation_and_continuation_refresh_actual_inventory() {
    for session_mcp in [false, true] {
        let h = Harness::new(None, session_mcp).await;
        h.turn("local work without a forge").await;
        let first = h.guidance();
        assert_eq!(first.len(), 1);
        assert!(first[0].starts_with(POLICY));
        let facts = root_facts(&first[0]);
        assert_eq!(facts["targets"], json!([]));
        assert_eq!(
            facts["reviewSelection"]["outcome"],
            json!({
                "state":"repository-unavailable","reason":"no-remote","selectionRequired":false
            })
        );
        let original = h.origin();
        let connection = h.manager.handles.lock().unwrap()[&h.row.id]
            .execution
            .connection()
            .unwrap();
        {
            let state = original.state.lock().unwrap();
            assert!(state.endpoint.is_none());
            let context = state.prompt_context.as_ref().unwrap();
            assert!(!context.acknowledged.load(Ordering::SeqCst));
            assert!(!context.requires_callback_ack);
            assert!(context.owner.is_ok());
        }
        h.git.git(
            &h.git.path,
            &[
                "remote",
                "add",
                "upstream",
                "https://github.com/team/project.git",
            ],
        );
        h.turn("continue after an external remote change").await;
        let next = h.guidance();
        assert_eq!(next.len(), 2);
        let facts = root_facts(&next[1]);
        assert_eq!(facts["targets"].as_array().unwrap().len(), 1);
        assert_github_facts(&facts["targets"][0], "team/project");
        assert_eq!(facts["reviewSelection"]["outcome"]["state"], "resolved");
        assert!(Arc::ptr_eq(&h.origin(), &original));
        assert!(Arc::ptr_eq(
            &h.manager.handles.lock().unwrap()[&h.row.id]
                .execution
                .connection()
                .unwrap(),
            &connection
        ));
        assert!(h
            .records("rpc.jsonl")
            .iter()
            .all(|r| r["method"] != "_intent/session/register_mcp_callback"));
        let ordinary = h
            .tool("return 'completed ordinary workspace operation'")
            .await;
        assert!(
            ordinary
                .to_string()
                .contains("completed ordinary workspace operation"),
            "{ordinary}"
        );
        assert!(!ordinary.to_string().contains(FACTS));
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_mixed_targets_and_saved_choice_remain_explicit() {
    let h = Harness::new(None, true).await;
    h.git.git(
        &h.git.path,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/team/project.git",
        ],
    );
    h.git.git(
        &h.git.path,
        &[
            "remote",
            "add",
            "gitlab",
            "https://gitlab.com/group/project.git",
        ],
    );
    h.turn("inspect the mixed repository").await;
    let facts = h.guidance();
    assert_eq!(facts.len(), 1);
    let root = root_facts(&facts[0]);
    let targets = root["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 2);
    assert_github_facts(
        targets
            .iter()
            .find(|row| row["target"]["provider"] == "github")
            .unwrap(),
        "team/project",
    );
    let gitlab = targets
        .iter()
        .find(|row| row["target"]["provider"] == "gitlab")
        .unwrap();
    assert_eq!(
        gitlab["target"],
        json!({"provider":"gitlab", "instanceBaseUrl":"https://gitlab.com", "projectPath":"group/project"})
    );
    // Cold boot without an account has not established a disconnected proof.
    assert_eq!(gitlab["availability"], "unknown");
    assert!(gitlab.get("connection").is_none());
    assert_eq!(
        root["reviewSelection"]["outcome"],
        json!({
            "state":"selection-required","reason":"ambiguous-targets"
        })
    );
    h.select(RepositorySelectionChange::ExplicitRemote {
        remote_name: "gitlab".into(),
    })
    .await;
    h.turn("continue after an explicit selection").await;
    let facts = h.guidance();
    assert_eq!(facts.len(), 2);
    let selected = root_facts(&facts[1]);
    assert_eq!(
        selected["reviewSelection"]["outcome"],
        json!({
            "state":"resolved","source":"explicit-remote","target":gitlab["target"]
        })
    );
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_resume_and_recreation_use_original_new_capture() {
    let h = Harness::new(None, true).await;
    h.turn("first ordinary turn").await;
    let old = h.origin();
    h.retire().await;
    h.turn("resumed ordinary turn").await;
    assert!(!Arc::ptr_eq(&old, &h.origin()));
    assert!(h
        .records("rpc.jsonl")
        .iter()
        .any(|r| r["method"] == "session/load"));
    assert_eq!(h.guidance().len(), 2);
    h.retire().await;
    h.manager
        .force_recreate
        .lock()
        .unwrap()
        .insert(h.row.id.clone());
    h.turn("recovery with a fresh provider session").await;
    assert_eq!(h.guidance().len(), 3);
    let sessions = h.records("sessions.jsonl");
    assert_eq!(
        sessions
            .iter()
            .map(|r| r["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["session/new", "session/load", "session/new"]
    );
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_retired_capture_cannot_enrich_a_replacement() {
    let h = Harness::new(None, true).await;
    let started = h.start().await;
    let original = started.turn.origin.clone();
    let mut captured = started.turn.prompt.unwrap();
    h.retire().await;
    h.daemon(async move {
        assert!(captured.prepare().await.is_none());
        captured.recapture();
        assert!(captured.prepare().await.is_none());
    })
    .await;
    h.turn("fresh recovery").await;
    assert!(!Arc::ptr_eq(&original, &h.origin()));
    assert_eq!(h.guidance().len(), 1);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_failed_load_recreates_with_a_distinct_original_owner() {
    let h = Harness::with_behavior(
        None,
        true,
        &json!({
            "loadSession":true,"rejectLoad":true,"response":"ordinary completion"
        }),
    )
    .await;
    h.turn("before losing the saved provider session").await;
    let original = h.origin();
    let old_id = h
        .services
        .store
        .get_agent_session(&h.row.id)
        .await
        .unwrap()
        .acp_session_id;
    h.retire().await;
    h.turn("recover after the provider rejects load").await;
    let new_id = h
        .services
        .store
        .get_agent_session(&h.row.id)
        .await
        .unwrap()
        .acp_session_id;
    assert_ne!(new_id, old_id);
    assert!(!Arc::ptr_eq(&original, &h.origin()));
    assert_eq!(h.guidance().len(), 2);
    let methods: Vec<_> = h
        .records("rpc.jsonl")
        .into_iter()
        .filter_map(|row| row["method"].as_str().map(str::to_owned))
        .filter(|method| method == "session/new" || method == "session/load")
        .collect();
    assert_eq!(methods, ["session/new", "session/load", "session/new"]);
    h.finish().await;
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_output_free_retry_recaptures_but_streamed_failure_never_replays() {
    for streamed in [false, true] {
        let mut behavior = json!({
            "loadSession":true,"response":"ordinary completion","promptRpcErrorAttempts":1,
            "promptRpcError":{"code":-32603,"message":"Internal error","data":"fetch failed (ECONNRESET)"}
        });
        if streamed {
            behavior["streamBeforeErrorText"] = json!("already visible provider output");
        }
        let h = Harness::with_behavior(None, true, &behavior).await;
        let started = h.start().await;
        let original = started.turn.origin.clone();
        let connection = started.turn.connection.clone();
        let result = h
            .dispatch(started, "one ordinary turn with a provider fetch error")
            .await;
        assert_eq!(result.is_ok(), !streamed, "{result:?}");
        let expected = if streamed { 1 } else { 2 };
        assert_eq!(h.records("prompts.jsonl").len(), expected);
        // The first prepared capture is one-use. An envelope on the retry
        // therefore requires the original retry boundary to capture anew.
        assert_eq!(h.guidance().len(), expected);
        assert!(Arc::ptr_eq(&original, &h.origin()));
        assert!(Arc::ptr_eq(
            connection.as_ref().unwrap(),
            &h.manager.handles.lock().unwrap()[&h.row.id]
                .execution
                .connection()
                .unwrap()
        ));
        assert_eq!(h.records("sessions.jsonl").len(), 1);
        h.finish().await;
    }
}

#[intent_test_macros::daemon_test]
async fn prompt_envelope_old_outbound_turn_resume_recovery_and_tools_keep_pinned_policy() {
    for stamp in ["<missing>", "2.9", "2.10", "3.0", "3.1", "unknown-future"] {
        let h = Harness::new(Some(stamp), true).await;
        h.turn("historical ordinary turn").await;
        let before = h.services.store.get_agent_session(&h.row.id).await.unwrap();
        assert!(!before
            .system_prompt
            .as_deref()
            .unwrap_or_default()
            .contains(POLICY));
        let response = h.tool("return 'historical ordinary tool response'").await;
        assert!(
            response
                .to_string()
                .contains("historical ordinary tool response"),
            "{response}"
        );
        assert!(!response.to_string().contains(FACTS));
        assert!(h.origin().state.lock().unwrap().prompt_context.is_none());
        h.retire().await;
        h.turn("historical resumed turn").await;
        h.retire().await;
        h.manager
            .force_recreate
            .lock()
            .unwrap()
            .insert(h.row.id.clone());
        h.turn("historical recovery turn").await;
        assert!(h.guidance().is_empty());
        for record in h.records("rpc.jsonl") {
            assert!(!record.to_string().contains(FACTS), "{stamp}: {record}");
            assert!(!record.to_string().contains("qualified review read"));
        }
        let after = h.services.store.get_agent_session(&h.row.id).await.unwrap();
        assert_eq!(after.harness_version, before.harness_version);
        assert_eq!(after.harness_features, before.harness_features);
        assert_eq!(after.system_prompt, before.system_prompt);
        assert_eq!(h.records("prompts.jsonl").len(), 3);
        h.finish().await;
    }
}

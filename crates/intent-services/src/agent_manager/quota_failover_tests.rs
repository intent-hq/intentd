//! Automatic quota failover to backup profiles (PROTOCOL §5.54 Backup
//! failover), driven through the real turn worker against the mock ACP agent
//! standing in for `claude-code` (`MOCK_AGENT_STANDS_IN_FOR`). The mock's
//! attempt counter (`MOCK_AGENT_ATTEMPT_FILE`) persists across spawns, so
//! "the first N attempts hit their quota" models the Default configuration
//! and then each backup in turn running out.

use super::dead_child_respawn_tests::mock_agent_script;
use super::role_reminder_tests::{session, workspace};
use super::*;
use crate::agent_manager::tests::EnvGuard;
use crate::events::{EventBus, SubscriptionFilter};
use intent_store::Store;

const WS: &str = "ws-quota-failover";

/// The bridge-wrapped 429 shape `intent_acp::is_quota_exceeded` classifies.
fn quota_error() -> Value {
    json!({
        "code": -32603,
        "message": "Internal error",
        "data": "{\"type\":\"rate_limit_error\",\"message\":\"usage limit reached\"}",
    })
}

struct Fixture {
    mgr: Arc<AgentManager>,
    bus: EventBus,
    agent: AgentId,
    ws: WorkspaceId,
    _db: tempfile::TempDir,
    scratch: tempfile::TempDir,
    _env: EnvGuard,
}

impl Fixture {
    /// A `claude-code` Default agent (model `opus`) with the given mock
    /// behavior and a fresh attempt counter.
    async fn new(behavior: Value) -> Self {
        Self::build(behavior, Some("opus"), None).await
    }

    /// A Default agent with the given stored model (`None` = provider
    /// default) and an optional session-open result (`MOCK_AGENT_SESSION_RESULT`).
    async fn build(behavior: Value, model: Option<&str>, session_result: Option<Value>) -> Self {
        let script = mock_agent_script();
        let session_result = session_result.map(|v| v.to_string());
        let scratch = crate::test_support::test_tempdir("quota-failover-");
        let attempts = scratch.path().join("attempts");
        let prompts = scratch.path().join("prompts.jsonl");
        let behavior = behavior.to_string();
        let env = EnvGuard::apply(&[
            ("MOCK_AGENT_SCRIPT_PATH", Some(script.as_str())),
            ("MOCK_AGENT_PROMPT_LOG", prompts.to_str()),
            ("MOCK_AGENT_BEHAVIOR", Some(behavior.as_str())),
            ("MOCK_AGENT_ATTEMPT_FILE", attempts.to_str()),
            ("MOCK_AGENT_STANDS_IN_FOR", Some("claude-code")),
            ("MOCK_AGENT_SESSION_RESULT", session_result.as_deref()),
            ("INTENTD_SPAWN_RETRY_BACKOFF_MS", Some("10,20")),
        ]);
        let db = crate::test_support::test_tempdir("quota-failover-db-");
        let store = Store::open(&db.path().join("store.db"))
            .await
            .expect("open store");
        let bus = EventBus::new(store.clone());
        let services = Services::new_with_file_secrets(
            store.clone(),
            intent_core::FileSecretStore::with_path(db.path().join("secrets.json")),
        )
        .with_event_bus(bus.clone());
        let ws = WorkspaceId::from(WS);
        store.insert_workspace(&workspace(&ws)).await.unwrap();
        let agent = AgentId::from("agent-quota-failover");
        let mut s = session(&agent, &ws, None);
        s.provider = Some("claude-code".to_string());
        s.model = model.map(str::to_string);
        store.insert_agent_session(&s).await.unwrap();
        let sink = Arc::new(BusEventSink::new(bus.clone()));
        Self {
            mgr: Arc::new(AgentManager::new(services, sink, 4)),
            bus,
            agent,
            ws,
            _db: db,
            scratch,
            _env: env,
        }
    }

    async fn profile(&self, name: &str, backup: bool) -> String {
        self.mgr
            .services
            .provider_profiles_create_impl(json!({
                "baseProviderId": "claude-code",
                "name": name,
                "backup": backup,
            }))
            .await
            .expect("create profile")["profile"]["providerRef"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    /// Send one user message and wait for the worker to settle; returns
    /// every event published meanwhile.
    async fn send(&self, text: &str) -> Vec<intent_core::Event> {
        let mut sub = self.bus.subscribe(SubscriptionFilter::default());
        self.mgr
            .send_message(
                self.agent.clone(),
                self.ws.clone(),
                text.to_string(),
                None,
                super::TurnOptions::default(),
            )
            .await
            .expect("send_message spawns the worker inline");
        timeout_settled(&self.mgr, &self.agent).await;
        let mut events = Vec::new();
        while let Ok(Some(batch)) =
            // fixed-deadline: allow — quiet-window drain after the worker settled
            tokio::time::timeout(Duration::from_millis(300), sub.recv()).await
        {
            events.extend(batch);
        }
        events
    }

    /// How many attempt-gated mock calls ran (the mock's counter file holds
    /// the NEXT attempt number); here, the `session/new` calls.
    fn gated_calls(&self) -> u32 {
        std::fs::read_to_string(self.scratch.path().join("attempts"))
            .ok()
            .and_then(|n| n.trim().parse::<u32>().ok())
            .map_or(0, |next| next - 1)
    }

    /// The text of every prompt the mock children received, in order.
    fn prompts(&self) -> Vec<String> {
        std::fs::read_to_string(self.scratch.path().join("prompts.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| {
                serde_json::from_str::<Value>(line).expect("prompt log line")["text"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    }

    async fn row(&self) -> AgentSession {
        self.mgr
            .services
            .store
            .get_agent_session(&self.agent)
            .await
            .unwrap()
    }

    async fn messages(&self) -> Vec<intent_core::AgentMessage> {
        self.mgr
            .services
            .store
            .get_agent_messages(&self.agent, None)
            .await
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mgr = self.mgr.clone();
        let agent = self.agent.clone();
        // Best-effort child teardown; the runtime may already be shutting down.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move { mgr.stop(&agent).await });
        }
    }
}

async fn timeout_settled(mgr: &AgentManager, agent: &AgentId) {
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            loop {
                let status = mgr
                    .services
                    .store
                    .get_agent_session(agent)
                    .await
                    .unwrap()
                    .status;
                if matches!(status, AgentStatus::RuntimeIdle | AgentStatus::Error)
                    && !mgr.is_busy(agent)
                    && mgr.workers.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await
    .expect("the worker settles");
}

fn of_type<'a>(events: &'a [intent_core::Event], ty: &str) -> Vec<&'a intent_core::Event> {
    events.iter().filter(|e| e.event_type == ty).collect()
}

fn notices<'a>(
    messages: &'a [intent_core::AgentMessage],
    ty: &str,
) -> Vec<&'a intent_core::AgentMessage> {
    messages
        .iter()
        .filter(|m| m.metadata.as_ref().and_then(|md| md["type"].as_str()) == Some(ty))
        .collect()
}

fn user_rows<'a>(
    messages: &'a [intent_core::AgentMessage],
    text: &str,
) -> Vec<&'a intent_core::AgentMessage> {
    messages
        .iter()
        .filter(|m| m.role == "user" && m.content[0]["text"] == json!(text))
        .collect()
}

/// The profile-change events a failover must not produce (activation
/// changes from processes starting and stopping are not profile edits).
fn profile_edits(events: &[intent_core::Event]) -> Vec<&intent_core::Event> {
    events
        .iter()
        .filter(|e| {
            e.event_type == "providers:profiles-changed" && e.data["change"] != json!("activation")
        })
        .collect()
}

/// A quota failure on the Default configuration moves the agent onto the
/// first backup in saved order (skipping the non-backup profile), with the
/// same model, and the same user turn completes there: one
/// `provider_failover` row, `agent:updated`, no `agent:failed`, no
/// `model_changed` row, no Error, the user row persisted once. The switch
/// is sticky for the next turn.
async fn assert_fails_over_to_first_backup(stream_before_error: Option<&str>) {
    let mut behavior = json!({
        "promptRpcError": quota_error(),
        "promptRpcErrorAttempts": 1,
        "response": "answered on the backup",
    });
    if let Some(text) = stream_before_error {
        behavior["streamBeforeErrorText"] = json!(text);
        behavior["toolCallBeforeError"] = json!("touch partial-marker");
    }
    let fx = Fixture::new(behavior).await;
    let _work = fx.profile("Work", false).await;
    let first = fx.profile("First", true).await;
    let _second = fx.profile("Second", true).await;

    let events = fx.send("please answer").await;

    let row = fx.row().await;
    assert_eq!(
        row.provider.as_deref(),
        Some(first.as_str()),
        "sticky switch"
    );
    assert_eq!(row.model.as_deref(), Some("opus"), "same model id");
    assert_eq!(row.status, AgentStatus::RuntimeIdle);
    assert!(row.stop_reason.is_none(), "no error persisted: {row:?}");

    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    let updated: Vec<_> = of_type(&events, intent_core::events::AGENT_UPDATED)
        .into_iter()
        .filter(|e| e.data["providerId"] == json!(first))
        .collect();
    assert_eq!(updated.len(), 1, "{events:#?}");
    assert_eq!(updated[0].data["modelId"], json!("opus"));
    assert!(profile_edits(&events).is_empty(), "{events:#?}");
    assert_eq!(of_type(&events, "agent:idle").len(), 1, "{events:#?}");

    let messages = fx.messages().await;
    let failover = notices(&messages, "provider_failover");
    assert_eq!(failover.len(), 1, "{messages:#?}");
    assert_eq!(failover[0].role, "system");
    assert_eq!(
        failover[0].metadata,
        Some(json!({
            "type": "provider_failover",
            "fromProvider": "claude-code",
            "toProvider": first,
            "model": "opus",
            "reason": "quota-exceeded",
        }))
    );
    assert_eq!(
        failover[0].content[0]["text"],
        json!("claude-code ran out of quota. Switched to the backup claude-code (First).")
    );
    assert!(
        of_type(&events, "agent:message")
            .iter()
            .any(|e| e.data["messageId"] == json!(failover[0].id)),
        "the notice is published live"
    );
    assert!(
        notices(&messages, "model_changed").is_empty(),
        "a failover never adds a model_changed row: {messages:#?}"
    );
    assert_eq!(user_rows(&messages, "please answer").len(), 1);
    let assistant: Vec<String> = messages
        .iter()
        .filter(|m| m.role == "assistant")
        .map(|m| m.content.to_string())
        .collect();
    assert!(
        assistant
            .iter()
            .any(|c| c.contains("answered on the backup")),
        "the backup completed the turn: {assistant:?}"
    );
    // What the backup was sent: the failed attempt's prompt, then the
    // backup's, on a fresh native session with Intent history replayed.
    let prompts = fx.prompts();
    assert_eq!(prompts.len(), 2, "{prompts:#?}");
    assert!(prompts[0].ends_with("please answer"), "{:?}", prompts[0]);
    let backup = &prompts[1];
    if let Some(text) = stream_before_error {
        assert!(
            assistant.iter().any(|c| c.contains(text)),
            "partial output of the failed attempt stays: {assistant:?}"
        );
        // Mid-turn: the backup continues the turn — it sees the user message,
        // the partial text AND the finished tool call, and gets the
        // continuation instruction instead of the message again.
        assert!(backup.contains("please answer"), "{backup}");
        assert!(backup.contains(text), "{backup}");
        assert!(
            backup.contains("touch partial-marker"),
            "the partial turn's tool call reaches the backup: {backup}"
        );
        assert!(
            backup.ends_with(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION),
            "{backup}"
        );
    } else {
        // Before any output: nothing to continue, the same message is sent.
        assert!(backup.ends_with("please answer"), "{backup}");
        assert!(
            !backup.contains(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION),
            "{backup}"
        );
    }

    // Sticky: the next turn runs on the backup with no further notice.
    let events = fx.send("and again").await;
    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    assert_eq!(fx.row().await.provider.as_deref(), Some(first.as_str()));
    let messages = fx.messages().await;
    assert_eq!(notices(&messages, "provider_failover").len(), 1);
    assert!(
        notices(&messages, "model_changed").is_empty(),
        "{messages:#?}"
    );
}

#[intent_test_macros::daemon_test]
async fn quota_failure_before_output_fails_over_to_the_first_backup() {
    assert_fails_over_to_first_backup(None).await;
}

#[intent_test_macros::daemon_test]
async fn quota_failure_after_partial_output_still_fails_over() {
    assert_fails_over_to_first_backup(Some("partial answer from the default")).await;
}

/// Every backup out of quota: each switch is recorded once, then the turn
/// fails exactly as today — one `agent:failed` with `quota-exceeded` naming
/// the last reference tried, the agent parked in Error on it.
#[intent_test_macros::daemon_test]
async fn exhausted_chain_fails_the_turn_as_today() {
    let fx = Fixture::new(json!({
        "promptRpcError": quota_error(),
        "promptRpcErrorAttempts": 3,
        "response": "unreached",
    }))
    .await;
    let first = fx.profile("First", true).await;
    let second = fx.profile("Second", true).await;

    let events = fx.send("please answer").await;

    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert_eq!(failed[0].data["errorCode"], json!("quota-exceeded"));
    assert_eq!(failed[0].data["providerId"], json!(second));
    let row = fx.row().await;
    assert_eq!(row.status, AgentStatus::Error);
    assert_eq!(row.provider.as_deref(), Some(second.as_str()));
    let messages = fx.messages().await;
    let hops: Vec<(Value, Value)> = notices(&messages, "provider_failover")
        .iter()
        .map(|m| {
            let md = m.metadata.as_ref().unwrap();
            (md["fromProvider"].clone(), md["toProvider"].clone())
        })
        .collect();
    assert_eq!(
        hops,
        vec![
            (json!("claude-code"), json!(first)),
            (json!(first), json!(second)),
        ]
    );
    assert_eq!(user_rows(&messages, "please answer").len(), 1);
    assert!(
        notices(&messages, "model_changed").is_empty(),
        "{messages:#?}"
    );
}

/// No backup profile (only a non-backup one): today's behavior, untouched.
#[intent_test_macros::daemon_test]
async fn quota_failure_without_backups_is_unchanged() {
    let fx = Fixture::new(json!({
        "promptRpcError": quota_error(),
        "response": "unreached",
    }))
    .await;
    let _work = fx.profile("Work", false).await;

    let events = fx.send("please answer").await;

    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert_eq!(failed[0].data["errorCode"], json!("quota-exceeded"));
    assert_eq!(failed[0].data["providerId"], json!("claude-code"));
    assert_eq!(fx.row().await.provider.as_deref(), Some("claude-code"));
    assert!(notices(&fx.messages().await, "provider_failover").is_empty());
}

/// A failure that is not a quota rejection never fails over, even with
/// backups saved.
#[intent_test_macros::daemon_test]
async fn non_quota_failures_never_fail_over() {
    let fx = Fixture::new(json!({
        "promptRpcError": { "code": -32603, "message": "Internal error", "data": "boom" },
        "response": "unreached",
    }))
    .await;
    let _first = fx.profile("First", true).await;

    let events = fx.send("please answer").await;

    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert!(
        failed[0].data.get("errorCode").is_none(),
        "{:?}",
        failed[0].data
    );
    let row = fx.row().await;
    assert_eq!(row.provider.as_deref(), Some("claude-code"));
    assert_eq!(row.status, AgentStatus::Error);
    assert!(notices(&fx.messages().await, "provider_failover").is_empty());
}

/// A quota rejection at session setup (`session/new`, the spawn-attempt
/// path) fails over IMMEDIATELY when a backup exists: no further
/// same-provider spawn retries (they cannot help a spent quota), the
/// backup's session opens and the turn completes.
#[intent_test_macros::daemon_test]
async fn session_setup_quota_failure_fails_over() {
    let fx = Fixture::new(json!({
        "sessionNewRpcError": quota_error(),
        "sessionNewRpcErrorAttempts": 1,
        "response": "answered on the backup",
    }))
    .await;
    let first = fx.profile("First", true).await;

    let events = fx.send("please answer").await;

    assert_eq!(
        fx.gated_calls(),
        2,
        "one rejected session/new on Default, then the backup's — no same-provider retry"
    );

    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    let row = fx.row().await;
    assert_eq!(row.provider.as_deref(), Some(first.as_str()));
    assert_eq!(row.status, AgentStatus::RuntimeIdle);
    let messages = fx.messages().await;
    assert_eq!(notices(&messages, "provider_failover").len(), 1);
    assert_eq!(user_rows(&messages, "please answer").len(), 1);
    assert!(
        !fx.mgr
            .spawn_attempt_provider
            .lock()
            .unwrap()
            .contains_key(&fx.agent),
        "the swallowed attempt's provider record is consumed"
    );
}

/// Every session setup out of quota: the spawn path's chain is bounded the
/// same way and ends in today's spawn failure.
#[intent_test_macros::daemon_test]
async fn session_setup_quota_exhaustion_fails_as_today() {
    let fx = Fixture::new(json!({
        "sessionNewRpcError": quota_error(),
        "response": "unreached",
    }))
    .await;
    let first = fx.profile("First", true).await;
    let second = fx.profile("Second", true).await;

    let events = fx.send("please answer").await;

    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert_eq!(failed[0].data["errorCode"], json!("quota-exceeded"));
    assert_eq!(
        failed[0].data["providerId"],
        json!(second),
        "last reference tried"
    );
    let row = fx.row().await;
    assert_eq!(row.status, AgentStatus::Error);
    assert_eq!(row.provider.as_deref(), Some(second.as_str()));
    let messages = fx.messages().await;
    let targets: Vec<Value> = notices(&messages, "provider_failover")
        .iter()
        .map(|m| m.metadata.as_ref().unwrap()["toProvider"].clone())
        .collect();
    assert_eq!(targets, vec![json!(first), json!(second)]);
}

/// With no backup, a quota rejection at session setup keeps today's
/// behavior exactly: every same-provider spawn retry runs, then the turn
/// fails with the quota signal.
#[intent_test_macros::daemon_test]
async fn session_setup_quota_failure_without_backups_keeps_todays_retries() {
    let fx = Fixture::new(json!({
        "sessionNewRpcError": quota_error(),
        "sessionNewRpcErrorAttempts": 100,
        "response": "unreached",
    }))
    .await;
    let _work = fx.profile("Work", false).await;

    let events = fx.send("please answer").await;

    assert_eq!(fx.gated_calls(), super::MAX_SPAWN_ATTEMPTS);
    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert_eq!(failed[0].data["errorCode"], json!("quota-exceeded"));
    assert_eq!(fx.row().await.provider.as_deref(), Some("claude-code"));
    assert!(notices(&fx.messages().await, "provider_failover").is_empty());
}

/// A user switch that lands while the failing attempt is in flight wins: the
/// quota failure belongs to the configuration that ran, so when the session
/// already names another one, nothing fails over and the user's choice is
/// never overwritten.
#[intent_test_macros::daemon_test]
async fn a_user_switch_during_the_failing_attempt_is_never_overridden() {
    let fx = Fixture::new(json!({ "response": "unused" })).await;
    let _first = fx.profile("First", true).await;
    let chosen = fx.profile("Chosen", false).await;
    fx.mgr
        .services
        .store
        .set_agent_session_model(&fx.ws, &fx.agent, "opus", Some(&chosen), &now_iso())
        .await
        .unwrap();
    fx.mgr.services.arm_quota_failover(&fx.agent, false);
    fx.mgr
        .services
        .note_quota_failover_attempt(&fx.agent, "claude-code", Some("opus"));

    assert!(
        !fx.mgr
            .services
            .try_quota_failover(&fx.ws, &fx.agent, Some("claude-code"))
            .await,
        "the failed attempt ran on Default, not on the user's new choice"
    );
    assert_eq!(fx.row().await.provider.as_deref(), Some(chosen.as_str()));
    assert!(notices(&fx.messages().await, "provider_failover").is_empty());
    assert!(
        !fx.mgr
            .services
            .has_quota_failover_candidate(&fx.agent, Some("claude-code"))
            .await
    );
}

/// Full worker path of the user-switch race: `change` runs while the failing
/// prompt is in flight (held by the mock's receipt gate) and stands in for
/// what `agent.setModel` persists. The late quota failure belongs to the
/// identity the attempt started with, so nothing fails over — the turn
/// fails as today and the user's choice stays.
async fn assert_mid_flight_change_wins(
    change: impl FnOnce(&Fixture, &str) -> (Option<String>, String),
) {
    let scratch = crate::test_support::test_tempdir("quota-failover-gate-");
    let entered = scratch.path().join("entered");
    let release = scratch.path().join("release");
    let fx = Fixture::new(json!({
        "promptRpcError": quota_error(),
        "promptRpcErrorAttempts": 1,
        "promptReceiptGate": {
            "suffix": "please answer",
            "enteredFile": entered.to_str().unwrap(),
            "releaseFile": release.to_str().unwrap(),
        },
        "response": "unreached",
    }))
    .await;
    let _first = fx.profile("First", true).await;
    let chosen = fx.profile("Chosen", false).await;

    let mut sub = fx.bus.subscribe(SubscriptionFilter::default());
    fx.mgr
        .send_message(
            fx.agent.clone(),
            fx.ws.clone(),
            "please answer".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("send_message spawns the worker inline");
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        async {
            while !entered.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("the prompt reaches the provider");
    let (provider, model) = change(&fx, &chosen);
    fx.mgr
        .services
        .store
        .set_agent_session_model(&fx.ws, &fx.agent, &model, provider.as_deref(), &now_iso())
        .await
        .unwrap();
    std::fs::write(&release, "go").unwrap();
    timeout_settled(&fx.mgr, &fx.agent).await;
    let mut events = Vec::new();
    while let Ok(Some(batch)) =
        // fixed-deadline: allow — quiet-window drain after the worker settled
        tokio::time::timeout(Duration::from_millis(300), sub.recv()).await
    {
        events.extend(batch);
    }

    let row = fx.row().await;
    assert_eq!(row.provider, provider, "the user's provider stays");
    assert_eq!(
        row.model.as_deref(),
        Some(model.as_str()),
        "the user's model stays"
    );
    let failed = of_type(&events, "agent:failed");
    assert_eq!(failed.len(), 1, "{events:#?}");
    assert_eq!(failed[0].data["errorCode"], json!("quota-exceeded"));
    assert_eq!(failed[0].data["providerId"], json!("claude-code"));
    assert!(notices(&fx.messages().await, "provider_failover").is_empty());
}

#[intent_test_macros::daemon_test]
async fn a_user_provider_switch_while_the_prompt_is_in_flight_wins_over_failover() {
    assert_mid_flight_change_wins(|_, chosen| (Some(chosen.to_owned()), "opus".to_owned())).await;
}

#[intent_test_macros::daemon_test]
async fn a_user_model_change_while_the_prompt_is_in_flight_wins_over_failover() {
    assert_mid_flight_change_wins(|_, _| (Some("claude-code".to_owned()), "sonnet".to_owned()))
        .await;
}

/// The failover replay keeps the partial turn for a queued-batch (live
/// flush) send too, where the ordinary boundary is the batch's first row;
/// without the failover mark the ordinary boundary is unchanged.
#[intent_test_macros::daemon_test]
async fn failover_replay_keeps_the_partial_turn_for_a_flushed_batch() {
    let fx = Fixture::new(json!({ "response": "unused" })).await;
    let append = |role: &'static str, text: &'static str, metadata: Option<Value>| {
        let store = fx.mgr.services.store.clone();
        let agent = fx.agent.clone();
        async move {
            store
                .append_agent_message_with_metadata(
                    &agent,
                    role,
                    &json!([{ "type": "text", "text": text }]),
                    metadata.as_ref(),
                    &now_iso(),
                )
                .await
                .unwrap();
        }
    };
    append("user", "earlier question", None).await;
    append("assistant", "earlier answer", None).await;
    append(
        "user",
        "batched request",
        Some(json!({ "queueInfo": { "queuedMessageId": "q-1" } })),
    )
    .await;
    append("assistant", "partial work before the quota", None).await;
    let flushed = vec!["q-1".to_string()];

    fx.mgr.recreated.lock().unwrap().insert(fx.agent.clone());
    let ordinary = fx
        .mgr
        .build_turn_body(&fx.agent, "batched request", Some(&flushed))
        .await;
    assert!(ordinary.contains("earlier answer"), "{ordinary}");
    assert!(!ordinary.contains("partial work"), "{ordinary}");
    assert!(ordinary.ends_with("batched request"), "{ordinary}");

    fx.mgr.recreated.lock().unwrap().insert(fx.agent.clone());
    fx.mgr
        .services
        .quota_failover_replays
        .lock()
        .unwrap()
        .insert(fx.agent.clone());
    let continued = fx
        .mgr
        .build_turn_body(&fx.agent, "batched request", Some(&flushed))
        .await;
    assert!(continued.contains("earlier answer"), "{continued}");
    assert!(continued.contains("batched request"), "{continued}");
    assert!(
        continued.contains("partial work before the quota"),
        "{continued}"
    );
    assert!(
        continued.ends_with(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION),
        "{continued}"
    );
    assert!(
        fx.mgr.services.quota_failover_replay_pending(&fx.agent),
        "the mark lasts for the message"
    );
    fx.mgr.services.arm_quota_failover(&fx.agent, true);
    assert!(fx.mgr.services.quota_failover_replay_pending(&fx.agent));
    fx.mgr.services.arm_quota_failover(&fx.agent, false);
    assert!(
        !fx.mgr.services.quota_failover_replay_pending(&fx.agent),
        "a new message clears it"
    );
}

fn resolved_on(selection: &str) -> ResolvedSpawn {
    ResolvedSpawn {
        provider: *intent_providers::provider_config(selection),
        selection: crate::provider_profiles::canonical_selection(selection),
        model: Some("opus".into()),
        reasoning_effort: None,
        cwd: std::env::temp_dir(),
        provider_binary: None,
        extra_env: std::collections::BTreeMap::default(),
        npx_fallback_binary: None,
        npx_fallback_package: None,
        unsloth_endpoint: None,
    }
}

/// A turn that failed over twice (Default's prompt ran out, the first
/// backup's session setup ran out too, the second backup succeeded) commits
/// Default → second backup at the successful start. Its two
/// `provider_failover` rows announce that hop, so no `model_changed` row is
/// added; a later manual switch still gets its own.
#[intent_test_macros::daemon_test]
async fn a_multi_hop_failover_adds_no_model_changed_row() {
    let fx = Fixture::new(json!({ "response": "unused" })).await;
    let first = fx.profile("First", true).await;
    let second = fx.profile("Second", true).await;
    let work = fx.profile("Work", false).await;
    fx.mgr
        .maybe_persist_model_change_notice(&fx.agent, &fx.ws, &resolved_on("claude-code"))
        .await;
    for (from, to) in [
        ("claude-code", first.as_str()),
        (first.as_str(), second.as_str()),
    ] {
        fx.mgr
            .services
            .store
            .append_agent_message_with_metadata(
                &fx.agent,
                "system",
                &json!([{ "type": "text", "text": "switched" }]),
                Some(&json!({
                    "type": "provider_failover", "fromProvider": from, "toProvider": to,
                    "model": "opus", "reason": "quota-exceeded",
                })),
                &now_iso(),
            )
            .await
            .unwrap();
    }

    fx.mgr
        .maybe_persist_model_change_notice(&fx.agent, &fx.ws, &resolved_on(&second))
        .await;
    assert!(
        notices(&fx.messages().await, "model_changed").is_empty(),
        "the failover chain already announces Default → Second"
    );
    let (_, committed) = fx
        .mgr
        .services
        .store
        .get_agent_session_last_turn_model(&fx.ws, &fx.agent)
        .await
        .unwrap();
    assert_eq!(committed.as_deref(), Some(second.as_str()));

    fx.mgr
        .maybe_persist_model_change_notice(&fx.agent, &fx.ws, &resolved_on(&work))
        .await;
    assert_eq!(
        notices(&fx.messages().await, "model_changed").len(),
        1,
        "a manual switch after the failover is still announced"
    );
}

/// A pre-output transport failure on the backup (silently redriven on a
/// fresh child) keeps continuing the cut-short turn: the redriven prompt
/// still carries the partial work and the continuation instruction.
#[intent_test_macros::daemon_test]
async fn a_silent_redrive_on_the_backup_still_continues_the_partial_turn() {
    let fx = Fixture::new(json!({
        "promptRpcError": quota_error(),
        "promptRpcErrorAttempts": 1,
        "streamBeforeErrorText": "partial answer from the default",
        "toolCallBeforeError": "touch partial-marker",
        "exitDuringPromptOnAttempt": 2,
        "response": "answered on the backup",
    }))
    .await;
    let first = fx.profile("First", true).await;

    let events = fx.send("please answer").await;

    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    assert_eq!(fx.row().await.provider.as_deref(), Some(first.as_str()));
    let prompts = fx.prompts();
    assert_eq!(
        prompts.len(),
        3,
        "default, backup (died), backup redriven: {prompts:#?}"
    );
    assert!(
        prompts[1].contains("touch partial-marker"),
        "{}",
        prompts[1]
    );
    // The redriven prompt continues the turn either way: through a replay
    // that carries the partial work again, or on the resumed native session
    // that already holds it.
    let redriven = &prompts[2];
    assert!(
        redriven.ends_with(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION),
        "{redriven}"
    );
    assert!(
        redriven.contains("touch partial-marker")
            || redriven.trim() == crate::quota_failover::QUOTA_FAILOVER_CONTINUATION,
        "{redriven}"
    );
    assert!(
        !redriven.trim_end().ends_with("please answer"),
        "{redriven}"
    );
}

/// An agent on the provider default (no stored model) has its model filled
/// at session open; that fill is the attempt's identity, not a user change,
/// so a quota failure still fails over — with the filled model id kept.
#[intent_test_macros::daemon_test]
async fn a_provider_default_agent_with_a_session_open_fill_still_fails_over() {
    let fx = Fixture::build(
        json!({
            "promptRpcError": quota_error(),
            "promptRpcErrorAttempts": 1,
            "response": "answered on the backup",
        }),
        None,
        Some(json!({"configOptions": [
            {"id": "model", "name": "Model", "category": "model", "type": "select",
             "currentValue": "mock-current",
             "options": [{"value": "mock-current", "name": "Mock Current"}]}
        ]})),
    )
    .await;
    let first = fx.profile("First", true).await;

    let events = fx.send("please answer").await;

    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    let row = fx.row().await;
    assert_eq!(row.provider.as_deref(), Some(first.as_str()));
    assert_eq!(
        row.model.as_deref(),
        Some("mock-current"),
        "the filled model is kept"
    );
    let messages = fx.messages().await;
    let failover = notices(&messages, "provider_failover");
    assert_eq!(failover.len(), 1, "{messages:#?}");
    assert_eq!(
        failover[0].metadata.as_ref().unwrap()["model"],
        json!("mock-current")
    );
}

/// A tool call the failed attempt completed WITHOUT any output (it printed
/// nothing) still reaches the backup, so the instruction not to repeat the
/// tool calls is accurate (review intentd#2581).
#[intent_test_macros::daemon_test]
async fn a_completed_tool_call_without_output_reaches_the_backup() {
    let fx = Fixture::new(json!({
        "promptRpcError": quota_error(),
        "promptRpcErrorAttempts": 1,
        "streamBeforeErrorText": "starting the migration",
        "toolCallBeforeError": "rm -rf build-cache",
        "toolCallBeforeErrorNoOutput": true,
        "response": "answered on the backup",
    }))
    .await;
    let _first = fx.profile("First", true).await;

    let events = fx.send("please answer").await;

    assert!(of_type(&events, "agent:failed").is_empty(), "{events:#?}");
    let prompts = fx.prompts();
    assert_eq!(prompts.len(), 2, "{prompts:#?}");
    let backup = &prompts[1];
    assert!(backup.contains("<current_turn_cut_short>"), "{backup}");
    assert!(backup.contains("please answer"), "{backup}");
    assert!(
        backup.contains("rm -rf build-cache"),
        "the silent call is listed: {backup}"
    );
    assert!(backup.contains("status=\"completed\""), "{backup}");
    assert!(backup.contains("(no output)"), "{backup}");
    assert!(
        backup.ends_with(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION),
        "{backup}"
    );
}

/// A cut-short turn that delivered saved groups (with an attachment):
/// the backup's prompt carries each request's text exactly once — in the
/// replayed turn section — and the attachment, but does not re-append the
/// group texts after the continuation (review intentd#2581).
#[intent_test_macros::daemon_test]
async fn a_cut_short_group_delivery_is_not_appended_again_after_the_continuation() {
    let fx = Fixture::new(json!({ "response": "unused" })).await;
    let store = fx.mgr.services.store.clone();
    let mut source_ids = Vec::new();
    for text in ["first grouped request", "second grouped request"] {
        let row = store
            .append_agent_message_with_metadata(
                &fx.agent,
                "user",
                &json!([{ "type": "text", "text": text }]),
                None,
                &now_iso(),
            )
            .await
            .unwrap();
        source_ids.push(row.id);
    }
    store
        .append_agent_message_with_metadata(
            &fx.agent,
            "assistant",
            &json!([
                { "type": "text", "text": "working on both" },
                {
                    "type": "tool_use", "id": "m-0", "name": "Bash",
                    "input": { "command": "make migrate" }, "toolCallId": "call-1",
                    "metadata": { "toolKind": "execute", "status": "completed" },
                },
            ]),
            None,
            &now_iso(),
        )
        .await
        .unwrap();
    let group =
        |source: &str, text: &str, image: Option<Value>| crate::agent_ops::QueuedDeliveryGroup {
            source_id: Some(source.to_owned()),
            is_prepend: false,
            content: text.to_owned(),
            image_blocks: image,
            file_blocks: None,
        };
    let options = super::TurnOptions {
        delivery_groups: Some(vec![
            group(
                &source_ids[0],
                "first grouped request",
                Some(json!([{ "data": "GROUP_IMG", "mimeType": "image/png" }])),
            ),
            group(&source_ids[1], "second grouped request", None),
        ]),
        ..super::TurnOptions::default()
    };
    fx.mgr.recreated.lock().unwrap().insert(fx.agent.clone());
    fx.mgr
        .services
        .quota_failover_replays
        .lock()
        .unwrap()
        .insert(fx.agent.clone());

    let prompt = fx
        .mgr
        .build_turn_prompt(&fx.agent, &fx.ws, "first grouped request", &options)
        .await
        .unwrap();
    let wire = serde_json::to_value(&prompt).unwrap();
    let blocks = wire.as_array().unwrap();
    let text: String = blocks
        .iter()
        .filter(|b| b["type"] == json!("text"))
        .map(|b| b["text"].as_str().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    for request in ["first grouped request", "second grouped request"] {
        assert_eq!(text.matches(request).count(), 1, "{request} once: {text}");
    }
    assert!(text.contains("make migrate"), "{text}");
    let continuation = text
        .find(crate::quota_failover::QUOTA_FAILOVER_CONTINUATION)
        .expect("continuation");
    assert!(
        !text[continuation..].contains("Message #"),
        "no group text re-appended after the continuation: {text}"
    );
    assert!(
        blocks
            .iter()
            .any(|b| b["type"] == json!("image") && b["data"] == json!("GROUP_IMG")),
        "the group attachment is still delivered: {wire}"
    );
}

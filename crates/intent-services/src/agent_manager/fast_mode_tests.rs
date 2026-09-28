use super::role_reminder_tests::{session, workspace};
use super::*;
use crate::events::EventBus;
use crate::test_support::test_tempdir;
use intent_store::Store;
use std::os::unix::fs::PermissionsExt;

const FIXTURE: &str = include_str!("../../tests/fixtures/fast-mode.mjs");

async fn fixture(
    provider: &str,
    model: &str,
    fail_off: bool,
) -> (AgentManager, AgentId, tempfile::TempDir) {
    let dir = test_tempdir("fast-mode-runtime-");
    let script = dir.path().join("adapter.mjs");
    std::fs::write(&script, format!("#!/usr/bin/env node\nconst provider = {provider:?}; const failOff = {fail_off}; const logPath = {};\n{FIXTURE}", json!(dir.path().join("calls.jsonl")))).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let bus = EventBus::new(store.clone());
    let registry = Arc::new(crate::SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[("providers.paths".into(), json!({provider:script}))])
        .unwrap();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_settings_registry(registry);
    let mgr = AgentManager::new(services, Arc::new(BusEventSink::new(bus)), 4);
    let ws = WorkspaceId::from("ws-1");
    let mut w = workspace(&ws);
    w.path = Some(dir.path().to_string_lossy().into_owned());
    store.insert_workspace(&w).await.unwrap();
    let id = AgentId::from("fast-agent");
    let mut s = session(&id, &ws, None);
    s.provider = Some(provider.into());
    s.model = Some(model.into());
    s.reasoning_effort = Some("high".into());
    s.is_background = true;
    s.parent_agent_id = Some(AgentId::from("parent"));
    store.insert_agent_session(&s).await.unwrap();
    spawn_fixture(&mgr, &id, provider, model, &dir).await;
    (mgr, id, dir)
}

async fn spawn_fixture(
    mgr: &AgentManager,
    id: &AgentId,
    provider: &str,
    model: &str,
    dir: &tempfile::TempDir,
) {
    // Explicit resolved binary isolates even Codex, whose production npx-only
    // policy deliberately ignores providers.paths adapter overrides.
    let config = intent_providers::find_provider(provider).unwrap();
    let script = dir.path().join("adapter.mjs");
    let mut opts = SpawnOptions::new(config);
    opts.provider_binary = Some(&script);
    opts.model = Some(model);
    opts.cwd = Some(dir.path());
    mgr.create_agent(
        id.clone(),
        WorkspaceId::from("ws-1"),
        "Fast fixture",
        "implementor",
        dir.path().to_path_buf(),
        &opts,
    )
    .await
    .unwrap();
}

fn preference(mgr: &AgentManager, provider: &str, value: bool) {
    mgr.services
        .settings_registry()
        .unwrap()
        .apply(&[("providers.fastMode".into(), json!({provider:value}))])
        .unwrap();
}

async fn turn(mgr: &AgentManager, id: &AgentId) -> Value {
    let sid = mgr
        .ensure_started(id, &WorkspaceId::from("ws-1"))
        .await
        .unwrap();
    let conn = mgr.handles.lock().unwrap()[id].connection.clone();
    conn.request(
        "session/prompt",
        json!({"sessionId":sid,"prompt":[{"type":"text","text":"inspect"}]}),
    )
    .await
    .unwrap()["native"]
        .clone()
}

fn calls(dir: &tempfile::TempDir) -> Vec<Value> {
    std::fs::read_to_string(dir.path().join("calls.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn fast_mode_setting_change_leaves_running_turn_untouched() {
    let (mgr, id, dir) = fixture("claude-code", "supported", false).await;
    let sid = mgr
        .ensure_started(&id, &WorkspaceId::from("ws-1"))
        .await
        .unwrap();
    let (conn, notifications) = {
        let handles = mgr.handles.lock().unwrap();
        (
            handles[&id].connection.clone(),
            handles[&id].notifications.clone(),
        )
    };
    let request_conn = conn.clone();
    let prompt = tokio::spawn(async move {
        request_conn
            .request(
                "session/prompt",
                json!({"sessionId":sid,"prompt":[{"type":"text","text":"hold"}]}),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut notifications = notifications.lock().await;
        while let Some(note) = notifications.recv().await {
            if note.method == "fixture/held" {
                return;
            }
        }
        panic!("fixture disconnected before prompt");
    })
    .await
    .unwrap();
    preference(&mgr, "claude-code", true);
    conn.request("fixture/release", json!({})).await.unwrap();
    assert_eq!(prompt.await.unwrap()["native"]["fastMode"], false);
    let control_count = calls(&dir)
        .iter()
        .filter(|c| c["params"]["configId"] == "fast")
        .count();
    assert_eq!(control_count, 1, "settings update sent no live control");
    assert_eq!(turn(&mgr, &id).await["fastMode"], true);
    mgr.stop(&id).await;
}

#[tokio::test]
async fn fast_mode_persistent_and_delegated_turns_reuse_adapter_and_clear_cold_resume() {
    for provider in ["claude-code", "codex"] {
        let (mgr, id, dir) = fixture(provider, "supported", false).await;
        let off = turn(&mgr, &id).await;
        assert_eq!(off["fastMode"], false, "native enabled default cleared");
        assert_eq!(off["serviceTier"], Value::Null);
        for enabled in [true, false, true, false] {
            preference(&mgr, provider, enabled);
            let state = turn(&mgr, &id).await;
            assert_eq!(state["pid"], off["pid"], "unchanged adapter");
            assert_eq!(state["sessionId"], off["sessionId"]);
            assert_eq!(state["fastMode"], enabled);
            assert_eq!(state["model"], "supported");
            assert_eq!(state["effort"], "high");
        }
        mgr.kill_child_only(&id).await;
        spawn_fixture(&mgr, &id, provider, "supported", &dir).await;
        mgr.start_session(
            &id,
            dir.path().to_path_buf(),
            intent_providers::find_provider(provider).unwrap(),
        )
        .await
        .unwrap();
        let resumed = turn(&mgr, &id).await;
        assert_ne!(resumed["pid"], off["pid"]);
        assert_eq!(resumed["sessionId"], off["sessionId"]);
        assert_eq!(resumed["fastMode"], false);
        assert_eq!(resumed["controls"], json!(["off"]));
        assert!(calls(&dir).iter().any(|c| c["method"] == "session/load"));
        mgr.stop(&id).await;
    }
}

#[tokio::test]
async fn fast_mode_absent_option_allows_off_then_rechecks_eligible_model() {
    let (mgr, id, _dir) = fixture("claude-code", "unsupported", false).await;
    let state = turn(&mgr, &id).await;
    assert_eq!(state["model"], "unsupported");
    assert_eq!(state["controls"], json!([]));
    preference(&mgr, "claude-code", true);
    assert_eq!(turn(&mgr, &id).await["controls"], json!([]));
    let conn = mgr.handles.lock().unwrap()[&id].connection.clone();
    let response = intent_acp::session::set_session_config_option_response(
        &conn,
        "fast-session",
        "model",
        "supported",
    )
    .await
    .unwrap();
    crate::fast_mode::refresh_options(
        &mut mgr
            .handles
            .lock()
            .unwrap()
            .get_mut(&id)
            .unwrap()
            .config_options,
        Some(&response),
    );
    mgr.apply_fast_mode(&id, "fast-session", "claude-code")
        .await
        .unwrap();
    let state = conn
        .request(
            "session/prompt",
            json!({"sessionId":"fast-session","prompt":[]}),
        )
        .await
        .unwrap();
    assert_eq!(state["native"]["controls"], json!(["on"]));
    assert_eq!(state["native"]["effort"], "high");
    mgr.stop(&id).await;
}

#[tokio::test]
async fn fast_mode_failed_off_blocks_prompt_without_recycling() {
    for provider in ["claude-code", "codex"] {
        let (mgr, id, dir) = fixture(provider, "supported", true).await;
        let err = mgr
            .ensure_started(&id, &WorkspaceId::from("ws-1"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("could not apply Fast mode off"),
            "{err}"
        );
        assert!(!calls(&dir).iter().any(|c| c["method"] == "session/prompt"));
        assert!(mgr.handle_is_live(&id));
        preference(&mgr, provider, true);
        assert_eq!(turn(&mgr, &id).await["fastMode"], true);
        assert_eq!(
            calls(&dir)
                .iter()
                .filter(|c| c["method"] == "initialize")
                .count(),
            1
        );
        mgr.stop(&id).await;
    }
}

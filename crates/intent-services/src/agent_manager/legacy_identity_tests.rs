//! Legacy session repair at turn start: every create pins provider / model /
//! effort, so a NULL column is a pre-pin row. `ensure_started` pins a NULL
//! provider onto the provider the last turn ran (else the current default)
//! and never re-reads `model.defaultProvider` for it again; session open
//! fills a NULL model / effort from what the provider reports.
//!
//! `mock` is the runnable provider (gated on `MOCK_AGENT_SCRIPT_PATH`);
//! `codex` plays a default that must NOT be spawned.

use super::dead_child_respawn_tests::mock_agent_script;
use super::disabled_provider_rehome_tests::{registry_manager, set};
use super::role_reminder_tests::session;
use super::*;
use crate::agent_manager::tests::EnvGuard;

const WS: &str = "ws-1";

fn env(script: &str, session_result: Option<&str>) -> EnvGuard {
    EnvGuard::apply(&[
        ("MOCK_AGENT_SCRIPT_PATH", Some(script)),
        ("MOCK_AGENT_BEHAVIOR", None),
        ("MOCK_AGENT_ATTEMPT_FILE", None),
        ("MOCK_AGENT_SESSION_RESULT", session_result),
    ])
}

async fn seed_legacy(
    mgr: &AgentManager,
    agent_id: &AgentId,
    model: Option<&str>,
    effort: Option<&str>,
    last_turn_provider: Option<&str>,
) {
    let ws = WorkspaceId::from(WS);
    let mut s = session(agent_id, &ws, None);
    s.provider = None;
    s.model = model.map(str::to_string);
    s.reasoning_effort = effort.map(str::to_string);
    mgr.services.store.insert_agent_session(&s).await.unwrap();
    if let Some(p) = last_turn_provider {
        // The mock always spawns on its default (`resolve_spawn` passes no
        // model for it), so its committed turns carry a NULL model.
        mgr.services
            .store
            .set_agent_session_last_turn_model(&ws, agent_id, None, p)
            .await
            .unwrap();
    }
}

async fn row(mgr: &AgentManager, agent_id: &AgentId) -> AgentSession {
    mgr.services
        .store
        .get_agent_session(agent_id)
        .await
        .unwrap()
}

async fn notice_types(mgr: &AgentManager, agent_id: &AgentId) -> Vec<String> {
    mgr.services
        .store
        .get_agent_messages(agent_id, None)
        .await
        .unwrap()
        .iter()
        .filter_map(|m| m.metadata.as_ref()?["type"].as_str().map(str::to_string))
        .collect()
}

/// The incident: a legacy row whose last turn ran `mock`, with the default
/// since changed to `codex`, keeps running `mock` and is pinned to it — no
/// provider hop and no `model_changed` row.
#[tokio::test]
async fn legacy_row_keeps_its_last_turn_provider_after_a_default_change() {
    let script = mock_agent_script();
    let _env = env(&script, None);
    let (mgr, _bus, _db, _cfg) = registry_manager().await;
    set(&mgr, "model.defaultProvider", json!("codex"));
    let agent_id = AgentId::from("agent-legacy-last-turn");
    seed_legacy(
        &mgr,
        &agent_id,
        Some("mock-model"),
        Some("high"),
        Some("mock"),
    )
    .await;

    mgr.ensure_started(&agent_id, &WorkspaceId::from(WS))
        .await
        .expect("spawns the last-turn provider, not the codex default");

    assert_eq!(row(&mgr, &agent_id).await.provider.as_deref(), Some("mock"));
    {
        let handles = mgr.handles.lock().unwrap();
        assert_eq!(
            handles
                .get(&agent_id)
                .expect("child spawned")
                .spawned_provider,
            "node",
            "the mock child, not codex"
        );
    }
    assert!(
        !notice_types(&mgr, &agent_id)
            .await
            .iter()
            .any(|t| t == "model_changed"),
        "no provider hop to announce"
    );
}

/// A legacy row that never ran a turn is pinned to the default in effect at
/// its first turn, and stays there after the default changes.
#[tokio::test]
async fn never_run_legacy_row_is_pinned_at_its_first_turn() {
    let script = mock_agent_script();
    let _env = env(&script, None);
    let (mgr, _bus, _db, _cfg) = registry_manager().await;
    set(&mgr, "model.defaultProvider", json!("mock"));
    let agent_id = AgentId::from("agent-legacy-never-run");
    seed_legacy(&mgr, &agent_id, Some("mock-model"), Some("high"), None).await;

    mgr.ensure_started(&agent_id, &WorkspaceId::from(WS))
        .await
        .expect("first turn on the current default");
    set(&mgr, "model.defaultProvider", json!("codex"));

    assert_eq!(row(&mgr, &agent_id).await.provider.as_deref(), Some("mock"));
    mgr.ensure_started(&agent_id, &WorkspaceId::from(WS))
        .await
        .expect("later turn still on the pinned provider");
}

/// A concurrent `agent.setModel` that pinned a provider first is never
/// overwritten by the repair's compare-and-set.
#[tokio::test]
async fn repair_never_overwrites_a_concurrently_pinned_provider() {
    let (mgr, _bus, _db, _cfg) = registry_manager().await;
    set(&mgr, "model.defaultProvider", json!("mock"));
    let agent_id = AgentId::from("agent-legacy-race");
    seed_legacy(&mgr, &agent_id, Some("mock-model"), None, Some("mock")).await;
    let stale = row(&mgr, &agent_id).await;
    let ws = WorkspaceId::from(WS);
    mgr.services
        .store
        .set_agent_session_model(&ws, &agent_id, "gpt-5", Some("codex"), &now_iso())
        .await
        .unwrap();

    let repaired = mgr
        .pin_legacy_session_provider(&agent_id, &ws, stale)
        .await
        .expect("repair");
    assert_eq!(
        repaired.provider.as_deref(),
        Some("codex"),
        "fresh row wins"
    );
    assert_eq!(
        row(&mgr, &agent_id).await.provider.as_deref(),
        Some("codex")
    );
}

/// Session open fills a NULL model / effort from the provider's reported
/// current option ids; set values are never overwritten.
#[tokio::test]
async fn session_open_fills_unset_model_and_effort() {
    let script = mock_agent_script();
    let result = json!({"configOptions": [
        {"id": "model", "name": "Model", "category": "model", "type": "select",
         "currentValue": "mock-current",
         "options": [{"value": "mock-current", "name": "Mock Current"}]},
        {"id": "effort", "name": "Effort", "category": "thought_level", "type": "select",
         "currentValue": "medium",
         "options": [{"value": "low", "name": "Low"}, {"value": "medium", "name": "Medium"}]}
    ]})
    .to_string();
    let _env = env(&script, Some(&result));
    let (mgr, _bus, _db, _cfg) = registry_manager().await;
    set(&mgr, "model.defaultProvider", json!("mock"));

    let unset = AgentId::from("agent-fill-unset");
    seed_legacy(&mgr, &unset, None, None, None).await;
    mgr.ensure_started(&unset, &WorkspaceId::from(WS))
        .await
        .expect("spawn");
    let filled = row(&mgr, &unset).await;
    assert_eq!(filled.model.as_deref(), Some("mock-current"));
    assert_eq!(filled.reasoning_effort.as_deref(), Some("medium"));

    let set_row = AgentId::from("agent-fill-set");
    seed_legacy(&mgr, &set_row, Some("mock-model"), Some("low"), None).await;
    mgr.ensure_started(&set_row, &WorkspaceId::from(WS))
        .await
        .expect("spawn");
    let kept = row(&mgr, &set_row).await;
    assert_eq!(kept.model.as_deref(), Some("mock-model"));
    assert_eq!(kept.reasoning_effort.as_deref(), Some("low"));
}

/// The adapter's `"default"` sentinel is not a level: the effort stays unset.
#[tokio::test]
async fn session_open_never_fills_the_default_effort_sentinel() {
    let script = mock_agent_script();
    let result = json!({"configOptions": [
        {"id": "effort", "name": "Effort", "category": "thought_level", "type": "select",
         "currentValue": "default",
         "options": [{"value": "default", "name": "Default"}, {"value": "high", "name": "High"}]}
    ]})
    .to_string();
    let _env = env(&script, Some(&result));
    let (mgr, _bus, _db, _cfg) = registry_manager().await;
    set(&mgr, "model.defaultProvider", json!("mock"));
    let agent_id = AgentId::from("agent-fill-sentinel");
    seed_legacy(&mgr, &agent_id, Some("mock-model"), None, None).await;
    mgr.ensure_started(&agent_id, &WorkspaceId::from(WS))
        .await
        .expect("spawn");
    assert_eq!(row(&mgr, &agent_id).await.reasoning_effort, None);
}

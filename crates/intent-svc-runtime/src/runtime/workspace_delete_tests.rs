//! The runtime's producers honour core's workspace deletion gate: the
//! runtime half of `tests/workspace_delete.rs`'s
//! `incremental_delete_refuses_new_producers_after_cleanup_sweeps`, which
//! pins that the gate stays closed after the cleanup sweeps.

use intent_core::{AgentId, BrowserTabInput, ClientId};
use serde_json::json;

use crate::browser_tabs::BrowserTabOpsExt;
use crate::hook_manager::HookServicesExt;
use crate::test_support::goldens::{seed_agent, setup};

#[tokio::test]
async fn hooks_and_browser_tabs_are_refused_while_the_workspace_is_being_deleted() {
    let (_t, svc, ws) = setup().await;
    let agent = AgentId::from("agent-late");
    seed_agent(&svc, &ws, &agent).await;
    let hook_spec = json!({"name":"late hook", "delayMs":60000, "code":"return {dispatch:false}"});
    let tab: BrowserTabInput = serde_json::from_value(json!({
        "tabId":"late-tab", "workspaceId":ws.0, "url":"http://localhost/"
    }))
    .unwrap();
    let host = ClientId::from("test-host");

    let deletion = svc.feature_workspace_mutations().delete_for_test(&ws).await;
    let hook = svc.hook_schedule_op(&ws, &agent, &hook_spec).await;
    let upsert = svc.browser_tab_upsert(host.clone(), tab.clone()).await;
    let sync = svc.browser_tabs_sync(host.clone(), vec![tab.clone()]).await;
    assert!(
        hook.unwrap_err().to_string().contains("being deleted"),
        "hook escaped the deletion gate"
    );
    assert!(upsert.unwrap_err().to_string().contains("being deleted"));
    assert!(sync.unwrap_err().to_string().contains("being deleted"));

    // Released, the same producers are admitted: the gate was the refusal.
    drop(deletion);
    svc.hook_schedule_op(&ws, &agent, &hook_spec)
        .await
        .expect("hook admitted once the deletion is gone");
    // The unregistered test host then fails on its own row, not the gate.
    let upsert = svc.browser_tab_upsert(host, tab).await;
    assert!(
        upsert
            .as_ref()
            .map_or_else(|e| !e.to_string().contains("being deleted"), |_| true),
        "tab still refused by the gate: {upsert:?}"
    );
}

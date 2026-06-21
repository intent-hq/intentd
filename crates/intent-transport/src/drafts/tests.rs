//! Unit tests for the `drafts.*` fast-path (§5.16, §15).

use std::collections::HashMap;
use std::sync::Mutex;

use intent_core::{AgentId, BoxFuture, ClientId, Draft, Result, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::*;

type Key = (String, String, String);

/// In-memory `WorkspaceApi` backing the draft triple store + a recorded
/// `draft:changed` log (kept here as `(ws, agent, client, hasDraft)`).
#[derive(Default)]
struct MemApi {
    minted: Mutex<Vec<String>>,
    drafts: Mutex<HashMap<Key, (String, String)>>,
    events: Mutex<Vec<(String, String, String, bool)>>,
}

impl WorkspaceApi for MemApi {
    fn upsert_client(
        &self,
        client_id: ClientId,
        _name: Option<String>,
        _capabilities: Option<Value>,
    ) -> BoxFuture<'_, Result<()>> {
        self.minted.lock().unwrap().push(client_id.0);
        Box::pin(async { Ok(()) })
    }

    fn draft_get(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<Option<Draft>>> {
        let got = self
            .drafts
            .lock()
            .unwrap()
            .get(&(
                workspace_id.0.clone(),
                agent_id.0.clone(),
                client_id.0.clone(),
            ))
            .cloned();
        Box::pin(async move {
            Ok(got.map(|(text, updated_at)| Draft {
                workspace_id,
                agent_id,
                client_id,
                text,
                updated_at,
            }))
        })
    }

    fn draft_set(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
        text: String,
    ) -> BoxFuture<'_, Result<Option<String>>> {
        let key = (
            workspace_id.0.clone(),
            agent_id.0.clone(),
            client_id.0.clone(),
        );
        let updated = if text.is_empty() {
            self.drafts.lock().unwrap().remove(&key);
            None
        } else {
            self.drafts
                .lock()
                .unwrap()
                .insert(key, (text, "t-now".to_string()));
            Some("t-now".to_string())
        };
        self.events.lock().unwrap().push((
            workspace_id.0,
            agent_id.0,
            client_id.0,
            updated.is_some(),
        ));
        Box::pin(async move { Ok(updated) })
    }

    fn draft_clear(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<()>> {
        self.drafts.lock().unwrap().remove(&(
            workspace_id.0.clone(),
            agent_id.0.clone(),
            client_id.0.clone(),
        ));
        self.events
            .lock()
            .unwrap()
            .push((workspace_id.0, agent_id.0, client_id.0, false));
        Box::pin(async { Ok(()) })
    }
}

fn parsed(frame: Option<String>) -> Value {
    serde_json::from_str(&frame.expect("a response frame")).unwrap()
}

fn req(id: i64, method: &str, params: Value) -> DraftRequest {
    classify(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })).unwrap()
}

#[tokio::test]
async fn get_is_null_for_anonymous_without_minting() {
    let api = MemApi::default();
    let mut binding: Option<ClientId> = None;
    let r = parsed(
        handle(
            req(
                1,
                "drafts.get",
                json!({ "workspaceId": "ws", "agentId": "a" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(r["result"], Value::Null);
    assert!(binding.is_none(), "a read never mints a client");
    assert!(api.minted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn set_then_get_round_trips_and_mints_anonymous_client() {
    let api = MemApi::default();
    let mut binding: Option<ClientId> = None;
    let set = parsed(
        handle(
            req(
                1,
                "drafts.set",
                json!({ "workspaceId": "ws", "agentId": "a", "text": "hi" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(set["result"]["ok"], json!(true));
    assert_eq!(set["result"]["updatedAt"], json!("t-now"));
    assert!(
        binding.is_some(),
        "a write mints + binds an anonymous client"
    );
    assert_eq!(api.minted.lock().unwrap().len(), 1);
    let get = parsed(
        handle(
            req(
                2,
                "drafts.get",
                json!({ "workspaceId": "ws", "agentId": "a" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(get["result"]["text"], json!("hi"));
}

#[tokio::test]
async fn set_empty_text_is_a_clear() {
    let api = MemApi::default();
    let mut binding: Option<ClientId> = Some(ClientId::from_string("cli-1"));
    handle(
        req(
            1,
            "drafts.set",
            json!({ "workspaceId": "ws", "agentId": "a", "text": "draft" }),
        ),
        &api,
        &mut binding,
    )
    .await;
    let cleared = parsed(
        handle(
            req(
                2,
                "drafts.set",
                json!({ "workspaceId": "ws", "agentId": "a", "text": "" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(cleared["result"]["ok"], json!(true));
    assert!(
        cleared["result"].get("updatedAt").is_none(),
        "an empty set clears (no updatedAt)"
    );
    assert!(api.drafts.lock().unwrap().is_empty(), "the row is deleted");
    // The last event signals hasDraft=false.
    assert!(!api.events.lock().unwrap().last().unwrap().3);
}

#[tokio::test]
async fn clear_is_idempotent_success() {
    let api = MemApi::default();
    let mut binding: Option<ClientId> = Some(ClientId::from_string("cli-1"));
    let r = parsed(
        handle(
            req(
                1,
                "drafts.clear",
                json!({ "workspaceId": "ws", "agentId": "a" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(r["result"]["ok"], json!(true));
}

#[tokio::test]
async fn missing_params_are_invalid_params() {
    let api = MemApi::default();
    let mut binding: Option<ClientId> = None;
    let no_agent = parsed(
        handle(
            req(1, "drafts.get", json!({ "workspaceId": "ws" })),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(no_agent["error"]["code"], json!(-32602));
    let no_text = parsed(
        handle(
            req(
                2,
                "drafts.set",
                json!({ "workspaceId": "ws", "agentId": "a" }),
            ),
            &api,
            &mut binding,
        )
        .await,
    );
    assert_eq!(no_text["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn two_clients_are_isolated() {
    let api = MemApi::default();
    let mut a: Option<ClientId> = Some(ClientId::from_string("cli-a"));
    let mut b: Option<ClientId> = Some(ClientId::from_string("cli-b"));
    handle(
        req(
            1,
            "drafts.set",
            json!({ "workspaceId": "ws", "agentId": "ag", "text": "from-a" }),
        ),
        &api,
        &mut a,
    )
    .await;
    let b_get = parsed(
        handle(
            req(
                2,
                "drafts.get",
                json!({ "workspaceId": "ws", "agentId": "ag" }),
            ),
            &api,
            &mut b,
        )
        .await,
    );
    assert_eq!(
        b_get["result"],
        Value::Null,
        "client B never sees client A's draft"
    );
}
